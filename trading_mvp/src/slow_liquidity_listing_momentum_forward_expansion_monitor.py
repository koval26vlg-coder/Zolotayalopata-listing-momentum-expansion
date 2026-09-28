from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from global_market_writer_claim import (
    claim_global_market_writer,
    consume_worker_handoff_receipt,
    release_global_market_writer,
)
from listing_momentum_exchange_expansion import (
    SUPPORTED_VENUES,
    DEFAULT_PREFLIGHT_PATH,
    ExpansionSpotOhlcvClient,
    fetch_current_snapshot_rows,
    resolve_proxy_timestamp,
)
from listing_spot_asset_class import (
    ASSET_CLASS_CRYPTO_TOKEN,
    DECLARATION_SOURCE,
    classify_spot_asset,
)
from slow_liquidity_listing_momentum_first_days_census import compute_window_stats
from slow_liquidity_listing_momentum_first_days_collector import (
    GRANULARITY,
    PROBE_BEFORE_SEC,
    WINDOW_SEC,
    classify_job_bars,
    collect_window_bars,
)
from slow_liquidity_spot_v2_official_page_discovery import canonical_hash
from adaptive_cadence import decide_cadence
from listing_expansion_backlog import Backlog
from listing_atomic_io import atomic_write_json


SCHEMA = "trading_mvp_slow_liquidity_listing_momentum_forward_expansion_monitor_planonly_v3"
PLAN_ID = "slow_liquidity_listing_momentum_forward_expansion_20260925_v18"
AUTOMATION_ID = "zolotyaylopata-listing-momentum-forward-expansion"
REPO_ROOT = Path(__file__).resolve().parents[2]
PLAN_PATH = (
    REPO_ROOT
    / "docs/plans"
    / "slow-liquidity-listing-momentum-forward-expansion-planonly-20260925-v18.json"
)
FORWARD_ROOT = Path("E:/trading_mvp/listing-momentum-forward-expansion")
TICKS_DIR = FORWARD_ROOT / "ticks"
BACKLOG_PATH = FORWARD_ROOT / "backlog-v18" / "events.jsonl"
BACKLOG_CONTRACT = {
    "schema": "expansion_backlog_event_v1",
    "generation_isolation": "new_v18_no_legacy_completion_import",
    "persist_before_cap": "normalized_discovery_snapshot_all_jobs_and_skips_then_fsynced_hash_chain",
    "instrument_key": ["plan_hash", "exchange", "symbol"],
    "proxy_anchor": "freeze_first_observed_hour_proxy_and_exact_received_time",
    "revision_policy": "quarantine_changed_source_base_classification_or_metadata_timestamp",
    "missing_symbol_policy": "retain_pending_never_infer_delisting",
    "selection": "least_recent_attempt_sequence_then_admission_sequence_then_event_id",
    "max_attempts_per_tick": 50,
    "attempt_reservation": "durable_before_request_crash_remains_retry",
    "minimum_retry_delay_sec": 300,
    "complete": "closed_window_exact_unique_hourly_grid_valid_ohlcv_descriptive_only",
    "outcome_commit": "raw_fsynced_immutable_receipt_manifest_state_terminal_ledger_then_queue_then_release",
    "corruption_policy": "fail_closed_preserve_tail_no_automatic_truncation",
    "terminal_identity": "COLLECTED_DESCRIPTIVE_tombstone_never_acceptance",
    "manifest_jobs_total": "actually_attempted_selected_slice",
    "pending_count": "all_queued_and_quarantined_records_including_deferred",
    "cadence_evidence": "proxy_only_no_official_promotion",
}
STATE_PATH = (
    REPO_ROOT
    / "exports/trading-mvp/analysis"
    / "slow_liquidity_listing_momentum_forward_expansion_state_20260817.json"
)
CLAIM_PATH = (
    REPO_ROOT
    / "docs/agent-log/active-market-data-writer-claim.json"
)
LEGACY_CLAIM_PATH = (
    REPO_ROOT
    / "docs/agent-log/active-market-data-writer-expansion-claim.json"
)
TERMINAL_ATTEMPTS_PATH = (
    REPO_ROOT / "docs" / "agent-log" / "run-gates"
    / "listing_momentum_forward_expansion_terminal_attempts.jsonl"
)
TERMINAL_ATTEMPT_SCHEMA = "trading_mvp_listing_momentum_forward_expansion_terminal_attempt_v1"
MAX_NEW_LISTINGS_PER_TICK = 50
MAX_RUNTIME_SEC = 600
TIMEOUT_SEC = 20
SLEEP_SEC = 0.25
EFFECTIVE_PAGE_SIZES = {
    "binance": 1000,
    "bybit": 1000,
    "okx": 300,
    "bitget": 1000,
}


class ExpansionMonitorError(ValueError):
    pass


def _require(value: bool, message: str) -> None:
    if not value:
        raise ExpansionMonitorError(message)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_ts(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp())


def _validate_plan(plan: Mapping[str, Any], plan_path: Path) -> None:
    _require(plan.get("schema") == SCHEMA, "expansion plan schema mismatch")
    _require(plan.get("plan_id") == PLAN_ID, "expansion plan id mismatch")
    _require(plan.get("plan_hash") == canonical_hash(plan), "expansion plan hash mismatch")
    _require(plan.get("research_only") is True, "expansion plan must be research-only")
    _require(plan.get("public_data_only") is True, "expansion plan must be public-data-only")
    _require(plan.get("private_api") is False, "private API is forbidden")
    _require(plan.get("replay_allowed") is False, "replay must remain blocked")
    _require(plan.get("evaluator_or_oos_allowed") is False, "evaluator/OOS must remain blocked")
    _require(tuple(plan.get("venues") or []) == SUPPORTED_VENUES, "expansion venue set mismatch")
    _require(plan.get("backlog_contract") == BACKLOG_CONTRACT, "backlog contract mismatch")
    _require((plan.get("tick") or {}).get("backlog_path") == str(BACKLOG_PATH), "backlog path mismatch")
    _require((plan.get("tick") or {}).get("max_new_listings_per_tick") == MAX_NEW_LISTINGS_PER_TICK,
             "tick cap mismatch")
    _require(
        (plan.get("guard_contract") or {}).get("combined_launcher_scheduler_routable")
        is False,
        "combined launcher must remain retired from scheduler routing",
    )
    preflight = (plan.get("source_bindings") or {}).get("preflight") or {}
    preflight_path = Path(str(preflight.get("path") or ""))
    _require(preflight_path.resolve() == DEFAULT_PREFLIGHT_PATH.resolve(), "preflight path mismatch")
    _require(preflight_path.is_file(), "preflight receipt missing")
    _require(_sha256_file(preflight_path) == preflight.get("file_sha256"), "preflight file sha mismatch")
    payload = json.loads(preflight_path.read_text(encoding="utf-8"))
    _require(payload.get("receipt_hash") == preflight.get("receipt_hash"), "preflight receipt binding mismatch")
    implementation = (plan.get("implementation") or {}).get("files") or []
    expected = {
        "expansion_adapter": REPO_ROOT / "trading_mvp/src/listing_momentum_exchange_expansion.py",
        "spot_asset_classifier": REPO_ROOT / "trading_mvp/src/listing_spot_asset_class.py",
        "expansion_monitor": Path(__file__).resolve(),
        "preflight_launcher": REPO_ROOT / "tools/start_listing_momentum_exchange_expansion_preflight_visible.ps1",
        "visible_tick_launcher": REPO_ROOT / "tools/start_listing_momentum_forward_expansion_tick_visible.ps1",
        "expansion_plan_generator": REPO_ROOT / "trading_mvp/src/slow_liquidity_listing_momentum_forward_expansion_plan.py",
        "cadence_policy": REPO_ROOT / "trading_mvp/src/adaptive_cadence.py",
        "expansion_backlog": REPO_ROOT / "trading_mvp/src/listing_expansion_backlog.py",
        "atomic_io": REPO_ROOT / "trading_mvp/src/listing_atomic_io.py",
    }
    by_role = {str(item.get("role")): item for item in implementation if isinstance(item, Mapping)}
    _require(set(by_role) == set(expected), "expansion implementation role set mismatch")
    for role, expected_path in expected.items():
        _require(expected_path.is_file(), f"expansion implementation missing: {role}")
        _require(
            Path(str(by_role[role].get("path") or "")).resolve() == expected_path.resolve(),
            f"expansion implementation path mismatch: {role}",
        )
        _require(
            str(by_role[role].get("sha256") or "") == _sha256_file(expected_path),
            f"expansion implementation sha mismatch: {role}",
        )
    del plan_path


def load_plan(path: Path = PLAN_PATH) -> dict[str, Any]:
    plan = json.loads(path.read_text(encoding="utf-8"))
    _validate_plan(plan, path)
    return plan


def load_baseline_keys(preflight_path: Path = DEFAULT_PREFLIGHT_PATH) -> set[tuple[str, str]]:
    payload = json.loads(preflight_path.read_text(encoding="utf-8"))
    keys: set[tuple[str, str]] = set()
    for venue_result in payload.get("venues") or []:
        venue = str(venue_result.get("exchange") or "")
        for row in venue_result.get("snapshot_rows") or []:
            symbol = str(row.get("symbol") or "").upper()
            if venue in SUPPORTED_VENUES and symbol:
                keys.add((venue, symbol))
    return keys


def diff_new_listings(
    baseline_keys: set[tuple[str, str]],
    current_rows: Sequence[Mapping[str, Any]],
    *,
    baseline_as_of_ts: int,
    now_ts: int,
) -> list[dict[str, Any]]:
    candidates: dict[tuple[str, str], dict[str, Any]] = {}
    for row in current_rows:
        venue = str(row.get("exchange") or "")
        symbol = str(row.get("symbol") or "").upper()
        base = str(row.get("base") or "").upper()
        if venue not in SUPPORTED_VENUES or not symbol or not base:
            continue
        if bool(row.get("is_delisted")) or (venue, symbol) in baseline_keys:
            continue
        proxy_ts, timestamp_source = resolve_proxy_timestamp(row, now_ts=now_ts)
        asset = classify_spot_asset(venue, base)
        candidate = {
            "exchange": venue,
            "base": base,
            "symbol": symbol,
            "listed_ts": proxy_ts,
            "timestamp_source": timestamp_source,
            "is_proxy_timestamp": timestamp_source.endswith("proxy"),
            "asset_class": asset.asset_class,
            "asset_class_source": asset.source,
            "asset_class_acceptance_eligible": asset.acceptance_eligible,
        }
        previous = candidates.get((venue, symbol))
        if previous is None or proxy_ts < previous["listed_ts"]:
            candidates[(venue, symbol)] = candidate
    results: list[dict[str, Any]] = []
    for key in sorted(candidates):
        entry = dict(candidates[key])
        if not entry["is_proxy_timestamp"] and entry["listed_ts"] < baseline_as_of_ts:
            entry["category"] = "backfill_or_relist_skip"
            entry["collect"] = False
        elif entry["listed_ts"] + WINDOW_SEC <= now_ts:
            entry["category"] = "new_listing_window_complete"
            entry["collect"] = True
        else:
            entry["category"] = "new_listing_in_progress"
            entry["collect"] = True
        results.append(entry)
    return results


def derive_forward_jobs(new_listings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for entry in new_listings:
        if not entry.get("collect"):
            continue
        proxy_ts = int(entry["listed_ts"])
        jobs.append(
            {
                "exchange": entry["exchange"],
                "base": entry["base"],
                "symbol": entry["symbol"],
                "proxy_ts": proxy_ts,
                "timestamp_source": entry["timestamp_source"],
                "asset_class": entry["asset_class"],
                "asset_class_source": entry["asset_class_source"],
                "asset_class_acceptance_eligible": entry[
                    "asset_class_acceptance_eligible"
                ],
                "probe_start_ts": ((proxy_ts - PROBE_BEFORE_SEC) // 3600) * 3600,
                "window_end_ts": ((proxy_ts + WINDOW_SEC) // 3600) * 3600,
                "category": entry["category"],
            }
        )
    jobs.sort(key=lambda job: (job["exchange"], job["base"], job["symbol"]))
    return jobs


def fetch_current_rows() -> tuple[list[dict[str, Any]], int]:
    return fetch_current_snapshot_rows(timeout_sec=TIMEOUT_SEC)


def _forward_row(job: Mapping[str, Any], bar: Any) -> dict[str, Any]:
    return {
        "monitor": PLAN_ID,
        "event_id": job.get("event_id"),
        "exchange": job["exchange"],
        "base": job["base"],
        "symbol": job["symbol"],
        "granularity": GRANULARITY,
        "ts": bar.ts,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "proxy_event_ts": job["proxy_ts"],
        "proxy_timestamp_source": job["timestamp_source"],
        "asset_class": job["asset_class"],
        "asset_class_source": job["asset_class_source"],
        "asset_class_acceptance_eligible": job[
            "asset_class_acceptance_eligible"
        ],
        "window_role": "first_days_forward_expansion",
    }


def _write_tick_manifest_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_write_json(path, payload)


def _coverage_complete(job: Mapping[str, Any], bars: Sequence[Any], now_ts: int) -> bool:
    """Acquisition coverage, not execution/strategy acceptance or a price signal."""
    if now_ts < int(job["window_end_ts"]):
        return False
    selected = [bar for bar in bars if job["proxy_ts"] <= bar.ts < job["window_end_ts"]]
    expected = list(range(((int(job["proxy_ts"])+3599)//3600)*3600, int(job["window_end_ts"]), 3600))
    if not expected or sorted(bar.ts for bar in selected) != expected:
        return False
    return all(all(math.isfinite(float(getattr(bar, field))) for field in
                   ("open", "high", "low", "close", "volume"))
               and 0 < bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high
               and bar.volume >= 0 for bar in selected)


def _finalize_incomplete_tick_manifest(
    path: Path,
    *,
    tick_id: str,
    started_at_utc: str,
    plan_hash: str,
    failure_reason: str,
    error: str,
    partial: Mapping[str, Any],
) -> dict[str, Any]:
    existing: dict[str, Any] = {}
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, Mapping):
                existing = dict(loaded)
        except (OSError, ValueError):
            existing = {}
    payload = {
        **existing,
        **dict(partial),
        "schema": "trading_mvp_slow_liquidity_listing_momentum_forward_expansion_tick_manifest_v1",
        "tick_id": tick_id,
        "status": "STOPPED_INCOMPLETE",
        "stop_reason": failure_reason,
        "retry_disposition": "RETRY_NEXT_INTERVAL",
        "pending_retry": True,
        "started_at_utc": started_at_utc,
        "finished_at_utc": utc_now_iso(),
        "plan_hash": plan_hash,
        "error": error,
        "recovery_evidence": "catch_finalized",
    }
    _write_tick_manifest_atomic(path, payload)
    return payload


def _append_terminal_attempt(
    *,
    claim: Mapping[str, Any],
    tick_id: str,
    started_at_utc: str,
    manifest_path: Path,
    terminal: Mapping[str, Any],
    primary_error: BaseException | None,
    finalization_error: str | None,
) -> None:
    """Persist independent terminal evidence while the exact writer claim is held."""
    owner_started_at = claim.get("owner_process_started_at_utc")
    _require(
        isinstance(owner_started_at, str) and bool(owner_started_at),
        "terminal attempt requires exact owner process start identity",
    )
    manifest_sha256 = None
    manifest_status = None
    manifest_error = None
    try:
        raw_manifest = manifest_path.read_bytes()
    except FileNotFoundError:
        manifest_error = "manifest_missing"
    except OSError as exc:
        manifest_error = f"{type(exc).__name__}: {exc}"
    else:
        manifest_sha256 = hashlib.sha256(raw_manifest).hexdigest()
        try:
            observed_manifest = json.loads(raw_manifest)
            if isinstance(observed_manifest, Mapping):
                manifest_status = observed_manifest.get("status")
            else:
                manifest_error = "manifest_not_object"
        except (ValueError, UnicodeError) as exc:
            manifest_error = f"{type(exc).__name__}: {exc}"
    payload = {
        "schema": TERMINAL_ATTEMPT_SCHEMA,
        "tick_id": tick_id,
        "attempt_id": tick_id,
        "run_id": claim["run_id"],
        "plan_id": PLAN_ID,
        "plan_hash": claim["plan_hash"],
        "owner_pid": claim["owner_pid"],
        "owner_process_started_at_utc": owner_started_at,
        "ownership_token_sha256": hashlib.sha256(
            str(claim["ownership_token"]).encode("ascii")
        ).hexdigest(),
        "claim_path": str(CLAIM_PATH.resolve()),
        "output_namespace": claim["output_namespace"],
        "started_at_utc": started_at_utc,
        "finished_at_utc": utc_now_iso(),
        "status": terminal["status"],
        "stop_reason": terminal["stop_reason"],
        "retry_disposition": terminal.get("retry_disposition"),
        "pending_retry": terminal["pending_retry"],
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": manifest_sha256,
        "manifest_status": manifest_status,
        "manifest_error": manifest_error,
        "primary_error": (
            f"{type(primary_error).__name__}: {primary_error}"
            if primary_error is not None else None
        ),
        "finalization_error": finalization_error,
        **{
            key: terminal.get(key)
            for key in (
                "jobs_total", "jobs_attempted", "jobs_succeeded", "jobs_failed",
                "jobs_pending_retry", "retry_queue",
            )
        },
    }
    encoded = (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
    TERMINAL_ATTEMPTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    # One global writer owns this append. Never truncate a prior attempt, even
    # after a short write; a torn tail requires explicit evidence recovery.
    with TERMINAL_ATTEMPTS_PATH.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell():
            handle.seek(-1, os.SEEK_END)
            _require(handle.read(1) == b"\n", "terminal attempt ledger has an incomplete tail")
        if handle.write(encoded) != len(encoded):
            raise OSError("terminal attempt ledger short write")
        handle.flush()
        os.fsync(handle.fileno())


def reconcile_running_tick_manifests(*, exclude_tick_id: str | None = None) -> dict[str, Any]:
    checked = 0
    reconciled = 0
    for tick_dir in _tick_dirs():
        if exclude_tick_id is not None and tick_dir.name == exclude_tick_id:
            continue
        manifest_path = tick_dir / "manifest.json"
        if not manifest_path.is_file():
            continue
        checked += 1
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(manifest, Mapping) or manifest.get("status") != "RUNNING":
            continue
        payload = dict(manifest)
        payload.update(
            {
                "status": "STOPPED_INCOMPLETE",
                "stop_reason": "interrupted_running_manifest",
                "retry_disposition": "RETRY_NEXT_INTERVAL",
                "pending_retry": True,
                "finished_at_utc": utc_now_iso(),
                "error": "startup reconciliation found an orphan RUNNING manifest",
                "recovery_evidence": "startup_reconciled",
            }
        )
        _write_tick_manifest_atomic(manifest_path, payload)
        reconciled += 1
    return {"checked": checked, "reconciled": reconciled}


def _assert_legacy_expansion_claim_absent() -> None:
    if LEGACY_CLAIM_PATH.exists():
        raise ExpansionMonitorError(
            "legacy expansion writer claim exists; migration is fail-closed until its ownership is resolved"
        )


def run_tick(
    plan: Mapping[str, Any],
    *,
    tick_id: str,
    clients: Mapping[str, Any],
    fetcher: Any = None,
    now_ts: int | None = None,
    claim_ownership_token: str | None = None,
) -> dict[str, Any]:
    _require(tick_id and all(char.isalnum() or char in "_-" for char in tick_id), "unsafe tick id")
    _assert_legacy_expansion_claim_absent()
    tick_dir = TICKS_DIR / tick_id
    _require(not tick_dir.exists(), f"tick {tick_id} already exists")
    now_ts = int(now_ts if now_ts is not None else time.time())
    deadline = time.monotonic() + MAX_RUNTIME_SEC
    fetcher = fetcher or fetch_current_rows
    started_utc = utc_now_iso()
    claim = claim_global_market_writer(
        CLAIM_PATH,
        run_id=f"{PLAN_ID}__{tick_id}",
        owner_pid=os.getpid(),
        owner_kind="listing_momentum_forward_expansion_monitor_tick",
        plan_hash=plan["plan_hash"],
        output_namespace=tick_dir,
        ownership_token=claim_ownership_token,
    )
    stop_reason = "tick_failed_before_terminal_manifest"
    release_failure_reason = "fetch_or_prepare_exception"
    release_final_status = (
        "STOPPED_INCOMPLETE;RETRY_NEXT_INTERVAL;"
        "reason=tick_failed_before_terminal_manifest"
    )
    primary_error: BaseException | None = None
    finalization_error: str | None = None
    terminal_attempt: dict[str, Any] = {}
    writer_identity = {
        "attempt_id": tick_id,
        "run_id": claim["run_id"],
        "writer_pid": claim["owner_pid"],
        "writer_process_started_at_utc": claim["owner_process_started_at_utc"],
    }
    rows_written = 0
    requests_made = 0
    job_summaries: list[dict[str, Any]] = []
    jobs: list[dict[str, Any]] = []
    retry_queue: list[dict[str, Any]] = []
    jobs_succeeded = 0
    jobs_failed = 0
    skipped: list[dict[str, Any]] = []
    baseline_as_of_ts = 0
    manifest_path = tick_dir / "manifest.json"
    backlog = None
    outcomes_path = None
    discovery_error = None
    discovered_job_count = 0
    try:
        tick_dir.mkdir(parents=True, exist_ok=True)
        release_failure_reason = "running_manifest_persistence_exception"
        _write_tick_manifest_atomic(
            manifest_path,
            {
                "schema": "trading_mvp_slow_liquidity_listing_momentum_forward_expansion_tick_manifest_v1",
                "tick_id": tick_id,
                **writer_identity,
                "status": "RUNNING",
                "evidence_stage": "CLAIMED_PRE_FETCH",
                "started_at_utc": started_utc,
                "plan_hash": plan["plan_hash"],
                "ownership_token": claim["ownership_token"],
                "claim_path": str(CLAIM_PATH),
                "jobs_total": None,
            },
        )
        release_failure_reason = "fetch_or_prepare_exception"
        reconcile_running_tick_manifests(exclude_tick_id=tick_id)
        backlog = Backlog(BACKLOG_PATH, str(plan["plan_hash"]))
        try:
            current_rows, snapshot_requests = fetcher()
        except Exception as exc:
            current_rows, snapshot_requests = [], 0
            discovery_error = f"{type(exc).__name__}: {exc}"
        requests_made += snapshot_requests
        baseline = (plan.get("source_bindings") or {}).get("preflight") or {}
        baseline_as_of_ts = int(baseline.get("baseline_as_of_ts") or 0)
        baseline_keys = load_baseline_keys(Path(str(baseline["path"])))
        new_listings = diff_new_listings(
            baseline_keys,
            current_rows,
            baseline_as_of_ts=baseline_as_of_ts,
            now_ts=now_ts,
        )
        skipped = [
            {
                key: entry[key]
                for key in (
                    "exchange",
                    "base",
                    "symbol",
                    "listed_ts",
                    "timestamp_source",
                    "asset_class",
                    "asset_class_source",
                    "asset_class_acceptance_eligible",
                    "category",
                )
            }
            for entry in new_listings
            if not entry["collect"]
        ]
        discovered_jobs = derive_forward_jobs(new_listings)
        discovered_job_count = len(discovered_jobs)
        known_instruments = {(e["job"]["exchange"], e["job"]["symbol"]) for e in backlog.entries.values()}
        known_revision_jobs = derive_forward_jobs([
            {**entry, "collect": True} for entry in new_listings
            if not entry["collect"] and (entry["exchange"], entry["symbol"]) in known_instruments
        ])
        # Durable full normalized evidence before ANY budget selection or OHLCV request.
        snapshot_path = tick_dir / "discovery.json"
        _write_tick_manifest_atomic(snapshot_path, {
            "schema": "expansion_discovery_snapshot_v1", "tick_id": tick_id,
            "plan_hash": plan["plan_hash"], "now_ts": now_ts,
            "received_at_utc": utc_now_iso(), "baseline_binding": baseline,
            "current_rows": current_rows, "jobs": discovered_jobs + known_revision_jobs, "skipped": skipped,
            "known_skipped_observations_for_revision_checks": len(known_revision_jobs),
            "requests_made": snapshot_requests, "discovery_error": discovery_error,
        })
        release_failure_reason = "backlog_admission_persistence_exception"
        backlog.admit(snapshot_path)
        jobs = backlog.select(now_ts, MAX_NEW_LISTINGS_PER_TICK)
        release_failure_reason = "collection_or_result_exception"
        stop_reason = "completed"
        with (tick_dir / "ohlcv.jsonl").open("w", encoding="utf-8") as handle:
            for job in jobs:
                if time.monotonic() > deadline:
                    stop_reason = "max_runtime_sec_exceeded"
                    break
                release_failure_reason = "backlog_attempt_persistence_exception"
                backlog.start(job["event_id"], tick_id, now_ts)
                release_failure_reason = "collection_or_result_exception"
                requests = 0
                try:
                    client = clients[job["exchange"]]
                    bars, requests = collect_window_bars(
                        client,
                        job,
                        candles_per_request=int(EFFECTIVE_PAGE_SIZES[job["exchange"]]),
                        sleep_sec=SLEEP_SEC,
                    )
                    requests_made += requests
                except Exception as exc:  # noqa: BLE001 - per-job resilience
                    bars = []
                    jobs_failed += 1
                    summary = {
                        "flags": ["request_error"],
                        "error": f"{type(exc).__name__}: {exc}",
                        "window_bar_count": 0,
                        "job_status": "FAILED_RETRY_NEXT_INTERVAL",
                        "retry_disposition": "RETRY_NEXT_INTERVAL",
                    }
                    retry_queue.append(
                        {
                            **job,
                            "failure_reason": "request_error",
                            "error": summary["error"],
                            "retry_disposition": "RETRY_NEXT_INTERVAL",
                        }
                    )
                else:
                    jobs_succeeded += 1
                    summary = classify_job_bars(job, bars)
                    summary["job_status"] = "SUCCEEDED"
                    if job["category"] == "new_listing_in_progress":
                        summary["flags"] = list(summary["flags"]) + ["window_in_progress"]
                summary.pop("window_bars", None)
                summary["coverage_complete"] = (
                    summary["job_status"] == "SUCCEEDED" and _coverage_complete(job, bars, now_ts))
                if not summary["coverage_complete"]:
                    summary["flags"] = list(summary["flags"]) + ["acquisition_incomplete"]
                summary.update(
                    {
                        "event_id": job["event_id"],
                        "exchange": job["exchange"],
                        "base": job["base"],
                        "symbol": job["symbol"],
                        "proxy_ts": job["proxy_ts"],
                        "timestamp_source": job["timestamp_source"],
                        "asset_class": job["asset_class"],
                        "asset_class_source": job["asset_class_source"],
                        "asset_class_acceptance_eligible": job[
                            "asset_class_acceptance_eligible"
                        ],
                        "category": job["category"],
                        "requests": requests,
                    }
                )
                job_summaries.append(summary)
                for bar in bars:
                    if job["proxy_ts"] <= bar.ts < job["window_end_ts"]:
                        handle.write(json.dumps(_forward_row(job, bar), ensure_ascii=False) + "\n")
                        rows_written += 1
            handle.flush()
            os.fsync(handle.fileno())
        if stop_reason == "max_runtime_sec_exceeded":
            for job in jobs[len(job_summaries) :]:
                retry_queue.append(
                    {
                        **job,
                        "failure_reason": "not_attempted_max_runtime",
                        "error": None,
                        "retry_disposition": "RETRY_NEXT_INTERVAL",
                    }
                )
        elif jobs_failed:
            if jobs_succeeded:
                stop_reason = "partial_job_request_error"
            else:
                stop_reason = "all_jobs_request_error"
        completed_ids = {j["event_id"] for j in job_summaries if j["coverage_complete"]}
        detailed_retries = {j["event_id"]: j for j in retry_queue if j.get("event_id")}
        retry_queue = [j for j in backlog.pending(now_ts) if j["event_id"] not in completed_ids]
        retry_queue = [{**j, **detailed_retries.get(j["event_id"], {})} for j in retry_queue]
        if discovery_error:
            retry_queue.append({"failure_reason": "discovery_snapshot_failed", "error": discovery_error,
                                "retry_disposition": "RETRY_NEXT_INTERVAL"})
        if retry_queue and stop_reason == "completed":
            stop_reason = "backlog_pending_next_interval"
        if not retry_queue and stop_reason == "completed":
            terminal_status = "COMPLETED"
        elif jobs_succeeded:
            terminal_status = "PARTIAL_RETRY_NEXT_INTERVAL"
        else:
            terminal_status = "STOPPED_INCOMPLETE"
        release_failure_reason = "terminal_manifest_persistence_exception"
        manifest = {
            "schema": "trading_mvp_slow_liquidity_listing_momentum_forward_expansion_tick_manifest_v1",
            "tick_id": tick_id,
            **writer_identity,
            "status": terminal_status,
            "stop_reason": stop_reason,
            "retry_disposition": "RETRY_NEXT_INTERVAL" if retry_queue else None,
            "pending_retry": bool(retry_queue),
            "started_at_utc": started_utc,
            "finished_at_utc": utc_now_iso(),
            "plan_hash": plan["plan_hash"],
            "now_ts": now_ts,
            "baseline_as_of_ts": baseline_as_of_ts,
            "new_listing_count": discovered_job_count,
            "discovery_error": discovery_error,
            "discovery_snapshot_path": str(snapshot_path),
            "discovery_snapshot_sha256": _sha256_file(snapshot_path),
            "backlog_path": str(BACKLOG_PATH),
            "backlog_total_count": len(backlog.entries),
            "backlog_pending_count": len(retry_queue) - int(bool(discovery_error)),
            "jobs_selected": len(jobs),
            "jobs_deferred": max(0, len(backlog.pending(now_ts)) - len(job_summaries)),
            "skipped_backfill_or_relist": skipped,
            "jobs_total": len(job_summaries),
            "jobs_attempted": len(job_summaries),
            "jobs_succeeded": jobs_succeeded,
            "jobs_failed": jobs_failed,
            "jobs_pending_retry": len(retry_queue),
            "jobs": job_summaries,
            "retry_queue": retry_queue,
            "rows_written": rows_written,
            "requests_made": requests_made,
        }
        outcomes_path = tick_dir / "acquisition-outcomes.json"
        _write_tick_manifest_atomic(outcomes_path, {
            "schema": "expansion_acquisition_outcomes_v1", "tick_id": tick_id,
            "plan_hash": plan["plan_hash"], "jobs": job_summaries,
            "raw_evidence": {"path": str((tick_dir / "ohlcv.jsonl").resolve()),
                             "sha256": _sha256_file(tick_dir / "ohlcv.jsonl")},
        })
        _write_tick_manifest_atomic(manifest_path, manifest)
        release_failure_reason = "state_rebuild_exception"
        rebuild_forward_state()
        release_final_status = terminal_status
        terminal_attempt = manifest
    except BaseException as exc:
        primary_error = exc
        release_final_status = (
            "STOPPED_INCOMPLETE;RETRY_NEXT_INTERVAL;"
            f"reason={release_failure_reason}"
        )
        terminal_attempt = {
            **writer_identity,
            "status": "STOPPED_INCOMPLETE",
            "stop_reason": release_failure_reason,
            "retry_disposition": "RETRY_NEXT_INTERVAL",
            "pending_retry": True,
            "now_ts": now_ts,
            "baseline_as_of_ts": baseline_as_of_ts,
            "new_listing_count": discovered_job_count,
            "discovery_error": discovery_error,
            "backlog_path": str(BACKLOG_PATH),
            "jobs_selected": len(jobs),
            "skipped_backfill_or_relist": skipped,
            "jobs_total": len(job_summaries),
            "jobs_attempted": len(job_summaries),
            "jobs_succeeded": jobs_succeeded,
            "jobs_failed": jobs_failed,
            "jobs_pending_retry": max(1, len(retry_queue)),
            "jobs": job_summaries,
            "retry_queue": retry_queue,
            "rows_written": rows_written,
            "requests_made": requests_made,
        }
        try:
            _finalize_incomplete_tick_manifest(
                manifest_path,
                tick_id=tick_id,
                started_at_utc=started_utc,
                plan_hash=str(plan["plan_hash"]),
                failure_reason=release_failure_reason,
                error=f"{type(exc).__name__}: {exc}",
                partial=terminal_attempt,
            )
        except Exception as finalize_exc:  # noqa: BLE001
            finalization_error = f"{type(finalize_exc).__name__}: {finalize_exc}"
            exc.add_note(
                "incomplete tick manifest finalization also failed: "
                f"{finalization_error}"
            )
        raise
    finally:
        durability_stage = "terminal_ledger_persistence_exception"
        try:
            _append_terminal_attempt(
                claim=claim,
                tick_id=tick_id,
                started_at_utc=started_utc,
                manifest_path=manifest_path,
                terminal=terminal_attempt,
                primary_error=primary_error,
                finalization_error=finalization_error,
            )
            if primary_error is None and backlog is not None and outcomes_path is not None:
                # Completion tombstones follow raw, receipt, manifest, state and ledger durability.
                durability_stage = "backlog_outcome_persistence_exception"
                backlog.finish(outcomes_path, now_ts)
        except BaseException as ledger_exc:
            # No successful durability barrier means no release or archive,
            # including when the terminal manifest could not be finalized.
            if primary_error is None:
                try:
                    recovery_pending = backlog.pending(now_ts) if backlog is not None else retry_queue
                    if discovery_error:
                        recovery_pending = [*recovery_pending, {"failure_reason": "discovery_snapshot_failed",
                            "error": discovery_error, "retry_disposition": "RETRY_NEXT_INTERVAL"}]
                    _finalize_incomplete_tick_manifest(
                        manifest_path,
                        tick_id=tick_id,
                        started_at_utc=started_utc,
                        plan_hash=str(plan["plan_hash"]),
                        failure_reason=durability_stage,
                        error=f"{type(ledger_exc).__name__}: {ledger_exc}",
                        partial={
                            **terminal_attempt,
                            "jobs_pending_retry": max(1, len(recovery_pending)),
                            "backlog_pending_count": len(recovery_pending)-int(bool(discovery_error)),
                            "retry_queue": recovery_pending,
                        },
                    )
                    rebuild_forward_state()
                except BaseException as finalize_exc:
                    ledger_exc.add_note(
                        "ledger failure manifest/state finalization also failed: "
                        f"{type(finalize_exc).__name__}: {finalize_exc}"
                    )
                ledger_exc.add_note(f"{durability_stage}; global writer claim retained")
                raise
            primary_error.add_note(
                "terminal attempt ledger persistence also failed; global writer claim retained: "
                f"{type(ledger_exc).__name__}: {ledger_exc}"
            )
        else:
            try:
                release_global_market_writer(
                    CLAIM_PATH,
                    run_id=f"{PLAN_ID}__{tick_id}",
                    owner_pid=int(claim["owner_pid"]),
                    ownership_token=str(claim["ownership_token"]),
                    final_status=release_final_status,
                    expected_plan_hash=str(claim["plan_hash"]),
                    expected_owner_process_started_at_utc=str(
                        claim["owner_process_started_at_utc"]
                    ),
                )
            except Exception as release_exc:  # noqa: BLE001
                if primary_error is None:
                    raise
                primary_error.add_note(
                    "global market writer release also failed: "
                    f"{type(release_exc).__name__}: {release_exc}"
                )
    return manifest


def _tick_dirs() -> list[Path]:
    if not TICKS_DIR.is_dir():
        return []
    return sorted(path for path in TICKS_DIR.iterdir() if path.is_dir())


def _window_asset_provenance(
    bars: Sequence[Mapping[str, Any]],
    job: Mapping[str, Any] | None,
) -> dict[str, Any]:
    records: list[tuple[str, str, bool]] = []
    for row in [*bars, *((job,) if job is not None else ())]:
        asset_class = str(row.get("asset_class") or "")
        source = str(row.get("asset_class_source") or "")
        if not asset_class or not source:
            continue
        eligible = row.get("asset_class_acceptance_eligible") is True
        if eligible and (
            asset_class != ASSET_CLASS_CRYPTO_TOKEN
            or source != DECLARATION_SOURCE
        ):
            return {
                "asset_class": "unclassified",
                "asset_class_source": "conflicting_asset_provenance",
                "asset_class_acceptance_eligible": False,
            }
        records.append((asset_class, source, eligible))
    if not records:
        return {
            "asset_class": "unclassified",
            "asset_class_source": "legacy_missing_asset_provenance",
            "asset_class_acceptance_eligible": False,
        }
    if len(set(records)) != 1:
        return {
            "asset_class": "unclassified",
            "asset_class_source": "conflicting_asset_provenance",
            "asset_class_acceptance_eligible": False,
        }
    asset_class, source, eligible = records[0]
    return {
        "asset_class": asset_class,
        "asset_class_source": source,
        "asset_class_acceptance_eligible": bool(
            eligible
            and asset_class == ASSET_CLASS_CRYPTO_TOKEN
            and source == DECLARATION_SOURCE
        ),
    }


def rebuild_forward_state() -> dict[str, Any]:
    # The declared hash of the plan this monitor runs under, read rather than validated.
    # Attribution is a label, not an authorisation: routing this through load_plan() would
    # make the rebuild depend on full plan validation, and a validation failure would then
    # silently label every window as belonging to no generation at all.
    try:
        current_plan_hash: str | None = str(
            json.loads(PLAN_PATH.read_text(encoding="utf-8"))["plan_hash"]
        )
    except (OSError, ValueError, KeyError, TypeError):
        current_plan_hash = None

    windows: dict[tuple[str, ...], dict[str, Any]] = {}
    ticks: list[dict[str, Any]] = []
    cadence_rows: list[dict[str, Any]] = []
    cadence_rows_considered = 0
    latest_backlog_manifest = None
    def identity(row):
        key = (str(row.get("exchange")), str(row.get("base")))
        return (*key, str(row["event_id"])) if row.get("event_id") else key
    for tick_dir in _tick_dirs():
        manifest_path = tick_dir / "manifest.json"
        if not manifest_path.is_file():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows = [row for row in manifest.get("jobs") or [] if isinstance(row, Mapping)]
        cadence_rows_considered += len(rows)
        # Only the generation being run. A marker left by a retired plan describes a
        # question that plan was asking, and answering today's cadence with it is how a
        # scheduler ends up pinned to its tightest interval by ten-day-old evidence.
        if current_plan_hash is None or manifest.get("plan_hash") == current_plan_hash:
            cadence_rows.extend(rows)
            if "backlog_pending_count" in manifest:
                latest_backlog_manifest = manifest
        tick_id = manifest.get("tick_id")
        tick_plan_hash = manifest.get("plan_hash")
        ticks.append(
            {
                "tick_id": tick_id,
                "status": manifest.get("status"),
                "new_listing_count": manifest.get("new_listing_count"),
                "rows_written": manifest.get("rows_written"),
                # Which plan authorised this tick. The output root is shared across
                # generations, so without this the state cannot say what it is a sample of.
                "plan_hash": tick_plan_hash,
            }
        )
        rows_path = tick_dir / "ohlcv.jsonl"
        if not rows_path.is_file():
            continue
        bars_by_key: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        for line in rows_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            bars_by_key.setdefault(identity(row), []).append(row)
        jobs_by_key = {
            identity(job): job
            for job in manifest.get("jobs") or []
            if isinstance(job, Mapping)
        }
        job_flags = {
            identity(job): job.get("flags") or []
            for job in manifest.get("jobs") or []
            if isinstance(job, Mapping)
        }
        for key, bars in bars_by_key.items():
            asset_provenance = _window_asset_provenance(
                bars, jobs_by_key.get(key)
            )
            windows[key] = {
                "exchange": key[0],
                "base": key[1],
                "event_id": key[2] if len(key) == 3 else None,
                "flags": job_flags.get(key, []),
                "window_complete": ((jobs_by_key.get(key) or {}).get("coverage_complete") is True
                                    if len(key) == 3 else "window_in_progress" not in job_flags.get(key, [])),
                "stats": compute_window_stats(bars),
                # The last tick to touch a pair replaces it outright rather than merging,
                # so a window comes from exactly one tick and this attribution is exact.
                "source_tick_id": tick_id,
                "source_plan_hash": tick_plan_hash,
                **asset_provenance,
            }
    ordered_windows = [windows[key] for key in sorted(windows)]
    in_progress = any(row.get("category") == "new_listing_in_progress" for row in cadence_rows)
    official = any(
        row.get("timestamp_source")
        and not str(row.get("timestamp_source")).endswith("proxy")
        for row in cadence_rows
    )
    cadence_observation = {
        "candidate": in_progress,
        "official_confirmed": official and in_progress,
        "exact_timestamp": official and in_progress,
        "source_class": "official" if official and in_progress else "proxy" if cadence_rows else None,
        "proxy_timestamp": not official and bool(cadence_rows),
        # What the observation was computed over, so a reader does not have to infer it.
        "scoped_to_plan_hash": current_plan_hash,
        "job_rows_in_scope": len(cadence_rows),
        "job_rows_in_store": cadence_rows_considered,
    }
    # Current queue manifests explicitly report projected post-commit pending
    # state. Reading the backlog here would precede its terminal commit barrier.
    backlog_pending_count = None
    if latest_backlog_manifest is not None:
        backlog_pending_count = latest_backlog_manifest["backlog_pending_count"]
        cadence_observation.update(
            candidate=backlog_pending_count > 0,
            official_confirmed=False, exact_timestamp=False,
            source_class="proxy" if backlog_pending_count else None,
            proxy_timestamp=backlog_pending_count > 0,
            queue_observation_tick_id=latest_backlog_manifest["tick_id"],
        )
    cadence = decide_cadence(cadence_observation)
    crypto_acceptance_window_count = sum(
        1
        for window in ordered_windows
        if window["asset_class"] == ASSET_CLASS_CRYPTO_TOKEN
        and window["asset_class_source"] == DECLARATION_SOURCE
        and window["asset_class_acceptance_eligible"] is True
    )
    # Per generation, because a pre-registered minimum counted over a pile of
    # generations is not the quantity the pre-registration named.
    windows_by_plan_hash: dict[str, dict[str, int]] = {}
    for window in ordered_windows:
        bucket = windows_by_plan_hash.setdefault(
            str(window.get("source_plan_hash")),
            {"windows": 0, "complete": 0, "crypto_acceptance": 0},
        )
        bucket["windows"] += 1
        if window["window_complete"]:
            bucket["complete"] += 1
        if (
            window["asset_class"] == ASSET_CLASS_CRYPTO_TOKEN
            and window["asset_class_source"] == DECLARATION_SOURCE
            and window["asset_class_acceptance_eligible"] is True
        ):
            bucket["crypto_acceptance"] += 1

    current = windows_by_plan_hash.get(current_plan_hash or "") or {
        "windows": 0,
        "complete": 0,
        "crypto_acceptance": 0,
    }

    payload: dict[str, Any] = {
        "schema": "trading_mvp_slow_liquidity_listing_momentum_forward_expansion_state_v1",
        "monitor": PLAN_ID,
        "evidence_class": "PROXY_DATE_FORWARD_ACCRUAL_EXPANSION",
        "acceptance_decision": "NONE_ACCRUAL_ONLY",
        "venues": list(SUPPORTED_VENUES),
        "ticks": ticks,
        "tick_count": len(ticks),
        "window_count": len(ordered_windows),
        "complete_window_count": sum(1 for window in ordered_windows if window["window_complete"]),
        "crypto_acceptance_window_count": crypto_acceptance_window_count,
        "descriptive_only_window_count": len(ordered_windows)
        - crypto_acceptance_window_count,
        "windows": ordered_windows,
        # The mixed totals above describe the store; these describe the sample. A minimum
        # that applies to windows governed by one plan has to be read from the second set.
        "windows_by_plan_hash": windows_by_plan_hash,
        "current_plan_hash": current_plan_hash,
        "current_plan_window_count": current["windows"],
        "current_plan_complete_window_count": current["complete"],
        "current_plan_crypto_acceptance_window_count": current["crypto_acceptance"],
        "backlog_pending_count": backlog_pending_count,
        "cadence_observation": cadence_observation,
        "adaptive_cadence": cadence.as_dict(),
    }
    payload["state_hash"] = canonical_hash(payload)
    _write_tick_manifest_atomic(STATE_PATH, payload)
    return payload


def tick_status() -> dict[str, Any]:
    if not STATE_PATH.is_file():
        return {"status": "NO_TICKS_YET", "monitor": PLAN_ID, "venues": list(SUPPORTED_VENUES)}
    payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {
        "status": "ACCRUING",
        "monitor": PLAN_ID,
        "venues": payload.get("venues"),
        "tick_count": payload["tick_count"],
        "window_count": payload["window_count"],
        "complete_window_count": payload["complete_window_count"],
        "state_hash": payload["state_hash"],
        "last_ticks": payload["ticks"][-3:],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", default=str(PLAN_PATH))
    parser.add_argument("--tick", action="store_true")
    parser.add_argument("--confirmed-visible-tick", action="store_true")
    parser.add_argument("--tick-id")
    parser.add_argument("--worker-handoff-token")
    parser.add_argument("--claim-ownership-token")
    parser.add_argument("--plan-hash")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--plan-check", action="store_true")
    args = parser.parse_args(argv)
    if args.status:
        print(json.dumps(tick_status(), ensure_ascii=False))
        return 0
    plan = load_plan(Path(args.plan))
    if args.plan_check:
        print(
            json.dumps(
                {
                    "status": "PLAN_OK",
                    "plan_hash": plan["plan_hash"],
                    "venues": list(SUPPORTED_VENUES),
                    "preflight_receipt_hash": (plan.get("source_bindings") or {}).get("preflight", {}).get("receipt_hash"),
                    "max_runtime_sec": MAX_RUNTIME_SEC,
                    "tick_output_root": str(TICKS_DIR),
                },
                ensure_ascii=False,
            )
        )
        return 0
    if not args.tick or not args.confirmed_visible_tick:
        raise SystemExit("no authorized action requested")
    if (
        not args.tick_id
        or not args.worker_handoff_token
        or not args.claim_ownership_token
        or not args.plan_hash
    ):
        raise SystemExit("tick requires a launcher-issued worker handoff")
    if args.plan_hash != plan["plan_hash"]:
        raise SystemExit("worker handoff plan hash mismatch")
    tick_id = args.tick_id
    handoff = consume_worker_handoff_receipt(
        CLAIM_PATH,
        receipt_path=REPO_ROOT / "docs" / "agent-log" / "run-gates" / "python-worker-handoffs" / f"{tick_id}.json",
        consumed_dir=REPO_ROOT / "docs" / "agent-log" / "run-gates" / "python-worker-handoffs" / "consumed",
        handoff_token=args.worker_handoff_token,
        attempt_id=tick_id,
        plan_hash=args.plan_hash,
        automation_id=AUTOMATION_ID,
    )
    expected_claim_run_id = f"{PLAN_ID}__{tick_id}"
    expected_output_namespace = str((TICKS_DIR / tick_id).resolve())
    if handoff["claim_run_id"] != expected_claim_run_id:
        raise SystemExit("worker handoff claim run mismatch")
    if handoff["claim_output_namespace"] != expected_output_namespace:
        raise SystemExit("worker handoff output namespace mismatch")
    claim_token_sha = hashlib.sha256(args.claim_ownership_token.encode("ascii")).hexdigest()
    if handoff["claim_ownership_token_sha256"] != claim_token_sha:
        raise SystemExit("worker handoff claim token mismatch")
    clients = {
        venue: ExpansionSpotOhlcvClient(venue, timeout_sec=TIMEOUT_SEC)
        for venue in SUPPORTED_VENUES
    }
    manifest = run_tick(
        plan,
        tick_id=tick_id,
        clients=clients,
        claim_ownership_token=args.claim_ownership_token,
    )
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "tick_id": manifest["tick_id"],
                "new_listing_count": manifest["new_listing_count"],
                "rows_written": manifest["rows_written"],
                "state": tick_status(),
            },
            ensure_ascii=False,
        )
    )
    return 0 if manifest["status"] == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
