from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from global_market_writer_claim import (
    claim_global_market_writer,
    release_global_market_writer,
)
from listing_event_history_collector import (
    Candle,
    CLIENTS,
    write_manifest,
)
from listing_event_history_collect_plan import INTERVAL_SECONDS
from slow_liquidity_spot_v2_official_page_discovery import (
    canonical_hash,
    canonical_json_bytes,
)


RUN_ID = "slow_liquidity_listing_momentum_first_days_collect_20260816"
PLAN_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs/plans"
    / "slow-liquidity-listing-momentum-first-days-collect-planonly-20260816.json"
)
MATERIALIZATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "exports/trading-mvp/analysis"
    / "slow_liquidity_listing_momentum_proxy_date_materialization_20260816.json"
)
PROXY_ACCEPTANCE_RECEIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs/agent-log/approvals"
    / "2026-08-16-slow-liquidity-listing-momentum-proxy-date-acceptance-approval.json"
)
GRANULARITY = "1h"
WINDOW_SEC = 3 * 86400
PROBE_BEFORE_SEC = 14 * 86400
TRUNCATION_TOLERANCE_SEC = 2 * 3600
SHORT_WINDOW_MIN_BARS = 70
PROGRESS_EVERY = 25
FLAG_KEYS = (
    "no_data",
    "request_error",
    "proxy_ts_after_first_bar",
    "history_truncated",
    "short_window",
)
REQUIRED_EXECUTION_KEYS = (
    "output_root",
    "claim_path",
    "launch_record_path",
    "jobs_sha256",
    "max_runtime_sec",
    "timeout_sec",
    "max_retries",
    "sleep_sec",
    "effective_page_sizes",
)
REQUIRED_IMPLEMENTATION_ROLES = frozenset(
    {
        "collector",
        "public_ohlcv_clients",
        "interval_contract",
        "global_writer_claim",
        "visible_launcher",
    }
)


class FirstDaysCollectError(ValueError):
    pass


def _require(value: bool, message: str) -> None:
    if not value:
        raise FirstDaysCollectError(message)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def symbol_for_exchange(exchange: str, base: str) -> str:
    if exchange == "gateio":
        return f"{base}_USDT"
    return f"{base}USDT"


def derive_first_days_jobs(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for record in records:
        base = str(record.get("base") or "").strip().upper()
        _require(bool(base), "materialization record without base")
        for venue in ("mexc", "gateio"):
            listed_ts = record.get(f"{venue}_listed_ts")
            if listed_ts is None:
                continue
            proxy_ts = int(float(listed_ts))
            _require(proxy_ts > 0, f"non-positive listed_ts for {venue}:{base}")
            jobs.append(
                {
                    "exchange": venue,
                    "base": base,
                    "symbol": symbol_for_exchange(venue, base),
                    "proxy_ts": proxy_ts,
                    "probe_start_ts": ((proxy_ts - PROBE_BEFORE_SEC) // 3600) * 3600,
                    "window_end_ts": ((proxy_ts + WINDOW_SEC) // 3600) * 3600,
                }
            )
    jobs.sort(key=lambda job: (job["exchange"], job["base"]))
    return jobs


def jobs_sha256(jobs: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(canonical_json_bytes(list(jobs))).hexdigest()


def load_and_validate_plan(plan_path: Path) -> dict[str, Any]:
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    _require(
        plan.get("schema")
        == "trading_mvp_slow_liquidity_listing_momentum_first_days_collect_planonly_v1",
        "plan schema mismatch",
    )
    _require(plan.get("plan_id") == RUN_ID, "plan id mismatch")
    _require(plan.get("plan_hash") == canonical_hash(plan), "plan hash mismatch")
    _require(
        plan.get("mode") == "PlanOnly" and plan.get("research_only") is True,
        "plan is not research-only PlanOnly",
    )
    _require(
        plan.get("public_data_only") is True and plan.get("private_api") is False,
        "plan is not public-data-only",
    )
    execution = plan.get("execution")
    _require(isinstance(execution, Mapping), "plan execution is invalid")
    missing_keys = [
        key
        for key in REQUIRED_EXECUTION_KEYS
        if key not in execution
    ]
    _require(
        not missing_keys,
        f"plan execution is missing collector keys: {missing_keys}",
    )

    implementation = plan.get("implementation")
    _require(
        isinstance(implementation, Mapping),
        "plan implementation is invalid",
    )
    implementation_files = implementation.get("files")
    _require(
        isinstance(implementation_files, list) and bool(implementation_files),
        "plan implementation files are invalid",
    )
    seen_roles: set[str] = set()
    for binding in implementation_files:
        _require(
            isinstance(binding, Mapping),
            "plan implementation file binding is invalid",
        )
        role = binding.get("role")
        path_value = binding.get("path")
        expected_sha256 = binding.get("sha256")
        _require(
            isinstance(role, str) and bool(role),
            "plan implementation file role is invalid",
        )
        _require(
            role not in seen_roles,
            f"implementation {role} binding is duplicated",
        )
        seen_roles.add(role)
        _require(
            isinstance(path_value, str) and bool(path_value),
            f"implementation {role} path is invalid",
        )
        _require(
            isinstance(expected_sha256, str)
            and len(expected_sha256) == 64
            and expected_sha256 == expected_sha256.lower()
            and all(
                character in "0123456789abcdef"
                for character in expected_sha256
            ),
            f"implementation {role} sha256 is invalid",
        )
        implementation_path = Path(path_value)
        _require(
            implementation_path.is_file(),
            f"implementation {role} file is missing",
        )
        try:
            current_sha256 = _sha256_file(implementation_path)
        except OSError as exc:
            raise FirstDaysCollectError(
                f"implementation {role} file is unreadable"
            ) from exc
        _require(
            current_sha256 == expected_sha256,
            f"implementation {role} sha256 mismatch",
        )
    missing_roles = sorted(REQUIRED_IMPLEMENTATION_ROLES - seen_roles)
    _require(
        not missing_roles,
        f"plan implementation roles are missing: {missing_roles}",
    )

    launch_record_path_value = execution["launch_record_path"]
    _require(
        isinstance(launch_record_path_value, str)
        and bool(launch_record_path_value.strip()),
        "launch record path is invalid",
    )
    launch_record_path = Path(launch_record_path_value)
    if launch_record_path.exists():
        try:
            launch_record = json.loads(
                launch_record_path.read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise FirstDaysCollectError("launch record is invalid") from exc
        _require(
            isinstance(launch_record, Mapping),
            "launch record is invalid",
        )
        _require(
            launch_record.get("run_id") == plan["plan_id"],
            "launch record run id mismatch",
        )
        _require(
            str(launch_record.get("status") or "").upper()
            not in {"COMPLETE", "COMPLETED"},
            "completed PlanOnly cannot be relaunched",
        )
    return plan


def load_and_validate_materialization(
    materialization_path: Path, plan: Mapping[str, Any]
) -> dict[str, Any]:
    payload = json.loads(materialization_path.read_text(encoding="utf-8"))
    content = {
        key: value for key, value in payload.items() if key != "materialization_hash"
    }
    _require(
        payload.get("materialization_hash") == canonical_hash(content),
        "materialization hash mismatch",
    )
    binding = (plan.get("source_bindings") or {}).get("materialization") or {}
    _require(
        payload.get("materialization_hash") == binding.get("materialization_hash"),
        "plan binds a different materialization",
    )
    receipt = payload.get("authorized_by_receipt") or {}
    parent = (plan.get("source_bindings") or {}).get("proxy_acceptance_receipt") or {}
    _require(
        receipt.get("receipt_hash") == parent.get("receipt_hash"),
        "materialization authorized by a different receipt",
    )
    return payload


def collect_window_bars(
    client: Any,
    job: Mapping[str, Any],
    *,
    candles_per_request: int,
    sleep_sec: float,
) -> tuple[list[Candle], int]:
    interval_sec = INTERVAL_SECONDS[GRANULARITY]
    client_limit = int(getattr(client, "max_candles_per_request", candles_per_request))
    request_limit = min(candles_per_request, max(1, client_limit))
    cursor = job["probe_start_ts"]
    range_end = job["window_end_ts"]
    requests_made = 0
    deduped: dict[int, Candle] = {}
    while cursor <= range_end:
        chunk_end = min(range_end, cursor + interval_sec * (request_limit - 1))
        candles = client.fetch_ohlcv(
            job["symbol"], GRANULARITY, cursor, chunk_end, request_limit
        )
        requests_made += 1
        for candle in candles:
            if cursor <= candle.ts <= chunk_end and candle.ts % interval_sec == 0:
                deduped[candle.ts] = candle
        cursor = chunk_end + interval_sec
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    return [deduped[key] for key in sorted(deduped)], requests_made


def classify_job_bars(
    job: Mapping[str, Any], bars: Sequence[Candle]
) -> dict[str, Any]:
    proxy_ts = int(job["proxy_ts"])
    window_end = int(job["window_end_ts"])
    window_bars = [bar for bar in bars if proxy_ts <= bar.ts < window_end]
    pre_bars = [bar for bar in bars if bar.ts < proxy_ts]
    flags: list[str] = []
    if not bars:
        flags.append("no_data")
    if pre_bars and bars[0].ts <= proxy_ts - 2 * TRUNCATION_TOLERANCE_SEC:
        flags.append("proxy_ts_after_first_bar")
    if window_bars and window_bars[0].ts > proxy_ts + TRUNCATION_TOLERANCE_SEC:
        flags.append("history_truncated")
    if 0 < len(window_bars) < SHORT_WINDOW_MIN_BARS:
        flags.append("short_window")
    return {
        "earliest_bar_ts": bars[0].ts if bars else None,
        "pre_window_bar_count": len(pre_bars),
        "window_bar_count": len(window_bars),
        "first_window_bar_ts": window_bars[0].ts if window_bars else None,
        "flags": flags,
        "window_bars": window_bars,
    }


def row_for(job: Mapping[str, Any], bar: Candle) -> dict[str, Any]:
    return {
        "run_id": RUN_ID,
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
        "window_role": "first_days",
    }


def _flag_census(job_summaries: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    census = {key: 0 for key in FLAG_KEYS}
    for summary in job_summaries:
        for flag in summary.get("flags") or []:
            if flag in census:
                census[flag] += 1
    return census


def run_collect(
    plan: Mapping[str, Any],
    materialization: Mapping[str, Any],
    *,
    output_root: Path,
    claim_path: Path,
) -> dict[str, Any]:
    execution = plan["execution"]
    jobs = derive_first_days_jobs(materialization.get("records") or [])
    _require(
        jobs_sha256(jobs) == execution["jobs_sha256"],
        "derived jobs do not match the plan binding",
    )
    output_root.mkdir(parents=True, exist_ok=True)
    output_jsonl = output_root / "ohlcv.jsonl"
    manifest_path = output_root / "manifest.json"
    _require(
        not output_jsonl.exists() and not manifest_path.exists(),
        "output namespace is not empty; refusing a second writer",
    )
    clients = {
        venue: CLIENTS[venue](
            timeout_sec=int(execution["timeout_sec"]),
            max_retries=int(execution["max_retries"]),
        )
        for venue in ("mexc", "gateio")
    }
    page_limits = dict(execution["effective_page_sizes"])
    deadline = time.monotonic() + float(execution["max_runtime_sec"])
    claim = claim_global_market_writer(
        claim_path,
        run_id=RUN_ID,
        owner_pid=os.getpid(),
        owner_kind="listing_momentum_first_days_collector",
        plan_hash=plan["plan_hash"],
        output_namespace=output_root,
    )
    started_utc = utc_now_iso()
    manifest = {
        "schema": "trading_mvp_slow_liquidity_listing_momentum_first_days_collect_manifest_v1",
        "run_id": RUN_ID,
        "status": "RUNNING",
        "started_at_utc": started_utc,
        "plan_hash": plan["plan_hash"],
        "materialization_hash": materialization["materialization_hash"],
        "ownership_token": claim["ownership_token"],
        "jobs_total": len(jobs),
    }
    write_manifest(manifest_path, manifest)
    rows_written = 0
    requests_made = 0
    job_summaries: list[dict[str, Any]] = []
    stop_reason = "completed"
    fatal_error: str | None = None
    try:
        with output_jsonl.open("w", encoding="utf-8") as handle:
            for index, job in enumerate(jobs):
                if time.monotonic() > deadline:
                    stop_reason = "max_runtime_sec_exceeded"
                    break
                client = clients[job["exchange"]]
                requests = 0
                try:
                    bars, requests = collect_window_bars(
                        client,
                        job,
                        candles_per_request=int(
                            page_limits.get(job["exchange"], page_limits["mexc"])
                        ),
                        sleep_sec=float(execution["sleep_sec"]),
                    )
                    requests_made += requests
                except Exception as exc:  # noqa: BLE001 - per-job resilience
                    bars = []
                    summary = {
                        "flags": ["request_error"],
                        "error": f"{type(exc).__name__}: {exc}",
                        "window_bar_count": 0,
                    }
                else:
                    summary = classify_job_bars(job, bars)
                summary.pop("window_bars", None)
                summary.update(
                    {
                        "exchange": job["exchange"],
                        "base": job["base"],
                        "symbol": job["symbol"],
                        "proxy_ts": job["proxy_ts"],
                        "requests": requests,
                    }
                )
                job_summaries.append(summary)
                for bar in bars:
                    if job["proxy_ts"] <= bar.ts < job["window_end_ts"]:
                        handle.write(
                            json.dumps(row_for(job, bar), ensure_ascii=False) + "\n"
                        )
                        rows_written += 1
                if (index + 1) % PROGRESS_EVERY == 0:
                    handle.flush()
                    print(
                        f"[{index + 1}/{len(jobs)}] rows={rows_written} "
                        f"requests={requests_made}",
                        flush=True,
                    )
    except Exception as exc:  # noqa: BLE001 - fatal collector error
        fatal_error = f"{type(exc).__name__}: {exc}"
        stop_reason = "fatal_error"
    finally:
        release_global_market_writer(
            claim_path,
            run_id=RUN_ID,
            owner_pid=int(claim["owner_pid"]),
            ownership_token=str(claim["ownership_token"]),
            final_status=stop_reason,
        )
    if fatal_error is not None:
        status = "FAILED"
    elif stop_reason == "completed":
        status = "COMPLETED"
    else:
        status = "STOPPED_INCOMPLETE"
    manifest.update(
        {
            "status": status,
            "stop_reason": stop_reason,
            "fatal_error": fatal_error,
            "finished_at_utc": utc_now_iso(),
            "jobs_processed": len(job_summaries),
            "rows_written": rows_written,
            "requests_made": requests_made,
            "flag_census": _flag_census(job_summaries),
            "output_jsonl": str(output_jsonl),
            "output_sha256": (
                _sha256_file(output_jsonl) if output_jsonl.exists() else None
            ),
            "jobs": job_summaries,
        }
    )
    write_manifest(manifest_path, manifest)
    return manifest


def read_status(output_root: Path) -> dict[str, Any]:
    manifest_path = output_root / "manifest.json"
    if not manifest_path.is_file():
        return {"status": "NOT_STARTED", "run_id": RUN_ID}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "status": manifest.get("status"),
        "stop_reason": manifest.get("stop_reason"),
        "jobs_total": manifest.get("jobs_total"),
        "jobs_processed": manifest.get("jobs_processed"),
        "rows_written": manifest.get("rows_written"),
        "flag_census": manifest.get("flag_census"),
        "output_sha256": manifest.get("output_sha256"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", default=str(PLAN_PATH))
    parser.add_argument("--materialization", default=str(MATERIALIZATION_PATH))
    parser.add_argument("--confirmed-visible-collect", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--output-root", default="")
    args = parser.parse_args(argv)
    plan_path = Path(args.plan)
    if args.status:
        output_root = (
            Path(args.output_root)
            if args.output_root
            else Path(
                json.loads(plan_path.read_text(encoding="utf-8"))["execution"][
                    "output_root"
                ]
            )
        )
        print(json.dumps(read_status(output_root), ensure_ascii=False))
        return 0
    if not args.confirmed_visible_collect:
        raise SystemExit("no authorized action requested")
    plan = load_and_validate_plan(plan_path)
    materialization = load_and_validate_materialization(
        Path(args.materialization), plan
    )
    execution = plan["execution"]
    output_root = Path(args.output_root) if args.output_root else Path(
        execution["output_root"]
    )
    claim_path = Path(execution["claim_path"])
    manifest = run_collect(
        plan, materialization, output_root=output_root, claim_path=claim_path
    )
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "stop_reason": manifest["stop_reason"],
                "fatal_error": manifest.get("fatal_error"),
                "jobs_processed": manifest["jobs_processed"],
                "rows_written": manifest["rows_written"],
                "flag_census": manifest["flag_census"],
            },
            ensure_ascii=False,
        )
    )
    return 0 if manifest["status"] == "COMPLETED" else 1


if __name__ == "__main__":
    _force_utf8 = getattr(sys.stdout, "reconfigure", None)
    if _force_utf8:
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            pass
    raise SystemExit(main())
