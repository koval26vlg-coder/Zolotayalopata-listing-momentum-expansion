from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from listing_event_normalizer import parse_ts


DEFAULT_CALENDAR_PATH = Path("exports/trading-mvp/listings/non_binance_listing_events.csv")
DEFAULT_ANALYSIS_DIR = Path("exports/trading-mvp/analysis")
DEFAULT_HISTORY_DIR = Path("exports/trading-mvp/listing-history")
DEFAULT_GRANULARITIES = ("1m", "5m", "1h")
INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
AVAILABILITY_ACCEPTED_DECISION = "LISTING_EVENT_HISTORY_AVAILABILITY_PREFLIGHT_ACCEPTED_READY_FOR_COLLECT_APPROVAL_PACKET"


@dataclass(frozen=True)
class HistoryEvent:
    event_id: str
    exchange: str
    base: str
    quote: str
    symbol: str
    event_ts: float
    listed_at_utc: str
    is_delisted: bool
    survivorship_status: str
    source_type: str

    @property
    def base_key(self) -> str:
        return f"{self.exchange}:{self.base}:{self.quote}"

    @property
    def status_priority(self) -> int:
        return 0 if self.is_delisted or self.survivorship_status == "current_non_tradable_snapshot" else 1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_exchange(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"gate", "gateio", "gate.io"}:
        return "gateio"
    if text in {"mexc", "mexc_spot"}:
        return "mexc"
    return text


def load_history_events(calendar_path: Path, *, quote: str = "USDT", exchanges: set[str] | None = None) -> list[HistoryEvent]:
    exchanges = exchanges or {"mexc", "gateio", "bitget"}
    events: list[HistoryEvent] = []
    with calendar_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            exchange = normalize_exchange(row.get("exchange"))
            if exchange not in exchanges:
                continue
            row_quote = str(row.get("quote") or quote).strip().upper()
            if row_quote != quote.upper():
                continue
            base = str(row.get("base") or "").strip().upper()
            symbol = str(row.get("symbol") or "").strip().upper()
            event_ts = parse_ts(row.get("listed_ts") or row.get("listed_at_utc") or row.get("first_trade_ts_utc"))
            if not base or not symbol or event_ts is None:
                continue
            is_delisted = str(row.get("is_delisted", "")).strip().lower() == "true"
            events.append(
                HistoryEvent(
                    event_id=str(row.get("event_id") or f"{exchange}:{symbol}:listing"),
                    exchange=exchange,
                    base=base,
                    quote=row_quote,
                    symbol=symbol,
                    event_ts=event_ts,
                    listed_at_utc=str(row.get("listed_at_utc") or iso_from_ts(event_ts)),
                    is_delisted=is_delisted,
                    survivorship_status=str(row.get("survivorship_status") or ""),
                    source_type=str(row.get("source_type") or ""),
                )
            )
    return sorted(events, key=lambda event: (event.status_priority, event.event_ts, event.exchange, event.symbol))


def _round_robin_by_exchange(events: list[HistoryEvent]) -> list[HistoryEvent]:
    grouped: dict[str, list[HistoryEvent]] = {}
    for event in events:
        grouped.setdefault(event.exchange, []).append(event)
    exchanges = sorted(grouped)
    ordered: list[HistoryEvent] = []
    while any(grouped.values()):
        for exchange in exchanges:
            group = grouped.get(exchange) or []
            if group:
                ordered.append(group.pop(0))
    return ordered


def select_history_events(
    events: list[HistoryEvent],
    *,
    target_events: int,
    max_events_per_base: int = 2,
    max_exchange_fraction: float = 0.60,
) -> list[HistoryEvent]:
    if target_events <= 0:
        return []
    selected: list[HistoryEvent] = []
    seen_event_ids: set[str] = set()
    base_counts: Counter[str] = Counter()
    exchange_counts: Counter[str] = Counter()
    exchange_cap = max(1, math.ceil(target_events * max_exchange_fraction))

    def try_add(event: HistoryEvent, *, enforce_base_cap: bool = True, enforce_exchange_cap: bool = True) -> None:
        if len(selected) >= target_events:
            return
        if event.event_id in seen_event_ids:
            return
        if enforce_base_cap and base_counts[event.base_key] >= max_events_per_base:
            return
        if enforce_exchange_cap and exchange_counts[event.exchange] >= exchange_cap:
            return
        selected.append(event)
        seen_event_ids.add(event.event_id)
        base_counts[event.base_key] += 1
        exchange_counts[event.exchange] += 1

    nontradable = [event for event in events if event.status_priority == 0]
    active = [event for event in events if event.status_priority != 0]
    for event in _round_robin_by_exchange(nontradable):
        try_add(event)
    for event in _round_robin_by_exchange(active):
        try_add(event)
    if len(selected) < target_events:
        for event in _round_robin_by_exchange(active):
            try_add(event, enforce_base_cap=False)
    if len(selected) < target_events:
        for event in _round_robin_by_exchange(events):
            try_add(event, enforce_base_cap=False, enforce_exchange_cap=False)
    return selected


def estimate_requests(
    *,
    selected_count: int,
    pre_window_sec: int,
    post_window_sec: int,
    granularities: tuple[str, ...],
    candles_per_request: int,
    request_rate_per_sec: float,
) -> dict[str, Any]:
    window_sec = pre_window_sec + post_window_sec
    by_granularity: dict[str, Any] = {}
    total_candles = 0
    total_requests = 0
    for granularity in granularities:
        interval_sec = INTERVAL_SECONDS[granularity]
        candles_per_event = math.ceil(window_sec / interval_sec) + 1
        requests_per_event = math.ceil(candles_per_event / candles_per_request)
        candles = candles_per_event * selected_count
        requests = requests_per_event * selected_count
        total_candles += candles
        total_requests += requests
        by_granularity[granularity] = {
            "interval_sec": interval_sec,
            "candles_per_event": candles_per_event,
            "requests_per_event": requests_per_event,
            "estimated_total_candles": candles,
            "estimated_total_requests": requests,
        }
    estimated_runtime_sec = total_requests / max(0.1, request_rate_per_sec)
    return {
        "window_sec": window_sec,
        "pre_window_sec": pre_window_sec,
        "post_window_sec": post_window_sec,
        "candles_per_request": candles_per_request,
        "request_rate_per_sec": request_rate_per_sec,
        "granularities": list(granularities),
        "by_granularity": by_granularity,
        "estimated_total_candles": total_candles,
        "estimated_total_requests": total_requests,
        "estimated_runtime_sec": estimated_runtime_sec,
        "estimated_runtime_min": estimated_runtime_sec / 60.0,
    }


def event_to_plan_row(event: HistoryEvent, *, pre_window_sec: int, post_window_sec: int) -> dict[str, Any]:
    start_ts = event.event_ts - pre_window_sec
    end_ts = event.event_ts + post_window_sec
    return {
        "event_id": event.event_id,
        "exchange": event.exchange,
        "base": event.base,
        "quote": event.quote,
        "symbol": event.symbol,
        "event_ts": event.event_ts,
        "event_iso": event.listed_at_utc,
        "window_start_ts": start_ts,
        "window_start_iso": iso_from_ts(start_ts),
        "window_end_ts": end_ts,
        "window_end_iso": iso_from_ts(end_ts),
        "is_delisted": event.is_delisted,
        "survivorship_status": event.survivorship_status,
        "source_type": event.source_type,
    }


def load_availability_preflight(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _probe_row_to_plan_row(row: dict[str, Any], *, pre_window_sec: int, post_window_sec: int) -> dict[str, Any]:
    event_ts = float(row["event_ts"])
    start_ts = float(row.get("full_window_start_ts") or (event_ts - pre_window_sec))
    end_ts = float(row.get("full_window_end_ts") or (event_ts + post_window_sec))
    event_iso = str(row.get("event_iso") or iso_from_ts(event_ts))
    return {
        "event_id": str(row["event_id"]),
        "exchange": normalize_exchange(row["exchange"]),
        "base": str(row["base"]).upper(),
        "quote": str(row.get("quote") or "USDT").upper(),
        "symbol": str(row["symbol"]).upper(),
        "event_ts": event_ts,
        "event_iso": event_iso,
        "window_start_ts": start_ts,
        "window_start_iso": iso_from_ts(start_ts),
        "window_end_ts": end_ts,
        "window_end_iso": iso_from_ts(end_ts),
        "is_delisted": bool(row.get("is_delisted", False)),
        "survivorship_status": str(row.get("survivorship_status") or "availability_probe_verified"),
        "source_type": str(row.get("source_type") or "availability_preflight_probe_ok"),
    }


def availability_ok_event_plan(
    availability_preflight: dict[str, Any] | None,
    *,
    granularities: tuple[str, ...],
    pre_window_sec: int,
    post_window_sec: int,
) -> list[dict[str, Any]]:
    if not availability_preflight:
        return []
    rows = availability_preflight.get("probe_rows") or []
    selected: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    requested = set(granularities)
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("probe_status") or "") != "ok":
            continue
        if str(row.get("granularity") or "") not in requested:
            continue
        exchange = normalize_exchange(row.get("exchange"))
        event_id = str(row.get("event_id") or "")
        if not exchange or not event_id:
            continue
        key = (exchange, event_id)
        if key in seen:
            continue
        selected.append(_probe_row_to_plan_row(row, pre_window_sec=pre_window_sec, post_window_sec=post_window_sec))
        seen.add(key)
    return selected


def load_previous_quality_report(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_previous_quality(report: dict[str, Any] | None) -> dict[str, Any]:
    if not report:
        return {
            "available": False,
            "accepted": None,
            "decision": None,
            "reasons": [],
            "metrics": {},
            "counts": {},
        }
    metrics = report.get("metrics") or {}
    counts = report.get("counts") or {}
    return {
        "available": True,
        "accepted": bool(report.get("accepted")),
        "decision": report.get("decision"),
        "reasons": list(report.get("reasons") or []),
        "metrics": {
            "selected_events": metrics.get("selected_events"),
            "ok_events": metrics.get("ok_events"),
            "ok_bases": metrics.get("ok_bases"),
            "ok_exchanges": metrics.get("ok_exchanges"),
            "ok_event_granularity_slots": metrics.get("ok_event_granularity_slots"),
            "ok_event_fraction": metrics.get("ok_event_fraction"),
            "ok_slot_fraction": metrics.get("ok_slot_fraction"),
            "api_error_slot_rate": metrics.get("api_error_slot_rate"),
            "max_single_exchange_ok_event_fraction": metrics.get("max_single_exchange_ok_event_fraction"),
        },
        "counts": {
            "rows_by_exchange": counts.get("rows_by_exchange") or {},
            "ok_rows_by_exchange": counts.get("ok_rows_by_exchange") or {},
            "error_rows_by_exchange": counts.get("error_rows_by_exchange") or {},
            "ok_events_by_exchange": counts.get("ok_events_by_exchange") or {},
        },
    }


def previous_quality_requires_revised_preflight(summary: dict[str, Any]) -> bool:
    if not summary.get("available"):
        return False
    if summary.get("accepted"):
        return False
    reasons = set(summary.get("reasons") or [])
    blocking_reasons = {
        "min_ok_exchanges",
        "max_single_exchange_ok_event_fraction",
        "max_api_error_slot_rate",
        "min_ok_events",
        "min_ok_event_granularity_slots",
    }
    return bool(reasons & blocking_reasons)


def build_history_collect_preview(
    *,
    calendar_path: Path,
    output_path: Path | None,
    run_id: str,
    quote: str = "USDT",
    target_events: int = 120,
    target_bases_min: int = 30,
    pre_window_sec: int = 3600,
    post_window_sec: int = 259200,
    max_events_per_base: int = 2,
    max_exchange_fraction: float = 0.60,
    min_exchange_count: int = 2,
    candles_per_request: int = 1000,
    request_rate_per_sec: float = 2.0,
    granularities: tuple[str, ...] = DEFAULT_GRANULARITIES,
    previous_quality_report_path: Path | None = None,
    require_two_venue_history_preflight: bool = False,
    availability_preflight_path: Path | None = None,
    use_availability_ok_events: bool = False,
) -> dict[str, Any]:
    events = load_history_events(calendar_path, quote=quote)
    previous_quality = summarize_previous_quality(load_previous_quality_report(previous_quality_report_path))
    quality_blocks_repeat = previous_quality_requires_revised_preflight(previous_quality)
    availability_preflight = load_availability_preflight(availability_preflight_path)
    availability_decision = str((availability_preflight or {}).get("decision") or "")
    availability_summary = (availability_preflight or {}).get("summary") or {}
    availability_event_plan = (
        availability_ok_event_plan(
            availability_preflight,
            granularities=granularities,
            pre_window_sec=pre_window_sec,
            post_window_sec=post_window_sec,
        )
        if use_availability_ok_events
        else []
    )
    use_explicit_availability_plan = bool(use_availability_ok_events and availability_event_plan)
    if use_explicit_availability_plan:
        selected_count = len(availability_event_plan)
        exchange_counts = Counter(str(row["exchange"]) for row in availability_event_plan)
        base_counts = Counter(f"{row['exchange']}:{row['base']}:{row['quote']}" for row in availability_event_plan)
        nontradable_count = sum(1 for row in availability_event_plan if row.get("is_delisted"))
        sample_events = availability_event_plan
    else:
        selected = select_history_events(
            events,
            target_events=target_events,
            max_events_per_base=max_events_per_base,
            max_exchange_fraction=max_exchange_fraction,
        )
        selected_count = len(selected)
        exchange_counts = Counter(event.exchange for event in selected)
        base_counts = Counter(event.base for event in selected)
        nontradable_count = sum(1 for event in selected if event.status_priority == 0)
        sample_events = [event_to_plan_row(event, pre_window_sec=pre_window_sec, post_window_sec=post_window_sec) for event in selected[:25]]
    estimate = estimate_requests(
        selected_count=selected_count,
        pre_window_sec=pre_window_sec,
        post_window_sec=post_window_sec,
        granularities=granularities,
        candles_per_request=candles_per_request,
        request_rate_per_sec=request_rate_per_sec,
    )
    run_dir = DEFAULT_HISTORY_DIR / run_id
    output_jsonl = run_dir / "ohlcv.jsonl"
    manifest_path = run_dir / "manifest.json"
    event_plan_path = run_dir / "event_plan.json"
    availability_pass_preview = (
        use_explicit_availability_plan
        and availability_decision == AVAILABILITY_ACCEPTED_DECISION
        and selected_count >= 2
        and len(base_counts) >= 2
        and len(exchange_counts) >= min_exchange_count
        and int(availability_summary.get("ok_exchanges") or 0) >= min_exchange_count
    )
    calendar_pass_preview = (
        not use_availability_ok_events
        and selected_count >= min(target_events, 100)
        and len(base_counts) >= target_bases_min
        and len(exchange_counts) >= min_exchange_count
        and nontradable_count > 0
        and not (quality_blocks_repeat and require_two_venue_history_preflight)
    )
    pass_preview = availability_pass_preview or calendar_pass_preview
    if pass_preview:
        decision = "LISTING_EVENT_HISTORY_COLLECT_PREVIEW_READY_AWAITING_EXPLICIT_APPROVAL"
    elif quality_blocks_repeat and require_two_venue_history_preflight:
        decision = "LISTING_EVENT_HISTORY_COLLECT_PREVIEW_BLOCKED_NEEDS_REVISED_TWO_VENUE_PREFLIGHT"
    else:
        decision = "LISTING_EVENT_HISTORY_COLLECT_PREVIEW_BLOCKED_INSUFFICIENT_EVENT_SET"
    result: dict[str, Any] = {
        "mode": "listing_event_history_collect_preview_planonly",
        "generated_at": utc_now_iso(),
        "decision": decision,
        "would_start": False,
        "research_only": True,
        "live_orders": False,
        "api_keys": False,
        "leverage_or_margin": False,
        "collect_allowed_now": False,
        "actual_collect_requires_explicit_user_approval": True,
        "visible_terminal_or_monitor_required": True,
        "hidden_background_collect_allowed": False,
        "replay_allowed_now": False,
        "grid_allowed_now": False,
        "paper_forward_allowed": False,
        "calendar_path": str(calendar_path),
        "run_id": run_id,
        "run_dir": str(run_dir),
        "expected_outputs": {
            "output_jsonl": str(output_jsonl),
            "manifest_path": str(manifest_path),
            "event_plan_path": str(event_plan_path),
            "stdout_path": str(run_dir / "collector.out.log"),
            "stderr_path": str(run_dir / "collector.err.log"),
        },
        "selection": {
            "candidate_events": len(events),
            "selected_events": selected_count,
            "selected_unique_bases": len(base_counts),
            "selected_exchange_count": len(exchange_counts),
            "selected_exchange_counts": dict(exchange_counts),
            "selected_nontradable_or_delisted_events": nontradable_count,
            "target_events": target_events,
            "target_bases_min": target_bases_min,
            "max_events_per_base": max_events_per_base,
            "max_exchange_fraction": max_exchange_fraction,
            "min_exchange_count": min_exchange_count,
            "event_plan_source": "explicit_sample_events" if use_explicit_availability_plan else "calendar_selection",
            "sample_events": sample_events,
        },
        "previous_quality_gate": previous_quality,
        "availability_preflight": {
            "enabled": bool(use_availability_ok_events),
            "path": str(availability_preflight_path) if availability_preflight_path else "",
            "available": availability_preflight is not None,
            "accepted": availability_decision == AVAILABILITY_ACCEPTED_DECISION,
            "decision": availability_decision,
            "summary": availability_summary,
        },
        "revised_two_venue_preflight_contract": {
            "required": bool((require_two_venue_history_preflight or quality_blocks_repeat) and not availability_pass_preview),
            "quality_blocks_repeat": bool(quality_blocks_repeat),
            "reason": (
                "Accepted availability preflight supplies the explicit two-venue event plan for the next visible collect."
                if availability_pass_preview
                else "Previous history collection failed two-venue quality gates; a new approval packet must prove expected MEXC/Gate historical coverage before actual collect."
                if quality_blocks_repeat
                else "No prior rejected quality report requires extra two-venue preflight."
            ),
            "minimum_preview_requirements": {
                "expected_two_venue_ok_events": 30,
                "expected_ok_bases": 20,
                "expected_ok_exchanges": min_exchange_count,
                "max_expected_api_error_slot_rate": 0.50,
                "max_single_exchange_expected_ok_event_fraction": 0.70,
            },
            "required_checks_before_actual_collect": [
                "probe_symbol_history_availability_for_mexc_and_gateio_per_event",
                "separate_endpoint_api_error_from_true_no_data_or_delisted",
                "emit_exchange_event_granularity_coverage_matrix",
                "retain_failed_or_delisted_events_as_negative_evidence",
                f"block_actual_collect_if_expected_ok_exchanges_lt_{min_exchange_count}",
            ],
        },
        "request_budget": estimate,
        "collector_contract": {
            "public_data_only": True,
            "api_keys_required": False,
            "live_orders": False,
            "exchanges": sorted(exchange_counts),
            "granularities": list(granularities),
            "event_plan_source": "preview_selection_sample_events" if use_explicit_availability_plan else "calendar_selection",
            "required_row_fields": [
                "exchange",
                "symbol",
                "base",
                "quote",
                "event_id",
                "event_ts",
                "granularity",
                "candle_ts",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "trade_count_if_available",
                "data_status",
            ],
            "survivorship_controls": [
                "selected delisted/non-tradable events must remain in manifest even if candles are missing",
                "missing API data becomes data_status=no_data_or_delisted, not a dropped row",
                "per-exchange failures are reported in manifest errors",
            ],
        },
        "blocked_actions": [
            "actual_collect_without_user_approval",
            "hidden_or_background_collect",
            "replay_on_current_ws_slice",
            "grid_search",
            "paper_forward",
            "live_orders",
            "api_keys",
            "leverage_or_margin",
        ],
        "next_valid_moves": (
            [
                "After explicit user approval, run the visible public OHLCV history collector using this preview contract.",
                "Write manifest with selected event ids, missing-data outcomes and request/error counts.",
                "After collect completes, run data-quality and listing-event normalizer before any replay.",
            ]
            if pass_preview
            else [
                "Implement a revised PlanOnly coverage preflight that probes expected MEXC/Gate historical availability before another actual collect.",
                "Do not repeat the same MEXC-only/Gate-error collection sample.",
                "Do not collect or replay until the preview proves plausible two-venue coverage.",
            ]
            if quality_blocks_repeat and require_two_venue_history_preflight
            else [
                "Improve the listing calendar/event selection before any collection or replay.",
                "Do not collect or replay with insufficient event diversity.",
            ]
        ),
        "command_after_explicit_approval": "",
        "command_after_explicit_approval_note": "Actual collector start command is intentionally not emitted yet; next step is implementation/run only after explicit user approval.",
        "output_path": str(output_path) if output_path else "",
    }
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def default_output_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return DEFAULT_ANALYSIS_DIR / f"listing_event_history_collect_preview_{stamp}.json"


def default_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"listing_event_history_collect_{stamp}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PlanOnly preview for visible listing-event OHLCV history collection.")
    parser.add_argument("--calendar", default=str(DEFAULT_CALENDAR_PATH))
    parser.add_argument("--output", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--quote", default="USDT")
    parser.add_argument("--target-events", type=int, default=120)
    parser.add_argument("--target-bases-min", type=int, default=30)
    parser.add_argument("--pre-window-sec", type=int, default=3600)
    parser.add_argument("--post-window-sec", type=int, default=259200)
    parser.add_argument("--max-events-per-base", type=int, default=2)
    parser.add_argument("--max-exchange-fraction", type=float, default=0.60)
    parser.add_argument("--min-exchange-count", type=int, default=2)
    parser.add_argument("--candles-per-request", type=int, default=1000)
    parser.add_argument("--request-rate-per-sec", type=float, default=2.0)
    parser.add_argument("--granularities", default=",".join(DEFAULT_GRANULARITIES))
    parser.add_argument("--previous-quality-report", default="")
    parser.add_argument("--require-two-venue-history-preflight", action="store_true")
    parser.add_argument("--availability-preflight", default="")
    parser.add_argument("--use-availability-ok-events", action="store_true")
    args = parser.parse_args(argv)
    granularities = tuple(part.strip() for part in args.granularities.split(",") if part.strip())
    unknown = [granularity for granularity in granularities if granularity not in INTERVAL_SECONDS]
    if unknown:
        raise SystemExit(f"unsupported granularities: {', '.join(unknown)}")
    output_path = Path(args.output) if args.output else default_output_path()
    result = build_history_collect_preview(
        calendar_path=Path(args.calendar),
        output_path=output_path,
        run_id=args.run_id or default_run_id(),
        quote=args.quote,
        target_events=args.target_events,
        target_bases_min=args.target_bases_min,
        pre_window_sec=args.pre_window_sec,
        post_window_sec=args.post_window_sec,
        max_events_per_base=args.max_events_per_base,
        max_exchange_fraction=args.max_exchange_fraction,
        min_exchange_count=args.min_exchange_count,
        candles_per_request=args.candles_per_request,
        request_rate_per_sec=args.request_rate_per_sec,
        granularities=granularities,
        previous_quality_report_path=Path(args.previous_quality_report) if args.previous_quality_report else None,
        require_two_venue_history_preflight=args.require_two_venue_history_preflight,
        availability_preflight_path=Path(args.availability_preflight) if args.availability_preflight else None,
        use_availability_ok_events=args.use_availability_ok_events,
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
