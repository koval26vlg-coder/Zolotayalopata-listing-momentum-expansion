from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CALENDAR_PATH = Path("exports/trading-mvp/listings/non_binance_listing_events.csv")
DEFAULT_CALENDAR_SUMMARY_PATH = Path("exports/trading-mvp/listings/non_binance_listing_events.summary.json")
DEFAULT_OUTPUT_DIR = Path("exports/trading-mvp/analysis")
DEFAULT_POST_WINDOW_SEC = 72 * 60 * 60
DEFAULT_PRE_WINDOW_SEC = 60 * 60
DEFAULT_MIN_OVERLAP_EVENTS = 100
DEFAULT_MIN_OVERLAP_BASES = 30
DEFAULT_MIN_OVERLAP_EXCHANGES = 2
DEFAULT_MIN_HISTORY_EVENTS = 30
DEFAULT_MIN_HISTORY_BASES = 30
DEFAULT_MIN_HISTORY_EXCHANGES = 2
DEFAULT_MAX_SINGLE_EXCHANGE_EVENT_FRACTION = 0.60


@dataclass(frozen=True)
class MarketWindow:
    start_ts: float
    end_ts: float
    start_iso: str
    end_iso: str
    span_sec: float


@dataclass(frozen=True)
class AcceptedMarket:
    exchange: str
    symbol: str
    base: str
    quote: str

    @property
    def key(self) -> str:
        return f"{self.exchange}:{self.symbol}"

    @property
    def base_key(self) -> str:
        return f"{self.exchange}:{self.base}:{self.quote}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_from_ts(ts: float | int | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value: Any) -> float | None:
    if value in (None, "", 0, "0"):
        return None
    if isinstance(value, (int, float)):
        return float(value) if float(value) > 0 else None
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
        return numeric if numeric > 0 else None
    except ValueError:
        pass
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return None


def normalize_exchange(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"gate", "gateio", "gate.io"}:
        return "gateio"
    if text in {"mexc", "mexc_spot"}:
        return "mexc"
    return text


def parse_symbol(exchange: str, symbol: str, quote: str = "USDT") -> tuple[str, str]:
    symbol_u = str(symbol or "").strip().upper()
    quote_u = str(quote or "USDT").strip().upper()
    if not symbol_u:
        return "", quote_u
    if "_" in symbol_u:
        base, pair_quote = symbol_u.rsplit("_", 1)
        return base, pair_quote
    if "-" in symbol_u:
        base, pair_quote = symbol_u.rsplit("-", 1)
        return base, pair_quote
    if symbol_u.endswith(quote_u) and len(symbol_u) > len(quote_u):
        return symbol_u[: -len(quote_u)], quote_u
    return symbol_u, quote_u


def canonical_symbol(exchange: str, base: str, quote: str = "USDT") -> str:
    exchange_n = normalize_exchange(exchange)
    base_u = str(base or "").strip().upper()
    quote_u = str(quote or "USDT").strip().upper()
    if exchange_n == "gateio":
        return f"{base_u}_{quote_u}"
    return f"{base_u}{quote_u}"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if text:
                rows.append(json.loads(text))
    return rows


def load_calendar_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def extract_market_window(manifest: dict[str, Any]) -> MarketWindow:
    start_ts = parse_ts(manifest.get("actual_first_ts"))
    end_ts = parse_ts(manifest.get("actual_last_ts"))
    if start_ts is None:
        start_ts = parse_ts(manifest.get("first_ts") or manifest.get("start_ts") or manifest.get("min_ts"))
    if end_ts is None:
        end_ts = parse_ts(manifest.get("last_ts") or manifest.get("end_ts") or manifest.get("max_ts"))
    if start_ts is None or end_ts is None:
        markets = manifest.get("markets") or []
        starts = [parse_ts(item.get("first_ts")) for item in markets if isinstance(item, dict)]
        ends = [parse_ts(item.get("last_ts")) for item in markets if isinstance(item, dict)]
        starts = [ts for ts in starts if ts is not None]
        ends = [ts for ts in ends if ts is not None]
        if start_ts is None and starts:
            start_ts = min(starts)
        if end_ts is None and ends:
            end_ts = max(ends)
    if start_ts is None or end_ts is None or end_ts <= start_ts:
        raise ValueError("market filter manifest does not expose a valid data time window")
    return MarketWindow(
        start_ts=start_ts,
        end_ts=end_ts,
        start_iso=str(manifest.get("actual_first_iso") or iso_from_ts(start_ts)),
        end_iso=str(manifest.get("actual_last_iso") or iso_from_ts(end_ts)),
        span_sec=end_ts - start_ts,
    )


def accepted_markets_from_manifest(manifest: dict[str, Any], quote: str = "USDT") -> list[AcceptedMarket]:
    markets: list[AcceptedMarket] = []
    raw_markets = manifest.get("accepted_markets") or []
    for item in raw_markets:
        if isinstance(item, str) and ":" in item:
            exchange, symbol = item.split(":", 1)
            exchange_n = normalize_exchange(exchange)
            base, pair_quote = parse_symbol(exchange_n, symbol, quote=quote)
            if exchange_n and base:
                markets.append(AcceptedMarket(exchange=exchange_n, symbol=symbol.upper(), base=base, quote=pair_quote))
    if not markets:
        for item in manifest.get("markets") or []:
            if not isinstance(item, dict):
                continue
            if item.get("accepted") is False:
                continue
            exchange_n = normalize_exchange(item.get("exchange"))
            symbol = str(item.get("symbol") or "").strip().upper()
            base, pair_quote = parse_symbol(exchange_n, symbol, quote=quote)
            if exchange_n and base:
                markets.append(AcceptedMarket(exchange=exchange_n, symbol=symbol, base=base, quote=pair_quote))
    return sorted({market.key: market for market in markets}.values(), key=lambda market: market.key)


def calendar_event_ts(row: dict[str, Any]) -> float | None:
    return parse_ts(row.get("listed_ts") or row.get("listed_at_utc") or row.get("first_trade_ts_utc"))


def event_market_keys(row: dict[str, Any], quote: str = "USDT") -> tuple[str, str, str, str, str]:
    exchange = normalize_exchange(row.get("exchange"))
    quote_u = str(row.get("quote") or quote).strip().upper()
    base = str(row.get("base") or "").strip().upper()
    symbol = str(row.get("symbol") or "").strip().upper()
    if not base and symbol:
        base, quote_u = parse_symbol(exchange, symbol, quote=quote_u)
    if not symbol and base:
        symbol = canonical_symbol(exchange, base, quote_u)
    exact_key = f"{exchange}:{symbol}"
    base_key = f"{exchange}:{base}:{quote_u}"
    return exchange, base, quote_u, symbol, exact_key if exchange and symbol else "", base_key if exchange and base else ""


def latest_market_filter_manifest(root: Path) -> Path | None:
    backtests = root / "exports" / "trading-mvp" / "backtests"
    if not backtests.exists():
        return None
    candidates = sorted(
        backtests.glob("ws_market_filter_manifest_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def default_output_path(root: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return root / DEFAULT_OUTPUT_DIR / f"listing_event_normalizer_planonly_{stamp}.json"


def normalize_listing_events_planonly(
    *,
    calendar_path: Path,
    calendar_summary_path: Path | None,
    market_manifest_path: Path,
    output_path: Path | None = None,
    quote: str = "USDT",
    pre_window_sec: int = DEFAULT_PRE_WINDOW_SEC,
    post_window_sec: int = DEFAULT_POST_WINDOW_SEC,
    min_overlap_events: int = DEFAULT_MIN_OVERLAP_EVENTS,
    min_overlap_bases: int = DEFAULT_MIN_OVERLAP_BASES,
    min_overlap_exchanges: int = DEFAULT_MIN_OVERLAP_EXCHANGES,
) -> dict[str, Any]:
    calendar_rows = load_calendar_rows(calendar_path)
    summary = load_json(calendar_summary_path) if calendar_summary_path and calendar_summary_path.exists() else {}
    manifest = load_json(market_manifest_path)
    market_window = extract_market_window(manifest)
    accepted_markets = accepted_markets_from_manifest(manifest, quote=quote)
    accepted_exact = {market.key for market in accepted_markets}
    accepted_base = {market.base_key for market in accepted_markets}
    accepted_by_exchange = Counter(market.exchange for market in accepted_markets)
    accepted_by_base = Counter(market.base for market in accepted_markets)

    rows_with_ts = 0
    rows_missing_ts = 0
    matched_current_market = 0
    partial_time_overlap = 0
    matched_and_time_overlap = 0
    matched_full_post_window = 0
    matched_full_pre_post_window = 0
    overlap_events: list[dict[str, Any]] = []
    full_window_events: list[dict[str, Any]] = []
    overlap_bases: set[str] = set()
    overlap_exchanges: set[str] = set()
    delisted_or_nontradable_overlap = 0
    event_exchange_counts: Counter[str] = Counter()
    calendar_exchange_counts: Counter[str] = Counter()

    for row in calendar_rows:
        exchange, base, row_quote, symbol, exact_key, base_key = event_market_keys(row, quote=quote)
        if exchange:
            calendar_exchange_counts[exchange] += 1
        event_ts = calendar_event_ts(row)
        if event_ts is None:
            rows_missing_ts += 1
            continue
        rows_with_ts += 1
        is_matched = exact_key in accepted_exact or base_key in accepted_base
        if is_matched:
            matched_current_market += 1
        event_start = event_ts - pre_window_sec
        event_end = event_ts + post_window_sec
        has_time_overlap = event_end >= market_window.start_ts and event_start <= market_window.end_ts
        has_full_post = event_ts >= market_window.start_ts and (event_ts + post_window_sec) <= market_window.end_ts
        has_full_pre_post = event_start >= market_window.start_ts and event_end <= market_window.end_ts
        if has_time_overlap:
            partial_time_overlap += 1
        if not (is_matched and has_time_overlap):
            continue
        matched_and_time_overlap += 1
        overlap_bases.add(base)
        overlap_exchanges.add(exchange)
        event_exchange_counts[exchange] += 1
        if str(row.get("is_delisted", "")).lower() == "true" or row.get("survivorship_status") == "current_non_tradable_snapshot":
            delisted_or_nontradable_overlap += 1
        overlap_record = {
            "event_id": row.get("event_id", ""),
            "exchange": exchange,
            "base": base,
            "quote": row_quote,
            "symbol": symbol,
            "listed_at_utc": row.get("listed_at_utc") or iso_from_ts(event_ts),
            "event_ts": event_ts,
            "matched_current_market": is_matched,
            "time_overlap": has_time_overlap,
            "full_post_window": has_full_post,
            "full_pre_post_window": has_full_pre_post,
            "survivorship_status": row.get("survivorship_status", ""),
            "is_delisted": row.get("is_delisted", ""),
            "source_type": row.get("source_type", ""),
        }
        overlap_events.append(overlap_record)
        if has_full_post:
            matched_full_post_window += 1
            full_window_events.append(overlap_record)
        if has_full_pre_post:
            matched_full_pre_post_window += 1

    enough_overlap = (
        matched_and_time_overlap >= min_overlap_events
        and len(overlap_bases) >= min_overlap_bases
        and len(overlap_exchanges) >= min_overlap_exchanges
    )
    if not calendar_rows:
        decision = "LISTING_EVENT_NORMALIZER_BLOCKED_EMPTY_CALENDAR"
        required_next_step = "build_bias_controlled_listing_calendar"
    elif not bool(summary.get("bias_control_pass", False)):
        decision = "LISTING_EVENT_NORMALIZER_BLOCKED_CALENDAR_NOT_BIAS_CONTROLLED"
        required_next_step = "fix_listing_calendar_bias_controls_before_replay"
    elif not enough_overlap:
        decision = "LISTING_EVENT_NORMALIZER_PLANONLY_INSUFFICIENT_OVERLAP_NEEDS_EVENT_OHLCV_HISTORY"
        required_next_step = "source_or_collect_visible_listing_event_ohlcv_history_before_replay"
    else:
        decision = "LISTING_EVENT_NORMALIZER_PLANONLY_READY_FOR_EVENT_REPLAY_PLANONLY"
        required_next_step = "implement_read_only_listing_event_replay_planonly_no_grid_no_live"

    result: dict[str, Any] = {
        "mode": "listing_event_normalizer_planonly",
        "generated_at": utc_now_iso(),
        "decision": decision,
        "required_next_step": required_next_step,
        "research_only": True,
        "would_start": False,
        "live_orders": False,
        "api_keys": False,
        "leverage_or_margin": False,
        "collect_allowed_now": False,
        "grid_allowed_now": False,
        "paper_forward_allowed": False,
        "replay_allowed_now": decision == "LISTING_EVENT_NORMALIZER_PLANONLY_READY_FOR_EVENT_REPLAY_PLANONLY",
        "calendar": {
            "path": str(calendar_path),
            "summary_path": str(calendar_summary_path) if calendar_summary_path else "",
            "rows": len(calendar_rows),
            "rows_with_timestamps": rows_with_ts,
            "rows_missing_timestamps": rows_missing_ts,
            "bias_control_pass": bool(summary.get("bias_control_pass", False)),
            "coverage_warnings": summary.get("coverage_warnings", []),
            "exchange_counts": dict(calendar_exchange_counts),
        },
        "market_data": {
            "manifest_path": str(market_manifest_path),
            "start_ts": market_window.start_ts,
            "end_ts": market_window.end_ts,
            "start_iso": market_window.start_iso,
            "end_iso": market_window.end_iso,
            "span_sec": market_window.span_sec,
            "span_hours": market_window.span_sec / 3600.0,
            "accepted_markets": len(accepted_markets),
            "accepted_exchange_counts": dict(accepted_by_exchange),
            "accepted_unique_bases": len(accepted_by_base),
        },
        "overlap": {
            "pre_window_sec": pre_window_sec,
            "post_window_sec": post_window_sec,
            "min_overlap_events": min_overlap_events,
            "min_overlap_bases": min_overlap_bases,
            "min_overlap_exchanges": min_overlap_exchanges,
            "matched_current_market_events": matched_current_market,
            "partial_time_overlap_events_any_market": partial_time_overlap,
            "matched_time_overlap_events": matched_and_time_overlap,
            "matched_full_post_window_events": matched_full_post_window,
            "matched_full_pre_post_window_events": matched_full_pre_post_window,
            "overlap_unique_bases": len(overlap_bases),
            "overlap_exchange_count": len(overlap_exchanges),
            "overlap_exchange_counts": dict(event_exchange_counts),
            "delisted_or_nontradable_overlap_events": delisted_or_nontradable_overlap,
            "sample_events": overlap_events[:25],
            "sample_full_window_events": full_window_events[:25],
        },
        "economics_policy": {
            "fee_assumption": "base/VIP0/no-volume only",
            "acceptance_requires": [
                "positive net expectancy after base fees/slippage/spread/event buffers",
                "chronological OOS by listing date",
                "walk-forward robustness",
                "stress with wider spreads, missed entry, and delist/freeze haircut",
            ],
        },
        "blocked_actions": [
            "live_orders",
            "api_keys",
            "leverage_or_margin",
            "grid_search",
            "paper_forward",
            "hidden_or_background_collect",
        ],
        "next_valid_moves": (
            [
                "Plan a visible, read-only event OHLCV history acquisition path; do not collect until explicitly approved.",
                "Keep delisted/frozen/no-trade events in the event set to avoid survivorship bias.",
                "Do not run replay/grid/live/API/paper-forward from the current WS slice because listing-event overlap is insufficient.",
            ]
            if not enough_overlap
            else [
                "Implement read-only listing event replay PlanOnly against the normalized event set.",
                "Keep grid/live/API/paper-forward blocked until OOS/walk-forward/stress/economics gates exist.",
            ]
        ),
        "output_path": str(output_path) if output_path else "",
    }
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def normalize_listing_history_events_planonly(
    *,
    history_jsonl_path: Path,
    history_manifest_path: Path,
    output_path: Path | None = None,
    min_history_events: int = DEFAULT_MIN_HISTORY_EVENTS,
    min_history_bases: int = DEFAULT_MIN_HISTORY_BASES,
    min_history_exchanges: int = DEFAULT_MIN_HISTORY_EXCHANGES,
    max_single_exchange_event_fraction: float = DEFAULT_MAX_SINGLE_EXCHANGE_EVENT_FRACTION,
) -> dict[str, Any]:
    rows = load_jsonl(history_jsonl_path)
    manifest = load_json(history_manifest_path)

    status_counts: Counter[str] = Counter()
    event_rows: dict[str, dict[str, Any]] = {}
    candles_by_event: dict[str, list[dict[str, Any]]] = {}
    rows_by_exchange: Counter[str] = Counter()
    ok_rows_by_exchange: Counter[str] = Counter()
    ok_events_by_exchange: Counter[str] = Counter()
    ok_bases: set[str] = set()
    ok_base_symbols: set[str] = set()
    ok_exchanges: set[str] = set()
    ok_event_ids: set[str] = set()

    for row in rows:
        status = str(row.get("data_status") or "").lower()
        status_counts[status] += 1
        exchange = normalize_exchange(row.get("exchange"))
        base = str(row.get("base") or "").strip().upper()
        quote = str(row.get("quote") or "USDT").strip().upper()
        event_id = str(row.get("event_id") or "")
        if exchange:
            rows_by_exchange[exchange] += 1
        if status != "ok" or not event_id:
            continue
        ok_event_ids.add(event_id)
        ok_exchanges.add(exchange)
        ok_bases.add(f"{exchange}:{base}:{quote}")
        ok_base_symbols.add(base)
        ok_rows_by_exchange[exchange] += 1
        event_rows.setdefault(
            event_id,
            {
                "event_id": event_id,
                "exchange": exchange,
                "symbol": str(row.get("symbol") or "").strip().upper(),
                "base": base,
                "quote": quote,
                "event_ts": row.get("event_ts"),
                "event_iso": row.get("event_iso"),
                "window_start_ts": row.get("window_start_ts"),
                "window_end_ts": row.get("window_end_ts"),
                "granularity": row.get("granularity"),
            },
        )
        candles_by_event.setdefault(event_id, []).append(row)

    for event_id, event in event_rows.items():
        ok_events_by_exchange[str(event["exchange"])] += 1

    normalized_events: list[dict[str, Any]] = []
    for event_id, event in sorted(event_rows.items(), key=lambda item: (str(item[1]["exchange"]), str(item[1]["event_iso"]), item[0])):
        candles = candles_by_event.get(event_id) or []
        candle_ts = sorted(float(row["candle_ts"]) for row in candles if row.get("candle_ts") not in (None, ""))
        event_ts = parse_ts(event.get("event_ts"))
        first_ts = candle_ts[0] if candle_ts else None
        last_ts = candle_ts[-1] if candle_ts else None
        pre_coverage_sec = max(0.0, event_ts - first_ts) if event_ts is not None and first_ts is not None else 0.0
        post_coverage_sec = max(0.0, last_ts - event_ts) if event_ts is not None and last_ts is not None else 0.0
        normalized_events.append(
            {
                **event,
                "ok_candle_rows": len(candles),
                "first_candle_ts": first_ts,
                "first_candle_iso": iso_from_ts(first_ts),
                "last_candle_ts": last_ts,
                "last_candle_iso": iso_from_ts(last_ts),
                "pre_coverage_sec": pre_coverage_sec,
                "post_coverage_sec": post_coverage_sec,
            }
        )

    ok_event_count = len(ok_event_ids)
    ok_exchange_count = len(ok_exchanges)
    max_single_fraction = (max(ok_events_by_exchange.values()) / ok_event_count) if ok_event_count else 1.0
    expected_rows = int(manifest.get("ohlcv_rows") or 0) + int(manifest.get("placeholder_rows") or 0)
    line_count_matches_manifest = len(rows) == expected_rows
    completed_all = int(manifest.get("completed_event_granularity_requests") or 0) == int(
        manifest.get("planned_event_granularity_requests") or 0
    )
    manifest_ok = bool(manifest.get("final")) and completed_all and int(manifest.get("errors") or 0) == 0
    enough_history = (
        manifest_ok
        and line_count_matches_manifest
        and ok_event_count >= min_history_events
        and len(ok_base_symbols) >= min_history_bases
        and ok_exchange_count >= min_history_exchanges
        and max_single_fraction <= max_single_exchange_event_fraction
    )
    decision = (
        "LISTING_EVENT_NORMALIZER_PLANONLY_READY_FOR_EVENT_REPLAY_PLANONLY"
        if enough_history
        else "LISTING_EVENT_HISTORY_NORMALIZER_BLOCKED_INSUFFICIENT_HISTORY_QUALITY"
    )
    result: dict[str, Any] = {
        "mode": "listing_event_normalizer_planonly",
        "source": "listing_event_history",
        "generated_at": utc_now_iso(),
        "decision": decision,
        "required_next_step": (
            "implement_read_only_listing_event_replay_planonly_no_grid_no_live"
            if enough_history
            else "revise_listing_event_history_collect_plan_or_resample"
        ),
        "research_only": True,
        "would_start": False,
        "live_orders": False,
        "api_keys": False,
        "leverage_or_margin": False,
        "collect_allowed_now": False,
        "grid_allowed_now": False,
        "paper_forward_allowed": False,
        "replay_allowed_now": enough_history,
        "history_data": {
            "jsonl_path": str(history_jsonl_path),
            "manifest_path": str(history_manifest_path),
            "run_id": manifest.get("run_id"),
            "manifest_final": bool(manifest.get("final")),
            "planned_event_granularity_requests": int(manifest.get("planned_event_granularity_requests") or 0),
            "completed_event_granularity_requests": int(manifest.get("completed_event_granularity_requests") or 0),
            "manifest_ohlcv_rows": int(manifest.get("ohlcv_rows") or 0),
            "manifest_placeholder_rows": int(manifest.get("placeholder_rows") or 0),
            "manifest_errors": int(manifest.get("errors") or 0),
            "line_count": len(rows),
            "expected_line_count_from_manifest": expected_rows,
            "line_count_matches_manifest": line_count_matches_manifest,
            "status_counts": dict(status_counts),
            "rows_by_exchange": dict(rows_by_exchange),
            "ok_rows_by_exchange": dict(ok_rows_by_exchange),
        },
        "history_coverage": {
            "min_history_events": min_history_events,
            "min_history_bases": min_history_bases,
            "min_history_exchanges": min_history_exchanges,
            "max_single_exchange_event_fraction": max_single_exchange_event_fraction,
            "ok_events": ok_event_count,
            "ok_unique_bases": len(ok_base_symbols),
            "ok_exchange_count": ok_exchange_count,
            "ok_events_by_exchange": dict(ok_events_by_exchange),
            "max_single_exchange_ok_event_fraction": max_single_fraction,
            "normalized_event_count": len(normalized_events),
            "sample_events": normalized_events[:25],
            "normalized_events": normalized_events,
        },
        "economics_policy": {
            "fee_assumption": "base/VIP0/no-volume only",
            "acceptance_requires": [
                "positive net expectancy after base fees/slippage/spread/event buffers",
                "chronological OOS by listing date",
                "walk-forward robustness",
                "stress with wider spreads, missed entry, and delist/freeze haircut",
            ],
        },
        "blocked_actions": [
            "live_orders",
            "api_keys",
            "leverage_or_margin",
            "grid_search",
            "paper_forward",
            "hidden_or_background_collect",
        ],
        "next_valid_moves": (
            [
                "Implement read-only listing event replay PlanOnly against the normalized history event set.",
                "Keep grid/live/API/paper-forward blocked until OOS/walk-forward/stress/economics gates exist.",
            ]
            if enough_history
            else [
                "Do not run replay/grid/live/API/paper-forward from insufficient history data.",
                "Resample events or improve history coverage while preserving no-data/delisted outcomes.",
            ]
        ),
        "output_path": str(output_path) if output_path else "",
    }
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only PlanOnly normalizer for listing-event drift/reversal research.")
    parser.add_argument("--calendar", default=str(DEFAULT_CALENDAR_PATH))
    parser.add_argument("--calendar-summary", default=str(DEFAULT_CALENDAR_SUMMARY_PATH))
    parser.add_argument("--market-manifest", default="")
    parser.add_argument("--history-jsonl", default="")
    parser.add_argument("--history-manifest", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--quote", default="USDT")
    parser.add_argument("--pre-window-sec", type=int, default=DEFAULT_PRE_WINDOW_SEC)
    parser.add_argument("--post-window-sec", type=int, default=DEFAULT_POST_WINDOW_SEC)
    parser.add_argument("--min-overlap-events", type=int, default=DEFAULT_MIN_OVERLAP_EVENTS)
    parser.add_argument("--min-overlap-bases", type=int, default=DEFAULT_MIN_OVERLAP_BASES)
    parser.add_argument("--min-overlap-exchanges", type=int, default=DEFAULT_MIN_OVERLAP_EXCHANGES)
    parser.add_argument("--min-history-events", type=int, default=DEFAULT_MIN_HISTORY_EVENTS)
    parser.add_argument("--min-history-bases", type=int, default=DEFAULT_MIN_HISTORY_BASES)
    parser.add_argument("--min-history-exchanges", type=int, default=DEFAULT_MIN_HISTORY_EXCHANGES)
    parser.add_argument("--max-single-exchange-event-fraction", type=float, default=DEFAULT_MAX_SINGLE_EXCHANGE_EVENT_FRACTION)
    args = parser.parse_args(argv)

    root = Path.cwd()
    output_path = Path(args.output) if args.output else default_output_path(root)
    if args.history_jsonl or args.history_manifest:
        if not args.history_jsonl or not args.history_manifest:
            raise SystemExit("--history-jsonl and --history-manifest must be provided together")
        result = normalize_listing_history_events_planonly(
            history_jsonl_path=Path(args.history_jsonl),
            history_manifest_path=Path(args.history_manifest),
            output_path=output_path,
            min_history_events=args.min_history_events,
            min_history_bases=args.min_history_bases,
            min_history_exchanges=args.min_history_exchanges,
            max_single_exchange_event_fraction=args.max_single_exchange_event_fraction,
        )
    else:
        market_manifest = Path(args.market_manifest) if args.market_manifest else latest_market_filter_manifest(root)
        if market_manifest is None:
            raise SystemExit("market filter manifest not found; pass --market-manifest")
        result = normalize_listing_events_planonly(
            calendar_path=Path(args.calendar),
            calendar_summary_path=Path(args.calendar_summary) if args.calendar_summary else None,
            market_manifest_path=market_manifest,
            output_path=output_path,
            quote=args.quote,
            pre_window_sec=args.pre_window_sec,
            post_window_sec=args.post_window_sec,
            min_overlap_events=args.min_overlap_events,
            min_overlap_bases=args.min_overlap_bases,
            min_overlap_exchanges=args.min_overlap_exchanges,
        )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
