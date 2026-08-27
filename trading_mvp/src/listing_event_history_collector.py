from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from listing_event_history_collect_plan import (
    DEFAULT_GRANULARITIES,
    INTERVAL_SECONDS,
    event_to_plan_row,
    load_history_events,
    select_history_events,
)


APPROVAL_TEXT = "подтверждаю visible listing-event OHLCV history collect"
DEFAULT_PREVIEW_PATH = Path("exports/trading-mvp/analysis/listing_event_history_collect_approval_packet_current.json")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_from_ts(ts: float | int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _force_utf8_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (ValueError, OSError):
                pass


@dataclass(frozen=True)
class Candle:
    ts: int
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    quote_volume: float | None
    trade_count: int | None = None


class PublicOhlcvClient:
    exchange = ""
    base_url = ""
    max_candles_per_request = 1000

    def __init__(self, timeout_sec: int = 15, max_retries: int = 2) -> None:
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.trust_env = False

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_sec)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(0.5 * (attempt + 1))
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"GET failed: {url}")

    def fetch_ohlcv(self, symbol: str, granularity: str, start_ts: int, end_ts: int, limit: int) -> list[Candle]:
        raise NotImplementedError


def parse_mexc_spot_klines(payload: Any) -> list[Candle]:
    if not isinstance(payload, list):
        return []
    rows: list[Candle] = []
    for item in payload:
        if not isinstance(item, list) or len(item) < 6:
            continue
        open_ms = _as_int(item[0])
        if open_ms is None:
            continue
        rows.append(
            Candle(
                ts=int(open_ms / 1000),
                open=_as_float(item[1]),
                high=_as_float(item[2]),
                low=_as_float(item[3]),
                close=_as_float(item[4]),
                volume=_as_float(item[5]),
                quote_volume=_as_float(item[7]) if len(item) > 7 else None,
                trade_count=_as_int(item[8]) if len(item) > 8 else None,
            )
        )
    return sorted(rows, key=lambda candle: candle.ts)


def parse_gate_spot_candles(payload: Any) -> list[Candle]:
    if not isinstance(payload, list):
        return []
    rows: list[Candle] = []
    for item in payload:
        if not isinstance(item, list) or len(item) < 6:
            continue
        ts = _as_int(item[0])
        if ts is None:
            continue
        # Gate spot candles are [timestamp, quote_volume, close, high, low, open, base_volume, complete].
        rows.append(
            Candle(
                ts=ts,
                open=_as_float(item[5]),
                high=_as_float(item[3]),
                low=_as_float(item[4]),
                close=_as_float(item[2]),
                volume=_as_float(item[6]) if len(item) > 6 else None,
                quote_volume=_as_float(item[1]),
                trade_count=None,
            )
        )
    return sorted(rows, key=lambda candle: candle.ts)


def parse_bitget_spot_candles(payload: Any) -> list[Candle]:
    rows_payload = payload.get("data", []) if isinstance(payload, dict) else payload
    if not isinstance(rows_payload, list):
        return []
    rows: list[Candle] = []
    for item in rows_payload:
        if not isinstance(item, list) or len(item) < 7:
            continue
        open_ms = _as_int(item[0])
        if open_ms is None:
            continue
        # Bitget spot candles are [timestamp_ms, open, high, low, close, base_volume, quote_volume, ...].
        rows.append(
            Candle(
                ts=int(open_ms / 1000),
                open=_as_float(item[1]),
                high=_as_float(item[2]),
                low=_as_float(item[3]),
                close=_as_float(item[4]),
                volume=_as_float(item[5]),
                quote_volume=_as_float(item[6]),
                trade_count=None,
            )
        )
    return sorted(rows, key=lambda candle: candle.ts)


class MexcSpotOhlcvClient(PublicOhlcvClient):
    exchange = "mexc"
    base_url = "https://api.mexc.com"
    max_candles_per_request = 500

    def fetch_ohlcv(self, symbol: str, granularity: str, start_ts: int, end_ts: int, limit: int) -> list[Candle]:
        interval = "60m" if granularity == "1h" else granularity
        payload = self._get(
            "/api/v3/klines",
            {
                "symbol": symbol.replace("_", "").upper(),
                "interval": interval,
                "startTime": int(start_ts * 1000),
                "endTime": int(end_ts * 1000),
                "limit": limit,
            },
        )
        return parse_mexc_spot_klines(payload)


class GateSpotOhlcvClient(PublicOhlcvClient):
    exchange = "gateio"
    base_url = "https://api.gateio.ws/api/v4"

    def fetch_ohlcv(self, symbol: str, granularity: str, start_ts: int, end_ts: int, limit: int) -> list[Candle]:
        payload = self._get(
            "/spot/candlesticks",
            {
                "currency_pair": symbol.upper(),
                "interval": granularity,
                "from": int(start_ts),
                "to": int(end_ts),
                "limit": limit,
            },
        )
        return parse_gate_spot_candles(payload)


class BitgetSpotOhlcvClient(PublicOhlcvClient):
    exchange = "bitget"
    base_url = "https://api.bitget.com"
    max_candles_per_request = 500

    GRANULARITY_MAP = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "1h": "1h",
        "4h": "4h",
    }

    def fetch_ohlcv(self, symbol: str, granularity: str, start_ts: int, end_ts: int, limit: int) -> list[Candle]:
        api_granularity = self.GRANULARITY_MAP.get(granularity)
        if api_granularity is None:
            raise ValueError(f"unsupported Bitget granularity: {granularity}")
        payload = self._get(
            "/api/v2/spot/market/candles",
            {
                "symbol": symbol.replace("_", "").upper(),
                "granularity": api_granularity,
                "startTime": int(start_ts * 1000),
                "endTime": int(end_ts * 1000),
                "limit": limit,
            },
        )
        return parse_bitget_spot_candles(payload)


CLIENTS = {
    "mexc": MexcSpotOhlcvClient,
    "gateio": GateSpotOhlcvClient,
    "bitget": BitgetSpotOhlcvClient,
}


def resolve_path(path: str | Path, *, repo_root: Path) -> Path:
    value = Path(path)
    if value.is_absolute():
        return value
    return repo_root / value


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_preview_contract(preview_or_packet_path: Path) -> dict[str, Any]:
    payload = read_json(preview_or_packet_path)
    if payload.get("mode") == "trading_listing_event_history_collect_approval_packet":
        preview_path = payload.get("preview", {}).get("path")
        if not preview_path:
            raise ValueError("approval packet does not contain preview.path")
        return read_json(Path(preview_path))
    return payload


def build_event_plan(preview: dict[str, Any], *, repo_root: Path, max_events: int = 0) -> list[dict[str, Any]]:
    calendar_path = resolve_path(preview["calendar_path"], repo_root=repo_root)
    selection = preview.get("selection") or {}
    budget = preview.get("request_budget") or {}
    if str(selection.get("event_plan_source") or "") == "explicit_sample_events":
        event_plan = [dict(row) for row in (selection.get("sample_events") or []) if isinstance(row, dict)]
        if max_events > 0:
            event_plan = event_plan[:max_events]
        return event_plan
    events = load_history_events(calendar_path, quote="USDT")
    selected = select_history_events(
        events,
        target_events=int(selection.get("target_events") or selection.get("selected_events") or 120),
        max_events_per_base=int(selection.get("max_events_per_base") or 2),
        max_exchange_fraction=float(selection.get("max_exchange_fraction") or 0.60),
    )
    if max_events > 0:
        selected = selected[:max_events]
    pre_window_sec = int(budget.get("pre_window_sec") or 3600)
    post_window_sec = int(budget.get("post_window_sec") or 259200)
    return [
        event_to_plan_row(event, pre_window_sec=pre_window_sec, post_window_sec=post_window_sec)
        for event in selected
    ]


def output_row(
    event: dict[str, Any],
    *,
    granularity: str,
    candle: Candle | None,
    data_status: str,
    error: str = "",
) -> dict[str, Any]:
    candle_ts = candle.ts if candle else None
    return {
        "exchange": event["exchange"],
        "symbol": event["symbol"],
        "base": event["base"],
        "quote": event["quote"],
        "event_id": event["event_id"],
        "event_ts": event["event_ts"],
        "event_iso": event["event_iso"],
        "window_start_ts": event["window_start_ts"],
        "window_end_ts": event["window_end_ts"],
        "granularity": granularity,
        "candle_ts": candle_ts,
        "candle_iso": iso_from_ts(candle_ts),
        "open": candle.open if candle else None,
        "high": candle.high if candle else None,
        "low": candle.low if candle else None,
        "close": candle.close if candle else None,
        "volume": candle.volume if candle else None,
        "quote_volume": candle.quote_volume if candle else None,
        "trade_count_if_available": candle.trade_count if candle else None,
        "data_status": data_status,
        "error": error,
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    for attempt in range(8):
        try:
            tmp.replace(path)
            return
        except PermissionError:
            if attempt == 7:
                raise
            time.sleep(0.05 * (attempt + 1))


def build_initial_manifest(
    *,
    run_id: str,
    preview_path: Path,
    output_jsonl: Path,
    manifest_path: Path,
    event_plan_path: Path,
    event_plan: list[dict[str, Any]],
    granularities: list[str],
    confirmed_approval_text: str,
) -> dict[str, Any]:
    return {
        "mode": "listing_event_history_collect",
        "run_id": run_id,
        "started_at": utc_now_iso(),
        "finished_at": None,
        "final": False,
        "decision": "LISTING_EVENT_HISTORY_COLLECT_RUNNING",
        "research_only": True,
        "public_data_only": True,
        "live_orders": False,
        "api_keys": False,
        "leverage_or_margin": False,
        "replay_allowed": False,
        "grid_allowed": False,
        "paper_forward_allowed": False,
        "approval_text_sha256": hashlib.sha256(confirmed_approval_text.encode("utf-8")).hexdigest(),
        "preview_path": str(preview_path),
        "output_jsonl": str(output_jsonl),
        "manifest_path": str(manifest_path),
        "event_plan_path": str(event_plan_path),
        "selected_events": len(event_plan),
        "granularities": granularities,
        "planned_event_granularity_requests": len(event_plan) * len(granularities),
        "completed_event_granularity_requests": 0,
        "http_requests": 0,
        "ohlcv_rows": 0,
        "placeholder_rows": 0,
        "errors": 0,
        "error_samples": [],
        "data_status_counts": {},
        "by_exchange": {},
        "by_granularity": {},
        "next_step_after_ready": "Run listing-event history data-quality; keep replay/grid/live/API/paper-forward blocked until quality gate sets replay_allowed=true.",
    }


def increment(mapping: dict[str, Any], key: str, amount: int = 1) -> None:
    mapping[key] = int(mapping.get(key) or 0) + amount


def fetch_window(
    *,
    client: PublicOhlcvClient,
    event: dict[str, Any],
    granularity: str,
    candles_per_request: int,
    sleep_sec: float,
) -> tuple[list[Candle], int]:
    interval_sec = INTERVAL_SECONDS[granularity]
    client_limit = int(getattr(client, "max_candles_per_request", candles_per_request))
    request_limit = min(candles_per_request, max(1, client_limit))
    start_ts = int(float(event["window_start_ts"]))
    end_ts = int(float(event["window_end_ts"]))
    cursor = start_ts
    requests_made = 0
    deduped: dict[int, Candle] = {}
    while cursor <= end_ts:
        chunk_end = min(end_ts, cursor + interval_sec * max(1, request_limit - 1))
        candles = client.fetch_ohlcv(event["symbol"], granularity, cursor, chunk_end, request_limit)
        requests_made += 1
        for candle in candles:
            if start_ts <= candle.ts <= end_ts:
                deduped[candle.ts] = candle
        cursor = chunk_end + interval_sec
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    return [deduped[key] for key in sorted(deduped)], requests_made


def collect_history(
    *,
    preview_path: Path,
    output_jsonl: Path,
    manifest_path: Path,
    event_plan_path: Path,
    confirmed_approval_text: str,
    candles_per_request: int = 1000,
    sleep_sec: float = 0.5,
    timeout_sec: int = 15,
    max_retries: int = 2,
    max_events: int = 0,
    progress_every: int = 10,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    if confirmed_approval_text != APPROVAL_TEXT:
        raise ValueError("exact listing-event history collect approval text is required")
    repo_root = repo_root or Path.cwd()
    preview = load_preview_contract(preview_path)
    run_id = str(preview.get("run_id") or output_jsonl.parent.name)
    budget = preview.get("request_budget") or {}
    granularities = [str(item) for item in (budget.get("granularities") or list(DEFAULT_GRANULARITIES))]
    unknown = [granularity for granularity in granularities if granularity not in INTERVAL_SECONDS]
    if unknown:
        raise ValueError(f"unsupported granularities: {', '.join(unknown)}")

    event_plan = build_event_plan(preview, repo_root=repo_root, max_events=max_events)
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    event_plan_path.parent.mkdir(parents=True, exist_ok=True)
    event_plan_path.write_text(json.dumps({"run_id": run_id, "events": event_plan}, indent=2, ensure_ascii=False), encoding="utf-8")

    manifest = build_initial_manifest(
        run_id=run_id,
        preview_path=preview_path,
        output_jsonl=output_jsonl,
        manifest_path=manifest_path,
        event_plan_path=event_plan_path,
        event_plan=event_plan,
        granularities=granularities,
        confirmed_approval_text=confirmed_approval_text,
    )
    write_manifest(manifest_path, manifest)

    clients = {
        exchange: client_cls(timeout_sec=timeout_sec, max_retries=max_retries)
        for exchange, client_cls in CLIENTS.items()
    }
    completed = 0
    with output_jsonl.open("w", encoding="utf-8", newline="\n") as out:
        for event in event_plan:
            exchange = str(event["exchange"])
            client = clients.get(exchange)
            if client is None:
                manifest["errors"] += len(granularities)
                manifest["error_samples"].append(f"unsupported exchange: {exchange}")
                continue
            for granularity in granularities:
                completed += 1
                exchange_stats = manifest["by_exchange"].setdefault(exchange, {"requests": 0, "rows": 0, "placeholders": 0, "errors": 0})
                granularity_stats = manifest["by_granularity"].setdefault(granularity, {"requests": 0, "rows": 0, "placeholders": 0, "errors": 0})
                try:
                    candles, requests_made = fetch_window(
                        client=client,
                        event=event,
                        granularity=granularity,
                        candles_per_request=candles_per_request,
                        sleep_sec=sleep_sec,
                    )
                    manifest["http_requests"] += requests_made
                    exchange_stats["requests"] += requests_made
                    granularity_stats["requests"] += requests_made
                    if candles:
                        for candle in candles:
                            out.write(json.dumps(output_row(event, granularity=granularity, candle=candle, data_status="ok"), ensure_ascii=False) + "\n")
                        out.flush()
                        manifest["ohlcv_rows"] += len(candles)
                        exchange_stats["rows"] += len(candles)
                        granularity_stats["rows"] += len(candles)
                        increment(manifest["data_status_counts"], "ok", len(candles))
                    else:
                        out.write(json.dumps(output_row(event, granularity=granularity, candle=None, data_status="no_data_or_delisted"), ensure_ascii=False) + "\n")
                        out.flush()
                        manifest["placeholder_rows"] += 1
                        exchange_stats["placeholders"] += 1
                        granularity_stats["placeholders"] += 1
                        increment(manifest["data_status_counts"], "no_data_or_delisted")
                except Exception as exc:  # noqa: BLE001 - collector must preserve failed event evidence.
                    message = f"{exchange}:{event['symbol']}:{granularity} {type(exc).__name__}: {exc}"
                    manifest["errors"] += 1
                    exchange_stats["errors"] += 1
                    granularity_stats["errors"] += 1
                    if len(manifest["error_samples"]) < 200:
                        manifest["error_samples"].append(message)
                    out.write(json.dumps(output_row(event, granularity=granularity, candle=None, data_status="api_error", error=message), ensure_ascii=False) + "\n")
                    out.flush()
                    manifest["placeholder_rows"] += 1
                    increment(manifest["data_status_counts"], "api_error")
                manifest["completed_event_granularity_requests"] = completed
                if progress_every > 0 and (completed % progress_every == 0 or completed == len(event_plan) * len(granularities)):
                    print(
                        f"[{utc_now_iso()}] listing-history progress "
                        f"{completed}/{len(event_plan) * len(granularities)} "
                        f"rows={manifest['ohlcv_rows']} placeholders={manifest['placeholder_rows']} "
                        f"errors={manifest['errors']} http={manifest['http_requests']}",
                        flush=True,
                    )
                write_manifest(manifest_path, manifest)

    manifest["finished_at"] = utc_now_iso()
    manifest["final"] = True
    manifest["decision"] = "LISTING_EVENT_HISTORY_COLLECT_COMPLETED_READY_FOR_DATA_QUALITY"
    write_manifest(manifest_path, manifest)
    return manifest


def main(argv: list[str] | None = None) -> int:
    _force_utf8_stdio()
    parser = argparse.ArgumentParser(description="Visible public OHLCV collector for listing-event history research.")
    parser.add_argument("--preview", default=str(DEFAULT_PREVIEW_PATH))
    parser.add_argument("--output-jsonl", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--event-plan", required=True)
    parser.add_argument("--confirmed-approval-text", required=True)
    parser.add_argument("--candles-per-request", type=int, default=1000)
    parser.add_argument("--sleep-sec", type=float, default=0.5)
    parser.add_argument("--timeout-sec", type=int, default=15)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=10)
    args = parser.parse_args(argv)
    manifest = collect_history(
        preview_path=Path(args.preview),
        output_jsonl=Path(args.output_jsonl),
        manifest_path=Path(args.manifest),
        event_plan_path=Path(args.event_plan),
        confirmed_approval_text=args.confirmed_approval_text,
        candles_per_request=args.candles_per_request,
        sleep_sec=args.sleep_sec,
        timeout_sec=args.timeout_sec,
        max_retries=args.max_retries,
        max_events=args.max_events,
        progress_every=args.progress_every,
        repo_root=Path.cwd(),
    )
    print(json.dumps(manifest, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
