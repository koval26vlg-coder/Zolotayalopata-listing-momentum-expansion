from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

import requests

from listing_spot_asset_class import classify_spot_asset


SCHEMA = "trading_mvp_listing_momentum_exchange_expansion_compatibility_preflight_v1"
PREFLIGHT_ID = "listing_momentum_exchange_expansion_preflight_20260817"
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PREFLIGHT_PATH = (
    REPO_ROOT
    / "exports/trading-mvp/analysis"
    / "listing_momentum_exchange_expansion_preflight_20260817.json"
)
QUOTE = "USDT"
TIMEOUT_SEC = 15
MAX_REQUESTS = 8
MAX_RUNTIME_SEC = 180
SUPPORTED_VENUES = ("binance", "bybit", "okx", "bitget")


class ExpansionPreflightError(ValueError):
    pass


@dataclass(frozen=True)
class VenueConfig:
    name: str
    snapshot_url: str
    ohlcv_url: str
    timestamp_method: str
    timestamp_quality: str
    symbol_format: str
    snapshot_source_type: str
    parse_snapshot: Callable[[Any], list[dict[str, Any]]]


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_hash(payload: Mapping[str, Any]) -> str:
    normalized = copy.deepcopy(dict(payload))
    normalized.pop("receipt_hash", None)
    return hashlib.sha256(_canonical_json_bytes(normalized)).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _epoch_seconds(value: Any, *, milliseconds: bool = False) -> int | None:
    if value in (None, "", 0, "0"):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric <= 0:
        return None
    if milliseconds:
        numeric /= 1000.0
    return int(numeric)


def _iso_from_ts(value: int | None) -> str:
    if value is None:
        return ""
    return datetime.fromtimestamp(value, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _status_is_active(venue: str, status: str) -> bool:
    normalized = status.strip().lower()
    if venue == "binance":
        return normalized in {"trading", "enabled", "1"}
    if venue == "bybit":
        return normalized in {"trading", "online", "enabled"}
    if venue == "okx":
        return normalized in {"live", "trading", "online"}
    return normalized in {"online", "trading", "enable", "enabled", "tradable"}


def _row(
    *,
    exchange: str,
    base: str,
    symbol: str,
    status: str,
    listed_ts: int | None,
    listing_timestamp_source: str,
    timestamp_quality: str,
    source_url: str,
    source_type: str,
    is_delisted: bool,
) -> dict[str, Any]:
    classification = classify_spot_asset(exchange, base)
    return {
        "exchange": exchange,
        "base": base.upper(),
        "quote": QUOTE,
        "symbol": symbol.upper(),
        "status": status,
        "is_delisted": bool(is_delisted),
        "listed_ts": listed_ts,
        "listed_at_utc": _iso_from_ts(listed_ts),
        "listing_timestamp_source": listing_timestamp_source,
        "timestamp_quality": timestamp_quality,
        "source_url": source_url,
        "source_type": source_type,
        "asset_class": classification.asset_class,
        "asset_class_source": classification.source,
        "asset_class_acceptance_eligible": classification.acceptance_eligible,
    }


def _spot_permission(item: Mapping[str, Any]) -> bool:
    if bool(item.get("isSpotTradingAllowed")):
        return True
    values: list[Any] = [item.get("permissions"), item.get("permissionSets")]
    flattened: list[str] = []
    for value in values:
        if isinstance(value, list):
            stack = list(value)
            while stack:
                current = stack.pop()
                if isinstance(current, list):
                    stack.extend(current)
                elif current is not None:
                    flattened.append(str(current).upper())
    return "SPOT" in flattened


def parse_binance_snapshot(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for item in payload.get("symbols") or []:
        if not isinstance(item, Mapping):
            continue
        quote = str(item.get("quoteAsset") or "").upper()
        base = str(item.get("baseAsset") or "").upper()
        symbol = str(item.get("symbol") or "").upper()
        if quote != QUOTE or not base or not symbol:
            continue
        status = str(item.get("status") or "")
        active = _status_is_active("binance", status) and _spot_permission(item)
        listed_ts = _epoch_seconds(item.get("onboardDate"), milliseconds=True)
        rows.append(
            _row(
                exchange="binance",
                base=base,
                symbol=symbol,
                status=status,
                listed_ts=listed_ts,
                listing_timestamp_source=(
                    "onboardDate_ms" if listed_ts is not None else ""
                ),
                timestamp_quality=(
                    "snapshot_timestamp" if listed_ts is not None else "proxy_required"
                ),
                source_url=VENUE_CONFIGS["binance"].snapshot_url,
                source_type=VENUE_CONFIGS["binance"].snapshot_source_type,
                is_delisted=not active,
            )
        )
    return sorted(rows, key=lambda row: (row["symbol"], row["base"]))


def parse_bybit_snapshot(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    result = payload.get("result") or {}
    items = result.get("list") if isinstance(result, Mapping) else []
    rows: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, Mapping):
            continue
        quote = str(item.get("quoteCoin") or "").upper()
        base = str(item.get("baseCoin") or "").upper()
        symbol = str(item.get("symbol") or "").upper()
        if quote != QUOTE or not base or not symbol:
            continue
        status = str(item.get("status") or "")
        listed_ts = _epoch_seconds(item.get("launchTime"), milliseconds=True)
        rows.append(
            _row(
                exchange="bybit",
                base=base,
                symbol=symbol,
                status=status,
                listed_ts=listed_ts,
                listing_timestamp_source=(
                    "launchTime_ms" if listed_ts is not None else ""
                ),
                timestamp_quality=(
                    "snapshot_timestamp" if listed_ts is not None else "proxy_required"
                ),
                source_url=VENUE_CONFIGS["bybit"].snapshot_url,
                source_type=VENUE_CONFIGS["bybit"].snapshot_source_type,
                is_delisted=not _status_is_active("bybit", status),
            )
        )
    return sorted(rows, key=lambda row: (row["symbol"], row["base"]))


def parse_okx_snapshot(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for item in payload.get("data") or []:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("instType") or "").upper() != "SPOT":
            continue
        quote = str(item.get("quoteCcy") or "").upper()
        base = str(item.get("baseCcy") or "").upper()
        symbol = str(item.get("instId") or "").upper()
        if quote != QUOTE or not base or not symbol:
            continue
        status = str(item.get("state") or "")
        listed_ts = _epoch_seconds(item.get("listTime"), milliseconds=True)
        rows.append(
            _row(
                exchange="okx",
                base=base,
                symbol=symbol,
                status=status,
                listed_ts=listed_ts,
                listing_timestamp_source=(
                    "listTime_ms" if listed_ts is not None else ""
                ),
                timestamp_quality=(
                    "snapshot_timestamp" if listed_ts is not None else "proxy_required"
                ),
                source_url=VENUE_CONFIGS["okx"].snapshot_url,
                source_type=VENUE_CONFIGS["okx"].snapshot_source_type,
                is_delisted=not _status_is_active("okx", status),
            )
        )
    return sorted(rows, key=lambda row: (row["symbol"], row["base"]))


def parse_bitget_snapshot(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for item in payload.get("data") or []:
        if not isinstance(item, Mapping):
            continue
        quote = str(item.get("quoteCoin") or "").upper()
        base = str(item.get("baseCoin") or "").upper()
        symbol = str(item.get("symbol") or "").upper()
        if quote != QUOTE or not base or not symbol:
            continue
        status = str(item.get("status") or "")
        listed_ts = _epoch_seconds(item.get("openTime"), milliseconds=True)
        off_ts = _epoch_seconds(item.get("offTime"), milliseconds=True)
        rows.append(
            _row(
                exchange="bitget",
                base=base,
                symbol=symbol,
                status=status,
                listed_ts=listed_ts,
                listing_timestamp_source=(
                    "openTime_ms_deprecated" if listed_ts is not None else ""
                ),
                timestamp_quality=(
                    "deprecated_snapshot_timestamp"
                    if listed_ts is not None
                    else "proxy_required"
                ),
                source_url=VENUE_CONFIGS["bitget"].snapshot_url,
                source_type=VENUE_CONFIGS["bitget"].snapshot_source_type,
                is_delisted=bool(off_ts) or not _status_is_active("bitget", status),
            )
        )
    return sorted(rows, key=lambda row: (row["symbol"], row["base"]))


VENUE_CONFIGS: dict[str, VenueConfig] = {
    "binance": VenueConfig(
        name="binance",
        snapshot_url="https://api.binance.com/api/v3/exchangeInfo",
        ohlcv_url="https://api.binance.com/api/v3/klines",
        timestamp_method="first_available_1h_kline_proxy_when_snapshot_has_no_onboardDate",
        timestamp_quality="proxy_required_for_current_spot_schema",
        symbol_format="BASEUSDT",
        snapshot_source_type="public_api_exchange_info_current_snapshot",
        parse_snapshot=parse_binance_snapshot,
    ),
    "bybit": VenueConfig(
        name="bybit",
        snapshot_url="https://api.bybit.com/v5/market/instruments-info?category=spot",
        ohlcv_url="https://api.bybit.com/v5/market/kline",
        timestamp_method="first_available_1h_kline_proxy_when_spot_launchTime_missing",
        timestamp_quality="proxy_required_for_current_spot_schema",
        symbol_format="BASEUSDT",
        snapshot_source_type="public_api_instruments_info_spot_current_snapshot",
        parse_snapshot=parse_bybit_snapshot,
    ),
    "okx": VenueConfig(
        name="okx",
        snapshot_url="https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        ohlcv_url="https://www.okx.com/api/v5/market/candles",
        timestamp_method="listTime_ms",
        timestamp_quality="snapshot_timestamp",
        symbol_format="BASE-USDT",
        snapshot_source_type="public_api_instruments_spot_current_snapshot",
        parse_snapshot=parse_okx_snapshot,
    ),
    "bitget": VenueConfig(
        name="bitget",
        snapshot_url="https://api.bitget.com/api/v2/spot/public/symbols",
        ohlcv_url="https://api.bitget.com/api/v2/spot/market/candles",
        timestamp_method="openTime_ms_deprecated_with_first_1h_kline_fallback",
        timestamp_quality="deprecated_snapshot_timestamp_or_proxy",
        symbol_format="BASEUSDT",
        snapshot_source_type="public_api_spot_symbols_current_snapshot",
        parse_snapshot=parse_bitget_snapshot,
    ),
}


def _ohlcv_params(venue: str, symbol: str) -> dict[str, Any]:
    probe_end_ts = int(time.time())
    return _window_params(
        venue,
        symbol,
        "1h",
        probe_end_ts - 2 * 3600,
        probe_end_ts,
        2,
    )


def _parse_ohlcv_rows(venue: str, payload: Any) -> list[dict[str, Any]]:
    if venue == "binance":
        raw_rows = payload if isinstance(payload, list) else []
    elif venue == "bybit":
        result = payload.get("result") if isinstance(payload, Mapping) else {}
        raw_rows = result.get("list") if isinstance(result, Mapping) else []
    else:
        raw_rows = payload.get("data") if isinstance(payload, Mapping) else []
    parsed: list[dict[str, Any]] = []
    for item in raw_rows or []:
        if not isinstance(item, list) or len(item) < 5:
            continue
        # All four public spot candle endpoints currently expose epoch
        # milliseconds, including OKX's ``market/candles`` response.
        ts = _epoch_seconds(item[0], milliseconds=True)
        if ts is None:
            continue
        values: list[float | None] = []
        for value in item[1:5]:
            try:
                number = float(value)
            except (TypeError, ValueError):
                number = None
            values.append(number if number is not None and math.isfinite(number) else None)
        if any(value is None for value in values):
            continue
        parsed.append(
            {
                "ts": ts,
                "open": values[0],
                "high": values[1],
                "low": values[2],
                "close": values[3],
                "volume": item[5] if len(item) > 5 else None,
                "quote_volume": item[6] if len(item) > 6 else None,
            }
        )
    return sorted(parsed, key=lambda row: row["ts"])


def _request_json(
    session: requests.Session,
    url: str,
    *,
    params: Mapping[str, Any] | None,
    timeout_sec: int,
) -> tuple[dict[str, Any], Any]:
    response = session.get(url, params=dict(params or {}), timeout=timeout_sec)
    body = response.content
    metadata = {
        "url": response.url,
        "status_code": response.status_code,
        "response_bytes": len(body),
        "response_sha256": _sha256_bytes(body),
    }
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise ExpansionPreflightError(f"non-json response from {response.url}") from exc
    return metadata, payload


@dataclass(frozen=True)
class Candle:
    ts: int
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    quote_volume: float | None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _window_params(
    venue: str,
    symbol: str,
    granularity: str,
    start_ts: int,
    end_ts: int,
    limit: int,
) -> dict[str, Any]:
    interval = {"15m": "15m", "1h": "1h", "4h": "4h"}.get(granularity)
    if interval is None:
        raise ValueError(f"unsupported granularity: {granularity}")
    if venue == "binance":
        return {
            "symbol": symbol,
            "interval": interval,
            "startTime": int(start_ts * 1000),
            "endTime": int(end_ts * 1000),
            "limit": limit,
        }
    if venue == "bybit":
        return {
            "category": "spot",
            "symbol": symbol,
            "interval": {"15m": "15", "1h": "60", "4h": "240"}[granularity],
            "start": int(start_ts * 1000),
            "end": int(end_ts * 1000),
            "limit": limit,
        }
    if venue == "okx":
        return {
            "instId": symbol,
            "bar": {"15m": "15m", "1h": "1H", "4h": "4H"}[granularity],
            "after": int(end_ts * 1000),
            "before": int(start_ts * 1000),
            "limit": min(limit, 300),
        }
    return {
        "symbol": symbol,
        "granularity": {"15m": "15min", "1h": "1h", "4h": "4h"}[granularity],
        "startTime": int(start_ts * 1000),
        "endTime": int(end_ts * 1000),
        "limit": min(limit, 1000),
    }


class ExpansionSpotOhlcvClient:
    """Public read-only spot OHLCV client for the expansion venues."""

    MAX_CANDLES_PER_REQUEST = {
        "binance": 1000,
        "bybit": 1000,
        "okx": 300,
        "bitget": 1000,
    }

    def __init__(self, exchange: str, *, timeout_sec: int = TIMEOUT_SEC) -> None:
        if exchange not in VENUE_CONFIGS:
            raise ValueError(f"unsupported expansion exchange: {exchange}")
        self.exchange = exchange
        self.base_url = VENUE_CONFIGS[exchange].ohlcv_url
        self.max_candles_per_request = self.MAX_CANDLES_PER_REQUEST[exchange]
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.session.trust_env = False

    def fetch_ohlcv(
        self,
        symbol: str,
        granularity: str,
        start_ts: int,
        end_ts: int,
        limit: int,
    ) -> list[Candle]:
        metadata, payload = _request_json(
            self.session,
            self.base_url,
            params=_window_params(
                self.exchange,
                symbol.upper(),
                granularity,
                int(start_ts),
                int(end_ts),
                min(int(limit), self.max_candles_per_request),
            ),
            timeout_sec=self.timeout_sec,
        )
        del metadata
        return [
            Candle(
                ts=int(row["ts"]),
                open=_as_float(row.get("open")),
                high=_as_float(row.get("high")),
                low=_as_float(row.get("low")),
                close=_as_float(row.get("close")),
                volume=_as_float(row.get("volume")),
                quote_volume=_as_float(row.get("quote_volume")),
            )
            for row in _parse_ohlcv_rows(self.exchange, payload)
        ]


def fetch_current_snapshot_rows(
    *, timeout_sec: int = TIMEOUT_SEC
) -> tuple[list[dict[str, Any]], int]:
    """Fetch one current public snapshot per expansion venue."""
    session = requests.Session()
    session.trust_env = False
    rows: list[dict[str, Any]] = []
    requests_made = 0
    for venue in SUPPORTED_VENUES:
        config = VENUE_CONFIGS[venue]
        _, payload = _request_json(
            session, config.snapshot_url, params=None, timeout_sec=timeout_sec
        )
        requests_made += 1
        rows.extend(config.parse_snapshot(payload))
    return rows, requests_made


def resolve_proxy_timestamp(row: Mapping[str, Any], *, now_ts: int) -> tuple[int, str]:
    listed_ts = _epoch_seconds(row.get("listed_ts"))
    if listed_ts is not None:
        return listed_ts, str(row.get("listing_timestamp_source") or "snapshot_timestamp")
    # Binance and Bybit's current spot snapshots do not expose a trustworthy
    # listing timestamp.  A detection-time proxy keeps the forward sample
    # explicit and prevents inventing an official announcement date.
    return (int(now_ts) // 3600) * 3600, "snapshot_diff_detection_time_proxy"


def choose_sample_symbol(rows: list[dict[str, Any]]) -> str | None:
    active = [row for row in rows if not row.get("is_delisted")]
    if not active:
        return None
    active.sort(key=lambda row: (row.get("base") != "BTC", row.get("symbol", "")))
    return str(active[0]["symbol"])


def _venue_result(
    config: VenueConfig,
    *,
    snapshot_meta: dict[str, Any],
    rows: list[dict[str, Any]],
    sample_symbol: str | None,
    ohlcv_meta: dict[str, Any] | None,
    candles: list[dict[str, Any]],
    error: str = "",
) -> dict[str, Any]:
    snapshot_ok = snapshot_meta.get("status_code") == 200 and bool(rows)
    ohlcv_ok = bool(ohlcv_meta and ohlcv_meta.get("status_code") == 200 and candles)
    status = "PASS" if snapshot_ok and sample_symbol and ohlcv_ok else "FAIL"
    return {
        "exchange": config.name,
        "status": status,
        "snapshot": {
            **snapshot_meta,
            "quote": QUOTE,
            "rows": len(rows),
            "active_rows": sum(1 for row in rows if not row["is_delisted"]),
            "timestamp_rows": sum(1 for row in rows if row.get("listed_ts") is not None),
            "timestamp_coverage": (
                sum(1 for row in rows if row.get("listed_ts") is not None) / len(rows)
                if rows
                else 0.0
            ),
            "sample_symbol": sample_symbol,
            "symbol_format": config.symbol_format,
        },
        "timestamp_contract": {
            "method": config.timestamp_method,
            "quality": config.timestamp_quality,
            "official_listing_timestamp": config.name == "okx",
            "proxy_allowed_for_accrual": True,
        },
        "ohlcv": {
            **(ohlcv_meta or {"status_code": None}),
            "interval": "1h",
            "sample_symbol": sample_symbol,
            "parsed_candles": len(candles),
            "schema_valid": bool(candles),
        },
        "snapshot_rows": rows,
        "error": error,
    }


def run_preflight(
    *,
    output_path: Path = DEFAULT_PREFLIGHT_PATH,
    timeout_sec: int = TIMEOUT_SEC,
) -> dict[str, Any]:
    started = time.monotonic()
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    session = requests.Session()
    session.trust_env = False
    results: list[dict[str, Any]] = []
    request_count = 0
    for venue in SUPPORTED_VENUES:
        config = VENUE_CONFIGS[venue]
        rows: list[dict[str, Any]] = []
        sample_symbol: str | None = None
        candles: list[dict[str, Any]] = []
        snapshot_meta: dict[str, Any] = {"status_code": None, "url": config.snapshot_url}
        ohlcv_meta: dict[str, Any] | None = None
        error = ""
        try:
            snapshot_meta, snapshot_payload = _request_json(
                session, config.snapshot_url, params=None, timeout_sec=timeout_sec
            )
            request_count += 1
            rows = config.parse_snapshot(snapshot_payload)
            sample_symbol = choose_sample_symbol(rows)
            if sample_symbol:
                ohlcv_meta, ohlcv_payload = _request_json(
                    session,
                    config.ohlcv_url,
                    params=_ohlcv_params(venue, sample_symbol),
                    timeout_sec=timeout_sec,
                )
                request_count += 1
                candles = _parse_ohlcv_rows(venue, ohlcv_payload)
            else:
                error = "no_active_usdt_spot_symbol"
        except Exception as exc:  # noqa: BLE001 - per-venue bounded probe
            error = f"{type(exc).__name__}: {exc}"
        results.append(
            _venue_result(
                config,
                snapshot_meta=snapshot_meta,
                rows=rows,
                sample_symbol=sample_symbol,
                ohlcv_meta=ohlcv_meta,
                candles=candles,
                error=error,
            )
        )
    elapsed = time.monotonic() - started
    payload: dict[str, Any] = {
        "schema": SCHEMA,
        "preflight_id": PREFLIGHT_ID,
        "generated_at_utc": generated_at,
        "status": "PASS" if all(item["status"] == "PASS" for item in results) else "FAIL",
        "research_only": True,
        "public_data_only": True,
        "private_api": False,
        "live_orders": False,
        "real_capital": False,
        "quote": QUOTE,
        "venues": results,
        "contract": {
            "request_count": request_count,
            "max_requests": MAX_REQUESTS,
            "max_runtime_sec": MAX_RUNTIME_SEC,
            "raw_payload_persisted": False,
            "snapshot_diff_allowed": True,
            "timestamp_policy": "official_timestamp_when_available_else_explicit_proxy",
            "ohlcv_granularity": "1h",
            "supported_venues": list(SUPPORTED_VENUES),
        },
        "elapsed_sec": round(elapsed, 3),
    }
    payload["receipt_hash"] = canonical_hash(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return payload


def load_preflight(path: Path = DEFAULT_PREFLIGHT_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != SCHEMA:
        raise ExpansionPreflightError("preflight schema mismatch")
    if payload.get("receipt_hash") != canonical_hash(payload):
        raise ExpansionPreflightError("preflight receipt hash mismatch")
    if tuple(payload.get("contract", {}).get("supported_venues") or []) != SUPPORTED_VENUES:
        raise ExpansionPreflightError("preflight venue set mismatch")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bounded public exchange expansion compatibility preflight")
    parser.add_argument("--output", default=str(DEFAULT_PREFLIGHT_PATH))
    parser.add_argument("--timeout-sec", type=int, default=TIMEOUT_SEC)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    output_path = Path(args.output)
    if args.check:
        payload = load_preflight(output_path)
    else:
        payload = run_preflight(output_path=output_path, timeout_sec=args.timeout_sec)
    summary = {
        "status": payload["status"],
        "preflight_id": payload["preflight_id"],
        "receipt_hash": payload["receipt_hash"],
        "output_path": str(output_path),
        "request_count": payload["contract"]["request_count"],
        "elapsed_sec": payload.get("elapsed_sec"),
        "venues": [
            {
                "exchange": item["exchange"],
                "status": item["status"],
                "rows": item["snapshot"]["rows"],
                "active_rows": item["snapshot"]["active_rows"],
                "timestamp_coverage": item["snapshot"]["timestamp_coverage"],
                "sample_symbol": item["snapshot"]["sample_symbol"],
                "parsed_candles": item["ohlcv"]["parsed_candles"],
                "timestamp_method": item["timestamp_contract"]["method"],
                "error": item.get("error", ""),
            }
            for item in payload["venues"]
        ],
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
