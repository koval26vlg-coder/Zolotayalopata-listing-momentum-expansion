from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from slow_liquidity_listing_momentum_first_days_collector import (
    PLAN_PATH as COLLECT_PLAN_PATH,
    RUN_ID as COLLECT_RUN_ID,
)
from slow_liquidity_spot_v2_official_page_discovery import canonical_hash


SCHEMA = "trading_mvp_slow_liquidity_listing_momentum_first_days_census_v1"
OUTPUT_JSONL = Path(
    "E:/trading_mvp/listing-momentum-first-days"
) / COLLECT_RUN_ID / "ohlcv.jsonl"
MANIFEST_PATH = (
    Path("E:/trading_mvp/listing-momentum-first-days") / COLLECT_RUN_ID / "manifest.json"
)
CENSUS_PATH = (
    Path(__file__).resolve().parents[2]
    / "exports/trading-mvp/analysis"
    / "slow_liquidity_listing_momentum_first_days_census_20260816.json"
)
PRIMARY_MIN_BARS = 70
LISTING_YEAR_BUCKETS = (
    ("le_2019", 0, 2019),
    ("2020_2021", 2020, 2021),
    ("2022_2023", 2022, 2023),
    ("2024_2025", 2024, 2025),
    ("2026", 2026, 9999),
)


class FirstDaysCensusError(ValueError):
    pass


def _require(value: bool, message: str) -> None:
    if not value:
        raise FirstDaysCensusError(message)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_collect_bindings() -> dict[str, Any]:
    plan = json.loads(COLLECT_PLAN_PATH.read_text(encoding="utf-8"))
    _require(plan.get("plan_id") == COLLECT_RUN_ID, "collect plan id mismatch")
    _require(plan.get("plan_hash") == canonical_hash(plan), "collect plan hash mismatch")
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    _require(
        manifest.get("run_id") == COLLECT_RUN_ID, "manifest run id mismatch"
    )
    _require(manifest.get("status") == "COMPLETED", "collect manifest not COMPLETED")
    _require(
        manifest.get("plan_hash") == plan.get("plan_hash"),
        "manifest binds a different plan",
    )
    _require(
        OUTPUT_JSONL.is_file(), "collect output jsonl missing"
    )
    actual_sha = _sha256_file(OUTPUT_JSONL)
    _require(
        manifest.get("output_sha256") == actual_sha,
        "collect output sha mismatch",
    )
    rows = [
        json.loads(line)
        for line in OUTPUT_JSONL.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    _require(
        len(rows) == int(manifest.get("rows_written") or -1),
        "row count does not match manifest",
    )
    return {"plan": plan, "manifest": manifest, "rows": rows, "output_sha256": actual_sha}


def _year_bucket(year: int) -> str:
    for label, low, high in LISTING_YEAR_BUCKETS:
        if low <= year <= high:
            return label
    return "unknown"


def _pct(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise FirstDaysCensusError("empty percentile input")
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _describe(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "mean": round(statistics.fmean(values), 6),
        "median": round(statistics.median(values), 6),
        "p10": round(_pct(values, 0.10), 6),
        "p90": round(_pct(values, 0.90), 6),
        "min": round(min(values), 6),
        "max": round(max(values), 6),
    }


def compute_window_stats(bars: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    bars = sorted(bars, key=lambda bar: int(bar["ts"]))
    n_bars = len(bars)
    if n_bars == 0:
        return {"n_bars": 0}
    first_open = float(bars[0]["open"])
    last_close = float(bars[-1]["close"])
    highs = [float(bar["high"]) for bar in bars]
    lows = [float(bar["low"]) for bar in bars]
    closes = [float(bar["close"]) for bar in bars]
    stats: dict[str, Any] = {
        "n_bars": n_bars,
        "first_open": first_open,
        "ret_72h": round(last_close / first_open - 1.0, 6) if n_bars >= 2 else None,
        "ret_24h": (
            round(closes[23] / first_open - 1.0, 6) if n_bars >= 24 else None
        ),
        "max_runup": round(max(highs) / first_open - 1.0, 6),
        "max_drawdown": round(min(lows) / first_open - 1.0, 6),
    }
    if n_bars >= 3:
        log_returns = []
        for previous, current in zip(closes, closes[1:]):
            if previous > 0 and current > 0:
                ratio = current / previous
                if ratio > 0:
                    log_returns.append(math.log(ratio))
        if len(log_returns) >= 2:
            stats["logret_1h_std"] = round(
                statistics.pstdev(log_returns), 8
            )
    volumes = [bar.get("volume") for bar in bars]
    if all(isinstance(volume, (int, float)) and volume is not None for volume in volumes):
        stats["base_volume_sum"] = round(sum(float(v) for v in volumes), 6)
    return stats


def build_census_payload(bindings: Mapping[str, Any]) -> dict[str, Any]:
    manifest = bindings["manifest"]
    rows = bindings["rows"]
    jobs = manifest.get("jobs") or []
    bars_by_key: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        bars_by_key[(str(row["exchange"]), str(row["base"]))].append(row)
    windows: list[dict[str, Any]] = []
    for job in sorted(jobs, key=lambda item: (item["exchange"], item["base"])):
        exchange = str(job["exchange"])
        base = str(job["base"])
        flags = job.get("flags") or []
        proxy_ts = int(job["proxy_ts"])
        bars = bars_by_key.get((exchange, base), [])
        listing_year = dt.datetime.fromtimestamp(proxy_ts, tz=dt.timezone.utc).year
        windows.append(
            {
                "exchange": exchange,
                "base": base,
                "flags": flags,
                "listing_year": listing_year,
                "listing_year_bucket": _year_bucket(listing_year),
                "primary": flags == [] and len(bars) >= PRIMARY_MIN_BARS,
                "stats": compute_window_stats(bars),
            }
        )
    primary = [w for w in windows if w["primary"]]
    flag_reconciliation: dict[str, int] = {}
    for window in windows:
        for flag in window["flags"] or ["clean"]:
            flag_reconciliation[flag] = flag_reconciliation.get(flag, 0) + 1
    clean_windows = [window for window in windows if window["flags"] == []]
    _require(
        flag_reconciliation.get("clean", 0) == len(clean_windows),
        "clean reconciliation mismatch",
    )
    _require(
        len(clean_windows) == len(primary),
        "clean windows must all be primary (classify semantics)",
    )
    _require(
        len(windows) == len(jobs),
        "window count does not reconcile with manifest jobs",
    )

    def group_stats(group: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        ret72 = [
            window["stats"]["ret_72h"]
            for window in group
            if window["stats"].get("ret_72h") is not None
        ]
        ret24 = [
            window["stats"]["ret_24h"]
            for window in group
            if window["stats"].get("ret_24h") is not None
        ]
        runup = [
            window["stats"]["max_runup"]
            for window in group
            if window["stats"].get("max_runup") is not None
        ]
        drawdown = [
            window["stats"]["max_drawdown"]
            for window in group
            if window["stats"].get("max_drawdown") is not None
        ]
        return {
            "windows": len(group),
            "ret_72h": _describe(ret72),
            "ret_24h": _describe(ret24),
            "max_runup": _describe(runup),
            "max_drawdown": _describe(drawdown),
            "share_ret_72h_positive": (
                round(sum(1 for value in ret72 if value > 0) / len(ret72), 4)
                if ret72
                else None
            ),
        }

    by_bucket: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for window in primary:
        by_bucket[window["listing_year_bucket"]].append(window)
    payload: dict[str, Any] = {
        "schema": SCHEMA,
        "run_id": COLLECT_RUN_ID,
        "generated_from_manifest_finished_at_utc": manifest.get("finished_at_utc"),
        "source_bindings": {
            "collect_plan_hash": manifest["plan_hash"],
            "collect_plan_file_sha256": _sha256_file(COLLECT_PLAN_PATH),
            "manifest_sha256": _sha256_file(MANIFEST_PATH),
            "output_sha256": bindings["output_sha256"],
            "rows": len(rows),
        },
        "evidence_class": "PROXY_DATE_DESCRIPTIVE_CENSUS",
        "acceptance_decision": "NONE_DESCRIPTIVE_ONLY",
        "limitations_inherited": [
            "SURVIVORSHIP_BIAS_CURRENT_SNAPSHOT",
            "TRADING_START_NOT_ANNOUNCEMENT",
            "GATE_10000_POINT_1H_DEPTH_LIMIT",
            "SAME_TICKER_STRING_PAIRING",
            "NO_IDENTITY_VERDICT",
        ],
        "window_counts": {
            "total_windows": len(windows),
            "primary_windows": len(primary),
            "primary_by_venue": {
                venue: sum(1 for w in primary if w["exchange"] == venue)
                for venue in sorted({w["exchange"] for w in primary})
            },
            "flag_reconciliation": flag_reconciliation,
        },
        "primary_stats_all": group_stats(primary),
        "primary_stats_by_venue": {
            venue: group_stats([w for w in primary if w["exchange"] == venue])
            for venue in sorted({w["exchange"] for w in primary})
        },
        "primary_stats_by_listing_year_bucket": {
            bucket: group_stats(by_bucket[bucket])
            for bucket in by_bucket
        },
        "windows": windows,
    }
    payload["census_hash"] = _content_hash(payload)
    return payload


def _content_hash(payload: Mapping[str, Any]) -> str:
    content = {key: value for key, value in payload.items() if key != "census_hash"}
    return canonical_hash(content)


def validate_census_payload(payload: Mapping[str, Any]) -> None:
    _require(payload.get("schema") == SCHEMA, "census schema mismatch")
    _require(
        payload.get("acceptance_decision") == "NONE_DESCRIPTIVE_ONLY",
        "census must not carry an acceptance decision",
    )
    _require(
        payload.get("evidence_class") == "PROXY_DATE_DESCRIPTIVE_CENSUS",
        "evidence class mismatch",
    )
    counts = payload.get("window_counts") or {}
    _require(
        int(counts.get("total_windows") or 0) > 0, "no windows in census"
    )
    _require(
        len(payload.get("windows") or []) == counts.get("total_windows"),
        "window list mismatch",
    )
    _require(
        payload.get("census_hash") == _content_hash(payload),
        "census hash mismatch",
    )


def write_census(output_path: Path) -> Path:
    bindings = load_collect_bindings()
    payload = build_census_payload(bindings)
    validate_census_payload(payload)
    content = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    if output_path.exists():
        _require(
            output_path.read_text(encoding="utf-8") == content,
            f"immutable artifact mismatch: {output_path}",
        )
        return output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-census", action="store_true")
    parser.add_argument("--output", default="")
    args = parser.parse_args(argv)
    if not args.write_census:
        raise SystemExit("no authorized action requested")
    output_path = Path(args.output) if args.output else CENSUS_PATH
    path = write_census(output_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    counts = payload["window_counts"]
    print(
        json.dumps(
            {
                "status": "CENSUS_WRITTEN",
                "path": str(path),
                "census_hash": payload["census_hash"],
                "primary_windows": counts["primary_windows"],
                "primary_by_venue": counts["primary_by_venue"],
                "flag_reconciliation": counts["flag_reconciliation"],
                "primary_stats_all": payload["primary_stats_all"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
