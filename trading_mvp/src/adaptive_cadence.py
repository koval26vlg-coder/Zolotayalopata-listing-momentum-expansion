"""Deterministic cadence policy shared by the listing research tracks.

The app scheduler may wake an orchestrator every five minutes, but the
orchestrator must only perform a network/write tick when the decision's
``next_interval_at_utc`` is due.  This keeps the external scheduler responsive
to an approaching event without turning it into a tight-loop collector.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Mapping


SEARCH_INTERVAL_SEC = 6 * 60 * 60
SOON_INTERVAL_SEC = 3 * 60 * 60
CONFIRMED_INTERVAL_SEC = 60 * 60
SCHEDULED_INTERVAL_SEC = 5 * 60
SOON_HORIZON_SEC = 72 * 60 * 60
SCHEDULED_HORIZON_SEC = 24 * 60 * 60
# How long an anchor stays meaningful after its own event. Beyond this the event is
# spent: there is nothing left to discover about a listing that already happened, and
# an anchor that keeps claiming otherwise pins the tightest cadence to a phantom.
# Sized to the first-days observation window, so a just-passed event is still watched.
EVENT_SPENT_AFTER_SEC = 72 * 60 * 60


class CadenceStage(str, Enum):
    SEARCH = "SEARCH"
    SOON = "SOON"
    CONFIRMED = "CONFIRMED"
    SCHEDULED = "SCHEDULED"


@dataclass(frozen=True)
class CadenceDecision:
    stage: CadenceStage
    interval_sec: int
    reason: str
    next_interval_at_utc: str
    event_eta_utc: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage.value,
            "interval_sec": self.interval_sec,
            "reason": self.reason,
            "next_interval_at_utc": self.next_interval_at_utc,
            "event_eta_utc": self.event_eta_utc,
        }


def _as_utc(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        try:
            parsed = datetime.fromtimestamp(float(value), timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _now_utc(value: Any = None) -> datetime:
    parsed = _as_utc(value)
    return parsed or datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _decision(
    stage: CadenceStage,
    interval_sec: int,
    reason: str,
    *,
    now: datetime,
    event_eta: datetime | None,
) -> CadenceDecision:
    return CadenceDecision(
        stage=stage,
        interval_sec=interval_sec,
        reason=reason,
        next_interval_at_utc=_iso(now + timedelta(seconds=interval_sec)),
        event_eta_utc=_iso(event_eta) if event_eta else None,
    )


def decide_cadence(observation: Mapping[str, Any] | None = None, *, now: Any = None) -> CadenceDecision:
    """Resolve the next research interval from causal, non-hindsight evidence.

    ``official_confirmed`` means an exchange/source has confirmed the event;
    it does not imply an exact time.  Only an exact official timestamp within
    24 hours may select the five-minute stage.  Proxy timestamps may describe
    a candidate and select ``SOON`` but can never select ``CONFIRMED`` or
    ``SCHEDULED`` on their own.
    """

    item = dict(observation or {})
    current = _now_utc(now)
    lifecycle = str(item.get("lifecycle_status") or item.get("status") or "").lower()
    if lifecycle in {"cancelled", "canceled", "expired", "delisted", "complete", "completed"}:
        return _decision(CadenceStage.SEARCH, SEARCH_INTERVAL_SEC, f"terminal_lifecycle:{lifecycle}", now=current, event_eta=None)

    event_eta = _as_utc(item.get("event_eta_utc") or item.get("official_spot_listing_ts") or item.get("official_event_ts"))
    official = bool(item.get("official_confirmed") or item.get("official_timestamp") or item.get("source_class") == "official")
    exact = bool(item.get("exact_timestamp") or item.get("exact_official_timestamp"))
    proxy = bool(item.get("proxy_timestamp") or item.get("source_class") == "proxy" or item.get("is_proxy_timestamp"))
    candidate = bool(item.get("candidate") or item.get("pre_market_active") or item.get("contract_present") or event_eta)

    if exact and official and not proxy and event_eta is not None:
        seconds_until = (event_eta - current).total_seconds()
        if 0 <= seconds_until <= SCHEDULED_HORIZON_SEC:
            return _decision(CadenceStage.SCHEDULED, SCHEDULED_INTERVAL_SEC, "exact_official_event_within_24h", now=current, event_eta=event_eta)
        if seconds_until < 0 and abs(seconds_until) <= SCHEDULED_INTERVAL_SEC:
            return _decision(CadenceStage.SCHEDULED, SCHEDULED_INTERVAL_SEC, "exact_official_event_due", now=current, event_eta=event_eta)

    # An anchor whose event has already passed is not an upcoming event, confirmed or
    # not. Without this the CONFIRMED branch had no time check at all: once
    # official_confirmed was set it held the hourly cadence forever. Measured
    # 2026-08-24, the crypto track was polling hourly against an event_eta of
    # 2025-09-01 and the pre-IPO track against 2026-05-07, both reporting CONFIRMED.
    if event_eta is not None:
        seconds_since = (current - event_eta).total_seconds()
        if seconds_since > EVENT_SPENT_AFTER_SEC:
            return _decision(
                CadenceStage.SEARCH,
                SEARCH_INTERVAL_SEC,
                "anchor_event_already_passed",
                now=current,
                event_eta=None,
            )

    # Official confirmation is enough for hourly discovery, even when the
    # exact exchange opening time is not yet published.
    if official and not proxy:
        return _decision(CadenceStage.CONFIRMED, CONFIRMED_INTERVAL_SEC, "official_event_confirmed_without_near_exact_time", now=current, event_eta=event_eta)

    if event_eta is not None:
        seconds_until = (event_eta - current).total_seconds()
        if 0 <= seconds_until <= SOON_HORIZON_SEC:
            return _decision(CadenceStage.SOON, SOON_INTERVAL_SEC, "candidate_event_within_72h", now=current, event_eta=event_eta)

    if candidate:
        return _decision(CadenceStage.SOON, SOON_INTERVAL_SEC, "candidate_or_pre_market_contract_present", now=current, event_eta=event_eta)

    return _decision(CadenceStage.SEARCH, SEARCH_INTERVAL_SEC, "no_qualified_upcoming_event", now=current, event_eta=event_eta)


__all__ = [
    "CadenceDecision",
    "CadenceStage",
    "CONFIRMED_INTERVAL_SEC",
    "SCHEDULED_INTERVAL_SEC",
    "SEARCH_INTERVAL_SEC",
    "SOON_INTERVAL_SEC",
    "SOON_HORIZON_SEC",
    "EVENT_SPENT_AFTER_SEC",
    "SCHEDULED_HORIZON_SEC",
    "decide_cadence",
]
