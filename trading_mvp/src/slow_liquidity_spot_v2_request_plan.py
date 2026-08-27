from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from slow_liquidity_official_identity_proposal import (
    COLLISION_FAIL_CLOSED_BASES,
    EXPECTED_BASES,
    EXPECTED_VENUES,
    PROPOSAL_ID,
    SOURCE_RUN_ID,
    collected_spot_instrument,
)


SCHEMA = "trading_mvp_slow_liquidity_spot_v2_request_plan_bindings_v1"
PLAN_ID = "slow_liquidity_spot_v2_request_plan_bindings_20260815"
HASH_METHOD = "sha256_canonical_json_excluding_plan_hash"
REPO_ROOT = Path(__file__).resolve().parents[2]
BINDINGS_PATH = (
    REPO_ROOT / "docs/plans/slow-liquidity-spot-v2-request-plan-bindings-20260815.json"
)
SPOT_V2_RUNTIME_PATH = (
    REPO_ROOT
    / "docs/plans/slow-liquidity-official-identity-runtime-manifest-20260815-spot-v2.json"
)
SPOT_V2_RUNTIME_FILE_SHA256 = (
    "43b30ac5faeba2a13ab4ef97f7e5b757eb97436c94f59c2f16d091d3c66ef3b8"
)
SPOT_V2_RUNTIME_HASH = (
    "bc726311f22b81608da2de86ee0b997fdbfb5545f9675deaecb5df25a245a416"
)
SPOT_V2_PROPOSAL_PATH = (
    REPO_ROOT
    / "docs/plans/drafts/"
    "slow-liquidity-official-asset-identity-verification-proposal-20260815-spot-v2.json"
)
SPOT_V2_PROPOSAL_HASH = (
    "4ff5732fed76dd70ab1208253dfdf617aa33ac9d55580dffe5d08d4f5cae86bf"
)
SPOT_V2_PROPOSAL_FILE_SHA256 = (
    "64bedf76b55a1bdada04c9b627f0df5c93cc47a329a709783cad16aa1ba02d48"
)
PAIR_FIELDS = {
    "venue",
    "base_ticker",
    "instrument_id",
    "collision_fail_closed",
    "official_source_bound",
}


class SpotV2RequestPlanError(ValueError):
    pass


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_hash(payload: Mapping[str, Any]) -> str:
    normalized = copy.deepcopy(dict(payload))
    normalized.pop("plan_hash", None)
    return hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()


def _require(value: bool, message: str) -> None:
    if not value:
        raise SpotV2RequestPlanError(message)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_spot_v2_request_plan_bindings(generated_at_utc: str) -> dict[str, Any]:
    pairs: list[dict[str, Any]] = []
    for base in EXPECTED_BASES:
        collision = base in COLLISION_FAIL_CLOSED_BASES
        for venue in EXPECTED_VENUES:
            pairs.append(
                {
                    "venue": venue,
                    "base_ticker": base,
                    "instrument_id": collected_spot_instrument(venue, base),
                    "collision_fail_closed": collision,
                    "official_source_bound": False,
                }
            )
    plan: dict[str, Any] = {
        "schema": SCHEMA,
        "plan_id": PLAN_ID,
        "mode": "PlanOnly",
        "status": "FROZEN_INSTRUMENT_BINDINGS_AWAIT_OFFICIAL_PAGE_DISCOVERY",
        "generated_at_utc": generated_at_utc,
        "research_only": True,
        "identity_evidence": False,
        "execution_authorized": False,
        "network_authorized": False,
        "not_substitutable_for_execution_request_plan_sha256": True,
        "consumer_runtime": PROPOSAL_ID,
        "source_history_run_id": SOURCE_RUN_ID,
        "market": "SPOT_USDT",
        "required_pair_count": len(EXPECTED_VENUES) * len(EXPECTED_BASES),
        "collision_fail_closed_bases": list(COLLISION_FAIL_CLOSED_BASES),
        "collision_ambiguity_disposition": "REJECT_EXCLUDE_FAIL_CLOSED",
        "source_bindings": {
            "spot_v2_proposal": {
                "path": str(SPOT_V2_PROPOSAL_PATH),
                "file_sha256": SPOT_V2_PROPOSAL_FILE_SHA256,
                "proposal_hash": SPOT_V2_PROPOSAL_HASH,
            },
            "spot_v2_runtime": {
                "path": str(SPOT_V2_RUNTIME_PATH),
                "file_sha256": SPOT_V2_RUNTIME_FILE_SHA256,
                "manifest_hash": SPOT_V2_RUNTIME_HASH,
            },
        },
        "pairs": pairs,
        "plan_hash_method": HASH_METHOD,
    }
    plan["plan_hash"] = canonical_hash(plan)
    validate_spot_v2_request_plan_bindings(plan)
    return plan


def validate_spot_v2_request_plan_bindings(plan: Mapping[str, Any]) -> None:
    _require(plan.get("schema") == SCHEMA, "request-plan bindings schema mismatch")
    _require(plan.get("plan_id") == PLAN_ID, "request-plan bindings id mismatch")
    _require(plan.get("mode") == "PlanOnly", "request-plan bindings mode mismatch")
    _require(
        plan.get("status")
        == "FROZEN_INSTRUMENT_BINDINGS_AWAIT_OFFICIAL_PAGE_DISCOVERY",
        "request-plan bindings status mismatch",
    )
    _require(plan.get("identity_evidence") is False, "bindings claimed identity evidence")
    _require(plan.get("execution_authorized") is False, "bindings authorized execution")
    _require(plan.get("network_authorized") is False, "bindings authorized network")
    _require(
        plan.get("not_substitutable_for_execution_request_plan_sha256") is True,
        "bindings claimed to be the execution request plan",
    )
    _require(plan.get("market") == "SPOT_USDT", "request-plan market mismatch")
    _require(plan.get("consumer_runtime") == PROPOSAL_ID, "consumer runtime mismatch")
    _require(
        tuple(plan.get("collision_fail_closed_bases") or ())
        == COLLISION_FAIL_CLOSED_BASES,
        "collision fail-closed bases mismatch",
    )
    observed_hash = plan.get("plan_hash")
    _require(
        type(observed_hash) is str and observed_hash == canonical_hash(plan),
        "request-plan bindings hash mismatch",
    )
    pairs = plan.get("pairs")
    _require(isinstance(pairs, list), "request-plan pairs are missing")
    _require(len(pairs) == 18, "request-plan pair count mismatch")
    seen: set[tuple[str, str]] = set()
    for item in pairs:
        _require(isinstance(item, dict), "request-plan pair is invalid")
        extra = set(item) - PAIR_FIELDS
        _require(not extra, "official source or extra identity fields are forbidden")
        venue = item.get("venue")
        base = item.get("base_ticker")
        _require(venue in EXPECTED_VENUES, "unsupported venue")
        _require(base in EXPECTED_BASES, "unsupported base")
        pair = (str(venue), str(base))
        _require(pair not in seen, "duplicate venue/base pair")
        seen.add(pair)
        expected_instrument = collected_spot_instrument(str(venue), str(base))
        _require(
            item.get("instrument_id") == expected_instrument,
            "collected spot instrument mismatch",
        )
        _require(
            item.get("collision_fail_closed") is (base in COLLISION_FAIL_CLOSED_BASES),
            "collision fail-closed flag mismatch",
        )
        _require(item.get("official_source_bound") is False, "official source already bound")
    expected_pairs = {
        (venue, base) for venue in EXPECTED_VENUES for base in EXPECTED_BASES
    }
    _require(seen == expected_pairs, "request-plan universe mismatch")
    sources = plan.get("source_bindings")
    _require(isinstance(sources, dict), "source bindings are missing")
    runtime = sources.get("spot_v2_runtime") or {}
    _require(
        runtime.get("file_sha256") == SPOT_V2_RUNTIME_FILE_SHA256
        and runtime.get("manifest_hash") == SPOT_V2_RUNTIME_HASH,
        "spot v2 runtime binding mismatch",
    )
    if SPOT_V2_RUNTIME_PATH.is_file():
        _require(
            _sha256_file(SPOT_V2_RUNTIME_PATH) == SPOT_V2_RUNTIME_FILE_SHA256,
            "spot v2 runtime file hash drifted",
        )
    if SPOT_V2_PROPOSAL_PATH.is_file():
        _require(
            _sha256_file(SPOT_V2_PROPOSAL_PATH) == SPOT_V2_PROPOSAL_FILE_SHA256,
            "spot v2 proposal file hash drifted",
        )


def write_spot_v2_request_plan_bindings(generated_at_utc: str) -> Path:
    plan = build_spot_v2_request_plan_bindings(generated_at_utc)
    payload = json.dumps(plan, indent=2, ensure_ascii=False) + "\n"
    if BINDINGS_PATH.exists():
        current = BINDINGS_PATH.read_text(encoding="utf-8")
        _require(current == payload, f"immutable artifact mismatch: {BINDINGS_PATH}")
        return BINDINGS_PATH
    BINDINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    BINDINGS_PATH.write_text(payload, encoding="utf-8")
    return BINDINGS_PATH
