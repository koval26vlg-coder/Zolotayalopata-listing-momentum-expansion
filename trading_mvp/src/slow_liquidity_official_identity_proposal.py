from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "trading_mvp_slow_liquidity_official_identity_proposal_v1"
PROPOSAL_ID = "slow_liquidity_official_asset_identity_verification_20260815_spot_v2"
SOURCE_RUN_ID = (
    "slow_liquidity_history_recollect_20260813_"
    "pagecap_provenance_slotintegrity_v6"
)
HASH_METHOD = "sha256_canonical_json_excluding_proposal_hash"
EXPECTED_BASES = (
    "STETH",
    "WEETH",
    "CC",
    "OKB",
    "RAIN",
    "MNT",
    "USDD",
    "BDX",
    "EDGE",
)
EXPECTED_VENUES = ("mexc", "gateio")
EXPECTED_SOURCE_BINDINGS = {
    "recollect_plan",
    "approval_receipt",
    "completed_launch",
    "collection_manifest",
    "technical_quality",
}
SOURCE_PLAN_HASH = "b7b2104eaad9404dbf97699d777276acbb187edaa03327dea8f8329f3e084632"
QUALITY_DECISION = (
    "SLOW_LIQUIDITY_HISTORY_RECOLLECT_QUALITY_ACCEPTED_"
    "AWAIT_OFFICIAL_IDENTITY_APPROVAL"
)
MEXC_METADATA_ENDPOINT = "https://api.mexc.com/api/v3/exchangeInfo"
GATE_METADATA_ENDPOINT = "https://api.gateio.ws/api/v4/spot/currency_pairs"
SPOT_MARKET = "SPOT_USDT"
COLLISION_FAIL_CLOSED_BASES = ("EDGE", "RAIN")
HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def collected_spot_instrument(venue: str, base: str, quote: str = "USDT") -> str:
    if venue == "gateio":
        return f"{base}_{quote}"
    if venue == "mexc":
        return f"{base}{quote}"
    raise IdentityProposalError(f"unsupported collected spot venue: {venue}")


def collected_spot_instruments(
    venues: Sequence[str] = EXPECTED_VENUES,
    bases: Sequence[str] = EXPECTED_BASES,
) -> dict[str, dict[str, str]]:
    return {
        venue: {base: collected_spot_instrument(venue, base) for base in bases}
        for venue in venues
    }


class IdentityProposalError(ValueError):
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
    normalized.pop("proposal_hash", None)
    return hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()


def _canonical_hash_excluding(payload: Mapping[str, Any], field: str) -> str:
    normalized = copy.deepcopy(dict(payload))
    normalized.pop(field, None)
    return hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise IdentityProposalError(f"cannot read required file: {path}") from exc


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityProposalError(f"invalid {label}: {path}") from exc
    if not isinstance(payload, dict):
        raise IdentityProposalError(f"{label} must be a JSON object")
    return payload


def _binding(path: Path, **extra: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(path.resolve()),
        "file_sha256": _sha256(path),
    }
    result.update(extra)
    return result


def _require(value: bool, message: str) -> None:
    if not value:
        raise IdentityProposalError(message)


def _require_hash(value: Any, label: str) -> str:
    if type(value) is not str or HASH_PATTERN.fullmatch(value) is None:
        raise IdentityProposalError(f"invalid {label}")
    return value


def _source_paths(repo_root: Path) -> dict[str, Path]:
    return {
        "plan": repo_root
        / "docs/plans/slow-liquidity-history-recollect-planonly-20260813-"
        "pagecap-provenance-slotintegrity-v6.json",
        "receipt": repo_root
        / "docs/agent-log/approvals/2026-08-13-slow-liquidity-history-"
        "recollect-pagecap-provenance-slotintegrity-v6-approval.json",
        "launch": repo_root
        / "docs/agent-log/run-gates/slow_liquidity_history_recollect_20260813_"
        "pagecap_provenance_slotintegrity_v6.launch.json",
        "manifest": Path(
            "E:/trading_mvp/slow-liquidity-history/"
            "slow_liquidity_history_recollect_20260813_"
            "pagecap_provenance_slotintegrity_v6/manifest.json"
        ),
        "quality": Path(
            "E:/ZolotyayLopata-data/exports/trading-mvp/analysis/"
            "slow_liquidity_history_recollect_quality_20260813_"
            "pagecap_provenance_slotintegrity_v6.json"
        ),
    }


def build_proposal(repo_root: str | Path, generated_at_utc: str) -> dict[str, Any]:
    root = Path(repo_root).expanduser().resolve()
    paths = _source_paths(root)
    plan = _load_json(paths["plan"], "source plan")
    receipt = _load_json(paths["receipt"], "approval receipt")
    launch = _load_json(paths["launch"], "launch record")
    manifest = _load_json(paths["manifest"], "collection manifest")
    quality = _load_json(paths["quality"], "technical quality report")

    _require(plan.get("plan_hash") == SOURCE_PLAN_HASH, "source plan hash changed")
    _require(
        plan.get("plan_hash_method") == "sha256_canonical_json_excluding_plan_hash",
        "source plan hash method changed",
    )
    _require(
        _canonical_hash_excluding(plan, "plan_hash") == SOURCE_PLAN_HASH,
        "source plan canonical hash changed",
    )
    universe = plan.get("universe")
    _require(isinstance(universe, dict), "source universe is missing")
    _require(
        tuple(universe.get("bases", [])) == EXPECTED_BASES,
        "source universe bases changed",
    )
    _require(universe.get("identity_bound") is False, "source was identity promoted")
    _require(
        universe.get("ticker_match_is_identity_evidence") is False,
        "ticker matching was promoted to identity evidence",
    )
    _require(
        universe.get("official_identity_verification_required_after_quality") is True,
        "source identity checkpoint changed",
    )
    _require(
        universe.get("minimum_verified_bases_after_exclusions") == 8,
        "minimum verified universe changed",
    )

    _require(receipt.get("status") == "APPROVED", "source receipt is not approved")
    _require(receipt.get("run_id") == SOURCE_RUN_ID, "receipt run mismatch")
    _require(receipt.get("plan_hash") == SOURCE_PLAN_HASH, "receipt plan mismatch")
    _require(launch.get("status") == "COMPLETE", "source launch is not complete")
    _require(launch.get("run_id") == SOURCE_RUN_ID, "launch run mismatch")
    _require(launch.get("plan_hash") == SOURCE_PLAN_HASH, "launch plan mismatch")
    _require(
        launch.get("terminal_ownership_verified") is True,
        "visible terminal ownership is not verified",
    )
    _require(manifest.get("run_id") == SOURCE_RUN_ID, "manifest run mismatch")
    _require(manifest.get("final") is True, "manifest is not final")
    _require(manifest.get("research_only") is True, "manifest is not research-only")
    _require(manifest.get("public_data_only") is True, "manifest is not public-only")
    _require(
        tuple(manifest.get("selected_bases", [])) == EXPECTED_BASES,
        "manifest bases changed",
    )
    _require(
        tuple(manifest.get("exchanges", [])) == EXPECTED_VENUES,
        "manifest venues changed",
    )
    _require(quality.get("accepted") is True, "technical quality was not accepted")
    _require(quality.get("decision") == QUALITY_DECISION, "quality decision changed")
    _require(
        quality.get("identity_verification_required") is True,
        "identity checkpoint is not required",
    )
    _require(
        quality.get("identity_verification_authorized") is False,
        "identity was already authorized",
    )
    _require(quality.get("evaluator_or_oos_authorized") is False, "OOS was authorized")

    proposal: dict[str, Any] = {
        "schema": SCHEMA,
        "proposal_id": PROPOSAL_ID,
        "mode": "PlanOnlyReviewProposal",
        "status": "AWAIT_EXACT_HASH_BOUND_APPROVAL",
        "generated_at_utc": generated_at_utc,
        "research_only": True,
        "supersedes": {
            "proposal_id": (
                "slow_liquidity_official_asset_identity_verification_20260813_v1"
            ),
            "reason": (
                "Identity must bind the collected USDT spot OHLCV instruments, "
                "not perpetual contracts. EDGE and RAIN are fail-closed on any "
                "ticker ambiguity."
            ),
        },
        "objective": (
            "Verify whether each exact collected MEXC and Gate USDT spot pair "
            "refers to the same canonical asset, using official public sources "
            "only. EDGE and RAIN are fail-closed on any ticker ambiguity."
        ),
        "source_bindings": {
            "recollect_plan": _binding(
                paths["plan"],
                plan_hash=SOURCE_PLAN_HASH,
            ),
            "approval_receipt": _binding(
                paths["receipt"],
                receipt_hash=receipt.get("receipt_hash"),
            ),
            "completed_launch": _binding(
                paths["launch"],
                run_id=SOURCE_RUN_ID,
                status="COMPLETE",
            ),
            "collection_manifest": _binding(
                paths["manifest"],
                run_id=SOURCE_RUN_ID,
                final=True,
            ),
            "technical_quality": _binding(
                paths["quality"],
                decision=QUALITY_DECISION,
                accepted=True,
            ),
        },
        "verification_scope": {
            "venues": list(EXPECTED_VENUES),
            "market": SPOT_MARKET,
            "collected_history_market": SPOT_MARKET,
            "identity_must_match_collected_ohlcv_instruments": True,
            "bases": list(EXPECTED_BASES),
            "base_count": len(EXPECTED_BASES),
            "quote": "USDT",
            "all_bases_must_be_reviewed": True,
            "category_exclusions_allowed": False,
            "symbol_blacklist_allowed": False,
            "minimum_verified_bases_after_exclusions": 8,
        },
        "official_source_contract": {
            "metadata_endpoints": [
                {
                    "venue": "mexc",
                    "url": MEXC_METADATA_ENDPOINT,
                    "method": "GET",
                },
                {
                    "venue": "gateio",
                    "url": GATE_METADATA_ENDPOINT,
                    "method": "GET",
                },
            ],
            "evidence_hosts": [
                {
                    "venue": "mexc",
                    "scheme": "https",
                    "host": "www.mexc.com",
                    "allowed_path_prefix": "/support/articles/",
                },
                {
                    "venue": "gateio",
                    "scheme": "https",
                    "host": "www.gate.com",
                    "allowed_path_prefix": "/announcements/article/",
                },
            ],
            "search_or_navigation_output_is_identity_evidence": False,
            "only_content_on_allowlisted_official_hosts_is_evidence": True,
            "http_redirect_following_allowed": False,
            "request_body_allowed": False,
            "private_or_auth_headers_allowed": False,
            "environment_proxies_allowed": False,
            "maximum_total_http_requests": 40,
            "maximum_attempts_per_url": 2,
            "maximum_response_bytes_per_request": 1_000_000,
            "max_runtime_sec": 600,
            "hard_output_cap_bytes": 20_000_000,
            "raw_response_persistence_allowed": False,
            "prices_or_funding_rates_persisted_allowed": False,
            "market_values_may_affect_identity_decision": False,
            "response_body_sha256_required": True,
            "evidence_locator_required": True,
            "evidence_fragment_sha256_required": True,
            "sanitized_evidence_fragment_required": True,
            "sanitized_evidence_fragment_max_bytes": 512,
            "free_form_evidence_text_allowed": False,
        },
        "identity_contract": {
            "ticker_match_is_identity_evidence": False,
            "asset_name_match_is_identity_evidence": False,
            "source_coin_id_is_identity_evidence": False,
            "economic_or_wrapped_asset_equivalence_allowed": False,
            "required_per_venue": [
                "official_source_url",
                "response_body_sha256",
                "instrument_id",
                "base_ticker",
                "canonical_asset_identifier_namespace",
                "canonical_asset_identifier_value",
                "canonical_asset_identifier_label",
                "evidence_locator_type",
                "evidence_locator_value",
                "evidence_fragment_sha256",
                "sanitized_evidence_fragment",
            ],
            "collected_spot_instruments": collected_spot_instruments(),
            "collision_fail_closed_bases": list(COLLISION_FAIL_CLOSED_BASES),
            "collision_ambiguity_disposition": "REJECT_EXCLUDE_FAIL_CLOSED",
            "same_underlying_acceptance": (
                "Both venues independently publish the same canonical asset "
                "identifier for the exact collected USDT spot instrument."
            ),
            "evm_identifier_comparison": "ASCII_CASE_INSENSITIVE",
            "non_evm_identifier_comparison": "EXACT",
            "missing_identifier_disposition": "UNRESOLVED_EXCLUDE_FAIL_CLOSED",
            "conflicting_identifier_disposition": "REJECT_EXCLUDE_FAIL_CLOSED",
            "verified_bases_below_minimum_disposition": (
                "INSUFFICIENT_IDENTITY_VERIFIED_UNIVERSE_NO_RESCOPE_"
                "WITHOUT_NEW_APPROVAL"
            ),
        },
        "output_contract": {
            "run_id": PROPOSAL_ID,
            "root": (
                "E:\\ZolotyayLopata-data\\exports\\trading-mvp\\"
                "slow-liquidity-official-identity"
            ),
            "run_output_path": (
                "E:\\ZolotyayLopata-data\\exports\\trading-mvp\\"
                "slow-liquidity-official-identity\\"
                f"{PROPOSAL_ID}"
            ),
            "required_files": ["identity-evidence.json", "manifest.json"],
            "immutable_exclusive_create": True,
            "overwrite_allowed": False,
            "raw_payload_files_allowed": False,
            "manifest_must_bind_source_files_and_response_hashes": True,
            "manifest_must_bind_sanitized_identifier_evidence": True,
        },
        "runtime_contract_after_approval": {
            "runtime_module": (
                "trading_mvp\\src\\"
                "slow_liquidity_official_identity_verification.py"
            ),
            "synthetic_tests": (
                "trading_mvp\\tests\\"
                "test_slow_liquidity_official_identity_verification.py"
            ),
            "top_level_visible_launcher": (
                "tools\\start_exact_approved_slow_liquidity_"
                "official_identity_visible.ps1"
            ),
            "preflight_only_required": True,
            "visible_terminal_required": True,
            "single_use": True,
            "global_writer_claim_required": True,
            "active_run_gate_must_not_be_running": True,
            "stopped_incomplete_retry_authorized": False,
        },
        "authorized_scope_after_exact_approval": {
            "offline_runtime_implementation": True,
            "offline_synthetic_tests": True,
            "immutable_approval_receipt_and_runtime_manifest": True,
            "preflight_only": True,
            "one_visible_public_read_only_identity_run": False,
            "official_source_content_read": False,
            "technical_identity_output_validation": False,
            "automatic_fixed_signal_or_evaluator_after_success": False,
            "automatic_oos_or_data_collection_after_success": False,
            "separate_exact_code_bound_execution_approval_required": True,
        },
        "authorization_now": {
            "proposal_freeze_allowed": True,
            "offline_runtime_implementation_allowed": False,
            "synthetic_runtime_tests_allowed": False,
            "official_source_content_read_allowed": False,
            "actual_network_run_allowed": False,
            "identity_claim_allowed": False,
            "candidate_planonly_creation_allowed": False,
            "evaluator_or_oos_allowed": False,
            "returns_or_pnl_allowed": False,
            "grid_or_retune_allowed": False,
            "execution_probe_allowed": False,
            "paper_or_live_allowed": False,
            "private_api_keys_allowed": False,
            "real_capital_allowed": False,
            "leverage_or_margin_allowed": False,
            "exact_user_approval_required": True,
        },
        "next_checkpoint": {
            "required_action": "REQUEST_EXACT_HASH_BOUND_IDENTITY_APPROVAL",
            "approval_phrase_template": (
                "Разрешаю slow liquidity official identity offline implementation "
                "and refreeze spot v2 "
                "по proposal_hash=<proposal_hash> и "
                "proposal_file_sha256=<proposal_file_sha256>: реализовать runtime "
                "и synthetic tests, создать immutable code-bound runtime manifest "
                "для спот-пар собранного v6 датасета. EDGE и RAIN fail-closed "
                "при любой двусмысленности. Без сети и identity output."
            ),
            "after_offline_implementation": (
                "Request a separate exact code-bound execution approval before any "
                "official source read or identity output."
            ),
        },
        "safety": {
            "network_accessed_while_freezing": False,
            "official_source_content_read_while_freezing": False,
            "identity_output_created_while_freezing": False,
            "global_writer_claim_created_while_freezing": False,
            "identity_claimed_while_freezing": False,
            "market_rows_read": False,
            "prices_or_funding_rates_read": False,
            "returns_or_pnl_read": False,
            "oos_run": False,
            "grid_or_retune": False,
            "paper_or_live": False,
            "private_api_keys": False,
            "real_capital": False,
            "leverage_or_margin": False,
        },
        "proposal_hash_method": HASH_METHOD,
    }
    proposal["proposal_hash"] = canonical_hash(proposal)
    validate_proposal(proposal, root)
    return proposal


def validate_proposal(
    proposal: Mapping[str, Any],
    repo_root: str | Path,
) -> None:
    _require(proposal.get("schema") == SCHEMA, "proposal schema mismatch")
    _require(proposal.get("proposal_id") == PROPOSAL_ID, "proposal id mismatch")
    _require(
        proposal.get("status") == "AWAIT_EXACT_HASH_BOUND_APPROVAL",
        "proposal status mismatch",
    )
    _require(proposal.get("proposal_hash_method") == HASH_METHOD, "hash method mismatch")
    observed_hash = _require_hash(proposal.get("proposal_hash"), "proposal hash")
    _require(observed_hash == canonical_hash(proposal), "proposal hash mismatch")

    scope = proposal.get("verification_scope")
    _require(isinstance(scope, dict), "verification scope is missing")
    _require(tuple(scope.get("bases", [])) == EXPECTED_BASES, "proposal bases changed")
    _require(tuple(scope.get("venues", [])) == EXPECTED_VENUES, "proposal venues changed")
    _require(scope.get("market") == SPOT_MARKET, "market changed")
    _require(scope.get("collected_history_market") == SPOT_MARKET, "collected history market changed")
    _require(
        scope.get("identity_must_match_collected_ohlcv_instruments") is True,
        "collected OHLCV instrument binding disabled",
    )
    _require(scope.get("base_count") == len(EXPECTED_BASES), "base count changed")
    _require(scope.get("quote") == "USDT", "quote changed")
    _require(scope.get("all_bases_must_be_reviewed") is True, "partial review enabled")
    _require(scope.get("category_exclusions_allowed") is False, "category exclusion enabled")
    _require(scope.get("symbol_blacklist_allowed") is False, "symbol blacklist enabled")
    _require(scope.get("minimum_verified_bases_after_exclusions") == 8, "minimum changed")

    source = proposal.get("official_source_contract")
    _require(isinstance(source, dict), "official source contract is missing")
    endpoints = source.get("metadata_endpoints")
    expected_endpoints = [
        {"venue": "mexc", "url": MEXC_METADATA_ENDPOINT, "method": "GET"},
        {"venue": "gateio", "url": GATE_METADATA_ENDPOINT, "method": "GET"},
    ]
    _require(endpoints == expected_endpoints, "official metadata endpoints changed")
    expected_hosts = [
        {
            "venue": "mexc",
            "scheme": "https",
            "host": "www.mexc.com",
            "allowed_path_prefix": "/support/articles/",
        },
        {
            "venue": "gateio",
            "scheme": "https",
            "host": "www.gate.com",
            "allowed_path_prefix": "/announcements/article/",
        },
    ]
    _require(source.get("evidence_hosts") == expected_hosts, "evidence hosts changed")
    _require(
        source.get("search_or_navigation_output_is_identity_evidence") is False,
        "search output promoted to evidence",
    )
    _require(
        source.get("only_content_on_allowlisted_official_hosts_is_evidence") is True,
        "official evidence allowlist disabled",
    )
    _require(source.get("maximum_total_http_requests") == 40, "request cap changed")
    _require(source.get("maximum_attempts_per_url") == 2, "attempt cap changed")
    _require(
        source.get("maximum_response_bytes_per_request") == 1_000_000,
        "response cap changed",
    )
    _require(source.get("max_runtime_sec") == 600, "runtime cap changed")
    _require(source.get("hard_output_cap_bytes") == 20_000_000, "output cap changed")
    _require(source.get("http_redirect_following_allowed") is False, "redirects enabled")
    _require(source.get("request_body_allowed") is False, "request body enabled")
    _require(
        source.get("private_or_auth_headers_allowed") is False,
        "private headers enabled",
    )
    _require(source.get("environment_proxies_allowed") is False, "proxies enabled")
    _require(source.get("raw_response_persistence_allowed") is False, "raw persistence enabled")
    _require(
        source.get("prices_or_funding_rates_persisted_allowed") is False,
        "market-value persistence enabled",
    )
    _require(
        source.get("market_values_may_affect_identity_decision") is False,
        "market values affect identity",
    )
    _require(source.get("response_body_sha256_required") is True, "body hash disabled")
    _require(source.get("evidence_locator_required") is True, "locator disabled")
    _require(
        source.get("evidence_fragment_sha256_required") is True,
        "fragment hash disabled",
    )
    _require(
        source.get("sanitized_evidence_fragment_required") is True,
        "sanitized evidence disabled",
    )
    _require(
        source.get("sanitized_evidence_fragment_max_bytes") == 512,
        "sanitized evidence cap changed",
    )
    _require(
        source.get("free_form_evidence_text_allowed") is False,
        "free-form evidence enabled",
    )

    identity = proposal.get("identity_contract")
    _require(isinstance(identity, dict), "identity contract is missing")
    _require(identity.get("ticker_match_is_identity_evidence") is False, "ticker promoted")
    _require(identity.get("asset_name_match_is_identity_evidence") is False, "name promoted")
    _require(
        identity.get("economic_or_wrapped_asset_equivalence_allowed") is False,
        "economic equivalence enabled",
    )
    _require(
        identity.get("missing_identifier_disposition")
        == "UNRESOLVED_EXCLUDE_FAIL_CLOSED",
        "missing identifier disposition changed",
    )
    _require(
        identity.get("conflicting_identifier_disposition")
        == "REJECT_EXCLUDE_FAIL_CLOSED",
        "conflicting identifier disposition changed",
    )
    expected_identity_fields = [
        "official_source_url",
        "response_body_sha256",
        "instrument_id",
        "base_ticker",
        "canonical_asset_identifier_namespace",
        "canonical_asset_identifier_value",
        "canonical_asset_identifier_label",
        "evidence_locator_type",
        "evidence_locator_value",
        "evidence_fragment_sha256",
        "sanitized_evidence_fragment",
    ]
    _require(
        identity.get("required_per_venue") == expected_identity_fields,
        "required identity evidence changed",
    )
    _require(
        identity.get("same_underlying_acceptance")
        == (
            "Both venues independently publish the same canonical asset "
            "identifier for the exact collected USDT spot instrument."
        ),
        "same-underlying rule changed",
    )
    _require(
        identity.get("collected_spot_instruments") == collected_spot_instruments(),
        "collected spot instruments changed",
    )
    _require(
        tuple(identity.get("collision_fail_closed_bases") or ())
        == COLLISION_FAIL_CLOSED_BASES,
        "collision fail-closed bases changed",
    )
    _require(
        identity.get("collision_ambiguity_disposition")
        == "REJECT_EXCLUDE_FAIL_CLOSED",
        "collision ambiguity disposition changed",
    )
    _require(
        identity.get("evm_identifier_comparison") == "ASCII_CASE_INSENSITIVE",
        "EVM comparison changed",
    )
    _require(
        identity.get("non_evm_identifier_comparison") == "EXACT",
        "non-EVM comparison changed",
    )

    authorization = proposal.get("authorization_now")
    _require(isinstance(authorization, dict), "authorization block is missing")
    _require(authorization.get("proposal_freeze_allowed") is True, "freeze disabled")
    _require(authorization.get("exact_user_approval_required") is True, "approval not required")
    for key, value in authorization.items():
        if key in {"proposal_freeze_allowed", "exact_user_approval_required"}:
            continue
        _require(value is False, f"unauthorized action enabled: {key}")

    bindings = proposal.get("source_bindings")
    _require(isinstance(bindings, dict), "source bindings are missing")
    _require(set(bindings) == EXPECTED_SOURCE_BINDINGS, "source binding set changed")
    root = Path(repo_root).expanduser().resolve()
    expected_paths = _source_paths(root)
    binding_specs = {
        "recollect_plan": ({"path", "file_sha256", "plan_hash"}, "plan"),
        "approval_receipt": ({"path", "file_sha256", "receipt_hash"}, "receipt"),
        "completed_launch": (
            {"path", "file_sha256", "run_id", "status"},
            "launch",
        ),
        "collection_manifest": (
            {"path", "file_sha256", "run_id", "final"},
            "manifest",
        ),
        "technical_quality": (
            {"path", "file_sha256", "decision", "accepted"},
            "quality",
        ),
    }
    for label, binding in bindings.items():
        _require(isinstance(binding, dict), f"invalid source binding: {label}")
        expected_keys, path_key = binding_specs[label]
        _require(set(binding) == expected_keys, f"{label} binding fields changed")
        expected = _require_hash(binding.get("file_sha256"), f"{label} file hash")
        path = Path(str(binding.get("path", ""))).expanduser().resolve()
        _require(path == expected_paths[path_key].resolve(), f"bound path changed: {label}")
        _require(path.is_file(), f"bound source is missing: {label}")
        _require(_sha256(path) == expected, f"bound source changed: {label}")

    output = proposal.get("output_contract")
    _require(isinstance(output, dict), "output contract is missing")
    expected_output_root = (
        "E:\\ZolotyayLopata-data\\exports\\trading-mvp\\"
        "slow-liquidity-official-identity"
    )
    _require(output.get("run_id") == PROPOSAL_ID, "output run id changed")
    _require(output.get("root") == expected_output_root, "output root changed")
    _require(
        output.get("run_output_path") == f"{expected_output_root}\\{PROPOSAL_ID}",
        "output path changed",
    )
    _require(
        output.get("required_files") == ["identity-evidence.json", "manifest.json"],
        "output files changed",
    )
    _require(output.get("immutable_exclusive_create") is True, "exclusive create disabled")
    _require(output.get("overwrite_allowed") is False, "overwrite enabled")
    _require(output.get("raw_payload_files_allowed") is False, "raw output enabled")
    _require(
        output.get("manifest_must_bind_source_files_and_response_hashes") is True,
        "manifest source binding disabled",
    )
    _require(
        output.get("manifest_must_bind_sanitized_identifier_evidence") is True,
        "manifest evidence binding disabled",
    )

    runtime = proposal.get("runtime_contract_after_approval")
    _require(isinstance(runtime, dict), "runtime contract is missing")
    _require(
        runtime.get("runtime_module")
        == "trading_mvp\\src\\slow_liquidity_official_identity_verification.py",
        "runtime path changed",
    )
    _require(
        runtime.get("synthetic_tests")
        == "trading_mvp\\tests\\test_slow_liquidity_official_identity_verification.py",
        "runtime tests path changed",
    )
    _require(
        runtime.get("top_level_visible_launcher")
        == "tools\\start_exact_approved_slow_liquidity_official_identity_visible.ps1",
        "launcher path changed",
    )
    for field in (
        "preflight_only_required",
        "visible_terminal_required",
        "single_use",
        "global_writer_claim_required",
        "active_run_gate_must_not_be_running",
    ):
        _require(runtime.get(field) is True, f"runtime safety disabled: {field}")
    _require(
        runtime.get("stopped_incomplete_retry_authorized") is False,
        "retry enabled",
    )

    after_approval = proposal.get("authorized_scope_after_exact_approval")
    _require(isinstance(after_approval, dict), "post-approval scope is missing")
    for field in (
        "offline_runtime_implementation",
        "offline_synthetic_tests",
        "immutable_approval_receipt_and_runtime_manifest",
        "preflight_only",
        "separate_exact_code_bound_execution_approval_required",
    ):
        _require(after_approval.get(field) is True, f"offline scope changed: {field}")
    for field in (
        "one_visible_public_read_only_identity_run",
        "official_source_content_read",
        "technical_identity_output_validation",
        "automatic_fixed_signal_or_evaluator_after_success",
        "automatic_oos_or_data_collection_after_success",
    ):
        _require(after_approval.get(field) is False, f"execution scope enabled: {field}")


def write_proposal(path: str | Path, proposal: Mapping[str, Any]) -> Path:
    output = Path(path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(proposal, ensure_ascii=False, indent=2) + "\n"
    try:
        with output.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
    except FileExistsError as exc:
        raise IdentityProposalError(f"proposal already exists: {output}") from exc
    return output


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--output")
    parser.add_argument("--generated-at-utc")
    parser.add_argument("--validate-only")
    parser.add_argument("--expected-file-sha256")
    parser.add_argument("--expected-proposal-hash")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.validate_only:
        if not args.expected_file_sha256 or not args.expected_proposal_hash:
            raise IdentityProposalError(
                "validate-only requires external file and proposal hashes"
            )
        path = Path(args.validate_only).expanduser().resolve()
        proposal = _load_json(path, "proposal")
        validate_proposal(proposal, args.repo_root)
        _require_hash(args.expected_file_sha256, "expected file hash")
        _require(_sha256(path) == args.expected_file_sha256, "proposal file hash mismatch")
        _require_hash(args.expected_proposal_hash, "expected proposal hash")
        _require(
            proposal.get("proposal_hash") == args.expected_proposal_hash,
            "expected proposal hash mismatch",
        )
        print(
            json.dumps(
                {
                    "status": "VALID_PLANONLY_NO_NETWORK",
                    "proposal_path": str(path),
                    "proposal_file_sha256": _sha256(path),
                    "proposal_hash": proposal["proposal_hash"],
                    "actual_network_run_allowed": False,
                    "identity_claim_allowed": False,
                },
                ensure_ascii=False,
            )
        )
        return 0

    if not args.output or not args.generated_at_utc:
        raise IdentityProposalError(
            "--output and --generated-at-utc are required when building"
        )
    proposal = build_proposal(args.repo_root, args.generated_at_utc)
    output = write_proposal(args.output, proposal)
    print(
        json.dumps(
            {
                "status": "FROZEN_PLANONLY_AWAIT_EXACT_APPROVAL",
                "proposal_path": str(output),
                "proposal_file_sha256": _sha256(output),
                "proposal_hash": proposal["proposal_hash"],
                "network_accessed": False,
                "identity_output_created": False,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except IdentityProposalError as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}), file=__import__("sys").stderr)
        raise SystemExit(2) from exc
