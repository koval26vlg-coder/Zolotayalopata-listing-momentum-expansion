from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

if __package__:
    from .slow_liquidity_official_identity_proposal import (
        COLLISION_FAIL_CLOSED_BASES,
        GATE_METADATA_ENDPOINT,
        IdentityProposalError,
        MEXC_METADATA_ENDPOINT,
        PROPOSAL_ID as SPOT_PROPOSAL_ID,
        collected_spot_instrument,
        validate_proposal,
    )
else:
    from slow_liquidity_official_identity_proposal import (
        COLLISION_FAIL_CLOSED_BASES,
        GATE_METADATA_ENDPOINT,
        IdentityProposalError,
        MEXC_METADATA_ENDPOINT,
        PROPOSAL_ID as SPOT_PROPOSAL_ID,
        collected_spot_instrument,
        validate_proposal,
    )


OFFLINE_RECEIPT_SCHEMA = (
    "trading_mvp_slow_liquidity_official_identity_offline_approval_receipt_v1"
)
RUNTIME_MANIFEST_SCHEMA = (
    "trading_mvp_slow_liquidity_official_identity_runtime_manifest_v1"
)
EXECUTION_MANIFEST_SCHEMA = (
    "trading_mvp_slow_liquidity_official_identity_execution_manifest_v1"
)
EXECUTION_RECEIPT_SCHEMA = (
    "trading_mvp_slow_liquidity_official_identity_execution_approval_receipt_v1"
)
IDENTITY_EVIDENCE_SCHEMA = (
    "trading_mvp_slow_liquidity_official_identity_evidence_v1"
)
IDENTITY_OUTPUT_MANIFEST_SCHEMA = (
    "trading_mvp_slow_liquidity_official_identity_output_manifest_v1"
)

PROPOSAL_ID = "slow_liquidity_official_asset_identity_verification_20260813_v1"
PHASE1_STATUS = (
    "FROZEN_OFFLINE_IMPLEMENTATION_AWAIT_EXACT_CODE_BOUND_EXECUTION_APPROVAL"
)
EXECUTION_APPROVED_STATUS = "FROZEN_WITH_EXACT_CODE_BOUND_EXECUTION_APPROVAL"
OFFLINE_AUTHORIZATION_TEXT = "разрешаю"
OFFLINE_SELECTED_PRIOR_RESPONSE_TEXT = (
    "Это разрешит только offline-код, тесты и manifest. Сеть и identity-output "
    "останутся запрещены. Ничего не запущено."
)
SPOT_V2_OFFLINE_AUTHORIZATION_TEXT = (
    "Разрешаю slow liquidity official identity offline implementation and "
    "refreeze spot v2 по "
    "proposal_hash=4ff5732fed76dd70ab1208253dfdf617aa33ac9d55580dffe5d08d4f5cae86bf "
    "и proposal_file_sha256=64bedf76b55a1bdada04c9b627f0df5c93cc47a329a709783cad16aa1ba02d48: "
    "реализовать runtime и synthetic tests, создать immutable code-bound runtime "
    "manifest для спот-пар собранного v6 датасета. EDGE и RAIN fail-closed при "
    "любой двусмысленности. Без сети и identity output."
)
SPOT_V2_OFFLINE_SELECTED_PRIOR_RESPONSE_TEXT = (
    "Это открывает только offline-код: runtime, тесты и новый hash-bound "
    "manifest. Не открывает чтение официальных страниц, identity-output, "
    "replay, OOS, paper и live."
)
REQUIRED_GUARD_DECISION = "RUN_SLOW_LIQUIDITY_OFFICIAL_IDENTITY_VERIFICATION"
REQUIRED_READINESS_SOURCE_STATUS = (
    "IDENTITY_RUNTIME_FROZEN_WITH_EXACT_CODE_BOUND_EXECUTION_APPROVAL"
)
REQUIRED_READINESS_CHECKPOINT_ID = "slow_liquidity_identity_execution_phase_2"
RUNTIME_REVISION_V5 = "v5"
RUNTIME_REVISION_V6 = "v6"
RUNTIME_REVISION_V7 = "v7"
RUNTIME_REVISION_SPOT_V2 = "spot_v2"
PARENT_IDENTITY_V4_RUNTIME_FILE_SHA256 = (
    "0001cb25541e56e225a962f3d1eb9b4ef18055eb891d3b392b2f65bca3e70924"
)
PARENT_IDENTITY_V4_RUNTIME_HASH = (
    "2e93b5c9b2bae219aa1072b3fd44a0af7e7b29728a6977ed3159eb643348269f"
)
TOPOLOGY_V4_RUNTIME_FILE_SHA256 = (
    "ddea956647b0110d079a191a0653dd66bd27e675df9d0b01b1e3e3f6b825aba6"
)
TOPOLOGY_V4_RUNTIME_HASH = (
    "9ab770ba4e3a857d5a2dee8ba74260a8d7d717080afb411abab009a3ccf508c0"
)
PARENT_IDENTITY_V5_RUNTIME_FILE_SHA256 = (
    "b1420ea01663d14f007546690ff3d57876b8860268db75c2e238bfe18723a8c5"
)
PARENT_IDENTITY_V5_RUNTIME_HASH = (
    "a357153b53d670c73c961d9fc175047d4fa7da69960d75910b00eed016a1eff6"
)
PARENT_IDENTITY_V6_RUNTIME_FILE_SHA256 = (
    "27ccc46587f2624f844f5ff34dd0a76ef54b9be7c663b66088532d3577bacae9"
)
PARENT_IDENTITY_V6_RUNTIME_HASH = (
    "fd03c1be6b71eb89eaedd2228131df2d8ce72c201f7674a4124edab8565853b8"
)
REPO_ROOT = Path(__file__).resolve().parents[2]
PARENT_IDENTITY_V4_RUNTIME_PATH = (
    REPO_ROOT
    / "docs/plans/slow-liquidity-official-identity-runtime-manifest-20260813-v4.json"
)
TOPOLOGY_V4_RUNTIME_PATH = (
    REPO_ROOT
    / "docs/plans/slow-liquidity-official-currentness-topology-runtime-manifest-20260814-v4.json"
)
PARENT_IDENTITY_V5_RUNTIME_PATH = (
    REPO_ROOT
    / "docs/plans/slow-liquidity-official-identity-runtime-manifest-20260814-v5.json"
)
PARENT_IDENTITY_V6_RUNTIME_PATH = (
    REPO_ROOT
    / "docs/plans/slow-liquidity-official-identity-runtime-manifest-20260814-v6.json"
)
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
MINIMUM_VERIFIED_BASES = 8
MAX_TOTAL_HTTP_REQUESTS = 40
MAX_ATTEMPTS_PER_URL = 2
MAX_RESPONSE_BYTES = 1_000_000
MAX_RUNTIME_SEC = 600
MAX_OUTPUT_BYTES = 20_000_000
MAX_SANITIZED_FRAGMENT_BYTES = 512
OFFICIAL_SPOT_METADATA_ENDPOINTS = {
    "mexc": MEXC_METADATA_ENDPOINT,
    "gateio": GATE_METADATA_ENDPOINT,
}
OFFICIAL_METADATA_ENDPOINTS = {
    "mexc": "https://contract.mexc.com/api/v1/contract/detail",
    "gateio": "https://api.gateio.ws/api/v4/futures/usdt/contracts",
}
OFFICIAL_EVIDENCE_HOSTS = {
    "mexc": ("www.mexc.com", "/support/articles/"),
    "gateio": ("www.gate.com", "/announcements/article/"),
}

HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
EVM_IDENTIFIER_PATTERN = re.compile(r"^0[xX][0-9a-fA-F]{40}$")
NAMESPACE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_:-]{0,63}$")
NON_EVM_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,255}$")
SAFE_LABEL_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,63}$")
SAFE_CONTRACT_PATTERN = re.compile(r"^[A-Z0-9._-]+_USDT$")
SAFE_MEXC_SPOT_PATTERN = re.compile(r"^[A-Z0-9]{2,20}USDT$")
SAFE_GATE_SPOT_PATTERN = re.compile(r"^[A-Z0-9]{2,20}_USDT$")
SAFE_OFFICIAL_PATH_PATTERN = re.compile(r"^/[A-Za-z0-9._~/-]+$")

EVIDENCE_FIELDS = {
    "venue",
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
}
REQUEST_PLAN_FIELDS = EVIDENCE_FIELDS - {
    "response_body_sha256",
    "evidence_fragment_sha256",
}


class IdentityVerificationError(ValueError):
    pass


@dataclass(frozen=True)
class FetchedResponse:
    requested_url: str
    final_url: str
    status: int
    body: bytes


@dataclass(frozen=True)
class IdentityEvidenceBundle:
    records: tuple[dict[str, Any], ...]
    response_body_hashes: tuple[str, ...]
    metadata_active_instruments: dict[str, tuple[str, ...]]
    missing_metadata_instruments: tuple[str, ...]
    request_count: int


@dataclass(frozen=True)
class ExecutionSnapshot:
    runtime_manifest_path: Path
    runtime_manifest_file_sha256: str
    runtime_manifest: dict[str, Any]
    execution_manifest_path: Path
    execution_manifest_file_sha256: str
    execution_manifest: dict[str, Any]
    execution_approval_receipt_path: Path
    execution_approval_receipt_file_sha256: str
    execution_approval_receipt: dict[str, Any]
    request_plan: tuple[dict[str, Any], ...]


_EXECUTION_STATE = {
    "network_accessed": False,
    "identity_output_created": False,
}


def canonical_json_bytes(payload: Any) -> bytes:
    try:
        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise IdentityVerificationError("payload is not canonical JSON") from exc


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant: {value}")


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _strict_json_loads(raw: str) -> Any:
    return json.loads(
        raw,
        parse_constant=_reject_json_constant,
        object_pairs_hook=_reject_duplicate_json_keys,
    )


def canonical_hash_without(payload: Mapping[str, Any], field: str) -> str:
    normalized = copy.deepcopy(dict(payload))
    normalized.pop(field, None)
    return hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()


def _json_file_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise IdentityVerificationError(message)


def _require_hash(value: Any, label: str) -> str:
    _require(type(value) is str and HASH_PATTERN.fullmatch(value) is not None, f"invalid {label}")
    return value


def _validate_timestamp(value: Any, label: str) -> str:
    _require(type(value) is str and value.endswith("Z"), f"invalid {label}")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise IdentityVerificationError(f"invalid {label}") from exc
    _require(parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(parsed), f"invalid {label}")
    return value


def _sha256_file(path: str | Path) -> str:
    resolved = Path(path).expanduser().resolve()
    try:
        return hashlib.sha256(resolved.read_bytes()).hexdigest()
    except OSError as exc:
        raise IdentityVerificationError(f"required file is unavailable: {resolved}") from exc


def _load_json(path: str | Path, label: str) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    try:
        value = _strict_json_loads(resolved.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise IdentityVerificationError(f"invalid or missing {label}: {resolved}") from exc
    _require(isinstance(value, dict), f"{label} must be a JSON object")
    return value


def _read_json_snapshot(
    path: str | Path, label: str
) -> tuple[Path, str, dict[str, Any]]:
    resolved = Path(path).expanduser().resolve()
    try:
        raw = resolved.read_bytes()
        value = _strict_json_loads(raw.decode("utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise IdentityVerificationError(
            f"invalid or missing {label}: {resolved}"
        ) from exc
    _require(isinstance(value, dict), f"{label} must be a JSON object")
    return resolved, hashlib.sha256(raw).hexdigest(), value


def _repo_root_from_proposal(path: Path) -> Path:
    for candidate in (path, *path.parents):
        if (candidate / "trading_mvp").is_dir() and (candidate / "docs").is_dir():
            return candidate.resolve()
    raise IdentityVerificationError("proposal is not inside a trading_mvp repository")


def _validate_exact_proposal(
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
) -> tuple[Path, dict[str, Any]]:
    path = Path(proposal_path).expanduser().resolve()
    _require_hash(expected_proposal_hash, "expected proposal hash")
    _require_hash(expected_proposal_file_sha256, "expected proposal file hash")
    _require(_sha256_file(path) == expected_proposal_file_sha256, "proposal file hash mismatch")
    proposal = _load_json(path, "proposal")
    _require(proposal.get("proposal_hash") == expected_proposal_hash, "proposal hash mismatch")
    proposal_id = proposal.get("proposal_id")
    if proposal_id == SPOT_PROPOSAL_ID:
        try:
            validate_proposal(proposal, _repo_root_from_proposal(path))
        except IdentityProposalError as exc:
            raise IdentityVerificationError(f"proposal validation failed: {exc}") from exc
    elif proposal_id == PROPOSAL_ID:
        _require(
            proposal.get("verification_scope", {}).get("market")
            == "USDT_SETTLED_LINEAR_PERPETUAL",
            "historical proposal market mismatch",
        )
    else:
        raise IdentityVerificationError("unknown identity proposal id")
    return path, proposal


def _offline_authorization_texts(
    proposal_payload: Mapping[str, Any],
) -> tuple[str, str]:
    proposal_id = proposal_payload.get("proposal_id")
    if proposal_id == SPOT_PROPOSAL_ID:
        return (
            SPOT_V2_OFFLINE_AUTHORIZATION_TEXT,
            SPOT_V2_OFFLINE_SELECTED_PRIOR_RESPONSE_TEXT,
        )
    if proposal_id == PROPOSAL_ID:
        return OFFLINE_AUTHORIZATION_TEXT, OFFLINE_SELECTED_PRIOR_RESPONSE_TEXT
    raise IdentityVerificationError("unknown identity proposal id")


def build_offline_approval_receipt(
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
    approved_at_utc: str,
    user_authorization_text: str,
    response_annotation_index: int,
) -> dict[str, Any]:
    path, proposal_payload = _validate_exact_proposal(
        proposal_path,
        expected_proposal_hash,
        expected_proposal_file_sha256,
    )
    expected_text, selected_prior = _offline_authorization_texts(proposal_payload)
    _validate_timestamp(approved_at_utc, "approval timestamp")
    _require(
        user_authorization_text == expected_text,
        "offline authorization text mismatch",
    )
    _require(response_annotation_index == 1, "authorization annotation binding mismatch")

    receipt: dict[str, Any] = {
        "schema": OFFLINE_RECEIPT_SCHEMA,
        "status": "APPROVED_OFFLINE_IMPLEMENTATION_ONLY",
        "approved_at_utc": approved_at_utc,
        "user_authorization_text": user_authorization_text,
        "response_annotation_index": response_annotation_index,
        "selected_prior_response_text": selected_prior,
        "authorization_interpretation": "EXACT_CONTEXT_BOUND_OFFLINE_ONLY",
        "proposal": {
            "path": str(path),
            "file_sha256": expected_proposal_file_sha256,
            "proposal_hash": expected_proposal_hash,
        },
        "authorized_scope": {
            "offline_runtime_implementation": True,
            "synthetic_tests": True,
            "approval_receipt_creation": True,
            "runtime_manifest_creation": True,
            "preflight_only": True,
            "official_source_content_read": False,
            "actual_network_run": False,
            "identity_output": False,
            "candidate_planonly_creation": False,
        },
        "prohibited_scope": {
            "collector_or_evaluator": True,
            "oos": True,
            "returns_or_pnl": True,
            "grid_or_retune": True,
            "execution_probe": True,
            "paper_or_live": True,
            "private_api": True,
            "real_capital": True,
            "leverage_or_margin": True,
        },
        "separate_exact_code_bound_execution_approval_required": True,
        "network_accessed_while_freezing": False,
        "identity_output_created_while_freezing": False,
        "global_writer_claim_created_while_freezing": False,
        "receipt_hash_method": "sha256_canonical_json_excluding_receipt_hash",
    }
    receipt["receipt_hash"] = canonical_hash_without(receipt, "receipt_hash")
    return receipt


def validate_offline_approval_receipt(
    receipt: Mapping[str, Any],
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
) -> None:
    _require(receipt.get("schema") == OFFLINE_RECEIPT_SCHEMA, "offline receipt schema mismatch")
    _require(receipt.get("status") == "APPROVED_OFFLINE_IMPLEMENTATION_ONLY", "offline receipt status mismatch")
    _validate_timestamp(receipt.get("approved_at_utc"), "approval timestamp")
    path, proposal_payload = _validate_exact_proposal(
        proposal_path,
        expected_proposal_hash,
        expected_proposal_file_sha256,
    )
    expected_text, selected_prior = _offline_authorization_texts(proposal_payload)
    _require(
        receipt.get("user_authorization_text") == expected_text,
        "offline authorization text mismatch",
    )
    _require(receipt.get("response_annotation_index") == 1, "authorization annotation binding mismatch")
    _require(
        receipt.get("selected_prior_response_text") == selected_prior,
        "offline selected response text mismatch",
    )
    _require(receipt.get("authorization_interpretation") == "EXACT_CONTEXT_BOUND_OFFLINE_ONLY", "authorization interpretation mismatch")
    _require(receipt.get("receipt_hash_method") == "sha256_canonical_json_excluding_receipt_hash", "receipt hash method mismatch")
    observed_hash = _require_hash(receipt.get("receipt_hash"), "receipt hash")
    _require(observed_hash == canonical_hash_without(receipt, "receipt_hash"), "receipt hash mismatch")

    proposal_binding = receipt.get("proposal")
    _require(isinstance(proposal_binding, dict), "offline receipt proposal binding is missing")
    _require(Path(str(proposal_binding.get("path", ""))).expanduser().resolve() == path, "offline receipt proposal path mismatch")
    _require(proposal_binding.get("file_sha256") == expected_proposal_file_sha256, "offline receipt proposal file hash mismatch")
    _require(proposal_binding.get("proposal_hash") == expected_proposal_hash, "offline receipt proposal hash mismatch")

    scope = receipt.get("authorized_scope")
    _require(isinstance(scope, dict), "offline receipt scope is missing")
    for key in (
        "offline_runtime_implementation",
        "synthetic_tests",
        "approval_receipt_creation",
        "runtime_manifest_creation",
        "preflight_only",
    ):
        _require(scope.get(key) is True, f"offline authorization disabled: {key}")
    for key in (
        "official_source_content_read",
        "actual_network_run",
        "identity_output",
        "candidate_planonly_creation",
    ):
        _require(scope.get(key) is False, f"unauthorized execution scope enabled: {key}")
    _require(receipt.get("separate_exact_code_bound_execution_approval_required") is True, "separate execution approval requirement removed")
    _require(receipt.get("network_accessed_while_freezing") is False, "network use claimed during offline freeze")
    _require(receipt.get("identity_output_created_while_freezing") is False, "identity output claimed during offline freeze")
    _require(receipt.get("global_writer_claim_created_while_freezing") is False, "writer claim created during offline freeze")


def _file_binding(path: str | Path) -> dict[str, str]:
    resolved = Path(path).expanduser().resolve()
    _require(resolved.is_file(), f"required runtime file is missing: {resolved}")
    return {"path": str(resolved), "file_sha256": _sha256_file(resolved)}


def build_runtime_manifest(
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
    approval_receipt_path: str | Path,
    runtime_module_path: str | Path,
    synthetic_tests_path: str | Path,
    launcher_path: str | Path,
    generated_at_utc: str,
    guard_checker_path: str | Path | None = None,
) -> dict[str, Any]:
    proposal, proposal_payload = _validate_exact_proposal(
        proposal_path,
        expected_proposal_hash,
        expected_proposal_file_sha256,
    )
    _validate_timestamp(generated_at_utc, "runtime manifest timestamp")
    receipt_path = Path(approval_receipt_path).expanduser().resolve()
    receipt = _load_json(receipt_path, "offline approval receipt")
    validate_offline_approval_receipt(
        receipt,
        proposal_path=proposal,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
    )
    receipt_binding = _file_binding(receipt_path)
    receipt_binding["receipt_hash"] = receipt["receipt_hash"]
    module_binding = _file_binding(runtime_module_path)
    tests_binding = _file_binding(synthetic_tests_path)
    launcher_binding = _file_binding(launcher_path)
    repo_root = _repo_root_from_proposal(proposal)
    checker_binding = _file_binding(
        guard_checker_path
        if guard_checker_path is not None
        else repo_root / "tools/check_trading_mvp_autopilot.ps1"
    )
    guard_module_binding = _file_binding(
        repo_root / "trading_mvp/src/autopilot_guard.py"
    )
    readiness_module_binding = _file_binding(
        repo_root / "trading_mvp/src/one_week_edge_sprint_readiness.py"
    )

    manifest: dict[str, Any] = {
        "schema": RUNTIME_MANIFEST_SCHEMA,
        "status": PHASE1_STATUS,
        "generated_at_utc": generated_at_utc,
        "research_only": True,
        "proposal": {
            "path": str(proposal),
            "file_sha256": expected_proposal_file_sha256,
            "proposal_hash": expected_proposal_hash,
        },
        "offline_approval_receipt": receipt_binding,
        "runtime": {
            "module_path": module_binding["path"],
            "module_sha256": module_binding["file_sha256"],
            "synthetic_tests_path": tests_binding["path"],
            "synthetic_tests_sha256": tests_binding["file_sha256"],
            "launcher_path": launcher_binding["path"],
            "launcher_sha256": launcher_binding["file_sha256"],
            "guard_checker_path": checker_binding["path"],
            "guard_checker_sha256": checker_binding["file_sha256"],
            "autopilot_guard_module_path": guard_module_binding["path"],
            "autopilot_guard_module_sha256": guard_module_binding["file_sha256"],
            "readiness_module_path": readiness_module_binding["path"],
            "readiness_module_sha256": readiness_module_binding["file_sha256"],
        },
        "verification_scope": copy.deepcopy(proposal_payload["verification_scope"]),
        "official_source_contract": copy.deepcopy(proposal_payload["official_source_contract"]),
        "identity_contract": copy.deepcopy(proposal_payload["identity_contract"]),
        "output_contract": copy.deepcopy(proposal_payload["output_contract"]),
        "execution_authorization": {
            "approved": False,
            "execution_approval_receipt": None,
            "actual_network_run_allowed": False,
            "official_source_content_read_allowed": False,
            "identity_output_allowed": False,
            "global_writer_claim_allowed": False,
            "separate_exact_code_bound_execution_approval_required": True,
            "runtime_can_mint_execution_approval": False,
            "launcher_can_mint_execution_approval": False,
            "manual_codex_checkpoint_after_direct_user_approval_required": True,
        },
        "preflight_contract": {
            "missing_or_invalid_execution_manifest_status": (
                "BLOCKED_AWAIT_EXACT_CODE_BOUND_EXECUTION_APPROVAL"
            ),
            "must_not_create_output_before_execution_approval": True,
            "must_not_access_network_before_execution_approval": True,
            "visible_terminal_required_for_actual_run": True,
            "global_writer_claim_required_for_actual_run": True,
            "active_run_gate_must_not_be_running": True,
            "authoritative_guard_exact_execution_decision_required": True,
            "direct_runtime_invocation_forbidden": True,
            "launcher_capability_required": True,
            "visible_console_process_membership_required": True,
            "single_use": True,
            "stopped_incomplete_retry_authorized": False,
        },
        "safety": {
            "network_accessed": False,
            "official_source_content_read": False,
            "identity_output_created": False,
            "global_writer_claim_created": False,
            "candidate_planonly_created": False,
            "collector_or_evaluator_run": False,
            "oos_or_returns_or_pnl_read": False,
            "grid_or_retune": False,
            "paper_or_live": False,
            "private_api_or_real_capital": False,
            "leverage_or_margin": False,
        },
        "manifest_hash_method": "sha256_canonical_json_excluding_manifest_hash",
    }
    manifest["manifest_hash"] = canonical_hash_without(manifest, "manifest_hash")
    return manifest


def _validate_v5_refreeze_lineage(
    *,
    parent_runtime_manifest_path: str | Path,
    topology_runtime_manifest_path: str | Path,
) -> dict[str, Any]:
    parent_path = Path(parent_runtime_manifest_path).expanduser().resolve()
    topology_path = Path(topology_runtime_manifest_path).expanduser().resolve()
    _require(
        parent_path == PARENT_IDENTITY_V4_RUNTIME_PATH,
        "identity v5 parent runtime path mismatch",
    )
    _require(
        topology_path == TOPOLOGY_V4_RUNTIME_PATH,
        "identity v5 topology runtime path mismatch",
    )
    _require(
        _sha256_file(parent_path) == PARENT_IDENTITY_V4_RUNTIME_FILE_SHA256,
        "identity v5 parent runtime file hash mismatch",
    )
    _require(
        _sha256_file(topology_path) == TOPOLOGY_V4_RUNTIME_FILE_SHA256,
        "identity v5 topology runtime file hash mismatch",
    )
    parent = _load_json(parent_path, "identity v4 parent runtime manifest")
    topology = _load_json(topology_path, "topology v4 runtime manifest")
    _require(
        parent.get("manifest_hash") == PARENT_IDENTITY_V4_RUNTIME_HASH
        and canonical_hash_without(parent, "manifest_hash")
        == PARENT_IDENTITY_V4_RUNTIME_HASH,
        "identity v5 parent runtime canonical hash mismatch",
    )
    parent_execution = parent.get("execution_authorization")
    _require(
        isinstance(parent_execution, dict)
        and parent_execution.get("approved") is False
        and parent_execution.get("actual_network_run_allowed") is False
        and parent_execution.get("identity_output_allowed") is False,
        "identity v5 parent execution boundary mismatch",
    )
    _require(
        topology.get("schema")
        == "trading_mvp_slow_liquidity_official_currentness_topology_runtime_manifest_v4"
        and topology.get("status")
        == "FROZEN_OFFLINE_V4_AWAIT_EXACT_TOPOLOGY_EXECUTION_APPROVAL"
        and topology.get("manifest_hash") == TOPOLOGY_V4_RUNTIME_HASH
        and canonical_hash_without(topology, "manifest_hash") == TOPOLOGY_V4_RUNTIME_HASH,
        "identity v5 topology runtime binding mismatch",
    )
    topology_execution = topology.get("execution_authorization")
    _require(
        isinstance(topology_execution, dict)
        and topology_execution.get("approved") is False
        and topology_execution.get("network_run_allowed") is False,
        "identity v5 topology execution boundary mismatch",
    )
    return {
        "identity_runtime_v4": {
            "path": str(parent_path),
            "file_sha256": PARENT_IDENTITY_V4_RUNTIME_FILE_SHA256,
            "manifest_hash": PARENT_IDENTITY_V4_RUNTIME_HASH,
            "execution_authorized": False,
        },
        "topology_runtime_v4": {
            "path": str(topology_path),
            "file_sha256": TOPOLOGY_V4_RUNTIME_FILE_SHA256,
            "manifest_hash": TOPOLOGY_V4_RUNTIME_HASH,
            "execution_authorized": False,
            "topology_output_consumed": False,
        },
    }


def _validate_exact_offline_parent_runtime(
    *,
    parent_runtime_manifest_path: str | Path,
    expected_path: Path,
    expected_file_sha256: str,
    expected_manifest_hash: str,
    expected_revision: str,
) -> dict[str, Any]:
    parent_path = Path(parent_runtime_manifest_path).expanduser().resolve()
    _require(parent_path == expected_path, "identity parent runtime path mismatch")
    _require(
        _sha256_file(parent_path) == expected_file_sha256,
        "identity parent runtime file hash mismatch",
    )
    parent = _load_json(parent_path, "identity parent runtime manifest")
    _require(
        parent.get("schema") == RUNTIME_MANIFEST_SCHEMA
        and parent.get("status") == PHASE1_STATUS
        and parent.get("runtime_revision") == expected_revision,
        "identity parent runtime contract mismatch",
    )
    _require(
        parent.get("manifest_hash") == expected_manifest_hash
        and canonical_hash_without(parent, "manifest_hash") == expected_manifest_hash,
        "identity parent runtime canonical hash mismatch",
    )
    parent_execution = parent.get("execution_authorization")
    _require(
        isinstance(parent_execution, dict)
        and parent_execution.get("approved") is False
        and parent_execution.get("execution_approval_receipt") is None
        and parent_execution.get("actual_network_run_allowed") is False
        and parent_execution.get("official_source_content_read_allowed") is False
        and parent_execution.get("identity_output_allowed") is False
        and parent_execution.get("global_writer_claim_allowed") is False,
        "identity parent execution boundary mismatch",
    )
    return {
        "path": str(parent_path),
        "file_sha256": expected_file_sha256,
        "manifest_hash": expected_manifest_hash,
        "runtime_revision": expected_revision,
        "execution_authorized": False,
    }


def _validate_v6_refreeze_lineage(
    *, parent_runtime_manifest_path: str | Path
) -> dict[str, Any]:
    return {
        "identity_runtime_v5": _validate_exact_offline_parent_runtime(
            parent_runtime_manifest_path=parent_runtime_manifest_path,
            expected_path=PARENT_IDENTITY_V5_RUNTIME_PATH,
            expected_file_sha256=PARENT_IDENTITY_V5_RUNTIME_FILE_SHA256,
            expected_manifest_hash=PARENT_IDENTITY_V5_RUNTIME_HASH,
            expected_revision=RUNTIME_REVISION_V5,
        ),
        "reason": "autopilot_guard_module_hash_drift_after_v5_freeze",
    }


def _validate_v7_refreeze_lineage(
    *, parent_runtime_manifest_path: str | Path
) -> dict[str, Any]:
    return {
        "identity_runtime_v6": _validate_exact_offline_parent_runtime(
            parent_runtime_manifest_path=parent_runtime_manifest_path,
            expected_path=PARENT_IDENTITY_V6_RUNTIME_PATH,
            expected_file_sha256=PARENT_IDENTITY_V6_RUNTIME_FILE_SHA256,
            expected_manifest_hash=PARENT_IDENTITY_V6_RUNTIME_HASH,
            expected_revision=RUNTIME_REVISION_V6,
        ),
        "reason": "integrity_repair_after_invalid_unapproved_execution_artifacts",
    }


def _v6_offline_refreeze_authorization() -> dict[str, Any]:
    return {
        "mode": "DIRECT_EXACT_USER_APPROVAL_NOT_MATERIALIZED_AS_NEW_RECEIPT",
        "existing_offline_receipt_reused": True,
        "new_approval_receipt_created": False,
        "network_accessed": False,
        "official_source_content_read": False,
        "identity_output_created": False,
        "topology_output_consumed": False,
        "separate_exact_network_execution_approval_required": True,
        "refreeze_reason": "autopilot_guard_module_hash_mismatch_on_v5",
    }


def _v7_offline_refreeze_authorization() -> dict[str, Any]:
    return {
        "mode": "DIRECT_USER_CONTINUATION_FROM_ACCEPTED_V6_BASELINE_OFFLINE_ONLY",
        "existing_offline_receipt_reused": True,
        "new_approval_receipt_created": False,
        "invalid_execution_approval_artifacts_reused": False,
        "network_accessed": False,
        "official_source_content_read": False,
        "identity_output_created": False,
        "topology_output_consumed": False,
        "separate_exact_network_execution_approval_required": True,
        "refreeze_reason": "integrity_repair_after_invalid_unapproved_execution_artifacts",
    }


def build_runtime_manifest_v5(
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
    approval_receipt_path: str | Path,
    parent_runtime_manifest_path: str | Path,
    topology_runtime_manifest_path: str | Path,
    runtime_module_path: str | Path,
    synthetic_tests_path: str | Path,
    launcher_path: str | Path,
    generated_at_utc: str,
    guard_checker_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest = build_runtime_manifest(
        proposal_path=proposal_path,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
        approval_receipt_path=approval_receipt_path,
        runtime_module_path=runtime_module_path,
        synthetic_tests_path=synthetic_tests_path,
        launcher_path=launcher_path,
        generated_at_utc=generated_at_utc,
        guard_checker_path=guard_checker_path,
    )
    manifest["runtime_revision"] = RUNTIME_REVISION_V5
    manifest["refreeze_lineage"] = _validate_v5_refreeze_lineage(
        parent_runtime_manifest_path=parent_runtime_manifest_path,
        topology_runtime_manifest_path=topology_runtime_manifest_path,
    )
    manifest["offline_refreeze_authorization"] = {
        "mode": "DIRECT_EXACT_USER_APPROVAL_NOT_MATERIALIZED_AS_NEW_RECEIPT",
        "existing_offline_receipt_reused": True,
        "new_approval_receipt_created": False,
        "network_accessed": False,
        "official_source_content_read": False,
        "identity_output_created": False,
        "topology_output_consumed": False,
        "separate_exact_network_execution_approval_required": True,
    }
    manifest["manifest_hash"] = canonical_hash_without(manifest, "manifest_hash")
    return manifest


def build_runtime_manifest_spot_v2(
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
    approval_receipt_path: str | Path,
    runtime_module_path: str | Path,
    synthetic_tests_path: str | Path,
    launcher_path: str | Path,
    generated_at_utc: str,
    guard_checker_path: str | Path | None = None,
) -> dict[str, Any]:
    _, proposal_payload = _validate_exact_proposal(
        proposal_path,
        expected_proposal_hash,
        expected_proposal_file_sha256,
    )
    _require(
        proposal_payload.get("proposal_id") == SPOT_PROPOSAL_ID,
        "spot v2 runtime requires the spot v2 proposal",
    )
    _require(
        (proposal_payload.get("verification_scope") or {}).get("market") == "SPOT_USDT",
        "spot v2 runtime market mismatch",
    )
    manifest = build_runtime_manifest(
        proposal_path=proposal_path,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
        approval_receipt_path=approval_receipt_path,
        runtime_module_path=runtime_module_path,
        synthetic_tests_path=synthetic_tests_path,
        launcher_path=launcher_path,
        generated_at_utc=generated_at_utc,
        guard_checker_path=guard_checker_path,
    )
    manifest["runtime_revision"] = RUNTIME_REVISION_SPOT_V2
    manifest["refreeze_lineage"] = {
        "supersedes_proposal_id": PROPOSAL_ID,
        "reason": "identity_must_bind_collected_spot_ohlcv_instruments",
    }
    manifest["offline_refreeze_authorization"] = {
        "mode": "EXACT_HASH_BOUND_SPOT_V2_OFFLINE_ONLY",
        "existing_offline_receipt_reused": False,
        "new_approval_receipt_created": True,
        "network_accessed": False,
        "official_source_content_read": False,
        "identity_output_created": False,
        "separate_exact_network_execution_approval_required": True,
        "refreeze_reason": "narrow_identity_to_collected_spot_pairs",
    }
    manifest["manifest_hash"] = canonical_hash_without(manifest, "manifest_hash")
    return manifest


def build_runtime_manifest_v7(
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
    approval_receipt_path: str | Path,
    parent_runtime_manifest_path: str | Path,
    runtime_module_path: str | Path,
    synthetic_tests_path: str | Path,
    launcher_path: str | Path,
    generated_at_utc: str,
    guard_checker_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest = build_runtime_manifest(
        proposal_path=proposal_path,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
        approval_receipt_path=approval_receipt_path,
        runtime_module_path=runtime_module_path,
        synthetic_tests_path=synthetic_tests_path,
        launcher_path=launcher_path,
        generated_at_utc=generated_at_utc,
        guard_checker_path=guard_checker_path,
    )
    manifest["runtime_revision"] = RUNTIME_REVISION_V7
    manifest["refreeze_lineage"] = _validate_v7_refreeze_lineage(
        parent_runtime_manifest_path=parent_runtime_manifest_path
    )
    manifest["offline_refreeze_authorization"] = (
        _v7_offline_refreeze_authorization()
    )
    manifest["manifest_hash"] = canonical_hash_without(manifest, "manifest_hash")
    return manifest


def validate_runtime_manifest(manifest: Mapping[str, Any]) -> None:
    _require(manifest.get("schema") == RUNTIME_MANIFEST_SCHEMA, "runtime manifest schema mismatch")
    _require(manifest.get("status") == PHASE1_STATUS, "runtime manifest status mismatch")
    _validate_timestamp(manifest.get("generated_at_utc"), "runtime manifest timestamp")
    _require(manifest.get("research_only") is True, "runtime manifest is not research-only")
    _require(manifest.get("manifest_hash_method") == "sha256_canonical_json_excluding_manifest_hash", "runtime manifest hash method mismatch")
    observed_hash = _require_hash(manifest.get("manifest_hash"), "runtime manifest hash")
    _require(observed_hash == canonical_hash_without(manifest, "manifest_hash"), "runtime manifest hash mismatch")

    authorization = manifest.get("execution_authorization")
    _require(isinstance(authorization, dict), "execution authorization block is missing")
    _require(authorization.get("approved") is False, "execution approval unexpectedly enabled")
    _require(authorization.get("execution_approval_receipt") is None, "execution approval receipt unexpectedly bound")
    for key in (
        "actual_network_run_allowed",
        "official_source_content_read_allowed",
        "identity_output_allowed",
        "global_writer_claim_allowed",
    ):
        _require(authorization.get(key) is False, f"network or execution permission enabled: {key}")
    _require(authorization.get("separate_exact_code_bound_execution_approval_required") is True, "separate execution approval requirement removed")
    _require(
        authorization.get("runtime_can_mint_execution_approval") is False,
        "runtime execution-approval minting unexpectedly enabled",
    )
    _require(
        authorization.get("launcher_can_mint_execution_approval") is False,
        "launcher execution-approval minting unexpectedly enabled",
    )
    _require(
        authorization.get("manual_codex_checkpoint_after_direct_user_approval_required")
        is True,
        "manual execution-approval checkpoint is disabled",
    )

    proposal = manifest.get("proposal")
    _require(isinstance(proposal, dict), "runtime proposal binding is missing")
    proposal_path = Path(str(proposal.get("path", ""))).expanduser().resolve()
    proposal_hash = _require_hash(proposal.get("proposal_hash"), "runtime proposal hash")
    proposal_file_hash = _require_hash(proposal.get("file_sha256"), "runtime proposal file hash")
    _, proposal_payload = _validate_exact_proposal(
        proposal_path,
        proposal_hash,
        proposal_file_hash,
    )
    for contract_name in (
        "verification_scope",
        "official_source_contract",
        "identity_contract",
        "output_contract",
    ):
        _require(
            manifest.get(contract_name) == proposal_payload.get(contract_name),
            f"runtime {contract_name} differs from the exact proposal",
        )

    receipt_binding = manifest.get("offline_approval_receipt")
    _require(isinstance(receipt_binding, dict), "offline approval receipt binding is missing")
    receipt_path = Path(str(receipt_binding.get("path", ""))).expanduser().resolve()
    _require(_sha256_file(receipt_path) == _require_hash(receipt_binding.get("file_sha256"), "offline receipt file hash"), "offline receipt file hash mismatch")
    receipt = _load_json(receipt_path, "offline approval receipt")
    _require(receipt.get("receipt_hash") == receipt_binding.get("receipt_hash"), "offline receipt canonical hash mismatch")
    validate_offline_approval_receipt(
        receipt,
        proposal_path=proposal_path,
        expected_proposal_hash=proposal_hash,
        expected_proposal_file_sha256=proposal_file_hash,
    )

    runtime = manifest.get("runtime")
    _require(isinstance(runtime, dict), "runtime file bindings are missing")
    historical_perp_runtime = (
        proposal_payload.get("proposal_id") == PROPOSAL_ID
        and (proposal_payload.get("verification_scope") or {}).get("market")
        == "USDT_SETTLED_LINEAR_PERPETUAL"
    )
    for prefix in (
        "module",
        "synthetic_tests",
        "launcher",
        "guard_checker",
        "autopilot_guard_module",
        "readiness_module",
    ):
        path = Path(str(runtime.get(f"{prefix}_path", ""))).expanduser().resolve()
        expected = _require_hash(runtime.get(f"{prefix}_sha256"), f"{prefix} file hash")
        if historical_perp_runtime:
            _require(path.exists(), f"{prefix} path is missing")
            continue
        _require(_sha256_file(path) == expected, f"{prefix} file hash mismatch")

    source = manifest.get("official_source_contract")
    _require(isinstance(source, dict), "official source contract is missing")
    _require(source.get("maximum_total_http_requests") == MAX_TOTAL_HTTP_REQUESTS, "request cap changed")
    _require(source.get("maximum_attempts_per_url") == MAX_ATTEMPTS_PER_URL, "attempt cap changed")
    _require(source.get("maximum_response_bytes_per_request") == MAX_RESPONSE_BYTES, "response cap changed")
    _require(source.get("max_runtime_sec") == MAX_RUNTIME_SEC, "runtime cap changed")
    _require(source.get("hard_output_cap_bytes") == MAX_OUTPUT_BYTES, "output cap changed")
    for key in (
        "http_redirect_following_allowed",
        "request_body_allowed",
        "private_or_auth_headers_allowed",
        "environment_proxies_allowed",
        "raw_response_persistence_allowed",
        "prices_or_funding_rates_persisted_allowed",
        "market_values_may_affect_identity_decision",
        "free_form_evidence_text_allowed",
    ):
        _require(source.get(key) is False, f"network safety changed: {key}")

    preflight = manifest.get("preflight_contract")
    _require(isinstance(preflight, dict), "preflight contract is missing")
    for key in (
        "must_not_create_output_before_execution_approval",
        "must_not_access_network_before_execution_approval",
        "visible_terminal_required_for_actual_run",
        "global_writer_claim_required_for_actual_run",
        "active_run_gate_must_not_be_running",
        "authoritative_guard_exact_execution_decision_required",
        "direct_runtime_invocation_forbidden",
        "launcher_capability_required",
        "visible_console_process_membership_required",
        "single_use",
    ):
        _require(preflight.get(key) is True, f"preflight safety disabled: {key}")
    _require(preflight.get("stopped_incomplete_retry_authorized") is False, "STOPPED_INCOMPLETE retry enabled")

    revision = manifest.get("runtime_revision")
    if revision is None:
        return
    _require(
        revision in (
            RUNTIME_REVISION_V5,
            RUNTIME_REVISION_V6,
            RUNTIME_REVISION_V7,
            RUNTIME_REVISION_SPOT_V2,
        ),
        "identity runtime revision mismatch",
    )
    lineage = manifest.get("refreeze_lineage")
    _require(isinstance(lineage, dict), "identity refreeze lineage is missing")
    if revision == RUNTIME_REVISION_V5:
        expected_lineage = _validate_v5_refreeze_lineage(
            parent_runtime_manifest_path=PARENT_IDENTITY_V4_RUNTIME_PATH,
            topology_runtime_manifest_path=TOPOLOGY_V4_RUNTIME_PATH,
        )
        _require(lineage == expected_lineage, "identity v5 refreeze lineage mismatch")
        authorization = manifest.get("offline_refreeze_authorization")
        _require(
            authorization
            == {
                "mode": "DIRECT_EXACT_USER_APPROVAL_NOT_MATERIALIZED_AS_NEW_RECEIPT",
                "existing_offline_receipt_reused": True,
                "new_approval_receipt_created": False,
                "network_accessed": False,
                "official_source_content_read": False,
                "identity_output_created": False,
                "topology_output_consumed": False,
                "separate_exact_network_execution_approval_required": True,
            },
            "identity v5 offline authorization mismatch",
        )
    elif revision == RUNTIME_REVISION_V6:
        expected_lineage = _validate_v6_refreeze_lineage(
            parent_runtime_manifest_path=PARENT_IDENTITY_V5_RUNTIME_PATH
        )
        _require(lineage == expected_lineage, "identity v6 refreeze lineage mismatch")
        _require(
            manifest.get("offline_refreeze_authorization")
            == _v6_offline_refreeze_authorization(),
            "identity v6 offline authorization mismatch",
        )
    elif revision == RUNTIME_REVISION_V7:
        expected_lineage = _validate_v7_refreeze_lineage(
            parent_runtime_manifest_path=PARENT_IDENTITY_V6_RUNTIME_PATH
        )
        _require(lineage == expected_lineage, "identity v7 refreeze lineage mismatch")
        _require(
            manifest.get("offline_refreeze_authorization")
            == _v7_offline_refreeze_authorization(),
            "identity v7 offline authorization mismatch",
        )
    else:
        _require(
            lineage
            == {
                "supersedes_proposal_id": PROPOSAL_ID,
                "reason": "identity_must_bind_collected_spot_ohlcv_instruments",
            },
            "spot v2 refreeze lineage mismatch",
        )
        _require(
            manifest.get("offline_refreeze_authorization")
            == {
                "mode": "EXACT_HASH_BOUND_SPOT_V2_OFFLINE_ONLY",
                "existing_offline_receipt_reused": False,
                "new_approval_receipt_created": True,
                "network_accessed": False,
                "official_source_content_read": False,
                "identity_output_created": False,
                "separate_exact_network_execution_approval_required": True,
                "refreeze_reason": "narrow_identity_to_collected_spot_pairs",
            },
            "spot v2 offline authorization mismatch",
        )
        scope = manifest.get("verification_scope") or {}
        identity = manifest.get("identity_contract") or {}
        _require(scope.get("market") == "SPOT_USDT", "spot v2 runtime market mismatch")
        _require(
            tuple(identity.get("collision_fail_closed_bases") or ())
            == COLLISION_FAIL_CLOSED_BASES,
            "spot v2 collision fail-closed bases mismatch",
        )


def _write_immutable_json(path: str | Path, payload: Mapping[str, Any]) -> Path:
    output = Path(path).expanduser().resolve()
    expected = _json_file_bytes(payload)
    if output.exists():
        try:
            current = output.read_bytes()
        except OSError as exc:
            raise IdentityVerificationError(f"cannot read immutable artifact: {output}") from exc
        _require(current == expected, f"immutable artifact mismatch: {output}")
        return output
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("xb") as handle:
            handle.write(expected)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise IdentityVerificationError(f"immutable artifact race: {output}") from exc
    return output


def freeze_offline_bundle_spot_v2(
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
    approval_receipt_path: str | Path,
    runtime_manifest_path: str | Path,
    runtime_module_path: str | Path,
    synthetic_tests_path: str | Path,
    launcher_path: str | Path,
    guard_checker_path: str | Path | None = None,
    approved_at_utc: str,
    generated_at_utc: str,
    user_authorization_text: str,
    response_annotation_index: int,
) -> dict[str, Any]:
    receipt = build_offline_approval_receipt(
        proposal_path=proposal_path,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
        approved_at_utc=approved_at_utc,
        user_authorization_text=user_authorization_text,
        response_annotation_index=response_annotation_index,
    )
    receipt_path = _write_immutable_json(approval_receipt_path, receipt)
    manifest = build_runtime_manifest_spot_v2(
        proposal_path=proposal_path,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
        approval_receipt_path=receipt_path,
        runtime_module_path=runtime_module_path,
        synthetic_tests_path=synthetic_tests_path,
        launcher_path=launcher_path,
        guard_checker_path=guard_checker_path,
        generated_at_utc=generated_at_utc,
    )
    validate_runtime_manifest(manifest)
    manifest_path = _write_immutable_json(runtime_manifest_path, manifest)
    return {
        "status": PHASE1_STATUS,
        "runtime_revision": RUNTIME_REVISION_SPOT_V2,
        "approval_receipt_path": str(receipt_path),
        "approval_receipt_file_sha256": _sha256_file(receipt_path),
        "approval_receipt_hash": receipt["receipt_hash"],
        "runtime_manifest_path": str(manifest_path),
        "runtime_manifest_file_sha256": _sha256_file(manifest_path),
        "runtime_manifest_hash": manifest["manifest_hash"],
        "network_accessed": False,
        "official_source_content_read": False,
        "identity_output_created": False,
        "separate_exact_code_bound_execution_approval_required": True,
    }


def freeze_offline_bundle(
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
    approval_receipt_path: str | Path,
    runtime_manifest_path: str | Path,
    runtime_module_path: str | Path,
    synthetic_tests_path: str | Path,
    launcher_path: str | Path,
    guard_checker_path: str | Path | None = None,
    approved_at_utc: str,
    generated_at_utc: str,
    user_authorization_text: str,
    response_annotation_index: int,
) -> dict[str, Any]:
    receipt = build_offline_approval_receipt(
        proposal_path=proposal_path,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
        approved_at_utc=approved_at_utc,
        user_authorization_text=user_authorization_text,
        response_annotation_index=response_annotation_index,
    )
    receipt_path = _write_immutable_json(approval_receipt_path, receipt)
    manifest = build_runtime_manifest(
        proposal_path=proposal_path,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
        approval_receipt_path=receipt_path,
        runtime_module_path=runtime_module_path,
        synthetic_tests_path=synthetic_tests_path,
        launcher_path=launcher_path,
        guard_checker_path=guard_checker_path,
        generated_at_utc=generated_at_utc,
    )
    manifest_path = _write_immutable_json(runtime_manifest_path, manifest)
    return {
        "status": PHASE1_STATUS,
        "approval_receipt_path": str(receipt_path),
        "approval_receipt_file_sha256": _sha256_file(receipt_path),
        "approval_receipt_hash": receipt["receipt_hash"],
        "runtime_manifest_path": str(manifest_path),
        "runtime_manifest_file_sha256": _sha256_file(manifest_path),
        "runtime_manifest_hash": manifest["manifest_hash"],
        "network_accessed": False,
        "identity_output_created": False,
        "separate_exact_code_bound_execution_approval_required": True,
    }


def freeze_offline_refreeze_v5(
    *,
    proposal_path: str | Path,
    expected_proposal_hash: str,
    expected_proposal_file_sha256: str,
    approval_receipt_path: str | Path,
    parent_runtime_manifest_path: str | Path,
    topology_runtime_manifest_path: str | Path,
    runtime_manifest_path: str | Path,
    runtime_module_path: str | Path,
    synthetic_tests_path: str | Path,
    launcher_path: str | Path,
    generated_at_utc: str,
    guard_checker_path: str | Path | None = None,
) -> dict[str, Any]:
    manifest = build_runtime_manifest_v5(
        proposal_path=proposal_path,
        expected_proposal_hash=expected_proposal_hash,
        expected_proposal_file_sha256=expected_proposal_file_sha256,
        approval_receipt_path=approval_receipt_path,
        parent_runtime_manifest_path=parent_runtime_manifest_path,
        topology_runtime_manifest_path=topology_runtime_manifest_path,
        runtime_module_path=runtime_module_path,
        synthetic_tests_path=synthetic_tests_path,
        launcher_path=launcher_path,
        generated_at_utc=generated_at_utc,
        guard_checker_path=guard_checker_path,
    )
    manifest_path = _write_immutable_json(runtime_manifest_path, manifest)
    return {
        "status": PHASE1_STATUS,
        "runtime_revision": RUNTIME_REVISION_V5,
        "runtime_manifest_path": str(manifest_path),
        "runtime_manifest_file_sha256": _sha256_file(manifest_path),
        "runtime_manifest_hash": manifest["manifest_hash"],
        "approval_receipt_created": False,
        "network_accessed": False,
        "official_source_content_read": False,
        "identity_output_created": False,
        "separate_exact_code_bound_execution_approval_required": True,
    }


def _validate_official_source_url(venue: str, value: Any) -> str:
    _require(type(value) is str and value != "", "official source URL is missing")
    expected_host, expected_prefix = OFFICIAL_EVIDENCE_HOSTS[venue]
    parsed = urllib.parse.urlsplit(value)
    _require(parsed.scheme == "https", "official source must use HTTPS")
    _require(parsed.username is None and parsed.password is None and parsed.port is None, "official source authority is invalid")
    _require(parsed.hostname == expected_host and parsed.netloc == expected_host, "official source host is not allowlisted")
    _require(parsed.query == "", "official source query is forbidden")
    _require(
        SAFE_OFFICIAL_PATH_PATTERN.fullmatch(parsed.path) is not None,
        "official source path contains unsafe encoding",
    )
    decoded_path = parsed.path
    for _ in range(3):
        decoded_path = urllib.parse.unquote(decoded_path)
    segments = decoded_path.split("/")
    _require(
        all(segment not in (".", "..") for segment in segments),
        "official source path contains a dot segment",
    )
    _require(
        decoded_path.startswith(expected_prefix)
        and len(decoded_path) > len(expected_prefix),
        "official source path is not allowlisted",
    )
    _require(parsed.fragment == "", "official source fragment is forbidden")
    return value


def _normalize_identifier(namespace: str, value: str) -> str:
    if namespace == "EVM_CONTRACT":
        _require(EVM_IDENTIFIER_PATTERN.fullmatch(value) is not None, "invalid canonical asset identifier")
        return value.lower()
    _require(NAMESPACE_PATTERN.fullmatch(namespace) is not None, "invalid canonical identifier namespace")
    _require(NON_EVM_IDENTIFIER_PATTERN.fullmatch(value) is not None, "invalid canonical asset identifier")
    return value


def _validated_evidence_record(record: Mapping[str, Any]) -> dict[str, Any]:
    _require(set(record) == EVIDENCE_FIELDS, "identity evidence field set changed")
    venue = record.get("venue")
    _require(venue in EXPECTED_VENUES, "identity evidence venue is invalid")
    base = record.get("base_ticker")
    _require(base in EXPECTED_BASES, "identity evidence base is invalid")
    expected_instrument = collected_spot_instrument(str(venue), str(base))
    _require(
        record.get("instrument_id") == expected_instrument,
        "collected spot instrument mismatch",
    )
    _validate_official_source_url(str(venue), record.get("official_source_url"))
    _require_hash(record.get("response_body_sha256"), "response body hash")
    namespace = record.get("canonical_asset_identifier_namespace")
    value = record.get("canonical_asset_identifier_value")
    _require(type(namespace) is str and type(value) is str and value != "", "canonical asset identifier is missing")
    normalized_identifier = _normalize_identifier(namespace, value)
    label = record.get("canonical_asset_identifier_label")
    _require(type(label) is str and SAFE_LABEL_PATTERN.fullmatch(label) is not None, "canonical identifier label is invalid")
    _require(
        record.get("evidence_locator_type")
        == "CANONICAL_REQUIRED_EXACT_UTF8_TOKENS_V1",
        "evidence locator type is invalid",
    )
    locator = record.get("evidence_locator_value")
    fragment = record.get("sanitized_evidence_fragment")
    _require(type(locator) is str and locator != "", "evidence locator is missing")
    _require(type(fragment) is str and fragment == locator, "sanitized evidence must equal the exact locator")
    expected_assertion = json.dumps(
        {
            "base_ticker": base,
            "canonical_asset_identifier_label": label,
            "canonical_asset_identifier_value": value,
            "instrument_id": expected_instrument,
            "venue": venue,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    _require(
        locator == expected_assertion,
        "canonical evidence assertion does not bind venue/base/instrument/label/identifier",
    )
    try:
        fragment_bytes = fragment.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise IdentityVerificationError("sanitized evidence fragment is invalid UTF-8") from exc
    _require(0 < len(fragment_bytes) <= MAX_SANITIZED_FRAGMENT_BYTES, "sanitized evidence fragment exceeds cap")
    fragment_hash = _require_hash(record.get("evidence_fragment_sha256"), "evidence fragment hash")
    _require(fragment_hash == hashlib.sha256(fragment_bytes).hexdigest(), "evidence fragment hash mismatch")
    normalized = copy.deepcopy(dict(record))
    normalized["normalized_canonical_asset_identifier"] = normalized_identifier
    return normalized


def build_identity_result(
    evidence_records: Sequence[Mapping[str, Any]],
    *,
    metadata_ambiguity_bases: Sequence[str] = (),
) -> dict[str, Any]:
    _require(not isinstance(evidence_records, (str, bytes)), "identity evidence must be a sequence")
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    sanitized_records: list[dict[str, Any]] = []
    for raw_record in evidence_records:
        _require(isinstance(raw_record, Mapping), "identity evidence record is invalid")
        record = _validated_evidence_record(raw_record)
        key = (record["base_ticker"], record["venue"])
        _require(key not in indexed, "duplicate venue/base identity evidence")
        indexed[key] = record
        sanitized_records.append(record)

    venue_order = {venue: index for index, venue in enumerate(EXPECTED_VENUES)}
    base_order = {base: index for index, base in enumerate(EXPECTED_BASES)}
    sanitized_records.sort(key=lambda item: (base_order[item["base_ticker"]], venue_order[item["venue"]]))

    verified: list[str] = []
    rejected: list[str] = []
    unresolved: list[str] = []
    decisions: dict[str, Any] = {}
    ambiguous_metadata = {
        str(base) for base in metadata_ambiguity_bases if str(base) in EXPECTED_BASES
    }
    for base in EXPECTED_BASES:
        venue_records = {venue: indexed.get((base, venue)) for venue in EXPECTED_VENUES}
        missing = [venue for venue, record in venue_records.items() if record is None]
        collision_base = base in COLLISION_FAIL_CLOSED_BASES
        if collision_base and (missing or base in ambiguous_metadata):
            rejected.append(base)
            decisions[base] = {
                "decision": "REJECT_EXCLUDE_FAIL_CLOSED",
                "reason": "AMBIGUOUS_KNOWN_TICKER_COLLISION",
                "missing_venues": missing,
                "metadata_ambiguous": base in ambiguous_metadata,
            }
            continue
        if missing:
            unresolved.append(base)
            decisions[base] = {
                "decision": "UNRESOLVED_EXCLUDE_FAIL_CLOSED",
                "reason": "MISSING_OFFICIAL_CANONICAL_IDENTIFIER",
                "missing_venues": missing,
            }
            continue
        left = venue_records["mexc"]
        right = venue_records["gateio"]
        assert left is not None and right is not None
        namespaces_match = (
            left["canonical_asset_identifier_namespace"]
            == right["canonical_asset_identifier_namespace"]
        )
        identifiers_match = (
            left["normalized_canonical_asset_identifier"]
            == right["normalized_canonical_asset_identifier"]
        )
        if not namespaces_match or not identifiers_match:
            rejected.append(base)
            decisions[base] = {
                "decision": "REJECT_EXCLUDE_FAIL_CLOSED",
                "reason": (
                    "AMBIGUOUS_KNOWN_TICKER_COLLISION"
                    if collision_base
                    else "CANONICAL_IDENTIFIER_CONFLICT"
                ),
                "mexc_identifier_sha256": hashlib.sha256(
                    left["canonical_asset_identifier_value"].encode("utf-8")
                ).hexdigest(),
                "gateio_identifier_sha256": hashlib.sha256(
                    right["canonical_asset_identifier_value"].encode("utf-8")
                ).hexdigest(),
            }
            continue
        if collision_base and base in ambiguous_metadata:
            rejected.append(base)
            decisions[base] = {
                "decision": "REJECT_EXCLUDE_FAIL_CLOSED",
                "reason": "AMBIGUOUS_KNOWN_TICKER_COLLISION",
                "metadata_ambiguous": True,
            }
            continue
        verified.append(base)
        decisions[base] = {
            "decision": "VERIFIED_SAME_CANONICAL_IDENTIFIER",
            "canonical_asset_identifier_namespace": left[
                "canonical_asset_identifier_namespace"
            ],
            "canonical_asset_identifier_value": left[
                "normalized_canonical_asset_identifier"
            ],
            "mexc_evidence_fragment_sha256": left["evidence_fragment_sha256"],
            "gateio_evidence_fragment_sha256": right["evidence_fragment_sha256"],
        }

    status = (
        "IDENTITY_VERIFIED_CANDIDATE_PLANONLY_REQUIRED"
        if len(verified) >= MINIMUM_VERIFIED_BASES
        else "INSUFFICIENT_IDENTITY_VERIFIED_UNIVERSE_NO_RESCOPE_WITHOUT_NEW_APPROVAL"
    )
    result: dict[str, Any] = {
        "schema": IDENTITY_EVIDENCE_SCHEMA,
        "status": status,
        "verified_bases": verified,
        "verified_base_count": len(verified),
        "rejected_bases": rejected,
        "unresolved_bases": unresolved,
        "base_decisions": decisions,
        "sanitized_official_evidence": sanitized_records,
        "all_bases_reviewed": True,
        "minimum_verified_bases": MINIMUM_VERIFIED_BASES,
        "rescope_authorized": False,
        "candidate_planonly_created": False,
        "data_collection_authorized": False,
        "evaluator_authorized": False,
        "oos_authorized": False,
        "returns_or_pnl_authorized": False,
        "result_hash_method": "sha256_canonical_json_excluding_result_hash",
    }
    result["result_hash"] = canonical_hash_without(result, "result_hash")
    return result


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        raise IdentityVerificationError("HTTP redirect is forbidden")


def fetch_official_response(url: str, timeout_sec: float = 20.0) -> FetchedResponse:
    _EXECUTION_STATE["network_accessed"] = True
    parsed = urllib.parse.urlsplit(url)
    _require(parsed.scheme == "https", "only HTTPS is allowed")
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _NoRedirectHandler(),
    )
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"User-Agent": "trading-mvp-identity-verification/1.0"},
    )
    try:
        with opener.open(request, timeout=timeout_sec) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
            final_url = response.geturl()
            status = int(response.status)
    except IdentityVerificationError:
        raise
    except urllib.error.HTTPError as exc:
        if 300 <= exc.code < 400:
            raise IdentityVerificationError("HTTP redirect is forbidden") from exc
        raise IdentityVerificationError(f"official source HTTP status {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise IdentityVerificationError("official source request failed") from exc
    return FetchedResponse(url, final_url, status, body)


def _validated_response_body(response: FetchedResponse, url: str) -> bytes:
    _require(isinstance(response, FetchedResponse), "fetcher returned an invalid response")
    _require(response.requested_url == url, "fetcher request URL mismatch")
    _require(response.final_url == url, "HTTP redirect is forbidden")
    _require(response.status == 200, "official source did not return HTTP 200")
    _require(type(response.body) is bytes, "official response body is invalid")
    _require(len(response.body) <= MAX_RESPONSE_BYTES, "official response exceeds response cap")
    return response.body


def _active_mexc_spot_instruments(payload: Any) -> tuple[str, ...]:
    _require(isinstance(payload, dict), "MEXC metadata response must be an object")
    rows = payload.get("symbols")
    _require(isinstance(rows, list), "MEXC metadata symbols must be an array")
    active: set[str] = set()
    for row in rows:
        _require(isinstance(row, dict), "MEXC metadata row must be an object")
        if str(row.get("quoteAsset") or "").upper() != "USDT":
            continue
        if row.get("isSpotTradingAllowed") is False:
            continue
        if str(row.get("status") or "").upper() not in {"1", "ENABLED", "TRADING"}:
            continue
        symbol = str(row.get("symbol") or "").upper()
        _require(
            SAFE_MEXC_SPOT_PATTERN.fullmatch(symbol) is not None,
            "MEXC active spot identifier is invalid",
        )
        base = str(row.get("baseAsset") or "").upper()
        _require(symbol == collected_spot_instrument("mexc", base), "MEXC active spot base mismatch")
        _require(symbol not in active, "MEXC duplicate active spot pair")
        active.add(symbol)
    return tuple(sorted(active))


def _active_gateio_spot_instruments(payload: Any) -> tuple[str, ...]:
    _require(isinstance(payload, list), "Gate metadata response must be an array")
    active: set[str] = set()
    for row in payload:
        _require(isinstance(row, dict), "Gate metadata row must be an object")
        if str(row.get("quote") or "").upper() != "USDT":
            continue
        if str(row.get("trade_status") or "").lower() != "tradable":
            continue
        name = str(row.get("id") or row.get("name") or "").upper()
        _require(
            SAFE_GATE_SPOT_PATTERN.fullmatch(name) is not None,
            "Gate active spot identifier is invalid",
        )
        base = str(row.get("base") or "").upper()
        if base:
            _require(
                name == collected_spot_instrument("gateio", base),
                "Gate active spot base mismatch",
            )
        _require(name not in active, "Gate duplicate active spot pair")
        active.add(name)
    return tuple(sorted(active))


def _active_mexc_instruments(payload: Any) -> tuple[str, ...]:
    _require(isinstance(payload, dict), "MEXC metadata response must be an object")
    _require(payload.get("success") is True and payload.get("code") in (0, "0"), "MEXC metadata response did not report success")
    rows = payload.get("data")
    _require(isinstance(rows, list), "MEXC metadata data must be an array")
    active: set[str] = set()
    for row in rows:
        _require(isinstance(row, dict), "MEXC metadata row must be an object")
        if str(row.get("quoteCoin") or "").upper() != "USDT":
            continue
        if str(row.get("settleCoin") or "").upper() != "USDT":
            continue
        if row.get("state") not in (0, "0") or row.get("apiAllowed") is False:
            continue
        symbol = str(row.get("symbol") or "").upper()
        _require(SAFE_CONTRACT_PATTERN.fullmatch(symbol) is not None, "MEXC active contract identifier is invalid")
        base = str(row.get("baseCoin") or "").upper()
        _require(base == symbol.removesuffix("_USDT"), "MEXC active contract base mismatch")
        _require(symbol not in active, "MEXC duplicate active contract")
        active.add(symbol)
    return tuple(sorted(active))


def _active_gateio_instruments(payload: Any) -> tuple[str, ...]:
    _require(isinstance(payload, list), "Gate metadata response must be an array")
    active: set[str] = set()
    for row in payload:
        _require(isinstance(row, dict), "Gate metadata row must be an object")
        if str(row.get("status") or "").lower() != "trading":
            continue
        if row.get("in_delisting") is True:
            continue
        name = str(row.get("name") or "").upper()
        _require(SAFE_CONTRACT_PATTERN.fullmatch(name) is not None, "Gate active contract identifier is invalid")
        _require(name not in active, "Gate duplicate active contract")
        active.add(name)
    return tuple(sorted(active))


def collision_metadata_ambiguity_bases(
    metadata_active_instruments: Mapping[str, Sequence[str]],
) -> tuple[str, ...]:
    ambiguous: list[str] = []
    for base in COLLISION_FAIL_CLOSED_BASES:
        for venue in EXPECTED_VENUES:
            expected = collected_spot_instrument(venue, base)
            instruments = tuple(metadata_active_instruments.get(venue) or ())
            related = [
                instrument
                for instrument in instruments
                if instrument == expected or instrument.startswith(base)
            ]
            if len(related) != 1 or expected not in related:
                ambiguous.append(base)
                break
    return tuple(ambiguous)


def _validate_request_plan_item(item: Mapping[str, Any]) -> dict[str, Any]:
    _require(set(item) == REQUEST_PLAN_FIELDS, "request plan field set changed")
    provisional = dict(item)
    fragment = provisional.get("sanitized_evidence_fragment")
    _require(type(fragment) is str, "sanitized evidence fragment is missing")
    provisional["response_body_sha256"] = "0" * 64
    provisional["evidence_fragment_sha256"] = hashlib.sha256(fragment.encode("utf-8")).hexdigest()
    _validated_evidence_record(provisional)
    return copy.deepcopy(dict(item))


def _assert_identity_tokens_in_official_text(
    item: Mapping[str, Any], text: str
) -> None:
    base = str(item["base_ticker"])
    _require(
        re.search(rf"(?<![A-Za-z0-9]){re.escape(base)}(?![A-Za-z0-9])", text)
        is not None,
        "exact identity evidence base ticker was not found",
    )
    label = str(item["canonical_asset_identifier_label"])
    label_pattern = r"[\s_.:-]*".join(re.escape(part) for part in re.split(r"[\s_.:-]+", label))
    _require(
        re.search(label_pattern, text, flags=re.IGNORECASE) is not None,
        "exact identity evidence identifier label was not found",
    )
    identifier = str(item["canonical_asset_identifier_value"])
    flags = (
        re.IGNORECASE
        if item["canonical_asset_identifier_namespace"] == "EVM_CONTRACT"
        else 0
    )
    _require(
        re.search(re.escape(identifier), text, flags=flags) is not None,
        "exact identity evidence canonical identifier was not found",
    )


def collect_identity_evidence(
    request_plan: Sequence[Mapping[str, Any]],
    *,
    fetch: Callable[[str], FetchedResponse] = fetch_official_response,
    monotonic: Callable[[], float] = time.monotonic,
) -> list[dict[str, Any]]:
    return list(
        collect_identity_evidence_bundle(
            request_plan,
            fetch=fetch,
            monotonic=monotonic,
        ).records
    )


def collect_identity_evidence_bundle(
    request_plan: Sequence[Mapping[str, Any]],
    *,
    fetch: Callable[[str], FetchedResponse] = fetch_official_response,
    monotonic: Callable[[], float] = time.monotonic,
    safety_check: Callable[[], None] | None = None,
) -> IdentityEvidenceBundle:
    _require(not isinstance(request_plan, (str, bytes)), "request plan must be a sequence")
    _require(
        0 < len(request_plan) <= MAX_TOTAL_HTTP_REQUESTS - len(OFFICIAL_SPOT_METADATA_ENDPOINTS),
        "request plan exceeds HTTP request cap after metadata checks",
    )
    started = monotonic()
    requests_by_url: dict[str, int] = {}
    request_count = 0
    response_hashes: list[str] = []

    def fetch_counted(url: str) -> bytes:
        nonlocal request_count
        if safety_check is not None:
            safety_check()
        _require(monotonic() - started <= MAX_RUNTIME_SEC, "identity verification runtime cap exceeded")
        _require(request_count < MAX_TOTAL_HTTP_REQUESTS, "identity verification HTTP request cap exceeded")
        requests_by_url[url] = requests_by_url.get(url, 0) + 1
        _require(requests_by_url[url] <= MAX_ATTEMPTS_PER_URL, "attempt cap per URL exceeded")
        request_count += 1
        body = _validated_response_body(fetch(url), url)
        if safety_check is not None:
            safety_check()
        response_hashes.append(hashlib.sha256(body).hexdigest())
        return body

    metadata: dict[str, tuple[str, ...]] = {}
    for venue in EXPECTED_VENUES:
        endpoint = OFFICIAL_SPOT_METADATA_ENDPOINTS[venue]
        body = fetch_counted(endpoint)
        try:
            payload = _strict_json_loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise IdentityVerificationError(f"{venue} metadata response is not valid JSON") from exc
        metadata[venue] = (
            _active_mexc_spot_instruments(payload)
            if venue == "mexc"
            else _active_gateio_spot_instruments(payload)
        )

    records: list[dict[str, Any]] = []
    missing_metadata: list[str] = []
    for raw_item in request_plan:
        _require(isinstance(raw_item, Mapping), "request plan item is invalid")
        item = _validate_request_plan_item(raw_item)
        venue = item["venue"]
        instrument = item["instrument_id"]
        if instrument not in metadata[venue]:
            missing_metadata.append(f"{venue}:{instrument}")
            continue
        url = item["official_source_url"]
        body = fetch_counted(url)
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise IdentityVerificationError("official response is not valid UTF-8") from exc
        _assert_identity_tokens_in_official_text(item, text)
        record = dict(item)
        record["response_body_sha256"] = hashlib.sha256(body).hexdigest()
        record["evidence_fragment_sha256"] = hashlib.sha256(
            item["sanitized_evidence_fragment"].encode("utf-8")
        ).hexdigest()
        records.append({key: record[key] for key in EVIDENCE_FIELDS})
    return IdentityEvidenceBundle(
        records=tuple(records),
        response_body_hashes=tuple(sorted(set(response_hashes))),
        metadata_active_instruments={
            venue: metadata[venue] for venue in EXPECTED_VENUES
        },
        missing_metadata_instruments=tuple(sorted(set(missing_metadata))),
        request_count=request_count,
    )


def validate_execution_manifest(
    execution_manifest: Mapping[str, Any],
    *,
    phase1_manifest: Mapping[str, Any],
    approval_receipt_snapshot: Mapping[str, Any] | None = None,
    approval_receipt_file_sha256: str | None = None,
) -> None:
    _require(
        set(execution_manifest)
        == {
            "schema",
            "status",
            "execution_authorized",
            "execution_approval",
            "phase1_runtime_manifest",
            "authorized_scope",
            "limits",
            "single_use",
            "stopped_incomplete_retry_authorized",
            "request_plan",
            "output_path",
            "manifest_hash_method",
            "manifest_hash",
        },
        "execution approval manifest field set changed",
    )
    _require(execution_manifest.get("schema") == EXECUTION_MANIFEST_SCHEMA, "execution approval manifest schema mismatch")
    _require(execution_manifest.get("status") == EXECUTION_APPROVED_STATUS, "separate exact execution approval is missing")
    _require(execution_manifest.get("execution_authorized") is True, "separate exact execution approval is missing")
    approval = execution_manifest.get("execution_approval")
    _require(isinstance(approval, dict) and approval.get("status") == "APPROVED", "separate exact execution approval is missing")
    _require(
        set(approval)
        == {
            "status",
            "path",
            "file_sha256",
            "receipt_hash",
            "user_approval_text",
            "approved_at_utc",
        },
        "execution approval binding field set changed",
    )
    _require(type(approval.get("user_approval_text")) is str and approval["user_approval_text"].strip() != "", "separate exact execution approval is missing")
    _validate_timestamp(approval.get("approved_at_utc"), "execution approval timestamp")
    _require_hash(approval.get("receipt_hash"), "execution approval receipt hash")
    approval_path = Path(str(approval.get("path", ""))).expanduser().resolve()
    expected_approval_sha = _require_hash(
        approval.get("file_sha256"), "execution approval file hash"
    )
    if approval_receipt_snapshot is None:
        observed_approval_sha = _sha256_file(approval_path)
        approval_receipt = _load_json(approval_path, "execution approval receipt")
    else:
        observed_approval_sha = approval_receipt_file_sha256
        approval_receipt = dict(approval_receipt_snapshot)
    _require(
        observed_approval_sha == expected_approval_sha,
        "execution approval receipt file hash mismatch",
    )
    _require(approval_receipt.get("schema") == EXECUTION_RECEIPT_SCHEMA, "execution approval receipt schema mismatch")
    _require(approval_receipt.get("status") == "APPROVED_SINGLE_USE", "execution approval receipt is not approved")
    _require(approval_receipt.get("receipt_hash_method") == "sha256_canonical_json_excluding_receipt_hash", "execution approval receipt hash method mismatch")
    _require(
        approval_receipt.get("receipt_hash")
        == canonical_hash_without(approval_receipt, "receipt_hash"),
        "execution approval receipt canonical hash mismatch",
    )
    _require(approval_receipt.get("receipt_hash") == approval.get("receipt_hash"), "execution approval receipt hash binding mismatch")
    _require(approval_receipt.get("user_approval_text") == approval.get("user_approval_text"), "execution approval text binding mismatch")
    _require(approval_receipt.get("approved_at_utc") == approval.get("approved_at_utc"), "execution approval timestamp binding mismatch")
    _require(
        set(approval_receipt)
        == {
            "schema",
            "status",
            "approved_at_utc",
            "user_approval_text",
            "approval_provenance",
            "phase1_runtime_manifest",
            "request_plan_sha256",
            "authorized_scope",
            "limits",
            "authoritative_guard_contract",
            "single_use",
            "stopped_incomplete_retry_authorized",
            "receipt_hash_method",
            "receipt_hash",
        },
        "execution approval receipt field set changed",
    )
    provenance = approval_receipt.get("approval_provenance")
    _require(
        isinstance(provenance, Mapping),
        "execution approval provenance is missing",
    )
    _require(
        dict(provenance)
        == {
            "mode": "MANUAL_CODEX_CHECKPOINT_AFTER_DIRECT_USER_APPROVAL",
            "runtime_minting_allowed": False,
            "launcher_minting_allowed": False,
        },
        "execution approval provenance is not an external manual checkpoint",
    )
    guard_contract = approval_receipt.get("authoritative_guard_contract")
    _require(
        isinstance(guard_contract, dict),
        "execution approval authoritative guard contract is missing",
    )
    _require(
        set(guard_contract)
        == {
            "required_guard_decision",
            "required_readiness_source_status",
            "required_readiness_checkpoint_id",
            "required_policy_file_sha256",
        },
        "execution approval authoritative guard contract field set changed",
    )
    _require(
        guard_contract.get("required_guard_decision") == REQUIRED_GUARD_DECISION,
        "execution approval guard decision mismatch",
    )
    _require(
        guard_contract.get("required_readiness_source_status")
        == REQUIRED_READINESS_SOURCE_STATUS,
        "execution approval readiness status mismatch",
    )
    _require(
        guard_contract.get("required_readiness_checkpoint_id")
        == REQUIRED_READINESS_CHECKPOINT_ID,
        "execution approval readiness checkpoint mismatch",
    )
    _require_hash(
        guard_contract.get("required_policy_file_sha256"),
        "execution approval required policy file hash",
    )

    binding = execution_manifest.get("phase1_runtime_manifest")
    _require(isinstance(binding, dict), "phase1 runtime manifest binding is missing")
    _require(binding.get("manifest_hash") == phase1_manifest.get("manifest_hash"), "phase1 runtime manifest hash mismatch")
    _require_hash(binding.get("file_sha256"), "phase1 runtime manifest file hash")
    _require(type(binding.get("path")) is str and binding["path"] != "", "phase1 runtime manifest path is missing")
    _require(approval_receipt.get("phase1_runtime_manifest") == binding, "execution receipt phase1 binding mismatch")

    authorization = execution_manifest.get("authorized_scope")
    _require(isinstance(authorization, dict), "execution authorized scope is missing")
    _require(
        set(authorization)
        == {
            "one_visible_public_read_only_identity_run",
            "official_source_content_read",
            "technical_identity_output",
            "global_writer_claim",
            "candidate_planonly_creation",
            "collector_or_evaluator",
            "oos",
            "returns_or_pnl",
            "grid_or_retune",
            "execution_probe",
            "paper_or_live",
            "private_api",
            "real_capital",
            "leverage_or_margin",
        },
        "execution authorized scope field set changed",
    )
    for key in (
        "one_visible_public_read_only_identity_run",
        "official_source_content_read",
        "technical_identity_output",
        "global_writer_claim",
    ):
        _require(authorization.get(key) is True, f"execution approval scope is missing: {key}")
    for key in (
        "candidate_planonly_creation",
        "collector_or_evaluator",
        "oos",
        "returns_or_pnl",
        "grid_or_retune",
        "execution_probe",
        "paper_or_live",
        "private_api",
        "real_capital",
        "leverage_or_margin",
    ):
        _require(authorization.get(key) is False, f"forbidden execution scope enabled: {key}")

    limits = execution_manifest.get("limits")
    _require(isinstance(limits, dict), "execution limits are missing")
    _require(
        set(limits)
        == {
            "maximum_total_http_requests",
            "maximum_attempts_per_url",
            "maximum_response_bytes_per_request",
            "max_runtime_sec",
            "hard_output_cap_bytes",
        },
        "execution limits field set changed",
    )
    _require(limits.get("maximum_total_http_requests") == MAX_TOTAL_HTTP_REQUESTS, "execution HTTP request cap changed")
    _require(limits.get("maximum_attempts_per_url") == MAX_ATTEMPTS_PER_URL, "execution attempt cap changed")
    _require(limits.get("maximum_response_bytes_per_request") == MAX_RESPONSE_BYTES, "execution response cap changed")
    _require(limits.get("max_runtime_sec") == MAX_RUNTIME_SEC, "execution runtime cap changed")
    _require(limits.get("hard_output_cap_bytes") == MAX_OUTPUT_BYTES, "execution output cap changed")
    _require(execution_manifest.get("single_use") is True, "execution is not single-use")
    _require(execution_manifest.get("stopped_incomplete_retry_authorized") is False, "STOPPED_INCOMPLETE retry enabled")
    request_plan = execution_manifest.get("request_plan")
    _require(isinstance(request_plan, list), "execution request plan is missing")
    _require(
        len(request_plan) == len(EXPECTED_BASES) * len(EXPECTED_VENUES),
        "execution request plan must cover all exact venue/base pairs",
    )
    observed_pairs: set[tuple[str, str]] = set()
    for item in request_plan:
        _require(isinstance(item, Mapping), "execution request plan item is invalid")
        validated = _validate_request_plan_item(item)
        pair = (validated["venue"], validated["base_ticker"])
        _require(pair not in observed_pairs, "execution request plan contains a duplicate venue/base pair")
        observed_pairs.add(pair)
    expected_pairs = {
        (venue, base) for venue in EXPECTED_VENUES for base in EXPECTED_BASES
    }
    _require(observed_pairs == expected_pairs, "execution request plan does not cover the exact universe")
    request_plan_hash = hashlib.sha256(canonical_json_bytes(request_plan)).hexdigest()
    _require(
        approval_receipt.get("request_plan_sha256") == request_plan_hash,
        "execution approval receipt request plan hash mismatch",
    )
    _require(approval_receipt.get("authorized_scope") == authorization, "execution approval scope receipt mismatch")
    _require(approval_receipt.get("limits") == limits, "execution approval limits receipt mismatch")
    _require(approval_receipt.get("single_use") is True, "execution approval receipt is not single-use")
    _require(
        approval_receipt.get("stopped_incomplete_retry_authorized") is False,
        "execution approval receipt authorizes STOPPED_INCOMPLETE retry",
    )
    required_approval_fragments = (
        "Разрешаю один видимый public read-only запуск",
        PROPOSAL_ID,
        f"runtime_manifest_path={binding['path']}",
        f"runtime_manifest_file_sha256={binding['file_sha256']}",
        f"runtime_manifest_hash={binding['manifest_hash']}",
        f"request_plan_sha256={request_plan_hash}",
        (
            "required_policy_file_sha256="
            f"{guard_contract['required_policy_file_sha256']}"
        ),
        f"maximum_total_http_requests={MAX_TOTAL_HTTP_REQUESTS}",
        f"maximum_attempts_per_url={MAX_ATTEMPTS_PER_URL}",
        f"maximum_response_bytes_per_request={MAX_RESPONSE_BYTES}",
        f"max_runtime_sec={MAX_RUNTIME_SEC}",
        f"hard_output_cap_bytes={MAX_OUTPUT_BYTES}",
        "STOPPED_INCOMPLETE не повторять",
        "Без candidate PlanOnly",
        "collector/evaluator",
        "OOS",
        "returns/PnL",
        "grid/retune",
        "execution probe",
        "paper/live",
        "private API",
        "реальных денег",
        "плеча или маржи",
    )
    approval_text = str(approval_receipt["user_approval_text"])
    _require(
        all(fragment in approval_text for fragment in required_approval_fragments),
        "execution approval text does not bind the exact runtime, request plan, limits, and prohibitions",
    )
    _require(execution_manifest.get("manifest_hash_method") == "sha256_canonical_json_excluding_manifest_hash", "execution manifest hash method mismatch")
    observed = _require_hash(execution_manifest.get("manifest_hash"), "execution manifest hash")
    _require(observed == canonical_hash_without(execution_manifest, "manifest_hash"), "execution manifest hash mismatch")



def load_execution_snapshot(
    *,
    runtime_manifest_path: str | Path,
    execution_manifest_path: str | Path,
    output_path: str | Path,
) -> ExecutionSnapshot:
    runtime_path, runtime_sha, phase1 = _read_json_snapshot(
        runtime_manifest_path, "phase1 runtime manifest"
    )
    validate_runtime_manifest(phase1)
    execution_path, execution_sha, execution = _read_json_snapshot(
        execution_manifest_path, "execution approval manifest"
    )
    approval_binding = execution.get("execution_approval")
    _require(isinstance(approval_binding, Mapping), "execution approval binding is missing")
    approval_path, approval_sha, approval = _read_json_snapshot(
        approval_binding.get("path", ""), "execution approval receipt"
    )
    validate_execution_manifest(
        execution,
        phase1_manifest=phase1,
        approval_receipt_snapshot=approval,
        approval_receipt_file_sha256=approval_sha,
    )
    phase1_binding = execution["phase1_runtime_manifest"]
    _require(
        Path(phase1_binding["path"]).expanduser().resolve() == runtime_path,
        "execution phase1 path mismatch",
    )
    _require(
        runtime_sha == phase1_binding["file_sha256"],
        "execution phase1 file hash mismatch",
    )
    expected_output = Path(
        phase1["output_contract"]["run_output_path"]
    ).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    _require(output == expected_output, "execution output path mismatch")
    _require(
        execution.get("output_path") == str(expected_output),
        "execution output binding mismatch",
    )
    _require(not output.exists(), "identity output already exists; duplicate run is forbidden")
    _require(
        approval_sha == approval_binding["file_sha256"],
        "execution approval receipt file hash mismatch",
    )
    _require(
        approval.get("receipt_hash") == approval_binding["receipt_hash"],
        "execution approval receipt hash binding mismatch",
    )
    return ExecutionSnapshot(
        runtime_manifest_path=runtime_path,
        runtime_manifest_file_sha256=runtime_sha,
        runtime_manifest=copy.deepcopy(phase1),
        execution_manifest_path=execution_path,
        execution_manifest_file_sha256=execution_sha,
        execution_manifest=copy.deepcopy(execution),
        execution_approval_receipt_path=approval_path,
        execution_approval_receipt_file_sha256=approval_sha,
        execution_approval_receipt=copy.deepcopy(approval),
        request_plan=tuple(
            copy.deepcopy(dict(item)) for item in execution["request_plan"]
        ),
    )


def validate_exact_guard_snapshot(
    guard: Mapping[str, Any], *, snapshot: ExecutionSnapshot
) -> None:
    _require(guard.get("status") == "ACTIVE", "authoritative guard is not active")
    _require(guard.get("stop_new_actions") is False, "authoritative guard stops new actions")
    _require(
        guard.get("decision") == REQUIRED_GUARD_DECISION,
        "authoritative guard exact execution decision mismatch",
    )
    usage = guard.get("usage")
    _require(isinstance(usage, Mapping), "authoritative guard usage is missing")
    _require(
        usage.get("status") == "AVAILABLE" and usage.get("decision") == "CONTINUE",
        "authoritative guard usage is unavailable",
    )
    remaining = usage.get("remaining_percent")
    _require(
        type(remaining) in (int, float) and float(remaining) > 15.0,
        "authoritative weekly quota blocks execution",
    )
    gate = guard.get("gate")
    _require(isinstance(gate, Mapping), "authoritative active-run gate is missing")
    _require(
        gate.get("status") not in ("RUNNING", "STOPPED_INCOMPLETE"),
        "authoritative active-run gate blocks execution",
    )
    receipt = snapshot.execution_manifest["execution_approval"]
    approval_payload = snapshot.execution_approval_receipt
    contract = approval_payload.get("authoritative_guard_contract")
    _require(
        isinstance(contract, Mapping),
        "execution approval authoritative guard contract is missing",
    )
    expected_contract = {
        "required_guard_decision": REQUIRED_GUARD_DECISION,
        "required_readiness_source_status": REQUIRED_READINESS_SOURCE_STATUS,
        "required_readiness_checkpoint_id": REQUIRED_READINESS_CHECKPOINT_ID,
        "required_policy_file_sha256": _require_hash(
            contract.get("required_policy_file_sha256"), "required policy file hash"
        ),
    }
    _require(
        dict(contract) == expected_contract,
        "execution approval authoritative guard contract field set changed",
    )
    _require(
        guard.get("policy_hash") == expected_contract["required_policy_file_sha256"],
        "authoritative guard policy hash mismatch",
    )
    readiness = guard.get("current_sprint_readiness")
    _require(isinstance(readiness, Mapping), "authoritative readiness is missing")
    _require(readiness.get("status") == "READY", "authoritative readiness is not ready")
    _require(
        readiness.get("source_status")
        == expected_contract["required_readiness_source_status"],
        "authoritative readiness execution state mismatch",
    )
    _require(
        readiness.get("execution_authorized") is True,
        "authoritative readiness does not authorize execution",
    )
    phase2 = readiness.get("official_identity_phase_2")
    _require(isinstance(phase2, Mapping), "authoritative identity phase2 binding is missing")
    request_plan_sha256 = hashlib.sha256(
        canonical_json_bytes(list(snapshot.request_plan))
    ).hexdigest()
    expected_phase2 = {
        "status": EXECUTION_APPROVED_STATUS,
        "runtime_manifest_file_sha256": snapshot.runtime_manifest_file_sha256,
        "runtime_manifest_hash": snapshot.runtime_manifest["manifest_hash"],
        "execution_approval_receipt_file_sha256": (
            snapshot.execution_approval_receipt_file_sha256
        ),
        "execution_approval_receipt_hash": receipt["receipt_hash"],
        "request_plan_sha256": request_plan_sha256,
    }
    for key, value in expected_phase2.items():
        _require(
            phase2.get(key) == value,
            f"authoritative identity phase2 binding mismatch: {key}",
        )
    checkpoints = readiness.get("approval_checkpoints")
    _require(isinstance(checkpoints, list), "authoritative approval checkpoints are missing")
    matching = [
        item
        for item in checkpoints
        if isinstance(item, Mapping)
        and item.get("id") == expected_contract["required_readiness_checkpoint_id"]
    ]
    _require(len(matching) == 1, "authoritative exact identity checkpoint is missing")
    _require(
        matching[0].get("status") == "APPROVED_SINGLE_USE",
        "authoritative exact identity checkpoint is not approved",
    )
    for key, value in expected_phase2.items():
        if key == "status":
            continue
        _require(
            matching[0].get(key) == value,
            f"authoritative exact identity checkpoint mismatch: {key}",
        )


def invoke_authoritative_guard(snapshot: ExecutionSnapshot) -> dict[str, Any]:
    runtime = snapshot.runtime_manifest["runtime"]
    for prefix in (
        "module",
        "launcher",
        "guard_checker",
        "autopilot_guard_module",
        "readiness_module",
    ):
        path = Path(runtime[f"{prefix}_path"]).expanduser().resolve()
        _require(
            _sha256_file(path) == runtime[f"{prefix}_sha256"],
            f"authoritative {prefix} hash mismatch",
        )
    checker = Path(runtime["guard_checker_path"]).expanduser().resolve()
    try:
        completed = subprocess.run(
            [
                "pwsh",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(checker),
                "-Json",
            ],
            cwd=str(
                _repo_root_from_proposal(
                    Path(snapshot.runtime_manifest["proposal"]["path"])
                )
            ),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityVerificationError("authoritative guard is unavailable") from exc
    _require(completed.returncode == 0, "authoritative guard failed")
    try:
        guard = _strict_json_loads(completed.stdout)
    except (json.JSONDecodeError, ValueError) as exc:
        raise IdentityVerificationError("authoritative guard returned invalid JSON") from exc
    _require(isinstance(guard, dict), "authoritative guard returned invalid JSON")
    validate_exact_guard_snapshot(guard, snapshot=snapshot)
    return guard


def validate_execution_snapshot_files_unchanged(snapshot: ExecutionSnapshot) -> None:
    bindings = (
        (
            snapshot.runtime_manifest_path,
            snapshot.runtime_manifest_file_sha256,
            "phase1 runtime manifest",
        ),
        (
            snapshot.execution_manifest_path,
            snapshot.execution_manifest_file_sha256,
            "execution approval manifest",
        ),
        (
            snapshot.execution_approval_receipt_path,
            snapshot.execution_approval_receipt_file_sha256,
            "execution approval receipt",
        ),
    )
    for path, expected, label in bindings:
        _require(_sha256_file(path) == expected, f"{label} changed after snapshot")

def _process_is_alive(process_id: int) -> bool:
    if process_id <= 0:
        return False
    if process_id == os.getpid():
        return True
    if os.name == "nt":
        try:
            import ctypes

            process_query_limited_information = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(
                process_query_limited_information,
                False,
                process_id,
            )
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        except (AttributeError, OSError):
            return False
    try:
        os.kill(process_id, 0)
    except OSError:
        return False
    return True


def _windows_process_snapshot(process_id: int) -> dict[str, Any]:
    _require(os.name == "nt", "visible process identity is supported only on Windows")
    escaped = str(int(process_id))
    command = (
        "$ErrorActionPreference='Stop';"
        f"$p=Get-CimInstance Win32_Process -Filter \"ProcessId={escaped}\";"
        "if($null -eq $p){exit 3};"
        "$p|Select-Object ProcessId,ParentProcessId,"
        "@{n='CreationDate';e={$_.CreationDate.ToUniversalTime().ToString('o')}},"
        "ExecutablePath,CommandLine|"
        "ConvertTo-Json -Compress"
    )
    try:
        result = subprocess.run(
            ["pwsh", "-NoProfile", "-Command", command],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityVerificationError("visible process identity is unavailable") from exc
    _require(result.returncode == 0, "visible process identity is unavailable")
    try:
        payload = _strict_json_loads(result.stdout)
    except (json.JSONDecodeError, ValueError) as exc:
        raise IdentityVerificationError("visible process identity is invalid") from exc
    _require(isinstance(payload, dict), "visible process identity is invalid")
    return payload


def validate_visible_launcher_capability(
    *,
    capability_path: str | Path,
    capability_token: str,
    owner_pid: int,
    writer_pid: int,
    launcher_path: str | Path,
    launcher_file_sha256: str,
    runtime_manifest_file_sha256: str,
    execution_manifest_file_sha256: str,
    output_path: str | Path,
) -> dict[str, Any]:
    _require(
        re.fullmatch(r"[0-9a-f]{64}", capability_token) is not None,
        "launcher capability token is invalid",
    )
    capability = _load_json(capability_path, "launcher capability")
    expected_fields = {
        "schema",
        "status",
        "run_id",
        "owner_pid",
        "writer_pid",
        "owner_process_creation_utc",
        "owner_executable_path",
        "owner_command_line_sha256",
        "writer_process_creation_utc",
        "writer_executable_path",
        "writer_command_line_sha256",
        "launcher_path",
        "launcher_file_sha256",
        "runtime_manifest_file_sha256",
        "execution_manifest_file_sha256",
        "output_path",
        "capability_token_sha256",
        "visible_console_verified",
        "single_use",
        "guard_decision",
        "policy_hash",
        "readiness_hash",
        "guard_observed_at_utc",
        "guard_checked_before_writer_claim",
        "created_at_utc",
    }
    _require(set(capability) == expected_fields, "launcher capability field set changed")
    _require(
        capability.get("schema")
        == "trading_mvp_slow_liquidity_official_identity_launcher_capability_v1",
        "launcher capability schema mismatch",
    )
    _require(capability.get("status") == "ACTIVE", "launcher capability is not active")
    _require(capability.get("run_id") == PROPOSAL_ID, "launcher capability run_id mismatch")
    _require(capability.get("owner_pid") == owner_pid, "launcher capability owner mismatch")
    _require(capability.get("writer_pid") == writer_pid, "launcher capability writer mismatch")
    _require(capability.get("visible_console_verified") is True, "visible console is not verified")
    _require(capability.get("single_use") is True, "launcher capability is not single-use")
    expected_launcher_path = Path(launcher_path).expanduser().resolve()
    expected_launcher_sha = _require_hash(
        launcher_file_sha256, "launcher capability expected launcher hash"
    )
    _require(
        Path(str(capability.get("launcher_path", ""))).expanduser().resolve()
        == expected_launcher_path,
        "launcher capability path mismatch",
    )
    _require(
        capability.get("launcher_file_sha256") == expected_launcher_sha
        and _sha256_file(expected_launcher_path) == expected_launcher_sha,
        "launcher capability file hash mismatch",
    )
    _require(
        capability.get("runtime_manifest_file_sha256")
        == runtime_manifest_file_sha256,
        "launcher capability runtime hash mismatch",
    )
    _require(
        capability.get("execution_manifest_file_sha256")
        == execution_manifest_file_sha256,
        "launcher capability execution hash mismatch",
    )
    _require(
        Path(capability.get("output_path", "")).expanduser().resolve()
        == Path(output_path).expanduser().resolve(),
        "launcher capability output mismatch",
    )
    _require(
        capability.get("capability_token_sha256")
        == hashlib.sha256(capability_token.encode("ascii")).hexdigest(),
        "launcher capability token mismatch",
    )
    owner = _windows_process_snapshot(owner_pid)
    writer = _windows_process_snapshot(writer_pid)
    owner_command_line = str(owner.get("CommandLine") or "")
    _require(
        str(owner.get("CreationDate")) == capability["owner_process_creation_utc"],
        "launcher capability owner process was replaced",
    )
    _require(
        str(owner.get("ExecutablePath")) == capability["owner_executable_path"],
        "launcher capability owner executable mismatch",
    )
    _require(
        hashlib.sha256(owner_command_line.encode()).hexdigest()
        == capability["owner_command_line_sha256"],
        "launcher capability owner command mismatch",
    )
    file_argument = re.search(
        r"(?i)(?:^|\s)-File(?:\s+|=)(?:\"([^\"]+)\"|'([^']+)'|(\S+))",
        owner_command_line,
    )
    _require(file_argument is not None, "visible owner did not execute a script file")
    executed_script = next(
        value for value in file_argument.groups() if value is not None
    )
    _require(
        Path(executed_script).expanduser().resolve() == expected_launcher_path,
        "visible owner did not execute the exact approved launcher",
    )
    _require(
        re.search(r"(?i)(?:^|\s)-VisibleWorker(?:\s|$)", owner_command_line)
        is not None,
        "visible owner is not the approved launcher worker",
    )
    _require(
        str(writer.get("CreationDate")) == capability["writer_process_creation_utc"],
        "launcher capability writer process was replaced",
    )
    _require(
        str(writer.get("ExecutablePath")) == capability["writer_executable_path"],
        "launcher capability writer executable mismatch",
    )
    _require(
        hashlib.sha256(str(writer.get("CommandLine") or "").encode()).hexdigest()
        == capability["writer_command_line_sha256"],
        "launcher capability writer command mismatch",
    )
    _require(
        int(writer.get("ParentProcessId") or 0) == owner_pid,
        "identity writer is not a child of the visible owner",
    )
    return capability


def validate_preclaim_guard_attestation(
    capability: Mapping[str, Any],
    execution_approval_receipt: Mapping[str, Any],
    *,
    now_utc: datetime | None = None,
) -> None:
    _require(
        capability.get("guard_checked_before_writer_claim") is True,
        "launcher guard was not checked before the writer claim",
    )
    contract = execution_approval_receipt.get("authoritative_guard_contract")
    _require(isinstance(contract, Mapping), "execution guard contract is missing")
    _require(
        capability.get("guard_decision") == contract.get("required_guard_decision"),
        "pre-claim guard decision mismatch",
    )
    _require(
        capability.get("policy_hash") == contract.get("required_policy_file_sha256"),
        "pre-claim guard policy hash mismatch",
    )
    _require_hash(capability.get("readiness_hash"), "pre-claim readiness hash")
    observed_text = _validate_timestamp(
        capability.get("guard_observed_at_utc"), "pre-claim guard timestamp"
    )
    observed = datetime.fromisoformat(observed_text[:-1] + "+00:00")
    current = now_utc or datetime.now(timezone.utc)
    _require(current.tzinfo is not None, "pre-claim guard comparison time is naive")
    age_sec = (current.astimezone(timezone.utc) - observed).total_seconds()
    _require(-5.0 <= age_sec <= 60.0, "pre-claim guard attestation is stale")


def wait_for_visible_launcher_capability(
    *,
    capability_path: str | Path,
    capability_token: str,
    owner_pid: int,
    writer_pid: int,
    launcher_path: str | Path,
    launcher_file_sha256: str,
    runtime_manifest_file_sha256: str,
    execution_manifest_file_sha256: str,
    output_path: str | Path,
    wait_sec: float,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    _require(0.0 <= wait_sec <= 10.0, "launcher capability wait cap is invalid")
    deadline = monotonic() + wait_sec
    last_error = "launcher capability was not created"
    while True:
        try:
            return validate_visible_launcher_capability(
                capability_path=capability_path,
                capability_token=capability_token,
                owner_pid=owner_pid,
                writer_pid=writer_pid,
                launcher_path=launcher_path,
                launcher_file_sha256=launcher_file_sha256,
                runtime_manifest_file_sha256=runtime_manifest_file_sha256,
                execution_manifest_file_sha256=execution_manifest_file_sha256,
                output_path=output_path,
            )
        except IdentityVerificationError as exc:
            last_error = str(exc)
        if monotonic() >= deadline:
            raise IdentityVerificationError(
                f"visible launcher capability failed: {last_error}"
            )
        sleep(0.05)


def validate_global_writer_claim(
    *,
    claim_path: str | Path,
    run_id: str,
    owner_pid: int,
    writer_pid: int,
    ownership_token: str,
    output_path: str | Path,
) -> dict[str, Any]:
    _require(re.fullmatch(r"[0-9a-f]{32}", ownership_token) is not None, "global writer claim token is invalid")
    claim = _load_json(claim_path, "global writer claim")
    _require(claim.get("schema") == "trading_mvp_global_market_writer_claim_v1", "global writer claim schema mismatch")
    _require(claim.get("project") == "trading_mvp", "global writer claim project mismatch")
    _require(claim.get("status") == "CLAIMED", "global writer claim is not active")
    _require(claim.get("run_id") == run_id, "global writer claim run_id mismatch")
    _require(claim.get("owner_kind") == "slow_liquidity_official_identity", "global writer claim owner kind mismatch")
    _require(claim.get("owner_pid") == owner_pid, "global writer claim owner PID mismatch")
    _require(writer_pid == os.getpid(), "runtime writer PID does not match the current process")
    _require(claim.get("writer_pid") == writer_pid, "global writer claim writer PID mismatch")
    _require(claim.get("terminal_pid") == owner_pid, "global writer claim terminal PID mismatch")
    _require(claim.get("ownership_token") == ownership_token, "global writer claim token mismatch")
    _require(_process_is_alive(owner_pid), "global writer claim visible owner is not alive")
    _require(_process_is_alive(writer_pid), "global writer claim runtime writer is not alive")
    _require(
        Path(str(claim.get("output_namespace", ""))).expanduser().resolve()
        == Path(output_path).expanduser().resolve(),
        "global writer claim output namespace mismatch",
    )
    _require(claim.get("research_only") is True, "global writer claim is not research-only")
    for key in ("live_orders", "private_api_keys", "real_capital", "leverage_or_margin"):
        _require(claim.get(key) is False, f"global writer claim safety mismatch: {key}")
    return claim


def wait_for_global_writer_claim(
    *,
    claim_path: str | Path,
    run_id: str,
    owner_pid: int,
    writer_pid: int,
    ownership_token: str,
    output_path: str | Path,
    wait_sec: float,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    _require(0.0 <= wait_sec <= 10.0, "global writer claim wait cap is invalid")
    deadline = monotonic() + wait_sec
    last_error = "global writer claim was not attached"
    while True:
        try:
            return validate_global_writer_claim(
                claim_path=claim_path,
                run_id=run_id,
                owner_pid=owner_pid,
                writer_pid=writer_pid,
                ownership_token=ownership_token,
                output_path=output_path,
            )
        except IdentityVerificationError as exc:
            last_error = str(exc)
        if monotonic() >= deadline:
            raise IdentityVerificationError(
                f"global writer claim attachment failed: {last_error}"
            )
        sleep(0.05)


def preflight_execution(
    *,
    runtime_manifest_path: str | Path,
    execution_manifest_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    runtime_path = Path(runtime_manifest_path).expanduser().resolve()
    execution_path = Path(execution_manifest_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    try:
        snapshot = load_execution_snapshot(
            runtime_manifest_path=runtime_path,
            execution_manifest_path=execution_path,
            output_path=output,
        )
    except IdentityVerificationError as exc:
        return {
            "status": "BLOCKED_AWAIT_EXACT_CODE_BOUND_EXECUTION_APPROVAL",
            "reason": str(exc),
            "network_accessed": False,
            "identity_output_created": False,
            "output_path": str(output),
        }
    return {
        "status": "READY_EXACT_CODE_BOUND_EXECUTION_APPROVAL",
        "runtime_manifest_path": str(runtime_path),
        "runtime_manifest_file_sha256": snapshot.runtime_manifest_file_sha256,
        "runtime_manifest_hash": snapshot.runtime_manifest["manifest_hash"],
        "execution_manifest_path": str(execution_path),
        "execution_manifest_file_sha256": snapshot.execution_manifest_file_sha256,
        "execution_manifest_hash": snapshot.execution_manifest["manifest_hash"],
        "output_path": str(output),
        "network_accessed": False,
        "identity_output_created": False,
    }


def _reject_prohibited_output_values(value: Any, path: str = "root") -> None:
    if isinstance(value, Mapping):
        forbidden_keys = {
            "raw_payload",
            "raw_response",
            "response_body",
            "funding_rate",
            "funding_rates",
            "price",
            "prices",
            "mark_price",
            "index_price",
        }
        for key, child in value.items():
            _require(key not in forbidden_keys, f"prohibited output field: {path}.{key}")
            _reject_prohibited_output_values(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_prohibited_output_values(child, f"{path}[{index}]")


def _validate_output_source_bindings(
    bindings: Mapping[str, Mapping[str, Any]]
) -> dict[str, dict[str, str]]:
    allowed_fields = {
        "proposal": {"path", "file_sha256", "proposal_hash"},
        "offline_approval_receipt": {"path", "file_sha256", "receipt_hash"},
        "phase1_runtime_manifest": {"path", "file_sha256", "manifest_hash"},
        "execution_manifest": {"path", "file_sha256", "manifest_hash"},
        "execution_approval_receipt": {"path", "file_sha256", "receipt_hash"},
    }
    _require(set(bindings) == set(allowed_fields), "identity output source binding set changed")
    normalized: dict[str, dict[str, str]] = {}
    for label, binding in bindings.items():
        _require(isinstance(binding, Mapping), f"identity output source binding is invalid: {label}")
        _require(
            set(binding) == allowed_fields[label],
            f"identity output source binding field set changed: {label}",
        )
        path = binding.get("path")
        _require(type(path) is str and path != "", f"{label} source path is missing")
        _require_hash(binding.get("file_sha256"), f"{label} source file hash")
        hash_field = next(
            field for field in ("proposal_hash", "receipt_hash", "manifest_hash")
            if field in allowed_fields[label]
        )
        _require_hash(binding.get(hash_field), f"{label} source canonical hash")
        normalized[label] = {key: str(binding[key]) for key in sorted(binding)}
    return normalized


def write_identity_output(
    *,
    output_path: str | Path,
    identity_result: Mapping[str, Any],
    run_id: str,
    proposal_hash: str,
    response_body_hashes: Sequence[str],
    generated_at_utc: str,
    source_bindings: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, str]:
    _require(run_id == PROPOSAL_ID, "identity output run id mismatch")
    _require_hash(proposal_hash, "proposal hash")
    _validate_timestamp(generated_at_utc, "identity output timestamp")
    _require(identity_result.get("schema") == IDENTITY_EVIDENCE_SCHEMA, "identity result schema mismatch")
    _require(identity_result.get("result_hash") == canonical_hash_without(identity_result, "result_hash"), "identity result hash mismatch")
    _reject_prohibited_output_values(identity_result)
    hashes = sorted(set(response_body_hashes))
    _require(len(hashes) > 0, "response body hash set is empty")
    for value in hashes:
        _require_hash(value, "response body hash")
    normalized_bindings = _validate_output_source_bindings(source_bindings or {})
    _reject_prohibited_output_values(normalized_bindings)

    output = Path(output_path).expanduser().resolve()
    _require(not output.exists(), "identity output already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    evidence_payload: dict[str, Any] = {
        "schema": IDENTITY_EVIDENCE_SCHEMA,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "proposal_hash": proposal_hash,
        "source_bindings": normalized_bindings,
        "identity_result": copy.deepcopy(dict(identity_result)),
        "raw_response_persisted": False,
        "prices_or_funding_rates_persisted": False,
        "candidate_planonly_created": False,
        "data_collection_authorized": False,
    }
    evidence_bytes = _json_file_bytes(evidence_payload)
    evidence_sha = hashlib.sha256(evidence_bytes).hexdigest()
    fragment_hashes = sorted(
        {
            item["evidence_fragment_sha256"]
            for item in identity_result.get("sanitized_official_evidence", [])
        }
    )
    manifest_payload: dict[str, Any] = {
        "schema": IDENTITY_OUTPUT_MANIFEST_SCHEMA,
        "run_id": run_id,
        "status": identity_result["status"],
        "final": True,
        "generated_at_utc": generated_at_utc,
        "proposal_hash": proposal_hash,
        "files": {
            "identity-evidence.json": {
                "file_sha256": evidence_sha,
                "bytes": len(evidence_bytes),
            }
        },
        "response_body_sha256": hashes,
        "sanitized_evidence_fragment_sha256": fragment_hashes,
        "identity_result_hash": identity_result["result_hash"],
        "raw_response_persisted": False,
        "prices_or_funding_rates_persisted": False,
        "candidate_planonly_created": False,
        "data_collection_authorized": False,
        "manifest_hash_method": "sha256_canonical_json_excluding_manifest_hash",
    }
    manifest_payload["manifest_hash"] = canonical_hash_without(manifest_payload, "manifest_hash")
    manifest_bytes = _json_file_bytes(manifest_payload)
    _require(len(evidence_bytes) + len(manifest_bytes) <= MAX_OUTPUT_BYTES, "identity output exceeds hard cap")

    stage = Path(tempfile.mkdtemp(prefix=f".{output.name}.stage.", dir=str(output.parent)))
    try:
        with (stage / "identity-evidence.json").open("xb") as handle:
            handle.write(evidence_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        with (stage / "manifest.json").open("xb") as handle:
            handle.write(manifest_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            stage.rename(output)
            _EXECUTION_STATE["identity_output_created"] = True
        except FileExistsError as exc:
            raise IdentityVerificationError("identity output already exists") from exc
    finally:
        if stage.exists():
            shutil.rmtree(stage, ignore_errors=True)
    return {
        "identity-evidence.json": str(output / "identity-evidence.json"),
        "manifest.json": str(output / "manifest.json"),
    }


def run_approved_identity_verification(
    *,
    runtime_manifest_path: str | Path,
    execution_manifest_path: str | Path,
    output_path: str | Path,
    global_writer_claim_path: str | Path,
    owner_pid: int,
    ownership_token: str,
    writer_claim_wait_sec: float,
    launcher_capability_path: str | Path,
    launcher_capability_token: str,
) -> dict[str, Any]:
    snapshot = load_execution_snapshot(
        runtime_manifest_path=runtime_manifest_path,
        execution_manifest_path=execution_manifest_path,
        output_path=output_path,
    )
    phase1 = snapshot.runtime_manifest
    execution = snapshot.execution_manifest
    capability = wait_for_visible_launcher_capability(
        capability_path=launcher_capability_path,
        capability_token=launcher_capability_token,
        owner_pid=owner_pid,
        writer_pid=os.getpid(),
        launcher_path=phase1["runtime"]["launcher_path"],
        launcher_file_sha256=phase1["runtime"]["launcher_sha256"],
        runtime_manifest_file_sha256=snapshot.runtime_manifest_file_sha256,
        execution_manifest_file_sha256=snapshot.execution_manifest_file_sha256,
        output_path=output_path,
        wait_sec=writer_claim_wait_sec,
    )
    validate_execution_snapshot_files_unchanged(snapshot)
    validate_preclaim_guard_attestation(
        capability,
        snapshot.execution_approval_receipt,
    )
    validate_execution_snapshot_files_unchanged(snapshot)
    claim = wait_for_global_writer_claim(
        claim_path=global_writer_claim_path,
        run_id=PROPOSAL_ID,
        owner_pid=owner_pid,
        writer_pid=os.getpid(),
        ownership_token=ownership_token,
        output_path=output_path,
        wait_sec=writer_claim_wait_sec,
    )
    _require(
        claim.get("plan_hash") == phase1["proposal"]["proposal_hash"],
        "global writer claim proposal hash mismatch",
    )

    def enforce_visible_ownership() -> None:
        validate_execution_snapshot_files_unchanged(snapshot)
        validate_visible_launcher_capability(
            capability_path=launcher_capability_path,
            capability_token=launcher_capability_token,
            owner_pid=owner_pid,
            writer_pid=os.getpid(),
            launcher_path=phase1["runtime"]["launcher_path"],
            launcher_file_sha256=phase1["runtime"]["launcher_sha256"],
            runtime_manifest_file_sha256=snapshot.runtime_manifest_file_sha256,
            execution_manifest_file_sha256=snapshot.execution_manifest_file_sha256,
            output_path=output_path,
        )
        validate_global_writer_claim(
            claim_path=global_writer_claim_path,
            run_id=PROPOSAL_ID,
            owner_pid=owner_pid,
            writer_pid=os.getpid(),
            ownership_token=ownership_token,
            output_path=output_path,
        )

    enforce_visible_ownership()
    started = time.monotonic()
    bundle = collect_identity_evidence_bundle(
        snapshot.request_plan,
        safety_check=enforce_visible_ownership,
    )
    _require(time.monotonic() - started <= MAX_RUNTIME_SEC, "identity verification runtime cap exceeded")
    result = build_identity_result(
        bundle.records,
        metadata_ambiguity_bases=collision_metadata_ambiguity_bases(
            bundle.metadata_active_instruments
        ),
    )
    enforce_visible_ownership()
    generated_at = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    written = write_identity_output(
        output_path=output_path,
        identity_result=result,
        run_id=PROPOSAL_ID,
        proposal_hash=phase1["proposal"]["proposal_hash"],
        response_body_hashes=bundle.response_body_hashes,
        generated_at_utc=generated_at,
        source_bindings={
            "proposal": {
                "path": phase1["proposal"]["path"],
                "file_sha256": phase1["proposal"]["file_sha256"],
                "proposal_hash": phase1["proposal"]["proposal_hash"],
            },
            "offline_approval_receipt": copy.deepcopy(
                phase1["offline_approval_receipt"]
            ),
            "phase1_runtime_manifest": {
                "path": str(Path(runtime_manifest_path).expanduser().resolve()),
                "file_sha256": snapshot.runtime_manifest_file_sha256,
                "manifest_hash": phase1["manifest_hash"],
            },
            "execution_manifest": {
                "path": str(Path(execution_manifest_path).expanduser().resolve()),
                "file_sha256": snapshot.execution_manifest_file_sha256,
                "manifest_hash": execution["manifest_hash"],
            },
            "execution_approval_receipt": {
                "path": str(snapshot.execution_approval_receipt_path),
                "file_sha256": snapshot.execution_approval_receipt_file_sha256,
                "receipt_hash": execution["execution_approval"]["receipt_hash"],
            },
        },
    )
    return {
        "status": result["status"],
        "run_id": PROPOSAL_ID,
        "verified_bases": result["verified_bases"],
        "rejected_bases": result["rejected_bases"],
        "unresolved_bases": result["unresolved_bases"],
        "http_request_count": bundle.request_count,
        "missing_metadata_instruments": list(bundle.missing_metadata_instruments),
        "written": written,
        "network_accessed": bool(_EXECUTION_STATE["network_accessed"]),
        "identity_output_created": bool(_EXECUTION_STATE["identity_output_created"]),
        "candidate_planonly_created": False,
        "data_collection_authorized": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--freeze-offline-bundle", action="store_true")
    action.add_argument("--freeze-offline-refreeze-v5", action="store_true")
    action.add_argument("--validate-runtime-manifest", action="store_true")
    action.add_argument("--preflight-execution", action="store_true")
    action.add_argument("--preflight-authoritative-execution", action="store_true")
    action.add_argument("--run-approved", action="store_true")
    parser.add_argument("--proposal-path")
    parser.add_argument("--expected-proposal-hash")
    parser.add_argument("--expected-proposal-file-sha256")
    parser.add_argument("--approval-receipt-path")
    parser.add_argument("--parent-runtime-manifest-path")
    parser.add_argument("--topology-runtime-manifest-path")
    parser.add_argument("--runtime-manifest-path")
    parser.add_argument("--runtime-module-path", default=str(Path(__file__).resolve()))
    parser.add_argument("--synthetic-tests-path")
    parser.add_argument("--launcher-path")
    parser.add_argument("--approved-at-utc")
    parser.add_argument("--generated-at-utc")
    parser.add_argument("--user-authorization-text")
    parser.add_argument("--response-annotation-index", type=int)
    parser.add_argument("--execution-manifest-path")
    parser.add_argument("--output-path")
    parser.add_argument("--global-writer-claim-path")
    parser.add_argument("--owner-pid", type=int)
    parser.add_argument("--ownership-token")
    parser.add_argument("--writer-claim-wait-sec", type=float, default=5.0)
    parser.add_argument("--launcher-capability-path")
    parser.add_argument("--launcher-capability-token")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.freeze_offline_refreeze_v5:
        required = (
            "proposal_path",
            "expected_proposal_hash",
            "expected_proposal_file_sha256",
            "approval_receipt_path",
            "parent_runtime_manifest_path",
            "topology_runtime_manifest_path",
            "runtime_manifest_path",
            "synthetic_tests_path",
            "launcher_path",
            "generated_at_utc",
        )
        _require(
            all(getattr(args, name) is not None for name in required),
            "identity v5 refreeze arguments are incomplete",
        )
        result = freeze_offline_refreeze_v5(
            proposal_path=args.proposal_path,
            expected_proposal_hash=args.expected_proposal_hash,
            expected_proposal_file_sha256=args.expected_proposal_file_sha256,
            approval_receipt_path=args.approval_receipt_path,
            parent_runtime_manifest_path=args.parent_runtime_manifest_path,
            topology_runtime_manifest_path=args.topology_runtime_manifest_path,
            runtime_manifest_path=args.runtime_manifest_path,
            runtime_module_path=args.runtime_module_path,
            synthetic_tests_path=args.synthetic_tests_path,
            launcher_path=args.launcher_path,
            generated_at_utc=args.generated_at_utc,
        )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    if args.freeze_offline_bundle:
        required = (
            "proposal_path",
            "expected_proposal_hash",
            "expected_proposal_file_sha256",
            "approval_receipt_path",
            "runtime_manifest_path",
            "synthetic_tests_path",
            "launcher_path",
            "approved_at_utc",
            "generated_at_utc",
            "user_authorization_text",
            "response_annotation_index",
        )
        _require(all(getattr(args, name) is not None for name in required), "offline freeze arguments are incomplete")
        result = freeze_offline_bundle(
            proposal_path=args.proposal_path,
            expected_proposal_hash=args.expected_proposal_hash,
            expected_proposal_file_sha256=args.expected_proposal_file_sha256,
            approval_receipt_path=args.approval_receipt_path,
            runtime_manifest_path=args.runtime_manifest_path,
            runtime_module_path=args.runtime_module_path,
            synthetic_tests_path=args.synthetic_tests_path,
            launcher_path=args.launcher_path,
            approved_at_utc=args.approved_at_utc,
            generated_at_utc=args.generated_at_utc,
            user_authorization_text=args.user_authorization_text,
            response_annotation_index=args.response_annotation_index,
        )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    if args.validate_runtime_manifest:
        _require(args.runtime_manifest_path is not None, "runtime manifest path is required")
        manifest = _load_json(args.runtime_manifest_path, "runtime manifest")
        validate_runtime_manifest(manifest)
        print(
            json.dumps(
                {
                    "status": PHASE1_STATUS,
                    "runtime_manifest_path": str(Path(args.runtime_manifest_path).resolve()),
                    "runtime_manifest_file_sha256": _sha256_file(args.runtime_manifest_path),
                    "runtime_manifest_hash": manifest["manifest_hash"],
                    "actual_network_run_allowed": False,
                    "identity_output_allowed": False,
                },
                ensure_ascii=False,
            )
        )
        return 0
    _require(args.runtime_manifest_path is not None, "runtime manifest path is required")
    _require(args.execution_manifest_path is not None, "execution manifest path is required")
    _require(args.output_path is not None, "output path is required")
    if args.preflight_execution or args.preflight_authoritative_execution:
        result = preflight_execution(
            runtime_manifest_path=args.runtime_manifest_path,
            execution_manifest_path=args.execution_manifest_path,
            output_path=args.output_path,
        )
        if result["status"] != "READY_EXACT_CODE_BOUND_EXECUTION_APPROVAL":
            print(json.dumps(result, ensure_ascii=False))
            return 2
        if args.preflight_authoritative_execution:
            snapshot = load_execution_snapshot(
                runtime_manifest_path=args.runtime_manifest_path,
                execution_manifest_path=args.execution_manifest_path,
                output_path=args.output_path,
            )
            guard = invoke_authoritative_guard(snapshot)
            result = {
                **result,
                "status": "READY_AUTHORITATIVE_EXACT_CODE_BOUND_EXECUTION",
                "guard_decision": guard["decision"],
                "policy_hash": guard["policy_hash"],
                "readiness_hash": guard["current_sprint_readiness"]["readiness_hash"],
                "guard_observed_at_utc": guard["observed_at_utc"],
            }
        print(json.dumps(result, ensure_ascii=False))
        return 0
    _require(args.global_writer_claim_path is not None, "global writer claim path is required")
    _require(args.owner_pid is not None and args.owner_pid > 0, "visible owner PID is required")
    _require(args.ownership_token is not None, "global writer ownership token is required")
    _require(args.launcher_capability_path is not None, "visible launcher capability is required")
    _require(args.launcher_capability_token is not None, "visible launcher capability token is required")
    result = run_approved_identity_verification(
        runtime_manifest_path=args.runtime_manifest_path,
        execution_manifest_path=args.execution_manifest_path,
        output_path=args.output_path,
        global_writer_claim_path=args.global_writer_claim_path,
        owner_pid=args.owner_pid,
        ownership_token=args.ownership_token,
        writer_claim_wait_sec=args.writer_claim_wait_sec,
        launcher_capability_path=args.launcher_capability_path,
        launcher_capability_token=args.launcher_capability_token,
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except IdentityVerificationError as exc:
        print(
            json.dumps(
                {
                    "status": "BLOCKED",
                    "reason": str(exc),
                    "network_accessed": bool(_EXECUTION_STATE["network_accessed"]),
                    "identity_output_created": bool(
                        _EXECUTION_STATE["identity_output_created"]
                    ),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
