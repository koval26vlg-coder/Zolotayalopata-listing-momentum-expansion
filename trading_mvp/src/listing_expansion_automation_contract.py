"""Immutable public-only wrapper contract; no network or runtime activation."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


REPO_ROOT = Path(__file__).resolve().parents[2]
PLAN_ID = "listing_momentum_expansion_automation_20260826_v4"
PLAN_RELATIVE_PATH = "docs/plans/listing-momentum-expansion-automation-planonly-20260826-v4.json"
SCHEMA = "trading_mvp_listing_expansion_automation_planonly_v1"
VENUES = ("binance", "bybit", "okx", "bitget")
WRAPPER_ARTIFACTS = {
    "automation_contract": "trading_mvp/src/listing_expansion_automation_contract.py",
    "automation_state": "trading_mvp/src/listing_automation_state.py",
    "automation_runner": "trading_mvp/src/listing_expansion_automation.py",
    "child_outcome_verifier": "trading_mvp/src/listing_expansion_child_evidence.py",
    "visible_automation_launcher": "tools/start_listing_momentum_expansion_automation_visible.ps1",
    "cadence_policy": "trading_mvp/src/adaptive_cadence.py",
    "active_gate_checker": "tools/check_active_run_gate.ps1",
    "global_writer_claim": "trading_mvp/src/global_market_writer_claim.py",
}


class AutomationContractError(ValueError):
    """Invalid, uncommitted or drifting automation authority."""


def canonical_hash(payload: Mapping[str, Any]) -> str:
    value = {key: item for key, item in payload.items() if key != "plan_hash"}
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise AutomationContractError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _invalid_constant(value):
    raise AutomationContractError(f"nonfinite JSON constant: {value}")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(_stable_bytes(path).decode("utf-8-sig"), object_pairs_hook=_object, parse_constant=_invalid_constant)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AutomationContractError(f"invalid JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise AutomationContractError(f"JSON object required: {path}")
    return value


def _stable_bytes(path: Path) -> bytes:
    try:
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
    except OSError as exc:
        raise AutomationContractError(f"artifact unavailable: {path}") from exc
    fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
    if any(getattr(before, key) != getattr(after, key) for key in fields) or len(raw) != after.st_size:
        raise AutomationContractError(f"artifact changed during read: {path}")
    return raw


def _inside(path: Path, root: Path) -> Path:
    if not path.is_absolute() or ".." in path.parts:
        raise AutomationContractError(f"noncanonical path: {path}")
    resolved = path.resolve()
    if os.path.normcase(str(resolved)) != os.path.normcase(str(path)):
        raise AutomationContractError(f"redirected artifact path: {path}")
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise AutomationContractError(f"artifact outside repository: {path}") from exc
    return resolved


def _sha(path: Path) -> str:
    return hashlib.sha256(_stable_bytes(path)).hexdigest()


def _utc(value: str) -> None:
    try:
        valid = isinstance(value, str) and value.endswith("Z") and datetime.fromisoformat(value[:-1] + "+00:00").tzinfo is not None
    except ValueError:
        valid = False
    if not valid:
        raise AutomationContractError("generated_at_utc must be an exact UTC timestamp")


def _local_dependencies(paths: list[Path], root: Path) -> set[Path]:
    """Bind local Python imports as bytes, without executing imported modules."""
    seen: set[Path] = set()
    queue = list(paths)
    while queue:
        path = queue.pop()
        if path in seen:
            continue
        seen.add(path)
        if path.suffix != ".py":
            continue
        try:
            tree = ast.parse(_stable_bytes(path), filename=str(path))
        except SyntaxError as exc:
            raise AutomationContractError(f"implementation syntax invalid: {path}") from exc
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            for name in names:
                candidates = (root.joinpath(*name.split(".")).with_suffix(".py"), root / "trading_mvp/src" / (name.split(".")[0] + ".py"))
                for candidate in candidates:
                    if candidate.is_file():
                        candidate = _inside(candidate, root)
                        if candidate not in seen:
                            queue.append(candidate)
    return seen


def build_plan(repo_root: Path, child_plan_path: Path, *, generated_at_utc: str) -> dict[str, Any]:
    root = repo_root.resolve(strict=True)
    _utc(generated_at_utc)
    child_path = _inside(child_plan_path, root)
    child = read_json(child_path)
    if child.get("plan_hash") != canonical_hash(child):
        raise AutomationContractError("child plan hash mismatch")
    for name in ("research_only", "public_data_only"):
        if child.get(name) is not True:
            raise AutomationContractError(f"child {name} must be true")
    for name in ("private_api", "live_orders", "real_capital", "leverage_or_margin", "replay_allowed", "evaluator_or_oos_allowed"):
        if child.get(name) is not False:
            raise AutomationContractError(f"child {name} must remain false")
    if tuple(child.get("venues", ())) != VENUES:
        raise AutomationContractError("child venue contract changed")
    if child.get("status") != "READY_FOR_VISIBLE_EXPANSION_TICKS" or (child.get("acceptance_policy") or {}).get("acceptance_decision") != "NONE_ACCRUAL_ONLY":
        raise AutomationContractError("child readiness/acceptance contract invalid")
    tick = child.get("tick")
    if not isinstance(tick, dict) or type(tick.get("max_runtime_sec")) is not int or not 1 <= tick["max_runtime_sec"] <= 600:
        raise AutomationContractError("child runtime must be bounded to 600 seconds")
    for name in ("state_path", "tick_output_root", "terminal_attempts_ledger_path"):
        if not isinstance(tick.get(name), str) or not Path(tick[name]).is_absolute() or ".." in Path(tick[name]).parts:
            raise AutomationContractError(f"child {name} missing or noncanonical")
    roles = {role: _inside(root / relative, root) for role, relative in WRAPPER_ARTIFACTS.items()}
    child_files = (child.get("implementation") or {}).get("files")
    if not isinstance(child_files, list) or not child_files:
        raise AutomationContractError("child implementation binding set missing")
    child_by_role = {}
    for row in child_files:
        if not isinstance(row, dict) or not isinstance(row.get("role"), str) or row["role"] in child_by_role:
            raise AutomationContractError("child implementation role duplicate or malformed")
        path = _inside(Path(str(row.get("path", ""))), root)
        if _sha(path) != row.get("sha256"):
            raise AutomationContractError(f"child implementation bytes mismatch: {row['role']}")
        child_by_role[row["role"]] = path
        if path not in roles.values():
            roles["child_" + row["role"]] = path
    if not {"expansion_monitor", "visible_tick_launcher"}.issubset(child_by_role):
        raise AutomationContractError("child monitor/launcher binding missing")
    for path in sorted(_local_dependencies(list(roles.values()), root)):
        if path not in roles.values():
            roles["dependency_" + path.relative_to(root).as_posix().replace("/", "_").replace(".", "_")] = path
    state_root = root / "docs/agent-log/run-gates/listing-expansion-automation"
    payload: dict[str, Any] = {
        "schema": SCHEMA, "plan_id": PLAN_ID, "mode": "PlanOnly",
        "status": "READY_FOR_SCHEDULER_ROUTED_BOUNDED_PUBLIC_RESEARCH",
        "generated_at_utc": generated_at_utc, "research_only": True, "public_data_only": True,
        "private_api": False, "live_orders": False, "real_capital": False,
        "leverage_or_margin": False, "replay_allowed": False, "evaluator_or_oos_allowed": False,
        "venues": list(VENUES), "acceptance_decision": "NONE_ACCRUAL_ONLY",
        "child_plan": {"path": str(child_path), "plan_id": child["plan_id"], "plan_hash": child["plan_hash"], "file_sha256": _sha(child_path)},
        "child_launcher_path": str(child_by_role["visible_tick_launcher"]),
        "child_monitor_path": str(child_by_role["expansion_monitor"]),
        "automation": {"automation_id": "zolotyaylopata-listing-expansion-canonical", "root": str(state_root),
                       "state_path": str(state_root / "state.json"), "attempts_path": str(state_root / "attempts.jsonl"),
                       "claim_path": str(state_root / "claim.json"), "claim_archive": str(state_root / "claim-archive"),
                       "wake_interval_sec": 300, "allowed_intervals_sec": [21600, 10800, 3600, 300],
                       "default_interval_sec": 21600, "max_runtime_sec": tick["max_runtime_sec"],
                       "handoff_timeout_sec": 30, "terminal_grace_sec": 60, "visible_worker_required": True,
                       "not_due_is_read_only": True, "retry_next_interval": True, "global_claim_owner": "child_python_collector"},
        "implementation": {"files": [{"role": role, "path": str(path), "sha256": _sha(path)} for role, path in sorted(roles.items())]},
        "plan_hash_method": "sha256_canonical_json_excluding_plan_hash",
    }
    if payload["automation"]["state_path"] == tick["state_path"]:
        raise AutomationContractError("automation state must be separate from market data-state")
    payload["plan_hash"] = canonical_hash(payload)
    return payload


def _committed_bytes(root: Path, path: Path) -> bytes:
    git = Path(r"C:\Program Files\Git\cmd\git.exe") if os.name == "nt" else Path("/usr/bin/git")
    try:
        result = subprocess.run([str(git), "cat-file", "blob", "HEAD:" + path.relative_to(root).as_posix()], cwd=root, capture_output=True, timeout=15, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        raise AutomationContractError("trusted Git read failed") from exc
    if result.returncode:
        raise AutomationContractError(f"artifact is not committed: {path}")
    return result.stdout


def validate_plan(path: Path, *, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    root = repo_root.resolve(strict=True)
    path = _inside(path.resolve(strict=True), root)
    plan = read_json(path)
    if path != root / PLAN_RELATIVE_PATH:
        raise AutomationContractError("wrapper plan identity/path mismatch")
    if plan.get("plan_hash") != canonical_hash(plan):
        raise AutomationContractError("wrapper plan hash mismatch")
    raw = _stable_bytes(path)
    if raw != _committed_bytes(root, path):
        raise AutomationContractError("wrapper plan differs from committed Git bytes")
    try:
        child_path = Path(plan["child_plan"]["path"])
        expected = build_plan(root, child_path, generated_at_utc=plan["generated_at_utc"])
    except (KeyError, TypeError) as exc:
        raise AutomationContractError("wrapper contract fields missing") from exc
    if plan != expected:
        raise AutomationContractError("wrapper implementation/contract bytes mismatch")
    for artifact in [child_path, *(Path(row["path"]) for row in expected["implementation"]["files"])]:
        if _stable_bytes(artifact) != _committed_bytes(root, artifact):
            raise AutomationContractError(f"implementation differs from committed Git bytes: {artifact}")
    if _stable_bytes(path) != raw:
        raise AutomationContractError("wrapper plan changed during validation")
    return plan


def write_immutable_plan(path: Path, plan: Mapping[str, Any]) -> None:
    raw = (json.dumps(dict(plan), ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise AutomationContractError(f"immutable plan already exists: {path}") from exc
    if _stable_bytes(path) != raw:
        raise AutomationContractError("immutable plan readback mismatch")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--check", action="store_true")
    actions.add_argument("--build", action="store_true")
    parser.add_argument("--plan", type=Path, default=REPO_ROOT / PLAN_RELATIVE_PATH)
    parser.add_argument("--child-plan", type=Path)
    parser.add_argument("--generated-at-utc")
    args = parser.parse_args(argv)
    try:
        if args.check:
            plan = validate_plan(args.plan)
            output = {"status": "PLAN_OK", "plan_id": plan["plan_id"], "plan_hash": plan["plan_hash"], "execution_performed": False}
        else:
            if args.child_plan is None or args.generated_at_utc is None:
                parser.error("build requires --child-plan and --generated-at-utc")
            output = build_plan(REPO_ROOT, args.child_plan, generated_at_utc=args.generated_at_utc)
        print(json.dumps(output, ensure_ascii=False))
        return 0
    except (AutomationContractError, OSError) as exc:
        print(json.dumps({"status": "PLAN_BLOCKED", "reason": str(exc), "execution_performed": False}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
