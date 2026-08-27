"""Decide what a child tick actually did, from evidence rather than from its own claim.

A child process announces its outcome on stdout. That announcement is the least
trustworthy thing about it: a worker can print COMPLETED and then die before writing
anything, print nothing at all after doing the work, or exit non-zero having printed
success. This adapter therefore treats the announcement as one witness among four, and
promotes an outcome only when all four agree exactly.

The four are the terminal line on stdout, the tick manifest, the handoff receipt that was
issued for this exact attempt, and the terminal-attempts ledger row. Each names the tick,
the plan hash, and the identity that did the work; every one of those must match, and a
single mismatch yields RETRY rather than a repaired guess.

Three habits run through it, and each exists because its absence has cost something:

* **Never substitute a near miss.** If the manifest for this tick is missing, another
  tick's manifest is not an answer, and neither is "the latest one". The same holds for
  ledger rows and handoff receipts.
* **Ambiguity is failure.** Two terminal objects on stdout are a failure even when they
  are byte-identical, because a reader that picks one has started guessing.
* **The adapter is pure.** It reads and returns; it writes nothing, mutates none of its
  arguments, and returns the same answer when called twice on the same evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

MAX_STDOUT_BYTES = 1024 * 1024

STATUS_COMPLETE = "COMPLETE"
STATUS_PARTIAL = "PARTIAL_RETRY_NEXT_INTERVAL"
STATUS_RETRY = "RETRY_NEXT_INTERVAL"

MANIFEST_SCHEMA = (
    "trading_mvp_slow_liquidity_listing_momentum_forward_expansion_tick_manifest_v1"
)
HANDOFF_SCHEMA = "trading_mvp_market_data_worker_handoff_v1"
LEDGER_SCHEMA = (
    "trading_mvp_listing_momentum_forward_expansion_terminal_attempt_v1"
)
AUTOMATION_ID = "zolotyaylopata-listing-momentum-forward-expansion"
CLAIM_OWNER_KIND = "listing_momentum_forward_expansion_monitor_tick"

MANIFEST_TERMINAL_STATUSES = frozenset({"COMPLETED", "PARTIAL_RETRY_NEXT_INTERVAL"})
TERMINAL_KEYS = frozenset({"status", "tick_id", "new_listing_count", "rows_written", "state"})
_TICK_ID_RE = re.compile(r"\A[A-Za-z0-9_.-]{1,128}\Z")
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_IN_PROGRESS_CATEGORY = "new_listing_in_progress"


class _Reject(Exception):
    """Internal: an evidence check refused. Carries the reason for the caller."""


def _reject(message: str) -> "_Reject":
    return _Reject(message)


def _is_int(value: Any) -> bool:
    # bool is an int in Python; a True that passed as a count would be a silent 1.
    return isinstance(value, int) and not isinstance(value, bool)


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate keys")
    return dict(pairs)


def _loads_strict(raw: str) -> Any:
    return json.loads(raw, object_pairs_hook=_no_duplicate_keys)


def _aware(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise _reject(f"{label} is not a timestamp")
    text = value.strip()
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _reject(f"{label} is not an ISO timestamp: {exc}") from exc
    if moment.tzinfo is None:
        # A naive timestamp is an unanswered question about which clock it came from.
        raise _reject(f"{label} has no timezone")
    return moment.astimezone(timezone.utc)


# How far apart two *readings* of one process's start time may be. The wrapper observes a
# launcher through psutil and the launcher records itself through .NET, and the two do not
# produce the same number: .NET prints seven fractional digits, psutil converts a float to
# microseconds, and measured over six spawns on this machine they differ by one microsecond
# in five of them. An earlier version compared these with `==` on the strength of a single
# measurement that happened to agree, and the wrapper then rejected roughly five of every
# six ticks it ran successfully - fifty jobs collected, manifest written, outcome recorded
# as a retry.
#
# One second is what ``global_market_writer_claim`` already uses for the same comparison,
# and it is the right order of magnitude: the check exists because Windows reuses PIDs, and
# a reused PID belongs to a process that started after the previous one exited, which is
# not a millisecond ago.
PROCESS_START_TOLERANCE_SEC = 1.0


def _same_instant(left: Any, right: Any, label: str) -> bool:
    """Whether two records carry the same moment, compared as moments not as text.

    For a value that was written once and copied into two records. A difference here is a
    contradiction between two accounts of one fact, so it is exact - and it is exact
    deliberately, not by omission: see ``_same_process_start`` for the case where two
    different measurements of one moment legitimately disagree."""
    return _aware(left, label) == _aware(right, label)


def _same_process_start(left: Any, right: Any, label: str) -> bool:
    """Whether two measurements name the same process start, within tolerance.

    For one process observed through two different APIs, which is not the same situation
    as one value copied twice. What this must still reject is a different process wearing
    a reused PID, and that one is seconds away at the very least."""
    delta = (_aware(left, label) - _aware(right, label)).total_seconds()
    return abs(delta) <= PROCESS_START_TOLERANCE_SEC


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _cadence_observation(candidate: bool) -> dict[str, Any]:
    """A proxy date is a proxy date however deep in the call stack it was produced.

    No key for an event ETA is emitted at all: an absent field cannot later be read as
    an official time that nobody established."""
    return {
        "candidate": bool(candidate),
        "proxy_timestamp": True,
        "source_class": "proxy",
        "official_confirmed": False,
        "exact_timestamp": False,
    }


def _retry(reason: str, *, candidate: bool = False) -> dict[str, Any]:
    return {
        "status": STATUS_RETRY,
        "reason": reason,
        "counts": {},
        "pending_jobs": [],
        "cadence_observation": _cadence_observation(candidate),
    }


# ---------------------------------------------------------------- evidence readers


def _terminal_from_stdout(stdout_text: Any) -> dict[str, Any]:
    if not isinstance(stdout_text, str):
        raise _reject("stdout is not text")
    if len(stdout_text.encode("utf-8")) > MAX_STDOUT_BYTES:
        raise _reject("stdout exceeds the readable bound")

    decoder = json.JSONDecoder(object_pairs_hook=_no_duplicate_keys)
    found: list[dict[str, Any]] = []
    index = 0
    while True:
        start = stdout_text.find("{", index)
        if start < 0:
            break
        try:
            value, end = decoder.raw_decode(stdout_text, start)
        except ValueError as exc:
            if "duplicate keys" in str(exc):
                raise _reject("a JSON object on stdout has duplicate keys") from exc
            index = start + 1
            continue
        index = end
        if isinstance(value, dict) and TERMINAL_KEYS.issubset(value.keys()):
            found.append(value)
    if not found:
        raise _reject("no terminal record was printed")
    if len(found) > 1:
        # Identical duplicates are still ambiguous: choosing one is guessing.
        raise _reject("more than one terminal record was printed")
    return found[0]


def _read_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise _reject("the manifest for this tick does not exist")
    try:
        payload = _loads_strict(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise _reject(f"the manifest is unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise _reject("the manifest is not an object")
    return payload


def _read_receipt(handoff_dir: Path, tick_id: str) -> dict[str, Any]:
    loose = sorted(handoff_dir.glob(f"{tick_id}*.json"))
    if loose:
        raise _reject("a handoff receipt is still unconsumed")
    consumed_dir = handoff_dir / "consumed"
    matches = sorted(consumed_dir.glob(f"{tick_id}*.json")) if consumed_dir.is_dir() else []
    if not matches:
        raise _reject("no consumed handoff receipt for this tick")
    if len(matches) > 1:
        raise _reject("more than one consumed handoff receipt for this tick")
    try:
        payload = _loads_strict(matches[0].read_text(encoding="utf-8"))
    except ValueError as exc:
        raise _reject(f"the handoff receipt is unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise _reject("the handoff receipt is not an object")
    return payload


def _read_ledger_row(path: Path, tick_id: str) -> tuple[dict[str, Any], str]:
    if not path.is_file():
        raise _reject("the terminal attempts ledger does not exist")
    raw = path.read_text(encoding="utf-8")
    if not raw.endswith("\n"):
        # A missing final newline is a torn write, not a formatting preference.
        raise _reject("the terminal attempts ledger ends mid-record")
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            row = _loads_strict(text)
        except ValueError as exc:
            raise _reject(f"the terminal attempts ledger is unreadable: {exc}") from exc
        if isinstance(row, dict) and row.get("tick_id") == tick_id:
            rows.append(row)
    if not rows:
        raise _reject("no terminal ledger row for this tick")
    if len(rows) > 1:
        raise _reject("more than one terminal ledger row for this tick")
    return rows[0], hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- checks


def _validate_plan(child_plan: Any) -> tuple[Path, Path, str]:
    if not isinstance(child_plan, Mapping) or not child_plan:
        raise _reject("the bound child plan is empty")
    plan_hash = child_plan.get("plan_hash")
    if not isinstance(plan_hash, str) or not _SHA256_RE.match(plan_hash):
        raise _reject("the bound child plan hash is malformed")
    tick = child_plan.get("tick")
    if not isinstance(tick, Mapping):
        raise _reject("the bound child plan has no tick contract")
    root = Path(str(tick.get("tick_output_root") or ""))
    ledger = Path(str(tick.get("terminal_attempts_ledger_path") or ""))
    if not str(root) or not root.is_absolute():
        raise _reject("the tick output root is not an absolute path")
    if not str(ledger) or not ledger.is_absolute():
        raise _reject("the terminal attempts ledger path is not absolute")
    return root, ledger, plan_hash


def _validate_manifest(
    manifest: Mapping[str, Any],
    *,
    tick_id: str,
    plan_hash: str,
    window: tuple[datetime, datetime],
) -> None:
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise _reject("the manifest schema is not the expected one")
    if manifest.get("tick_id") != tick_id:
        raise _reject("the manifest names a different tick")
    if manifest.get("plan_hash") != plan_hash:
        raise _reject("the manifest was produced under a different plan")
    if manifest.get("status") not in MANIFEST_TERMINAL_STATUSES:
        raise _reject("the manifest status is not terminal")

    for field in ("new_listing_count", "jobs_total", "jobs_attempted", "jobs_succeeded",
                  "jobs_failed", "jobs_pending_retry", "rows_written", "requests_made"):
        value = manifest.get(field)
        if not _is_int(value) or value < 0:
            raise _reject(f"the manifest count {field} is not a whole number")

    jobs = manifest.get("jobs")
    retry_queue = manifest.get("retry_queue")
    if not isinstance(jobs, list) or not isinstance(retry_queue, list):
        raise _reject("the manifest job lists are malformed")
    if len(jobs) != manifest["jobs_total"]:
        raise _reject("the manifest job list does not match its own total")
    if manifest["jobs_attempted"] != manifest["jobs_total"]:
        raise _reject("the manifest attempted count does not match its total")
    if manifest["jobs_succeeded"] + manifest["jobs_failed"] != manifest["jobs_attempted"]:
        raise _reject("the manifest outcome counts do not add up")
    if len(retry_queue) != manifest["jobs_pending_retry"]:
        raise _reject("the manifest retry queue does not match its pending count")
    if not isinstance(manifest.get("pending_retry"), bool):
        raise _reject("the manifest pending_retry flag is malformed")
    if manifest["pending_retry"] != (manifest["jobs_pending_retry"] > 0):
        raise _reject("the manifest pending_retry flag contradicts its retry queue")

    started = _aware(manifest.get("started_at_utc"), "manifest started_at_utc")
    finished = _aware(manifest.get("finished_at_utc"), "manifest finished_at_utc")
    outer_started, outer_finished = window
    # The outer start is compared at second granularity so a manifest that records
    # whole seconds is not rejected for being coarser than its parent's clock.
    if started < outer_started.replace(microsecond=0) or started > outer_finished:
        raise _reject("the manifest started outside the launch window")
    if finished < outer_started.replace(microsecond=0) or finished > outer_finished:
        raise _reject("the manifest finished outside the launch window")
    if finished < started:
        raise _reject("the manifest finished before it started")


def _validate_receipt(
    receipt: Mapping[str, Any],
    *,
    tick_id: str,
    plan_id: str,
    plan_hash: str,
    identity: Mapping[str, Any],
    namespace: Path,
    window: tuple[datetime, datetime],
) -> None:
    if receipt.get("schema") != HANDOFF_SCHEMA:
        raise _reject("the handoff receipt schema is not the expected one")
    if receipt.get("status") != "ISSUED":
        raise _reject("the handoff receipt is not the issued record")
    if receipt.get("project") != "trading_mvp":
        raise _reject("the handoff receipt belongs to another project")
    if receipt.get("automation_id") != AUTOMATION_ID:
        raise _reject("the handoff receipt belongs to another automation")
    if receipt.get("attempt_id") != tick_id:
        raise _reject("the handoff receipt names a different attempt")
    if receipt.get("plan_hash") != plan_hash:
        raise _reject("the handoff receipt was issued under a different plan")
    if not _is_int(receipt.get("wrapper_pid")) or receipt.get("wrapper_pid") != identity["pid"]:
        raise _reject("the handoff receipt names a different wrapper process")
    if not _same_process_start(
        receipt.get("wrapper_process_started_at_utc"),
        identity["started_at_utc"],
        "handoff wrapper_process_started_at_utc",
    ):
        raise _reject("the handoff receipt names a wrapper with a different start time")
    if receipt.get("claim_run_id") != f"{plan_id}__{tick_id}":
        raise _reject("the handoff receipt names a different run")
    if receipt.get("claim_owner_kind") != CLAIM_OWNER_KIND:
        raise _reject("the handoff receipt names a different owner kind")
    if receipt.get("claim_owner_pid") is not None:
        raise _reject("the handoff receipt already names a claim owner")
    if receipt.get("claim_owner_process_started_at_utc") is not None:
        raise _reject("the handoff receipt already names a claim owner start time")
    if receipt.get("claim_must_exist") is not False:
        raise _reject("the handoff receipt requires a pre-existing claim")
    if Path(str(receipt.get("claim_output_namespace") or "")) != namespace:
        raise _reject("the handoff receipt names a different output namespace")
    for field in ("handoff_token_sha256", "claim_ownership_token_sha256"):
        value = receipt.get(field)
        if not isinstance(value, str) or not _SHA256_RE.match(value):
            raise _reject(f"the handoff receipt {field} is malformed")
    issued = _aware(receipt.get("issued_at_utc"), "handoff issued_at_utc")
    outer_started, outer_finished = window
    if issued < outer_started.replace(microsecond=0) or issued > outer_finished:
        raise _reject("the handoff receipt was issued outside the launch window")


def _validate_ledger_row(
    row: Mapping[str, Any],
    *,
    tick_id: str,
    plan_id: str,
    plan_hash: str,
    manifest: Mapping[str, Any],
    manifest_path: Path,
    manifest_sha256: str,
    claim_path: Path,
    namespace: Path,
    ownership_token: str,
    window: tuple[datetime, datetime],
) -> None:
    if row.get("schema") != LEDGER_SCHEMA:
        raise _reject("the terminal ledger schema is not the expected one")
    if row.get("attempt_id") != tick_id:
        raise _reject("the terminal ledger row names a different attempt")
    if row.get("plan_id") != plan_id or row.get("plan_hash") != plan_hash:
        raise _reject("the terminal ledger row was written under a different plan")
    if row.get("run_id") != f"{plan_id}__{tick_id}":
        raise _reject("the terminal ledger row names a different run")
    if not _is_int(row.get("owner_pid")) or row.get("owner_pid") != manifest.get("writer_pid"):
        raise _reject("the terminal ledger row names a different writer")
    if not _same_instant(
        row.get("owner_process_started_at_utc"),
        manifest.get("writer_process_started_at_utc"),
        "terminal ledger owner_process_started_at_utc",
    ):
        raise _reject("the terminal ledger row names a writer with a different start time")
    if row.get("ownership_token_sha256") != ownership_token:
        raise _reject("the terminal ledger row carries a different ownership token")
    if Path(str(row.get("claim_path") or "")) != claim_path:
        raise _reject("the terminal ledger row names a different claim")
    if Path(str(row.get("output_namespace") or "")) != namespace:
        raise _reject("the terminal ledger row names a different output namespace")
    if Path(str(row.get("manifest_path") or "")) != manifest_path:
        raise _reject("the terminal ledger row names a different manifest")
    if row.get("manifest_sha256") != manifest_sha256:
        raise _reject("the terminal ledger row records different manifest bytes")
    for field in ("status", "manifest_status"):
        if row.get(field) != manifest.get("status"):
            raise _reject(f"the terminal ledger {field} contradicts the manifest")
    if row.get("pending_retry") != manifest.get("pending_retry"):
        raise _reject("the terminal ledger pending_retry contradicts the manifest")
    if not _same_instant(
        row.get("started_at_utc"), manifest.get("started_at_utc"),
        "terminal ledger started_at_utc",
    ):
        raise _reject("the terminal ledger started_at_utc contradicts the manifest")

    # Ordering, not equality. The child writes the manifest, rebuilds its state, and only
    # then appends this row with a fresh clock reading - three seconds later on the first
    # real tick. The manifest records when the tick finished; the ledger records when the
    # attempt was recorded. What must hold is that the record does not predate the thing
    # it records, and that neither escapes the window the launch happened in.
    recorded = _aware(row.get("finished_at_utc"), "terminal ledger finished_at_utc")
    finished = _aware(manifest.get("finished_at_utc"), "manifest finished_at_utc")
    outer_started, outer_finished = window
    if recorded < finished:
        raise _reject("the terminal ledger was recorded before the tick finished")
    if recorded > outer_finished or recorded < outer_started.replace(microsecond=0):
        raise _reject("the terminal ledger was recorded outside the launch window")


# ---------------------------------------------------------------- entry point


def verify_child_outcome(
    *,
    stdout_text: Any,
    exit_code: Any,
    child_plan: Any,
    child_identity: Any,
    started_at_utc: Any,
    finished_at_utc: Any,
    repo_root: Any,
) -> dict[str, Any]:
    """Return the outcome the evidence supports, never the one that was claimed."""
    try:
        if not _is_int(exit_code):
            raise _reject("the child exit code is not a whole number")
        if not isinstance(child_identity, Mapping):
            raise _reject("the child identity is missing")
        identity = {
            "pid": child_identity.get("pid"),
            "started_at_utc": child_identity.get("started_at_utc"),
        }
        if not _is_int(identity["pid"]):
            raise _reject("the child pid is not a whole number")
        _aware(identity["started_at_utc"], "child started_at_utc")

        outer_started = _aware(started_at_utc, "launch started_at_utc")
        outer_finished = _aware(finished_at_utc, "launch finished_at_utc")
        if outer_finished < outer_started:
            raise _reject("the launch window ends before it begins")
        window = (outer_started, outer_finished)

        root, ledger_path, plan_hash = _validate_plan(child_plan)
        plan_id = str(child_plan.get("plan_id") or "")
        claim_path = Path(str((child_plan.get("tick") or {}).get("claim_path") or ""))

        terminal = _terminal_from_stdout(stdout_text)
        tick_id = terminal.get("tick_id")
        if not isinstance(tick_id, str) or not _TICK_ID_RE.match(tick_id):
            raise _reject("the terminal record names an unusable tick id")
        if terminal.get("status") not in MANIFEST_TERMINAL_STATUSES:
            raise _reject("the terminal record status is not terminal")
        if not _is_int(terminal.get("new_listing_count")) or not _is_int(
            terminal.get("rows_written")
        ):
            raise _reject("the terminal record counts are malformed")

        manifest_path = root / tick_id / "manifest.json"
        manifest = _read_manifest(manifest_path)
        manifest_sha256 = _sha256_file(manifest_path)
        _validate_manifest(
            manifest, tick_id=tick_id, plan_hash=plan_hash, window=window
        )
        for field in ("status", "new_listing_count", "rows_written"):
            if terminal.get(field) != manifest.get(field):
                raise _reject(f"the terminal record {field} contradicts the manifest")

        namespace = manifest_path.parent
        handoff_dir = Path(repo_root) / "docs/agent-log/run-gates/python-worker-handoffs"
        receipt = _read_receipt(handoff_dir, tick_id)
        _validate_receipt(
            receipt,
            tick_id=tick_id,
            plan_id=plan_id,
            plan_hash=plan_hash,
            identity=identity,
            namespace=namespace,
            window=window,
        )

        row, ledger_sha256 = _read_ledger_row(ledger_path, tick_id)
        _validate_ledger_row(
            row,
            tick_id=tick_id,
            plan_id=plan_id,
            plan_hash=plan_hash,
            manifest=manifest,
            manifest_path=manifest_path,
            manifest_sha256=manifest_sha256,
            claim_path=claim_path,
            namespace=namespace,
            ownership_token=str(receipt.get("claim_ownership_token_sha256")),
            window=window,
        )

        jobs: Iterable[Mapping[str, Any]] = manifest.get("jobs") or []
        candidate = any(
            isinstance(job, Mapping) and job.get("category") == _IN_PROGRESS_CATEGORY
            for job in jobs
        )
        pending_jobs = [dict(job) for job in manifest.get("retry_queue") or []]
        counts = {
            field: manifest[field]
            for field in (
                "new_listing_count", "jobs_total", "jobs_attempted", "jobs_succeeded",
                "jobs_failed", "jobs_pending_retry", "rows_written", "requests_made",
            )
        }

        if manifest["status"] == "COMPLETED":
            if exit_code != 0:
                raise _reject("a non-zero exit cannot carry a completed outcome")
            if manifest["jobs_pending_retry"] or pending_jobs:
                raise _reject("a completed outcome cannot leave work pending")
            status = STATUS_COMPLETE
        else:
            # PARTIAL means some work landed and some is genuinely queued to retry.
            # Without both halves it is simply a retry, and calling it partial would
            # overstate what the tick achieved.
            if manifest["jobs_succeeded"] <= 0:
                raise _reject("no job succeeded, so the outcome is a retry")
            if not pending_jobs:
                raise _reject("a partial outcome needs a real retry queue")
            status = STATUS_PARTIAL

        return {
            "status": status,
            "reason": None,
            "child_tick_id": tick_id,
            "child_manifest_path": str(manifest_path),
            "child_manifest_sha256": manifest_sha256,
            "child_terminal_ledger_sha256": ledger_sha256,
            "counts": counts,
            "pending_jobs": pending_jobs,
            "cadence_observation": _cadence_observation(candidate),
        }
    except _Reject as refusal:
        return _retry(str(refusal))
    except (OSError, ValueError) as exc:
        return _retry(f"the child evidence could not be read: {exc}")


__all__ = ["MAX_STDOUT_BYTES", "verify_child_outcome"]
