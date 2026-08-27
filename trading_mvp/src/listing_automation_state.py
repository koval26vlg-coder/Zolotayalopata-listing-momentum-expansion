"""The automation state machine, isolated from any collector so it can be tested.

Every failure this engine guards against was observed in the listing runtimes rather than
imagined. A worker that died between doing its work and recording it left the state
RUNNING forever. A stale claim outlived its owner and blocked the next tick. A recycled
PID looked like the same live worker. A tick that was not due still touched files and so
still cost something.

The rules that follow from that:

* **A terminal record is written before the state.** The ledger is the commit point. If
  persisting the state then fails, the outcome is still recoverable, and ``reconcile``
  replays it exactly once rather than inventing a second one.
* **Nothing is reaped on a timeout.** An unresolved handoff stays unresolved however long
  it has been. Guessing that an unseen worker is dead is how two workers end up writing
  at once, and no elapsed time makes that guess safe.
* **A claim outlives its own release.** It is dropped only when every process it names has
  actually exited, so a retry cannot start while a child is still running.
* **NOT_DUE writes nothing at all** - no mutex, no claim, no state - so the cheap path
  stays cheap.
* **Identity is a pair.** A process is the same process only if its PID *and* its start
  time match; a PID alone is a number the operating system reuses.
* **A handoff token is never stored**, only its digest, so reading every file on disk does
  not let anyone bind a worker.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

# The cadence ladder the listing plans declare. An interval outside it is refused rather
# than rounded, so a typo cannot quietly become a schedule nobody reviewed.
FROZEN_CADENCE_SECONDS: frozenset[int] = frozenset({300, 3600, 10800, 21600})
DEFAULT_CADENCE_SECONDS = 21600

STATUS_DUE = "DUE"
STATUS_NOT_DUE = "NOT_DUE"
STATUS_LAUNCHING = "LAUNCHING"
STATUS_ALREADY_RUNNING = "ALREADY_RUNNING"
STATUS_CLAIM_UNRESOLVED = "CLAIM_UNRESOLVED"

OUTCOME_COMPLETE = "COMPLETE"
OUTCOME_PARTIAL = "PARTIAL_RETRY_NEXT_INTERVAL"
OUTCOME_RETRY = "RETRY_NEXT_INTERVAL"
TERMINAL_OUTCOMES = (OUTCOME_COMPLETE, OUTCOME_PARTIAL, OUTCOME_RETRY)

_PROCESS_LOCK = threading.Lock()


class AutomationStateError(RuntimeError):
    """Refusal to act on an ambiguous or tampered state."""


@dataclass(frozen=True)
class ProcessIdentity:
    """A PID is not an identity. A PID plus its start time is."""

    pid: int
    started_at_utc: str

    def as_dict(self) -> dict[str, Any]:
        return {"pid": int(self.pid), "started_at_utc": str(self.started_at_utc)}

    def matches(self, other: "ProcessIdentity | None") -> bool:
        if other is None:
            return False
        return int(self.pid) == int(other.pid) and str(self.started_at_utc) == str(
            other.started_at_utc
        )


@dataclass(frozen=True)
class ProbeResult:
    """What the operating system could tell us: LIVE, DEAD or UNKNOWN.

    UNKNOWN is a real answer and is never collapsed into DEAD."""

    status: str
    identity: ProcessIdentity | None = None


@dataclass(frozen=True)
class Binding:
    plan_id: str
    plan_hash: str
    child_plan_hash: str


class AutomationPaths:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.state = self.root / "state.json"
        self.claim = self.root / "claim.json"
        self.mutex = self.root / "claim.json.mutex"
        self.ledger = self.root / "attempts.jsonl"
        self.claim_archive = self.root / "claim-archive"


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _canonical(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _row_hash(row: Mapping[str, Any]) -> str:
    body = {k: v for k, v in row.items() if k != "row_sha256"}
    return hashlib.sha256(_canonical(body)).hexdigest()


class AutomationEngine:
    def __init__(
        self,
        paths: AutomationPaths,
        binding: Binding,
        *,
        now: Callable[[], datetime],
        process_probe: Callable[[int], ProbeResult],
    ) -> None:
        self.paths = paths
        self.binding = binding
        self._now = now
        self._probe = process_probe

    # ---------------------------------------------------------------- persistence

    def _write_json(self, path: Path, payload: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}.{secrets.token_hex(4)}")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(path)

    def _write_state(self, state: Mapping[str, Any]) -> None:
        self._write_json(self.paths.state, state)

    def _append_ledger(self, row: dict[str, Any]) -> None:
        row["row_sha256"] = _row_hash(row)
        self.paths.ledger.parent.mkdir(parents=True, exist_ok=True)
        with self.paths.ledger.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def _ledger_rows(self) -> list[dict[str, Any]]:
        if not self.paths.ledger.exists():
            return []
        rows: list[dict[str, Any]] = []
        for line in self.paths.ledger.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            row = json.loads(text)
            if row.get("row_sha256") != _row_hash(row):
                raise AutomationStateError(
                    "LEDGER_HASH_MISMATCH: a terminal record has been altered"
                )
            rows.append(row)
        return rows

    def _read_state(self) -> dict[str, Any] | None:
        if not self.paths.state.exists():
            return None
        try:
            state = json.loads(self.paths.state.read_text(encoding="utf-8"))
        except (ValueError, OSError) as exc:
            raise AutomationStateError(f"STATE_UNREADABLE: {exc}") from exc
        if not isinstance(state, dict):
            raise AutomationStateError("STATE_UNREADABLE: not an object")
        if (
            state.get("plan_id") != self.binding.plan_id
            or state.get("plan_hash") != self.binding.plan_hash
            or state.get("child_plan_hash") != self.binding.child_plan_hash
        ):
            raise AutomationStateError(
                "BINDING_MISMATCH: the state belongs to a different plan"
            )
        return state

    def _read_claim(self) -> dict[str, Any] | None:
        if not self.paths.claim.exists():
            return None
        # Same discipline as the state: a corrupt claim is the engine refusing, not a
        # raw decoder error escaping past every caller that catches AutomationStateError.
        try:
            claim = json.loads(self.paths.claim.read_text(encoding="utf-8"))
        except (ValueError, OSError) as exc:
            raise AutomationStateError(f"CLAIM_UNREADABLE: {exc}") from exc
        if not isinstance(claim, dict):
            raise AutomationStateError("CLAIM_UNREADABLE: not an object")
        return claim

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _validate_cadence(cadence_seconds: Any) -> int:
        # bool is an int in Python, and a True that became 1 second would be a schedule
        # nobody chose, so it is rejected before the membership test.
        if isinstance(cadence_seconds, bool) or not isinstance(cadence_seconds, int):
            raise AutomationStateError(f"CADENCE_INVALID: {cadence_seconds!r}")
        if cadence_seconds not in FROZEN_CADENCE_SECONDS:
            raise AutomationStateError(f"CADENCE_INVALID: {cadence_seconds!r}")
        return cadence_seconds

    def _identity_from(self, payload: Any) -> ProcessIdentity | None:
        if not isinstance(payload, Mapping):
            return None
        try:
            return ProcessIdentity(int(payload["pid"]), str(payload["started_at_utc"]))
        except (KeyError, TypeError, ValueError):
            return None

    def _is_live(self, identity: ProcessIdentity | None) -> str:
        """LIVE only when the probe returns the very same process."""
        if identity is None:
            return "DEAD"
        result = self._probe(identity.pid)
        if result.status == "LIVE":
            # A recycled PID is a different process wearing the same number.
            return "LIVE" if identity.matches(result.identity) else "DEAD"
        if result.status == "DEAD":
            return "DEAD"
        return "UNKNOWN"

    def _blank_state(self, cadence_seconds: int, moment: datetime) -> dict[str, Any]:
        return {
            "schema": "trading_mvp_listing_automation_state_v1",
            "plan_id": self.binding.plan_id,
            "plan_hash": self.binding.plan_hash,
            "child_plan_hash": self.binding.child_plan_hash,
            "status": "IDLE",
            "pending_retry": False,
            "cadence_seconds": cadence_seconds,
            "next_interval_at_utc": _iso(moment),
            "last_attempt_id": None,
            "last_started_at_utc": None,
            "last_finished_at_utc": None,
            "last_error": None,
            "worker_pid": None,
            "updated_at_utc": _iso(moment),
        }

    def _due(self, state: Mapping[str, Any], moment: datetime) -> bool:
        raw = str(state.get("next_interval_at_utc") or "")
        if not raw:
            return True
        try:
            due_at = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return True
        return moment >= due_at

    # ---------------------------------------------------------------- public API

    def initialize(self, *, cadence_seconds: int = DEFAULT_CADENCE_SECONDS) -> dict[str, Any]:
        cadence = self._validate_cadence(cadence_seconds)
        moment = self._now()
        existing = self._read_state() if self.paths.state.exists() else None
        if existing is not None:
            return {
                "status": STATUS_DUE if self._due(existing, moment) else STATUS_NOT_DUE,
                "next_interval_at_utc": existing.get("next_interval_at_utc"),
            }
        state = self._blank_state(cadence, moment)
        self._write_state(state)
        # The mutex is a persistent lock inode, created once with the state. NOT_DUE must
        # never reach here, which is what keeps the cheap path free of any write at all.
        self.paths.mutex.touch(exist_ok=True)
        return {"status": STATUS_DUE, "next_interval_at_utc": state["next_interval_at_utc"]}

    def read_due(self) -> dict[str, Any]:
        """The cheap path. It reads and returns; it writes nothing."""
        moment = self._now()
        state = self._read_state()
        if state is None:
            return {"status": STATUS_DUE, "next_interval_at_utc": None}
        due = self._due(state, moment)
        return {
            "status": STATUS_DUE if due else STATUS_NOT_DUE,
            "next_interval_at_utc": state.get("next_interval_at_utc"),
            "state_status": state.get("status"),
        }

    def status(self) -> dict[str, Any]:
        return self.read_due()

    def inspect(self) -> dict[str, Any]:
        """What a caller may say about this automation without changing it.

        ``read_due`` answers only the cheap question. A launcher standing in front of a
        scheduler has to answer a second one - is somebody already running - and the
        honest answer to that lives in the claim, not in the state. Probing it read-only
        keeps a launcher from having to call ``begin_attempt`` just to look, which would
        create the very claim it was asking about."""
        verdict = self.read_due()
        claim = self._read_claim()
        if claim is None:
            return {**verdict, "claim": "ABSENT"}

        worker = self._identity_from(claim.get("worker"))
        child = self._identity_from(claim.get("child"))
        if worker is None:
            # An unbound handoff cannot be called free or busy; either answer would be
            # a guess about a process nobody recorded.
            return {**verdict, "claim": "UNRESOLVED", "attempt_id": claim.get("attempt_id")}
        for identity in (worker, child):
            if identity is None:
                continue
            liveness = self._is_live(identity)
            if liveness == "UNKNOWN":
                return {**verdict, "claim": "UNRESOLVED", "attempt_id": claim.get("attempt_id")}
            if liveness == "LIVE":
                return {
                    **verdict,
                    "claim": "RUNNING",
                    "attempt_id": claim.get("attempt_id"),
                    "worker_pid": identity.pid,
                }
        return {**verdict, "claim": "STALE", "attempt_id": claim.get("attempt_id")}

    def begin_attempt(self) -> dict[str, Any]:
        moment = self._now()
        state = self._read_state()
        if state is not None and not self._due(state, moment):
            return {"status": STATUS_NOT_DUE, "next_interval_at_utc": state.get("next_interval_at_utc")}

        with _PROCESS_LOCK:
            claim = self._read_claim()
            if claim is not None:
                resolved = self._resolve_claim(claim, moment)
                if resolved["status"] != "RELEASED":
                    return resolved
                claim = None

            if state is None:
                self.initialize()
                state = self._read_state()
            assert state is not None

            attempt_id = f"attempt_{_iso(moment).replace(':', '').replace('-', '')}_{secrets.token_hex(4)}"
            token = secrets.token_hex(32)
            self.paths.root.mkdir(parents=True, exist_ok=True)
            self.paths.mutex.touch(exist_ok=True)

            record = {
                "schema": "trading_mvp_listing_automation_claim_v1",
                "attempt_id": attempt_id,
                "handoff_token_sha256": hashlib.sha256(token.encode()).hexdigest(),
                "claimed_at_utc": _iso(moment),
                "plan_id": self.binding.plan_id,
                "plan_hash": self.binding.plan_hash,
                "worker": None,
                "child": None,
            }
            try:
                descriptor = os.open(
                    self.paths.claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY
                )
            except FileExistsError:
                return {"status": STATUS_ALREADY_RUNNING}
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, indent=2) + "\n")

            self._append_ledger(
                {
                    "kind": "ATTEMPT_STARTED",
                    "attempt_id": attempt_id,
                    "at_utc": _iso(moment),
                    "plan_id": self.binding.plan_id,
                    "plan_hash": self.binding.plan_hash,
                }
            )
            state.update(
                {
                    "status": "LAUNCHING",
                    "last_attempt_id": attempt_id,
                    "last_started_at_utc": _iso(moment),
                    "updated_at_utc": _iso(moment),
                }
            )
            self._write_state(state)
            return {
                "status": STATUS_LAUNCHING,
                "attempt_id": attempt_id,
                "handoff_token": token,
            }

    def _require_handoff(self, attempt_id: str, token: str) -> dict[str, Any]:
        claim = self._read_claim()
        if claim is None:
            raise AutomationStateError("HANDOFF_MISMATCH: no claim is held")
        digest = hashlib.sha256(str(token).encode()).hexdigest()
        if claim.get("attempt_id") != attempt_id or claim.get("handoff_token_sha256") != digest:
            raise AutomationStateError("HANDOFF_MISMATCH: attempt or token does not match")
        return claim

    def bind_worker(self, attempt_id: str, token: str, identity: ProcessIdentity) -> dict[str, Any]:
        with _PROCESS_LOCK:
            claim = self._require_handoff(attempt_id, token)
            moment = self._now()
            claim["worker"] = identity.as_dict()
            self._write_json(self.paths.claim, claim)
            state = self._read_state() or {}
            state.update(
                {"status": "RUNNING", "worker_pid": identity.pid, "updated_at_utc": _iso(moment)}
            )
            self._write_state(state)
            return {"status": "BOUND", "attempt_id": attempt_id}

    def attach_child(self, attempt_id: str, token: str, identity: ProcessIdentity) -> dict[str, Any]:
        with _PROCESS_LOCK:
            claim = self._require_handoff(attempt_id, token)
            claim["child"] = identity.as_dict()
            self._write_json(self.paths.claim, claim)
            return {"status": "ATTACHED", "attempt_id": attempt_id}

    def finish_attempt(
        self,
        attempt_id: str,
        token: str,
        *,
        outcome: str,
        reason: str | None = None,
        cadence_seconds: int | None = None,
        no_worker_spawned: bool = False,
    ) -> dict[str, Any]:
        with _PROCESS_LOCK:
            claim = self._require_handoff(attempt_id, token)
            if outcome not in TERMINAL_OUTCOMES:
                raise AutomationStateError(f"OUTCOME_INVALID: {outcome!r}")
            moment = self._now()
            state = self._read_state() or {}
            cadence = (
                self._validate_cadence(cadence_seconds)
                if cadence_seconds is not None
                else int(state.get("cadence_seconds") or DEFAULT_CADENCE_SECONDS)
            )

            child = self._identity_from(claim.get("child"))
            if outcome == OUTCOME_COMPLETE and child is not None:
                if self._is_live(child) != "DEAD":
                    raise AutomationStateError(
                        "CHILD_NOT_EXITED: a completion cannot be recorded while a child runs"
                    )

            pending = outcome != OUTCOME_COMPLETE
            next_at = _iso(moment + timedelta(seconds=cadence))
            state_after = dict(state)
            state_after.update(
                {
                    "status": outcome,
                    "pending_retry": pending,
                    "cadence_seconds": cadence,
                    "next_interval_at_utc": next_at,
                    "last_attempt_id": attempt_id,
                    "last_finished_at_utc": _iso(moment),
                    "last_error": reason,
                    "worker_pid": None,
                    "updated_at_utc": _iso(moment),
                }
            )
            # The ledger is the commit point: written before the state, so a failure to
            # persist the state leaves an outcome that reconcile can replay exactly once.
            self._append_ledger(
                {
                    "kind": "TERMINAL",
                    "attempt_id": attempt_id,
                    "at_utc": _iso(moment),
                    "outcome": outcome,
                    "reason": reason,
                    "plan_id": self.binding.plan_id,
                    "plan_hash": self.binding.plan_hash,
                    "no_worker_spawned": bool(no_worker_spawned),
                    "state_after": state_after,
                }
            )
            self._write_state(state_after)

            if no_worker_spawned and claim.get("worker") is None:
                # Only an explicit statement that nothing was created releases an unbound
                # claim. A generic failure might have spawned a worker we cannot see.
                self._archive_claim(claim, moment, "NO_WORKER_SPAWNED")

            return {
                "state_status": outcome,
                "pending_retry": pending,
                "next_interval_at_utc": next_at,
                "last_attempt_id": attempt_id,
            }

    def record_preflight_failure(
        self, reason: str, *, cadence_seconds: int | None = None
    ) -> dict[str, Any]:
        with _PROCESS_LOCK:
            moment = self._now()
            state = self._read_state() or self._blank_state(DEFAULT_CADENCE_SECONDS, moment)
            cadence = (
                self._validate_cadence(cadence_seconds)
                if cadence_seconds is not None
                else int(state.get("cadence_seconds") or DEFAULT_CADENCE_SECONDS)
            )
            next_at = _iso(moment + timedelta(seconds=cadence))
            state_after = dict(state)
            state_after.update(
                {
                    "status": OUTCOME_RETRY,
                    "pending_retry": True,
                    "cadence_seconds": cadence,
                    "next_interval_at_utc": next_at,
                    "last_error": reason,
                    "worker_pid": None,
                    "updated_at_utc": _iso(moment),
                }
            )
            self._append_ledger(
                {
                    "kind": "PREFLIGHT_FAILURE",
                    "at_utc": _iso(moment),
                    "reason": reason,
                    "plan_id": self.binding.plan_id,
                    "plan_hash": self.binding.plan_hash,
                    "state_after": state_after,
                }
            )
            self._write_state(state_after)
            return {
                "state_status": OUTCOME_RETRY,
                "pending_retry": True,
                "next_interval_at_utc": next_at,
            }

    def _archive_claim(self, claim: Mapping[str, Any], moment: datetime, disposition: str) -> None:
        self.paths.claim_archive.mkdir(parents=True, exist_ok=True)
        archived = dict(claim)
        archived["released_at_utc"] = _iso(moment)
        archived["disposition"] = disposition
        name = f"{claim.get('attempt_id') or 'unknown'}.{disposition}.json"
        self._write_json(self.paths.claim_archive / name, archived)
        if self.paths.claim.exists():
            self.paths.claim.unlink()

    def _resolve_claim(self, claim: Mapping[str, Any], moment: datetime) -> dict[str, Any]:
        """Decide whether a held claim may be released. Never on elapsed time alone."""
        worker = self._identity_from(claim.get("worker"))
        child = self._identity_from(claim.get("child"))
        if worker is None:
            # An unbound handoff is unresolved until someone says what happened to it.
            return {"status": STATUS_CLAIM_UNRESOLVED, "attempt_id": claim.get("attempt_id")}
        for identity in (worker, child):
            if identity is None:
                continue
            liveness = self._is_live(identity)
            if liveness == "UNKNOWN":
                return {"status": STATUS_CLAIM_UNRESOLVED, "attempt_id": claim.get("attempt_id")}
            if liveness == "LIVE":
                return {"status": STATUS_ALREADY_RUNNING, "attempt_id": claim.get("attempt_id")}
        self._archive_claim(claim, moment, "OWNERS_EXITED")
        return {"status": "RELEASED", "attempt_id": claim.get("attempt_id")}

    def reconcile(self) -> dict[str, Any]:
        with _PROCESS_LOCK:
            moment = self._now()
            state = self._read_state()
            rows = self._ledger_rows()
            claim = self._read_claim()

            terminal = None
            for row in reversed(rows):
                if row.get("kind") in {"TERMINAL", "PREFLIGHT_FAILURE"}:
                    terminal = row
                    break

            # A terminal record that never reached the state is replayed from the ledger,
            # exactly once, rather than being written again.
            if terminal is not None and state is not None:
                after = terminal.get("state_after") or {}
                # Replay when the state does not already carry this outcome - a status
                # whitelist would miss a preflight failure recorded from IDLE. The attempt
                # must still match, so a previous attempt's outcome cannot be replayed
                # onto a newer one.
                if after and after != state:
                    same_attempt = (
                        terminal.get("kind") == "PREFLIGHT_FAILURE"
                        or terminal.get("attempt_id") == state.get("last_attempt_id")
                    )
                    if same_attempt:
                        self._write_state(after)
                        state = dict(after)

            result: dict[str, Any] = {
                "status": "IDLE",
                "state_status": (state or {}).get("status"),
                "pending_retry": bool((state or {}).get("pending_retry")),
                "next_interval_at_utc": (state or {}).get("next_interval_at_utc"),
                "last_attempt_id": (state or {}).get("last_attempt_id"),
            }
            if claim is None:
                return result

            resolved = self._resolve_claim(claim, moment)
            result["status"] = resolved["status"]
            if resolved["status"] != "RELEASED":
                return result

            # The owner is gone. If it never recorded an outcome, the attempt is a retry.
            if state is not None and state.get("status") in {"RUNNING", "LAUNCHING"}:
                cadence = int(state.get("cadence_seconds") or DEFAULT_CADENCE_SECONDS)
                next_at = _iso(moment + timedelta(seconds=cadence))
                state_after = dict(state)
                state_after.update(
                    {
                        "status": OUTCOME_RETRY,
                        "pending_retry": True,
                        "next_interval_at_utc": next_at,
                        "last_finished_at_utc": _iso(moment),
                        "last_error": "worker exited without recording an outcome",
                        "worker_pid": None,
                        "updated_at_utc": _iso(moment),
                    }
                )
                self._append_ledger(
                    {
                        "kind": "TERMINAL",
                        "attempt_id": state.get("last_attempt_id"),
                        "at_utc": _iso(moment),
                        "outcome": OUTCOME_RETRY,
                        "reason": "worker exited without recording an outcome",
                        "plan_id": self.binding.plan_id,
                        "plan_hash": self.binding.plan_hash,
                        "no_worker_spawned": False,
                        "state_after": state_after,
                    }
                )
                self._write_state(state_after)
                result.update(
                    {
                        "state_status": OUTCOME_RETRY,
                        "pending_retry": True,
                        "next_interval_at_utc": next_at,
                        "last_attempt_id": state_after.get("last_attempt_id"),
                    }
                )
            return result


__all__ = [
    "AutomationEngine",
    "AutomationPaths",
    "AutomationStateError",
    "Binding",
    "DEFAULT_CADENCE_SECONDS",
    "FROZEN_CADENCE_SECONDS",
    "ProbeResult",
    "ProcessIdentity",
]
