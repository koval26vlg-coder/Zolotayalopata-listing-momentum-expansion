"""Run one expansion tick on schedule, and record honestly what came of it.

This is the wrapper the scheduler wakes. It owns no collection logic of its own; the
child monitor does the work in a visible window, and this process decides three things
around it: whether the tick is due at all, whether a previous attempt is still running,
and - once the child has exited - what actually happened.

The third question is the one that used to be answered badly. A wrapper that believes a
child's exit code will report success for a run that wrote nothing, and a wrapper that
believes the child's own stdout will report success for a run that printed COMPLETED and
then died. So the outcome here comes from ``verify_child_outcome``, which cross-examines
four independent records, and this module simply carries that verdict into the state.

What it deliberately does not do:

* **It does not kill a slow child.** If the child is still running when the wrapper's
  patience runs out, the attempt is left open rather than concluded. The next wake finds
  a claim whose child is still alive and reports ALREADY_RUNNING, which is true. Reaping
  on elapsed time would turn "taking longer than expected" into "failed", and would free
  a claim the child is still writing under.
* **It does not write on the cheap path.** A wake that is not due reads the state and
  exits. That is what makes a five-minute scheduler tick affordable at six-hour cadence.
* **It does not resolve an ambiguous claim.** An unbound handoff, or a process the
  operating system will not answer for, ends the wake with a non-zero exit and no state
  change, because either is a question for a person.

The visible-window requirement is met by streaming the child's output through this
process rather than by opening a second window: the scheduler launches this wrapper
visibly, and everything the child prints appears there as it happens.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

try:
    import psutil
except ImportError:  # pragma: no cover - the launcher refuses without it
    psutil = None  # type: ignore[assignment]

from adaptive_cadence import decide_cadence
from listing_automation_state import (
    AutomationEngine,
    AutomationPaths,
    AutomationStateError,
    Binding,
    ProbeResult,
    ProcessIdentity,
    STATUS_ALREADY_RUNNING,
    STATUS_CLAIM_UNRESOLVED,
    STATUS_LAUNCHING,
    STATUS_NOT_DUE,
)
from listing_expansion_automation_contract import (
    PLAN_RELATIVE_PATH,
    REPO_ROOT,
    AutomationContractError,
    validate_plan,
)
from listing_expansion_child_evidence import verify_child_outcome

OUTCOME_RETRY = "RETRY_NEXT_INTERVAL"


class ExpansionAutomationError(RuntimeError):
    """The wake cannot proceed, and no state may be written on a guess."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _powershell() -> Path:
    """PowerShell by absolute path, never by search order.

    Resolving an interpreter through PATH lets whatever is earliest on it decide what
    runs (CWE-426). The scheduler runs this unattended, so the interpreter is addressed
    where Windows actually keeps it."""
    system_root = os.environ.get("SystemRoot") or r"C:\Windows"
    path = Path(system_root) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    if not path.is_file():
        raise ExpansionAutomationError(f"PowerShell not found at {path}")
    return path


def _identity(pid: int) -> ProcessIdentity:
    """A process's identity: its number and the moment it started.

    The start time is what makes the number mean something. Windows reuses PIDs, and a
    claim held by "pid 4242" is worthless once some unrelated process is wearing it."""
    if psutil is None:
        raise ExpansionAutomationError("psutil is required to identify a process exactly")
    started = psutil.Process(int(pid)).create_time()
    return ProcessIdentity(
        pid=int(pid), started_at_utc=_iso(datetime.fromtimestamp(started, timezone.utc))
    )


def _probe(pid: int) -> ProbeResult:
    """Ask the operating system about a process, and pass on what it says.

    An access error is UNKNOWN, not DEAD. Treating "I may not look" as "it is gone"
    would release a claim the process is still writing under."""
    if psutil is None:
        return ProbeResult(status="UNKNOWN")
    try:
        process = psutil.Process(int(pid))
        started = process.create_time()
        if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
            return ProbeResult(status="DEAD")
    except psutil.NoSuchProcess:
        return ProbeResult(status="DEAD")
    except (psutil.AccessDenied, psutil.Error, OSError, ValueError):
        return ProbeResult(status="UNKNOWN")
    return ProbeResult(
        status="LIVE",
        identity=ProcessIdentity(
            pid=int(pid),
            started_at_utc=_iso(datetime.fromtimestamp(started, timezone.utc)),
        ),
    )


def _engine(plan: Mapping[str, Any]) -> AutomationEngine:
    automation = plan["automation"]
    return AutomationEngine(
        AutomationPaths(Path(automation["root"])),
        Binding(
            plan_id=str(plan["plan_id"]),
            plan_hash=str(plan["plan_hash"]),
            child_plan_hash=str(plan["child_plan"]["plan_hash"]),
        ),
        now=_now,
        process_probe=_probe,
    )


def _stream_child(
    process: "subprocess.Popen[str]", deadline_sec: int
) -> tuple[str | None, int | None]:
    """Relay the child's output as it arrives, and return it with the exit code.

    Returns ``(None, None)`` when the child outlives the deadline. Nothing is killed and
    nothing is concluded in that case - the caller leaves the attempt open."""
    collected: list[str] = []
    assert process.stdout is not None
    try:
        for line in process.stdout:
            collected.append(line)
            sys.stdout.write(line)
            sys.stdout.flush()
        exit_code = process.wait(timeout=deadline_sec)
    except subprocess.TimeoutExpired:
        return None, None
    return "".join(collected), int(exit_code)


def run_once(
    plan_path: Path, *, repo_root: Path = REPO_ROOT, dry_run: bool = False
) -> dict[str, Any]:
    """One scheduler wake, start to finish."""
    plan = validate_plan(plan_path, repo_root=repo_root)
    automation = plan["automation"]
    engine = _engine(plan)

    engine.initialize(cadence_seconds=int(automation["default_interval_sec"]))
    reconciled = engine.reconcile()

    due = engine.read_due()
    if due["status"] == STATUS_NOT_DUE:
        # The cheap path, and the common one. It has written nothing.
        return {
            "status": STATUS_NOT_DUE,
            "plan_id": plan["plan_id"],
            "next_interval_at_utc": due.get("next_interval_at_utc"),
            "reconciled": reconciled,
            "execution_performed": False,
        }
    if dry_run:
        return {
            "status": "DUE_DRY_RUN",
            "plan_id": plan["plan_id"],
            "next_interval_at_utc": due.get("next_interval_at_utc"),
            "reconciled": reconciled,
            "execution_performed": False,
        }

    attempt = engine.begin_attempt()
    if attempt["status"] in (STATUS_NOT_DUE, STATUS_ALREADY_RUNNING, STATUS_CLAIM_UNRESOLVED):
        return {
            "status": attempt["status"],
            "plan_id": plan["plan_id"],
            "attempt_id": attempt.get("attempt_id"),
            "execution_performed": False,
        }
    if attempt["status"] != STATUS_LAUNCHING:
        raise ExpansionAutomationError(f"unexpected attempt status: {attempt['status']!r}")

    attempt_id = str(attempt["attempt_id"])
    token = str(attempt["handoff_token"])

    try:
        engine.bind_worker(attempt_id, token, _identity(os.getpid()))
        shell = _powershell()
        launcher = Path(str(plan["child_launcher_path"]))
        child_plan_path = Path(str(plan["child_plan"]["path"]))
    except (ExpansionAutomationError, AutomationStateError, OSError) as exc:
        engine.finish_attempt(
            attempt_id, token, outcome=OUTCOME_RETRY,
            reason=f"preflight_failed: {exc}", no_worker_spawned=True,
        )
        raise

    started_at = _iso(_now())
    process = subprocess.Popen(
        [
            str(shell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", str(launcher),
            "-PlanPath", str(child_plan_path),
            "-VisibleWorker", "-ScheduledTick",
        ],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    try:
        child_identity = _identity(process.pid)
        engine.attach_child(attempt_id, token, child_identity)
    except (AutomationStateError, ExpansionAutomationError, OSError) as exc:
        # The child is already running and owns the market-data claim; concluding the
        # attempt here would free this claim under a live writer. Leave it open.
        raise ExpansionAutomationError(
            f"child launched but could not be bound to the attempt: {exc}"
        ) from exc

    patience = int(automation["max_runtime_sec"]) + int(automation["terminal_grace_sec"])
    stdout_text, exit_code = _stream_child(process, patience)
    finished_at = _iso(_now())

    if stdout_text is None or exit_code is None:
        # Still running. Say so and stop; the next wake will find it alive.
        return {
            "status": "CHILD_STILL_RUNNING",
            "plan_id": plan["plan_id"],
            "attempt_id": attempt_id,
            "child_pid": process.pid,
            "patience_sec": patience,
            "execution_performed": True,
            "attempt_left_open": True,
        }

    verdict = verify_child_outcome(
        stdout_text=stdout_text,
        exit_code=exit_code,
        child_plan=json.loads(child_plan_path.read_text(encoding="utf-8")),
        child_identity=child_identity.as_dict(),
        started_at_utc=started_at,
        finished_at_utc=finished_at,
        repo_root=repo_root,
    )

    cadence = decide_cadence(verdict["cadence_observation"])
    finished = engine.finish_attempt(
        attempt_id,
        token,
        outcome=verdict["status"],
        reason=verdict.get("reason"),
        cadence_seconds=int(cadence.interval_sec),
    )
    return {
        "status": verdict["status"],
        "reason": verdict.get("reason"),
        "plan_id": plan["plan_id"],
        "attempt_id": attempt_id,
        "child_exit_code": exit_code,
        "child_tick_id": verdict.get("child_tick_id"),
        "child_manifest_sha256": verdict.get("child_manifest_sha256"),
        "counts": verdict.get("counts") or {},
        "pending_jobs": len(verdict.get("pending_jobs") or []),
        "cadence": cadence.as_dict(),
        "next_interval_at_utc": finished.get("next_interval_at_utc"),
        "execution_performed": True,
    }


def launch_probe(
    plan_path: Path, *, repo_root: Path = REPO_ROOT
) -> dict[str, Any]:
    """What a scheduler may be told before anything is started.

    The due coordinator expects its launcher to answer in one breath and return: is a
    worker already alive, is this not due after all, or may one be started. It then waits
    on the pid the launcher hands back. So this reads and reports; it starts nothing and
    claims nothing, because a probe that called begin_attempt to find out whether it
    could begin would create the claim it was asking about.

    A held-but-unresolved claim is reported as its own answer rather than as either
    branch: an unbound handoff means nobody recorded what happened to a process, and
    guessing in either direction is how a second collector gets launched beside a first.
    """
    plan = validate_plan(plan_path, repo_root=repo_root)
    engine = _engine(plan)
    engine.initialize(cadence_seconds=int(plan["automation"]["default_interval_sec"]))
    verdict = engine.inspect()

    common = {
        "plan_id": plan["plan_id"],
        "plan_hash": plan["plan_hash"],
        "next_interval_at_utc": verdict.get("next_interval_at_utc"),
        "state_status": verdict.get("state_status"),
        "execution_performed": False,
    }
    claim = verdict.get("claim")
    if claim == "RUNNING":
        # Reported before due-ness: a live worker is the more consequential truth, and
        # answering NOT_DUE while one is running is exactly the handoff the coordinator
        # refuses as unable to establish that nobody is collecting.
        return {**common, "verdict": "ALREADY_RUNNING", "worker_pid": verdict.get("worker_pid")}
    if claim == "UNRESOLVED":
        return {**common, "verdict": "UNRESOLVED", "attempt_id": verdict.get("attempt_id")}
    if verdict.get("status") == STATUS_NOT_DUE:
        return {**common, "verdict": "NOT_DUE"}
    return {**common, "verdict": "LAUNCH"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, default=REPO_ROOT / PLAN_RELATIVE_PATH)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--status", action="store_true", help="read the state; write nothing")
    actions.add_argument("--reconcile", action="store_true", help="replay unrecorded terminal evidence")
    actions.add_argument("--dry-run", action="store_true", help="validate and report due-ness only")
    actions.add_argument("--tick", action="store_true", help="run one attempt if due")
    actions.add_argument(
        "--launch-probe", action="store_true",
        help="report what a scheduler may do next; starts nothing and claims nothing",
    )
    args = parser.parse_args(argv)

    try:
        if args.status:
            plan = validate_plan(args.plan, repo_root=args.repo_root)
            payload = {
                "plan_id": plan["plan_id"],
                "plan_hash": plan["plan_hash"],
                **_engine(plan).status(),
                "execution_performed": False,
            }
        elif args.launch_probe:
            payload = launch_probe(args.plan, repo_root=args.repo_root)
        elif args.reconcile:
            plan = validate_plan(args.plan, repo_root=args.repo_root)
            payload = {
                "plan_id": plan["plan_id"],
                "reconciled": _engine(plan).reconcile(),
                "execution_performed": False,
            }
        else:
            payload = run_once(args.plan, repo_root=args.repo_root, dry_run=args.dry_run)
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    except (
        AutomationContractError,
        AutomationStateError,
        ExpansionAutomationError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "status": "BLOCKED",
                    "reason": f"{type(exc).__name__}: {exc}",
                    "execution_performed": False,
                },
                ensure_ascii=False,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
