from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

if os.name == "nt":
    import msvcrt
else:  # pragma: no cover - Windows is the production host
    import fcntl

try:
    import psutil
except ImportError:  # pragma: no cover - deployment fallback remains fail-closed
    psutil = None


CLAIM_SCHEMA = "trading_mvp_global_market_writer_claim_v1"
RECOVERY_SCHEMA = "trading_mvp_global_market_writer_claim_recovery_v1"
WORKER_HANDOFF_SCHEMA = "trading_mvp_market_data_worker_handoff_v1"
SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


class GlobalMarketWriterClaimError(RuntimeError):
    pass


@contextmanager
def _claim_transaction_lock(
    claim_path: str | Path,
    *,
    timeout_seconds: float = 5.0,
) -> Iterator[Path]:
    """Serialize every canonical claim mutation on one persistent lock inode."""

    target = Path(claim_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.with_name(f"{target.name}.transaction.lock")
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    handle = lock_path.open("a+b", buffering=0)
    acquired = False
    try:
        if lock_path.stat().st_size == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        while True:
            handle.seek(0)
            try:
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:  # pragma: no cover - Windows is the production host
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except OSError as exc:
                if time.monotonic() >= deadline:
                    raise GlobalMarketWriterClaimError(
                        f"global writer claim transaction lock is busy: {lock_path}"
                    ) from exc
                time.sleep(0.01)
        yield lock_path
    finally:
        if acquired:
            try:
                handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:  # pragma: no cover - Windows is the production host
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                # Closing the descriptor releases the OS lock even if explicit
                # unlock fails; the persistent lock file must never be deleted.
                pass
        handle.close()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _process_started_at_utc(process_id: int) -> str | None:
    if psutil is None:
        return None
    try:
        started_at = psutil.Process(int(process_id)).create_time()
    except (OSError, psutil.Error, ValueError):
        return None
    return datetime.fromtimestamp(started_at, timezone.utc).isoformat()


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")


def _read_claim(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise GlobalMarketWriterClaimError("global writer claim must be an object")
    if value.get("schema") != CLAIM_SCHEMA:
        raise GlobalMarketWriterClaimError("global writer claim schema mismatch")
    return value


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_bytes(_json_bytes(payload))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _assert_owner(
    claim: Mapping[str, Any],
    *,
    run_id: str,
    owner_pid: int,
    ownership_token: str,
    expected_plan_hash: str | None = None,
    expected_owner_process_started_at_utc: str | None = None,
) -> None:
    if claim.get("project") != "trading_mvp":
        raise GlobalMarketWriterClaimError("global writer claim project mismatch")
    if claim.get("status") != "CLAIMED":
        raise GlobalMarketWriterClaimError("global writer claim status mismatch")
    if str(claim.get("run_id") or "") != run_id:
        raise GlobalMarketWriterClaimError("global writer claim run_id mismatch")
    if int(claim.get("owner_pid") or 0) != int(owner_pid):
        raise GlobalMarketWriterClaimError("global writer claim owner_pid mismatch")
    if str(claim.get("ownership_token") or "") != ownership_token:
        raise GlobalMarketWriterClaimError("global writer claim token mismatch")
    if (
        expected_plan_hash is not None
        and str(claim.get("plan_hash") or "") != expected_plan_hash
    ):
        raise GlobalMarketWriterClaimError("global writer claim plan_hash mismatch")
    if (
        expected_owner_process_started_at_utc is not None
        and str(claim.get("owner_process_started_at_utc") or "")
        != expected_owner_process_started_at_utc
    ):
        raise GlobalMarketWriterClaimError(
            "global writer claim owner process start mismatch"
        )


def _parse_aware_utc_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise GlobalMarketWriterClaimError(
            "global writer claim process start timestamp is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise GlobalMarketWriterClaimError(
            "global writer claim process start timestamp must include a timezone"
        )
    return parsed.astimezone(timezone.utc)


def _validate_recovery_claim(raw_claim: bytes) -> dict[str, Any]:
    try:
        claim = json.loads(raw_claim.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GlobalMarketWriterClaimError(
            "global writer claim is not valid JSON"
        ) from exc
    if not isinstance(claim, dict):
        raise GlobalMarketWriterClaimError("global writer claim must be an object")
    if claim.get("schema") != CLAIM_SCHEMA:
        raise GlobalMarketWriterClaimError("global writer claim schema mismatch")
    if claim.get("project") != "trading_mvp":
        raise GlobalMarketWriterClaimError("global writer claim project mismatch")
    if claim.get("status") != "CLAIMED":
        raise GlobalMarketWriterClaimError("global writer claim is not active")
    run_id = claim.get("run_id")
    if not isinstance(run_id, str) or SAFE_RUN_ID.fullmatch(run_id) is None:
        raise GlobalMarketWriterClaimError("global writer claim run_id is invalid")
    ownership_token = claim.get("ownership_token")
    if (
        not isinstance(ownership_token, str)
        or re.fullmatch(r"[0-9a-f]{32}", ownership_token) is None
    ):
        raise GlobalMarketWriterClaimError(
            "global writer claim ownership_token is invalid"
        )
    plan_hash = claim.get("plan_hash")
    if (
        not isinstance(plan_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", plan_hash) is None
    ):
        raise GlobalMarketWriterClaimError("global writer claim plan_hash is invalid")
    owner_kind = claim.get("owner_kind")
    if not isinstance(owner_kind, str) or SAFE_RUN_ID.fullmatch(owner_kind) is None:
        raise GlobalMarketWriterClaimError("global writer claim owner_kind is invalid")
    output_namespace = claim.get("output_namespace")
    if (
        not isinstance(output_namespace, str)
        or not output_namespace.strip()
        or not Path(output_namespace).is_absolute()
    ):
        raise GlobalMarketWriterClaimError(
            "global writer claim output_namespace is invalid"
        )
    claimed_at_utc = claim.get("claimed_at_utc")
    if not isinstance(claimed_at_utc, str) or not claimed_at_utc.strip():
        raise GlobalMarketWriterClaimError(
            "global writer claim claimed_at_utc is invalid"
        )
    _parse_aware_utc_timestamp(claimed_at_utc)
    owner_pid = claim.get("owner_pid")
    if isinstance(owner_pid, bool) or not isinstance(owner_pid, int) or owner_pid <= 0:
        raise GlobalMarketWriterClaimError("global writer claim owner_pid is invalid")
    for pid_field in ("writer_pid", "terminal_pid"):
        pid_value = claim.get(pid_field)
        if pid_value is not None and (
            isinstance(pid_value, bool)
            or not isinstance(pid_value, int)
            or pid_value <= 0
        ):
            raise GlobalMarketWriterClaimError(
                f"global writer claim {pid_field} is invalid"
            )
    if "owner_process_started_at_utc" in claim:
        started_at = claim["owner_process_started_at_utc"]
        if started_at is not None:
            if not isinstance(started_at, str) or not started_at.strip():
                raise GlobalMarketWriterClaimError(
                    "global writer claim process start timestamp is invalid"
                )
            _parse_aware_utc_timestamp(started_at)
    return claim


def _probe_owner_process(owner_pid: int) -> tuple[str, str | None]:
    if psutil is None:
        return "UNAVAILABLE", None
    try:
        started_at = psutil.Process(owner_pid).create_time()
    except psutil.NoSuchProcess:
        return "DEAD", None
    except (OSError, psutil.AccessDenied):
        return "UNAVAILABLE", None
    return (
        "LIVE",
        datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
    )


def _recovery_result(
    target: Path,
    *,
    status: str,
    reason: str,
    claim: Mapping[str, Any] | None = None,
    claim_sha256: str | None = None,
    observed_process_started_at_utc: str | None = None,
    archive_path: Path | None = None,
) -> dict[str, Any]:
    return {
        "schema": RECOVERY_SCHEMA,
        "status": status,
        "reason": reason,
        "recovered": status == "STALE_RECOVERED",
        "claim_path": str(target),
        "run_id": claim.get("run_id") if claim is not None else None,
        "owner_pid": claim.get("owner_pid") if claim is not None else None,
        "owner_process_started_at_utc": (
            claim.get("owner_process_started_at_utc")
            if claim is not None
            else None
        ),
        "claimed_at_utc": claim.get("claimed_at_utc") if claim is not None else None,
        "observed_process_started_at_utc": observed_process_started_at_utc,
        "claim_sha256": claim_sha256,
        "archive_path": str(archive_path) if archive_path is not None else None,
    }


def _claim_global_market_writer_unlocked(
    claim_path: str | Path,
    *,
    run_id: str,
    owner_pid: int,
    owner_kind: str,
    plan_hash: str | None,
    output_namespace: str | Path,
    writer_pid: int | None = None,
    terminal_pid: int | None = None,
    owner_process_started_at_utc: str | None = None,
    ownership_token: str | None = None,
) -> dict[str, Any]:
    if SAFE_RUN_ID.fullmatch(run_id) is None:
        raise ValueError("run_id contains unsafe characters")
    if int(owner_pid) <= 0:
        raise ValueError("owner_pid must be positive")
    if writer_pid is not None and int(writer_pid) <= 0:
        raise ValueError("writer_pid must be positive")
    token = ownership_token or secrets.token_hex(16)
    if re.fullmatch(r"[0-9a-f]{32}", token) is None:
        raise ValueError("ownership_token must be 32 lowercase hex characters")
    process_started_at = owner_process_started_at_utc or _process_started_at_utc(owner_pid)
    if process_started_at is not None:
        try:
            datetime.fromisoformat(process_started_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("owner_process_started_at_utc must be an ISO timestamp") from exc
    target = Path(claim_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "schema": CLAIM_SCHEMA,
        "project": "trading_mvp",
        "status": "CLAIMED",
        "run_id": run_id,
        "owner_pid": int(owner_pid),
        "writer_pid": int(writer_pid) if writer_pid else None,
        "terminal_pid": int(terminal_pid) if terminal_pid else None,
        "owner_kind": owner_kind,
        "ownership_token": token,
        "plan_hash": plan_hash,
        "output_namespace": str(Path(output_namespace).expanduser().resolve()),
        "claimed_at_utc": utc_now(),
        "owner_process_started_at_utc": process_started_at,
        "research_only": True,
        "live_orders": False,
        "private_api_keys": False,
        "real_capital": False,
        "leverage_or_margin": False,
    }
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except FileExistsError as exc:
        try:
            existing = _read_claim(target)
            detail = (
                f"run_id={existing.get('run_id')} "
                f"owner_pid={existing.get('owner_pid')} "
                f"writer_pid={existing.get('writer_pid')}"
            )
        except (OSError, ValueError, json.JSONDecodeError) as read_exc:
            detail = f"unreadable existing claim: {type(read_exc).__name__}"
        raise GlobalMarketWriterClaimError(
            f"GLOBAL_MARKET_WRITER_CLAIM_EXISTS: {detail}"
        ) from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_json_bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        target.unlink(missing_ok=True)
        raise
    return payload


def claim_global_market_writer(
    claim_path: str | Path,
    *,
    run_id: str,
    owner_pid: int,
    owner_kind: str,
    plan_hash: str | None,
    output_namespace: str | Path,
    writer_pid: int | None = None,
    terminal_pid: int | None = None,
    owner_process_started_at_utc: str | None = None,
    ownership_token: str | None = None,
    lock_timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    with _claim_transaction_lock(
        claim_path,
        timeout_seconds=lock_timeout_seconds,
    ):
        return _claim_global_market_writer_unlocked(
            claim_path,
            run_id=run_id,
            owner_pid=owner_pid,
            owner_kind=owner_kind,
            plan_hash=plan_hash,
            output_namespace=output_namespace,
            writer_pid=writer_pid,
            terminal_pid=terminal_pid,
            owner_process_started_at_utc=owner_process_started_at_utc,
            ownership_token=ownership_token,
        )


def consume_worker_handoff_receipt(
    claim_path: str | Path,
    *,
    receipt_path: str | Path,
    consumed_dir: str | Path,
    handoff_token: str,
    attempt_id: str,
    plan_hash: str,
    automation_id: str,
    lock_timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    """Validate and atomically consume one launcher-issued worker handoff.

    A receipt either binds an already-live canonical claim (derivative wrappers)
    or binds the exact token/identity of the canonical claim the spot worker must
    create before its first network or output operation.
    """

    if SAFE_RUN_ID.fullmatch(attempt_id) is None:
        raise GlobalMarketWriterClaimError("worker handoff attempt_id is invalid")
    if re.fullmatch(r"[0-9a-f]{64}", plan_hash) is None:
        raise GlobalMarketWriterClaimError("worker handoff plan_hash is invalid")
    if re.fullmatch(r"[0-9a-f]{32}", handoff_token) is None:
        raise GlobalMarketWriterClaimError("worker handoff token is invalid")
    if not automation_id or len(automation_id) > 160:
        raise GlobalMarketWriterClaimError("worker handoff automation_id is invalid")

    target_claim = Path(claim_path).expanduser().resolve()
    target_receipt = Path(receipt_path).expanduser().resolve()
    archive_root = Path(consumed_dir).expanduser().resolve()
    if target_receipt.name != f"{attempt_id}.json":
        raise GlobalMarketWriterClaimError("worker handoff receipt path is not attempt-bound")
    if archive_root != target_receipt.parent / "consumed":
        raise GlobalMarketWriterClaimError("worker handoff consumed directory is invalid")

    with _claim_transaction_lock(
        target_claim,
        timeout_seconds=lock_timeout_seconds,
    ):
        try:
            raw_receipt = target_receipt.read_bytes()
            receipt = json.loads(raw_receipt.decode("utf-8-sig"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GlobalMarketWriterClaimError(
                "worker handoff receipt is missing or unreadable"
            ) from exc
        if not isinstance(receipt, dict):
            raise GlobalMarketWriterClaimError("worker handoff receipt must be an object")
        exact_fields = {
            "schema": WORKER_HANDOFF_SCHEMA,
            "status": "ISSUED",
            "project": "trading_mvp",
            "automation_id": automation_id,
            "attempt_id": attempt_id,
            "plan_hash": plan_hash,
        }
        for field, expected in exact_fields.items():
            if receipt.get(field) != expected:
                raise GlobalMarketWriterClaimError(
                    f"worker handoff receipt {field} mismatch"
                )
        expected_handoff_sha = hashlib.sha256(handoff_token.encode("ascii")).hexdigest()
        if not hmac.compare_digest(
            str(receipt.get("handoff_token_sha256") or ""), expected_handoff_sha
        ):
            raise GlobalMarketWriterClaimError("worker handoff token mismatch")

        wrapper_pid = receipt.get("wrapper_pid")
        wrapper_started = receipt.get("wrapper_process_started_at_utc")
        if not isinstance(wrapper_pid, int) or wrapper_pid <= 0:
            raise GlobalMarketWriterClaimError("worker handoff wrapper_pid is invalid")
        if not isinstance(wrapper_started, str) or not wrapper_started:
            raise GlobalMarketWriterClaimError(
                "worker handoff wrapper process identity is missing"
            )
        wrapper_status, observed_wrapper_started = _probe_owner_process(wrapper_pid)
        if wrapper_status != "LIVE" or observed_wrapper_started is None:
            raise GlobalMarketWriterClaimError("worker handoff wrapper process is not live")
        stored_wrapper_start = _parse_aware_utc_timestamp(wrapper_started)
        observed_wrapper_start = _parse_aware_utc_timestamp(observed_wrapper_started)
        if abs((stored_wrapper_start - observed_wrapper_start).total_seconds()) > 1.0:
            raise GlobalMarketWriterClaimError(
                "worker handoff wrapper process identity mismatch"
            )

        claim_run_id = str(receipt.get("claim_run_id") or "")
        claim_owner_kind = str(receipt.get("claim_owner_kind") or "")
        claim_token_sha = str(receipt.get("claim_ownership_token_sha256") or "")
        claim_output_namespace = str(receipt.get("claim_output_namespace") or "")
        if SAFE_RUN_ID.fullmatch(claim_run_id) is None:
            raise GlobalMarketWriterClaimError("worker handoff claim_run_id is invalid")
        if not claim_owner_kind or len(claim_owner_kind) > 160:
            raise GlobalMarketWriterClaimError("worker handoff claim owner_kind is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", claim_token_sha) is None:
            raise GlobalMarketWriterClaimError("worker handoff claim token hash is invalid")
        try:
            resolved_output_namespace = str(Path(claim_output_namespace).expanduser().resolve())
        except (OSError, ValueError) as exc:
            raise GlobalMarketWriterClaimError(
                "worker handoff claim output namespace is invalid"
            ) from exc
        if claim_output_namespace != resolved_output_namespace:
            raise GlobalMarketWriterClaimError(
                "worker handoff claim output namespace is not canonical"
            )

        claim_must_exist = receipt.get("claim_must_exist")
        if not isinstance(claim_must_exist, bool):
            raise GlobalMarketWriterClaimError(
                "worker handoff claim_must_exist is invalid"
            )
        if claim_must_exist:
            try:
                claim = _validate_recovery_claim(target_claim.read_bytes())
            except (OSError, GlobalMarketWriterClaimError) as exc:
                raise GlobalMarketWriterClaimError(
                    "worker handoff canonical claim is missing or invalid"
                ) from exc
            claim_owner_pid = receipt.get("claim_owner_pid")
            claim_owner_started = receipt.get("claim_owner_process_started_at_utc")
            if not isinstance(claim_owner_pid, int) or claim_owner_pid <= 0:
                raise GlobalMarketWriterClaimError(
                    "worker handoff claim owner_pid is invalid"
                )
            claim_exact = {
                "run_id": claim_run_id,
                "owner_pid": claim_owner_pid,
                "owner_kind": claim_owner_kind,
                "plan_hash": plan_hash,
                "output_namespace": claim_output_namespace,
                "owner_process_started_at_utc": claim_owner_started,
            }
            for field, expected in claim_exact.items():
                if claim.get(field) != expected:
                    raise GlobalMarketWriterClaimError(
                        f"worker handoff canonical claim {field} mismatch"
                    )
            observed_claim_token_sha = hashlib.sha256(
                str(claim.get("ownership_token") or "").encode("ascii")
            ).hexdigest()
            if not hmac.compare_digest(claim_token_sha, observed_claim_token_sha):
                raise GlobalMarketWriterClaimError(
                    "worker handoff canonical claim token mismatch"
                )
        elif target_claim.exists():
            raise GlobalMarketWriterClaimError(
                "worker handoff expected canonical claim to be absent"
            )

        # Re-read immediately before the atomic move.  Any mutation leaves both
        # the active receipt and canonical claim untouched and fails closed.
        try:
            if target_receipt.read_bytes() != raw_receipt:
                raise GlobalMarketWriterClaimError(
                    "worker handoff receipt changed before consume"
                )
        except OSError as exc:
            raise GlobalMarketWriterClaimError(
                "worker handoff receipt disappeared before consume"
            ) from exc
        archive_root.mkdir(parents=True, exist_ok=True)
        archive_path = archive_root / (
            f"{attempt_id}.{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
        )
        os.replace(target_receipt, archive_path)
        if archive_path.read_bytes() != raw_receipt:
            if not target_receipt.exists():
                os.replace(archive_path, target_receipt)
            raise GlobalMarketWriterClaimError(
                "worker handoff receipt identity changed during consume"
            )
        return {
            "schema": WORKER_HANDOFF_SCHEMA,
            "status": "CONSUMED",
            "attempt_id": attempt_id,
            "plan_hash": plan_hash,
            "claim_run_id": claim_run_id,
            "claim_owner_kind": claim_owner_kind,
            "claim_ownership_token_sha256": claim_token_sha,
            "claim_output_namespace": claim_output_namespace,
            "claim_must_exist": claim_must_exist,
            "wrapper_pid": wrapper_pid,
            "wrapper_process_started_at_utc": wrapper_started,
            "archive_path": str(archive_path),
        }


def _attach_writer_pid_unlocked(
    claim_path: str | Path,
    *,
    run_id: str,
    owner_pid: int,
    ownership_token: str,
    writer_pid: int,
) -> dict[str, Any]:
    if int(writer_pid) <= 0:
        raise ValueError("writer_pid must be positive")
    target = Path(claim_path).expanduser().resolve()
    claim = _read_claim(target)
    _assert_owner(
        claim,
        run_id=run_id,
        owner_pid=owner_pid,
        ownership_token=ownership_token,
    )
    claim["writer_pid"] = int(writer_pid)
    claim["writer_attached_at_utc"] = utc_now()
    _write_json_atomic(target, claim)
    return claim


def attach_writer_pid(
    claim_path: str | Path,
    *,
    run_id: str,
    owner_pid: int,
    ownership_token: str,
    writer_pid: int,
    lock_timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    with _claim_transaction_lock(
        claim_path,
        timeout_seconds=lock_timeout_seconds,
    ):
        return _attach_writer_pid_unlocked(
            claim_path,
            run_id=run_id,
            owner_pid=owner_pid,
            ownership_token=ownership_token,
            writer_pid=writer_pid,
        )


def _release_global_market_writer_unlocked(
    claim_path: str | Path,
    *,
    run_id: str,
    owner_pid: int,
    ownership_token: str,
    final_status: str,
    archive_dir: str | Path | None = None,
    expected_plan_hash: str | None = None,
    expected_owner_process_started_at_utc: str | None = None,
) -> Path:
    target = Path(claim_path).expanduser().resolve()
    claim = _read_claim(target)
    _assert_owner(
        claim,
        run_id=run_id,
        owner_pid=owner_pid,
        ownership_token=ownership_token,
        expected_plan_hash=expected_plan_hash,
        expected_owner_process_started_at_utc=expected_owner_process_started_at_utc,
    )
    claim["status"] = "RELEASED"
    claim["final_status"] = final_status
    claim["released_at_utc"] = utc_now()
    claim["owner_pid"] = None
    claim["writer_pid"] = None
    archive_root = (
        Path(archive_dir).expanduser().resolve()
        if archive_dir is not None
        else target.parent / "global-writer-claim-archive"
    )
    archive_root.mkdir(parents=True, exist_ok=True)
    archive_path = archive_root / (
        f"{run_id}.{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    )
    os.replace(target, archive_path)
    try:
        _write_json_atomic(archive_path, claim)
    except OSError:
        pass
    return archive_path


def release_global_market_writer(
    claim_path: str | Path,
    *,
    run_id: str,
    owner_pid: int,
    ownership_token: str,
    final_status: str,
    archive_dir: str | Path | None = None,
    expected_plan_hash: str | None = None,
    expected_owner_process_started_at_utc: str | None = None,
    lock_timeout_seconds: float = 5.0,
) -> Path:
    with _claim_transaction_lock(
        claim_path,
        timeout_seconds=lock_timeout_seconds,
    ):
        return _release_global_market_writer_unlocked(
            claim_path,
            run_id=run_id,
            owner_pid=owner_pid,
            ownership_token=ownership_token,
            final_status=final_status,
            archive_dir=archive_dir,
            expected_plan_hash=expected_plan_hash,
            expected_owner_process_started_at_utc=expected_owner_process_started_at_utc,
        )


def _recover_stale_global_market_writer_claim_unlocked(
    claim_path: str | Path,
    *,
    archive_dir: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(claim_path).expanduser().resolve()
    if not target.exists():
        return _recovery_result(
            target,
            status="ABSENT",
            reason="claim_absent",
        )

    try:
        first_raw_claim = target.read_bytes()
        first_claim_sha256 = hashlib.sha256(first_raw_claim).hexdigest()
        claim = _validate_recovery_claim(first_raw_claim)
    except (OSError, GlobalMarketWriterClaimError):
        return _recovery_result(
            target,
            status="BLOCKED",
            reason="claim_unreadable_or_invalid",
        )

    owner_pid = int(claim["owner_pid"])
    process_status, observed_started_at = _probe_owner_process(owner_pid)
    if process_status == "UNAVAILABLE":
        return _recovery_result(
            target,
            status="BLOCKED",
            reason="process_identity_unavailable",
            claim=claim,
            claim_sha256=first_claim_sha256,
        )

    stored_started_at = claim.get("owner_process_started_at_utc")
    if process_status == "LIVE":
        if stored_started_at is None:
            return _recovery_result(
                target,
                status="LIVE_PRESERVED",
                reason="owner_process_live_missing_identity",
                claim=claim,
                claim_sha256=first_claim_sha256,
                observed_process_started_at_utc=observed_started_at,
            )
        stored_start = _parse_aware_utc_timestamp(str(stored_started_at))
        observed_start = _parse_aware_utc_timestamp(str(observed_started_at))
        if abs((stored_start - observed_start).total_seconds()) <= 1.0:
            return _recovery_result(
                target,
                status="LIVE_PRESERVED",
                reason="owner_process_live_exact_identity",
                claim=claim,
                claim_sha256=first_claim_sha256,
                observed_process_started_at_utc=observed_started_at,
            )
        stale_reason = "owner_process_identity_mismatch"
    else:
        stale_reason = "owner_process_dead"

    try:
        second_raw_claim = target.read_bytes()
    except OSError:
        return _recovery_result(
            target,
            status="BLOCKED",
            reason="claim_changed_before_archive",
            claim=claim,
            claim_sha256=first_claim_sha256,
            observed_process_started_at_utc=observed_started_at,
        )
    if hashlib.sha256(second_raw_claim).hexdigest() != first_claim_sha256:
        return _recovery_result(
            target,
            status="BLOCKED",
            reason="claim_changed_before_archive",
            claim=claim,
            claim_sha256=first_claim_sha256,
            observed_process_started_at_utc=observed_started_at,
        )

    archive_root = (
        Path(archive_dir).expanduser().resolve()
        if archive_dir is not None
        else target.parent / "global-writer-claim-archive"
    )
    try:
        archive_root.mkdir(parents=True, exist_ok=True)
        archive_path = archive_root / (
            f"{claim['run_id']}.stale."
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}."
            f"{first_claim_sha256[:12]}.json"
        )
        if archive_path.exists():
            raise FileExistsError(str(archive_path))
        os.replace(target, archive_path)
    except OSError:
        return _recovery_result(
            target,
            status="BLOCKED",
            reason="claim_archive_failed",
            claim=claim,
            claim_sha256=first_claim_sha256,
            observed_process_started_at_utc=observed_started_at,
        )

    try:
        archived_raw_claim = archive_path.read_bytes()
    except OSError:
        return _recovery_result(
            target,
            status="BLOCKED",
            reason="archive_evidence_unreadable",
            claim=claim,
            claim_sha256=first_claim_sha256,
            observed_process_started_at_utc=observed_started_at,
            archive_path=archive_path,
        )
    if hashlib.sha256(archived_raw_claim).hexdigest() != first_claim_sha256:
        if not target.exists():
            try:
                os.replace(archive_path, target)
            except OSError:
                pass
        return _recovery_result(
            target,
            status="BLOCKED",
            reason="archive_evidence_hash_mismatch",
            claim=claim,
            claim_sha256=first_claim_sha256,
            observed_process_started_at_utc=observed_started_at,
            archive_path=archive_path if archive_path.exists() else None,
        )
    return _recovery_result(
        target,
        status="STALE_RECOVERED",
        reason=stale_reason,
        claim=claim,
        claim_sha256=first_claim_sha256,
        observed_process_started_at_utc=observed_started_at,
        archive_path=archive_path,
    )


def recover_stale_global_market_writer_claim(
    claim_path: str | Path,
    *,
    archive_dir: str | Path | None = None,
    lock_timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    with _claim_transaction_lock(
        claim_path,
        timeout_seconds=lock_timeout_seconds,
    ):
        return _recover_stale_global_market_writer_claim_unlocked(
            claim_path,
            archive_dir=archive_dir,
        )


def inspect_global_market_writer_claim(
    claim_path: str | Path,
) -> dict[str, Any] | None:
    target = Path(claim_path).expanduser().resolve()
    return _read_claim(target) if target.exists() else None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage the single global trading_mvp market-writer claim."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    claim = subparsers.add_parser("claim")
    claim.add_argument("--path", required=True)
    claim.add_argument("--run-id", required=True)
    claim.add_argument("--owner-pid", type=int, required=True)
    claim.add_argument("--owner-kind", required=True)
    claim.add_argument("--plan-hash")
    claim.add_argument("--output-namespace", required=True)
    claim.add_argument("--writer-pid", type=int)
    claim.add_argument("--terminal-pid", type=int)
    claim.add_argument("--owner-process-started-at-utc")

    attach = subparsers.add_parser("attach")
    attach.add_argument("--path", required=True)
    attach.add_argument("--run-id", required=True)
    attach.add_argument("--owner-pid", type=int, required=True)
    attach.add_argument("--ownership-token", required=True)
    attach.add_argument("--writer-pid", type=int, required=True)

    release = subparsers.add_parser("release")
    release.add_argument("--path", required=True)
    release.add_argument("--run-id", required=True)
    release.add_argument("--owner-pid", type=int, required=True)
    release.add_argument("--ownership-token", required=True)
    release.add_argument("--final-status", required=True)
    release.add_argument("--archive-dir")
    release.add_argument("--plan-hash")
    release.add_argument("--owner-process-started-at-utc")

    recover_stale = subparsers.add_parser("recover-stale")
    recover_stale.add_argument("--path", required=True)
    recover_stale.add_argument("--archive-dir")

    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--path", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.command == "claim":
        result: Any = claim_global_market_writer(
            args.path,
            run_id=args.run_id,
            owner_pid=args.owner_pid,
            owner_kind=args.owner_kind,
            plan_hash=args.plan_hash,
            output_namespace=args.output_namespace,
            writer_pid=args.writer_pid,
            terminal_pid=args.terminal_pid,
            owner_process_started_at_utc=args.owner_process_started_at_utc,
        )
    elif args.command == "attach":
        result = attach_writer_pid(
            args.path,
            run_id=args.run_id,
            owner_pid=args.owner_pid,
            ownership_token=args.ownership_token,
            writer_pid=args.writer_pid,
        )
    elif args.command == "release":
        result = {
            "archive_path": str(
                release_global_market_writer(
                    args.path,
                    run_id=args.run_id,
                    owner_pid=args.owner_pid,
                    ownership_token=args.ownership_token,
                    final_status=args.final_status,
                    archive_dir=args.archive_dir,
                    expected_plan_hash=args.plan_hash,
                    expected_owner_process_started_at_utc=(
                        args.owner_process_started_at_utc
                    ),
                )
            )
        }
    elif args.command == "recover-stale":
        result = recover_stale_global_market_writer_claim(
            args.path,
            archive_dir=args.archive_dir,
        )
    else:
        result = inspect_global_market_writer_claim(args.path)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
