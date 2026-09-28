"""Same-directory durable staging and bounded Windows atomic-replace recovery.

This is not a writer lock. Callers must retain their existing writer/plan guards.
Only replacement is retried; a payload is serialized and staged once. A Windows
access/share/lock denial may be transient, as reproduced in isolated fixtures.
Persistent denials still fail after five attempts and preserve the old destination
and the failed staging file. No destination unlink or copy-over fallback is used.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Mapping


WINDOWS_REPLACE_DELAYS_SECONDS = (0.025, 0.05, 0.1, 0.2)
WINDOWS_REPLACE_RETRY_ERRORS = frozenset({5, 32, 33})


def atomic_write_bytes(path: Path | str, payload: bytes) -> None:
    """Replace a file after fsync+close; retain staging on every write failure.

    The total programmed retry wait is 375 ms, with at most five replace calls.
    This bounds scheduling delay, not OS-call duration. Windows error 5 can also
    mean permanent denial; such a denial is never treated as successful. Errors
    keep their original type/code, with a traceback note naming retained staging.
    """
    if not isinstance(payload, bytes):
        raise TypeError("atomic_write_bytes requires immutable bytes")
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f"{target.name}.tmp.", dir=target.parent)
    staged = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(len(WINDOWS_REPLACE_DELAYS_SECONDS) + 1):
            try:
                os.replace(staged, target)
                return
            except OSError as exc:
                retryable = (
                    os.name == "nt"
                    and getattr(exc, "winerror", None) in WINDOWS_REPLACE_RETRY_ERRORS
                )
                if not retryable or attempt == len(WINDOWS_REPLACE_DELAYS_SECONDS):
                    raise
                time.sleep(WINDOWS_REPLACE_DELAYS_SECONDS[attempt])
    except BaseException as exc:
        exc.add_note(f"Atomic write failed; prior destination was not unlinked; staged payload retained at {staged}")
        raise


def atomic_write_json(path: Path | str, payload: Mapping[str, Any]) -> None:
    """Preserve the automation JSON encoding contract, serializing before I/O."""
    data = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    atomic_write_bytes(path, data)


__all__ = ["atomic_write_bytes", "atomic_write_json"]
