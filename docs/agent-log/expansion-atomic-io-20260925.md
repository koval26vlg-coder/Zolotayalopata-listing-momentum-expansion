# Expansion atomic file operations: 2026-09-25

Status: **BOUNDED_TRANSIENT_REPLACE_RECOVERY_IMPLEMENTED_NOT_ACTIVATED**.

Scope is the dedicated Expansion checkout and temporary test directories. No
production state, ledger, claims, historical plans, scheduler, registry, market API,
credential, commit or deployment was changed by this subtask. The wrapper/child
immutable successors must bind the new helper before runtime activation.

## Observed fault and investigation

The first fresh run of the existing terminal-claim cleanup tests reproduced the
reported failure, without any fault injection:

```text
Ran 2 tests
FAILED (errors=2)
test_retry_retains_claim_while_attached_child_is_live_or_unknown:
  LIVE: PermissionError [WinError 5] at listing_automation_state.py:144
  UNKNOWN: PermissionError [WinError 5] at listing_automation_state.py:144
```

Both failures were the `state.json` replacement inside `bind_worker`, in separate
`listing-terminal-safety-*` temporary directories. The state engine writes and
closes its own staging handle before replacement; tests do not retain an open
reader. Nothing in this run attempted production state mutation.

A smaller control removed all automation/claim logic: 120 sequential operations
created a new temporary JSON file, closed it, then used `os.replace` on the same
destination in a newly allocated temporary directory. Eight operations failed
spontaneously with Windows error 5, errno 13: sequence numbers 15, 38, 44, 58, 69,
70, 71, 72. In all eight cases:

- original destination and staging file both still existed;
- target mode was `0o100666`, Windows file attributes 32 (archive, not read-only);
- **the exact same source/destination rename succeeded after one 25 ms wait**;
- no rewrite, permission change, destination deletion or new staging file occurred.

Observed replacement+wait durations were 26.360--28.538 ms. This establishes a
transient filesystem replacement denial independent of the claim state machine.
It does **not** identify the process/filter responsible. An antivirus or external
reader remains an unverified explanation, not a finding. Windows error 5 can also
mean permanent denial and cannot safely be treated as success.

A real-kernel regression separately opens an existing target using `CreateFileW`
with read/write sharing but without delete sharing. Replacement fails while this
known handle is alive and succeeds after closing it during the first backoff.
That verifies the intended sharing-denial recovery path; it does not prove that
this handle pattern caused the uncontrolled error-5 observations above.

## Repair contract

New module: `trading_mvp/src/listing_atomic_io.py`.

Public APIs:

```python
atomic_write_bytes(path: Path | str, payload: bytes) -> None
atomic_write_json(path: Path | str, payload: Mapping[str, Any]) -> None
```

- JSON is serialized before filesystem mutation. UTF-8, two-space indentation and
  terminal LF are explicit; future state writes have deterministic LF rather than
  relying on Windows text-mode newline translation. Old files are not rewritten
  as a migration.
- Staging is created exclusively using `mkstemp` in the destination directory.
- Payload is written once, flushed, fsynced and closed before `os.replace`.
- Only Windows `winerror` 5 (access), 32 (share), 33 (lock) are eligible for the
  fixed 25/50/100/200 ms backoff, at most five replacement calls and 375 ms of
  programmed waits. OS syscall duration itself cannot be bounded by this helper.
- Retries use the same closed staging file and destination. No network/request or
  collector retry loop is introduced.
- Persistent errors propagate with their original exception type and code. Other
  errors, including generic permission errors without a Windows code, are not
  retried. A traceback note names the retained staging path.
- On failure, prior destination and staging remain. There is no unlink-destination,
  copy-over or non-atomic fallback, and no automatic cleanup of evidence files.
- This helper supplies atomic replacement, **not mutual exclusion**. Callers must
  retain their existing writer/gate/token/lineage guards. It does not repair a
  missing permission, absent disk space, permanent open handle or competing writer.
- `AutomationEngine._write_json` delegates to this helper. Terminal ledger-before-
  state ordering, pending-retry behavior and claim release rules are unchanged.

`fsync` on staging improves write durability but is not a claim of universal
power-loss durability for filesystem directory metadata.

## Verification

Tests were written against the old implementation first. Initial new suite:

```text
Ran 6 tests
FAILED (failures=5)
```

The five expected failures covered missing bounded retry, missing fsync,
replacement despite injected fsync failure, and absent retained-path diagnostics.
The existing ledger-before-state recovery behavior already passed and was retained.

After implementation and additional OS-boundary/serialization coverage:

```powershell
& 'C:\Program Files\Python313\python.exe' -B -m unittest discover `
  -s trading_mvp/tests -p 'test_listing_atomic_io.py' -v
# Ran 10 tests, OK

& 'C:\Program Files\Python313\python.exe' -B -m unittest discover `
  -s trading_mvp/tests -p 'test_listing_terminal_claim_cleanup.py' -v
# Ran 2 tests, OK
```

New coverage includes exact byte preservation, serialization-before-I/O,
transient/persistent Windows errors, non-retryable errors, real delete-sharing
conflict, staging fsync+close ordering, disk-full failure, retained old state and
claim, and exactly-once terminal-ledger recovery after state-write failure.

A bounded post-fix fixture experiment performed 240 writes and independent JSON
readbacks, with 240 actual replacement calls, no observed spontaneous denials and
no remaining staging files, in 1.109 seconds. It is a finite local observation,
not proof of infinite stability. The real-handle and injected-denial tests exercise
the retry path even when this uncontrolled stress run has no denial.

## Files owned by this subtask

- Changed `trading_mvp/src/listing_automation_state.py`.
- Added `trading_mvp/src/listing_atomic_io.py`.
- Added `trading_mvp/tests/test_listing_atomic_io.py`.
- Added this receipt.

At implementation freeze, SHA-256:

| File | SHA-256 |
|---|---|
| `listing_automation_state.py` | `ed0d35fa4c6b8cfe0f7f37bf1819ac8787ebf511f783527e98f6134823c78bcb` |
| `listing_atomic_io.py` | `4861784be6f902ed39aa6b5521d8df4fae283e9a14ed787581c691e060ba7b1a` |
| `test_listing_atomic_io.py` | `0c9ff64133d9e7a52adb95a94e6aa4671d8e4f1294a3b5bf46563c911e8b4ba4` |

Parent integration owns the queue, other monitor writes, immutable plans and
`.gitattributes`. This receipt alone does not certify that full runtime, whose
plan rebind and independent combined tests are separate gates.
