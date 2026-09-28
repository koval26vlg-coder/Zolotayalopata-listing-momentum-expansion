# Expansion failure diagnostics, 2026-09-25

Status: `DIAGNOSTICS_FIXED_NOT_CAPTURE_RECOVERED`.

This package is offline development only. It does not run discovery, collect candles,
change the universe or budget cap, repair production state, release claims, commit,
promote a registry, or enable any scheduler.

## Evidence and cause

The wrapper's production attempts ledger last records
`attempt_20260922T113956.669711Z_bf65cbd8` ending at
`2026-09-22T11:39:58.651578Z` with `RETRY_NEXT_INTERVAL` and
`no terminal record was printed`.

The child terminal ledger stops earlier, at
`expansion_tick_20260921T1405035711880Z`. Its durable record and manifest agree:

- `status=STOPPED_INCOMPLETE`, `jobs_total=1510`, `jobs_attempted=0`;
- `primary_error=ExpansionMonitorError: new listings 1510 exceed tick cap 50`;
- `retry_queue=[]`, `jobs_pending_retry=1` (a generic retry marker, not a durable
  list of the 1510 candidates).

The first recorded cap failure was `expansion_tick_20260902T1345085121996Z`, at
66 candidates. The ledger contains 53 cap failures in total. This is a persistent
collector blocker, not evidence that 1510 genuine new listings were found.

The immutable preflight baseline has 2698 rows: Binance 733, Bybit 409, OKX 367,
Bitget 1189. Current snapshots and the proposed job list are not retained in failed
cap manifests. Therefore the exact composition and cause of the historical jump to
1510 cannot be reconstructed from the retained evidence alone. Do not repair that
baseline by guessing, raise the cap, or label those candidates acceptance-eligible.

The old child plan fixes the cap at 50. It does not specify a durable discovered-event
registry and cap-defer queue. Implementing bounded progress without starvation or lost
events needs a separate reviewed child-plan successor, not a silent slice of the list.

The child monitor records a failed tick durably and then reraises. Its CLI only prints
the success-shaped terminal JSON when `run_tick` returns. The four-witness wrapper
therefore refuses the absent terminal, correctly, but used to discard the actual
traceback diagnostic. The wrapper also omitted `-Json` when calling the launcher, so a
preflight refusal was returned only as human text. These are the defects fixed here.

No child receipt or terminal attempt exists for the final September 22 launches.
Their precise original preflight failure remains unknown: the original output was not
retained. A read-only `-PreflightOnly -Json` on September 25 returned `ok=true`,
`PLAN_OK`, gate `READY_FOR_POSTPROCESS`, and writer claim `ABSENT`. This does not
retroactively identify the September 22 cause or prove a market-data tick will work.

## Change

- Pass `-Json` to the existing child launcher.
- For non-zero exit plus absent terminal JSON, preserve a bounded recognized failure
  diagnostic in the retry reason, explicitly labelled `unverified child output`.
- Keep the result `RETRY_NEXT_INTERVAL`, empty counts, no manifest identity and no
  cadence escalation. No success/evidence verification rule was relaxed.
- Preserve a strict 512-character diagnostic bound. A zero exit with no terminal still
  fails closed without adopting a stale diagnostic.

The monitor, child launcher, child plan v17, baseline and old wrapper plans are unchanged.

## RED / GREEN evidence

Baseline: `python -m unittest discover -s trading_mvp/tests -p "test_*.py"`:
`Ran 49 tests ... OK`.

After adding the diagnostic regressions, before editing production code:

```text
Ran 35 tests in 0.306s
FAILED (failures=4)
AssertionError: 'unverified child output' not found in 'no terminal record was printed'
AssertionError: 'new listings 1510 exceed tick cap 50' not found in 'no terminal record was printed'
AssertionError: 'active_run_gate_invalid' not found in 'no terminal record was printed'
AssertionError: '-Json' not found in ['pwsh.exe', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', 'fixture.ps1', '-PlanPath', 'fixture.json', '-VisibleWorker', '-ScheduledTick']
```

After the minimal code fix: `Ran 54 tests in 0.346s ... OK`.

The successor-plan tests then failed three assertions against the old v11 identity and
lineage, before new version constants or plan bytes were written:
`Ran 5 tests in 0.003s ... FAILED (failures=3)`.

Final dedicated offline suite after versioned generation:

```text
Ran 55 tests in 0.571s
OK
```

The tests include rebuilding the new plan from its existing generator and comparing the
entire resulting object, not just its declared hash.

### Independent verification caveat

A subsequent parent-agent full-suite run did **not** reproduce an entirely green run:
55 tests ran, 53 passed and two existing terminal-claim-cleanup tests errored with
`PermissionError: [WinError 5] Access is denied` at
`listing_automation_state.py:144`, `temporary.replace(path)`. The targets were
temporary-directory `state.json` and `claim.json`, not production state.

The requested single targeted rerun of those two tests then returned:

```text
Ran 2 tests in 0.058s
OK
```

Both the engine implementation and these two tests have no diff from HEAD. The engine
writes and closes a unique temporary file, then uses `Path.replace` for atomic replacement;
the test uses isolated temporary directories. The cause of the transient access denial
has not been identified. Filesystem sharing or an external scanner is a hypothesis, not
a finding, and an engine-level reliability defect is not ruled out. No retry loop or
engine change was added. The verification result is therefore **55 passed in the initial
run, independent full-suite run flaky (2 WinError5), targeted rerun 2 passed**, not an
unqualified claim of stable all-green execution. Runtime activation remains blocked.

## Immutable successor and activation boundary

New wrapper: `listing_momentum_expansion_automation_20260925_v12`.

- Canonical plan hash:
  `2150fdb52cc034afc1e4f75d530ce1a5bf039b9df8c931394faafc29ac8dbe52`.
- File SHA-256:
  `06dfb0f6bd23b658a6f1e208a576447bc296ccf7ceb5bb5d21ee375394e3c997`.
- Superseded v11 file SHA-256 remains
  `8eb133a18ee0a0a7f0dcf96a4e373139e9361e8c55d539226dd45b247d231795`.
- Child v17 canonical hash remains
  `9f90509ff04a48ca8897b4056fba2a37d8a366be744ed36b0a5c4c5104dfbc1d`;
  child `--plan-check` returns `PLAN_OK`.

The read-only wrapper `--check` returns the expected fail-closed result:

```text
PLAN_BLOCKED: artifact is not committed: ...listing-momentum-expansion-automation-planonly-20260925-v12.json
execution_performed=false
```

This package is intentionally not an operational recovery. Before later activation:
review and resolve the cap/discovery backlog contract; commit the reviewed immutable
package; preserve/archive the existing wrapper-state directory according to `AGENTS.md`;
initialize a new state via the launch probe; re-pin and publish the canonical registry;
then re-bind the coordinator only with explicit activation authority. The old state must
not be relabelled with the new hash. None of these activation steps was performed here.

Existing dirty terminal ledger, market-data state and consumed handoffs were preserved.
