# ZolotyayLopata — listing momentum expansion runtime

The scheduler-routable runtime for the spot listing-momentum **expansion** track:
Binance, Bybit, OKX, Bitget. Public data only, research only, no orders and no capital.

## Why this is a separate repository

The canonical strategy registry pins each routable runtime to a specific commit of the
repository its artifacts live in, and the validator refuses when that repository has moved
past the pin. In a repository people work in, HEAD moves constantly — including on the very
commit that updates the registry, which leaves HEAD one commit past the pin it just wrote.
A routable runtime therefore has to live somewhere HEAD moves only when the runtime itself
changes. That is what this repository is for, and it is the same reason the spot-v2 monitor
and the pre-market capture runtimes were carved out before it.

## What is here

Only what a PlanOnly artifact binds by sha256, copied byte-for-byte from the working
repository, plus the plans that bind them — reissued here, because a plan records absolute
paths and those changed with the move.

- `trading_mvp/src/` — the wrapper (contract, state engine, runner, child-evidence
  verifier) and the child monitor it launches, with their transitive local imports.
- `tools/` — the visible launchers and the active-run gate checker.
- `docs/plans/` — the immutable PlanOnly files.

The internal layout mirrors the working repository rather than flattening to `src/` the way
the older carve-outs did. Every module resolves its repository root as `parents[2]` and
imports its siblings by bare name, so mirroring means nothing had to be rewritten — and a
rewrite is exactly where a byte-for-byte copy would have stopped being one.

## What is deliberately absent

No collected data. The 33 windows gathered before the carve-out stay in the working
repository as legacy evidence: they were collected under monitor plan v5 and cannot be
renamed into evidence for the current plan. Windows collected here start from zero, under
v14, which is the point of the exercise.

## Line endings

`core.autocrlf` is true on this machine, so every bound file carries an explicit `-text`
attribute. Without it a fresh checkout rewrites the bytes the plans hash and every binding
fails validation — not at edit time, which is what makes it easy to miss.
