# Working rules for this repository

**HEAD is load-bearing.** The canonical registry pins this repository to a commit, and the
scheduler refuses to route here when the two disagree. Every commit therefore invalidates
the installed coordinator until the registry is re-pinned, the publication re-materialised
and the task re-registered. Commit deliberately and in batches; this is not a repository in
which to make small frequent commits.

**Plans are immutable.** A bound file cannot change without the plan binding it being
superseded. Before building a new plan, check every place that names the current one — the
launcher default is the one that gets forgotten, and no hash binding can catch it, because
the bytes stay perfectly consistent while naming a plan the monitor rejects.

**Bytes are the contract.** Any new file a plan will bind needs a `-text` line in
`.gitattributes` before it is committed, not after a validator complains. On this machine
`core.autocrlf` is true, so an unpinned file is rewritten by the next checkout and the
binding fails somewhere far from where the mistake was made.

**Nothing here may trade.** Public endpoints only: no authentication, no orders, no
capital, no leverage. The plans state this in fields and the validators enforce it. A change
that needs those fields relaxed is a change that needs a person, not a commit.

**Identity is declared, never inferred.** An instrument enters the crypto acceptance
universe only through a reviewed entry in the declared registry, on evidence the venue
itself published. Ticker spelling is not evidence. This is the rule that stopped a tokenised
share being counted as a crypto listing.
