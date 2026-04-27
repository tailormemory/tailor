# Conventions

Project-wide conventions that aren't load-bearing in code but are load-bearing
in operations and forensics. New conventions land here when they have a
reason behind them that won't be obvious to a reader six months later.

---

## Operational artifact naming

Backups, audit JSON, and similar operational artifacts use the convention
`<file>.backup.<reason>_<timestamp>`.

Example:

```
db/chroma.sqlite3.backup.pre_track3a_20260425_183953
db/9ad2790b-7fe2-4242-b2f3-5e1d04a5b3dd.backup.pre_track3a_20260425_183953.tgz
```

The `<reason>` slot is **load-bearing** — it preserves forensic context that
source code cannot. Source code records the *current* design; operational
artifacts record what was actually attempted, in what order, and why.

The Apr 25 Track 3a recovery (1100-write idempotent re-upsert pattern, now
formalised in [`scripts/lib/chroma_persist.py`](../scripts/lib/chroma_persist.py))
would have been substantially harder to reconstruct without these names.
At the time the technique was a one-off shell session; the only
durable record was the backup filename. When v1.2.3 brought the pattern
back as a real feature, the backup mtimes plus the `pre_track3a` reason
slot were dispositive evidence the technique had been used and produced
a recoverable outcome.

Treat this convention as **non-optional for any operational backup**.
A backup named `chroma.sqlite3.backup.20260425` tells you *when* but not
*why* — and "why" is the part that matters when you're debugging a
silent regression months later.

### Reason slot guidelines

- Use lowercase, hyphen- or underscore-separated identifiers.
- Prefer `pre_<intervention>` for backups taken before an action; `post_<intervention>` for after.
- Reference a specific procedure, ticket, or incident — not just a vague label like `manual` or `safety`.
- Keep it short (≤ 40 chars). The full forensic narrative belongs in
  `CHANGELOG.md` or a `docs/` write-up; the filename is the index.

### Timestamp format

`YYYYMMDD_HHMMSS` in local time. Sortable, unambiguous, no timezone
ambiguity if the system timezone is consistent. Avoid ISO 8601 with `:`
characters in filenames — they're legal on Unix but break on Windows
shares and confuse some tooling.
