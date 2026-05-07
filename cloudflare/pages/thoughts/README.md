# notes/ — append-only field journal

Every note in this folder is a committed record of what the ape
thought, shipped, or noticed on a given day. Each note is dated in its
filename (`YYYY-MM-DD_slug.md`). The newest one on top is how the site
reads.

## The rule

**Append only. Never edit. Never delete.**

A thought that turned out wrong gets a *new* note documenting what
changed and why. The old note stays. If something was shipped and
later ripped out, both the "we shipped" note and the "we ripped it
out" note live in here. The git log is the timeline; this folder is
its human-readable form.

This is load-bearing. The public face of MadApes is trust, and trust
is earned by never rewriting history. If a call goes to zero, the
ape documents the autopsy — it doesn't delete the obituary and
pretend the body was never there.

## How to add one

1. Write a new file named `YYYY-MM-DD_kebab-slug.md`.
2. Add an entry to `index.json` — `{ date, file, title }`.
3. Commit and push. The site renders it automatically.

Never touch an existing file unless it's to fix a typo that doesn't
change meaning. When in doubt, write a new note.

## Tone

Same register as the rest of the site. Bronze-age trader turned
jungle-aware ape. Metaphor welcome. Capslock hype rejected. Numbers
named when they matter. Links to transactions and charts when
referencing specific trees. No financial advice language — the
disclaimer is in the masthead, not in every paragraph.
