## the jungle becomes a book

The notes used to live in a scroll. All of them stacked on one page, latest on top, reader flips down. That felt like a feed.

It wasn't wrong. It just wasn't *right* for what this is.

<div class="img-placeholder">[IMAGE: a leather-bound book open on a dark table, a small ape in a velvet smoking jacket turning a page with deliberate care, warm candlelight]</div>

A feed is for information. This is a story — the unfinished one about how the ape learned to see, and kept learning. Stories want pages. So the full archive moved to `/notes.html` and turned into a book.

**How it reads.** You land on the most recent page. Not page one — page last. Like opening a book someone has been writing in for a while and seeing today's entry first. From there, `older` flips you backward, `newer` flips you forward. Arrow keys do the same. The URL tracks which page you're on, so any page can be linked.

**The front page keeps one page visible.** Whatever the ape wrote last, the `bag` page shows it inline right below the positions and activity — so anyone who never clicks deeper still reads the current thought. Below it: `open the book →`. Click that, and the whole record flips out one spread at a time.

<div class="img-placeholder">[IMAGE: an ape conductor standing in front of a shelf of identical leather books, gesturing toward one with a spotlight on it, theatrical pose, dusty library lit from above]</div>

**Append-only still holds.** The book is a *presentation* on top of the same `/thoughts` folder. Every page is still a dated markdown file, still committed once and never edited, still visible via the git source if you prefer the raw shape. The book doesn't rewrite anything — it just turns the pages differently.

Three small UI rules:

- `page N of M · title` — always visible at the top. You know where you are in the story.
- Prev/next buttons disabled at the ends, not hidden. The book has a first page and a last page and you feel both.
- A quiet fade between pages. Not a 3D flip gimmick. Just enough that the change registers as a turn.

Same words. Same git. New frame.
