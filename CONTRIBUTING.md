# Contributing to Exciton

Thanks for your interest. A few notes before you open a PR.

## Scope

In scope:

- Bug fixes and reproducible-issue reports
- Performance improvements (especially RPC efficiency — the system is RPC-bound)
- New signal classifications, distribution / momentum / forensics signals
- New MCP tools that surface existing data
- New ingestion sources (additional DEXes, indexers, etc.)
- Documentation improvements

Out of scope (please don't open PRs for these without discussing first):

- New Telegram surfaces beyond the existing two-bot model — the rate-limit + 409 reasoning is load-bearing
- LLM-in-the-pipeline changes — the design choice is "deterministic Rust signals, LLM only at the operator surface." Latency and reproducibility were the reasons.
- Wholesale refactors of the SQLite schema. Migrations are painful; small additive changes are fine.

## Workflow

1. **Open an issue first** if the change is non-trivial. Saves both of us writing-then-discarding code.
2. Fork, branch from `main`, work in your fork.
3. Keep PRs focused. One concern per PR.
4. `cargo build` and `cargo test` must pass. The CI will check.
5. New runtime behavior needs a test, even if just a small one.
6. No new dependencies without a paragraph in the PR explaining why an existing dep can't do the job.

## Style

- `cargo fmt` on every change. The repo uses default rustfmt.
- Comments explain *why* (non-obvious constraints, subtle invariants), not *what*. The code already says what.
- No new top-level files in `docs/` without justification — most documentation belongs in inline rustdoc or in `README.md` / `docs/ARCHITECTURE.md`.
- Tests live in `tests/` for integration and inline `#[cfg(test)]` for unit. Examples in `examples/` are full programs, not test fixtures.

## Reporting a bug

Please include:

- Exciton version (commit SHA)
- Rust version (`rustc --version`)
- A minimal reproduction or, if not minimal, a snippet of the offending log lines
- What you expected vs. what happened

Bugs that touch live trading or money loss should be reported privately — see [`SECURITY.md`](SECURITY.md).

## Naming

The project is `exciton` — crate name, binary, Docker image, env vars (`EXCITON_*`), and on-disk paths (`/etc/exciton`, `/opt/exciton`). The only places the word `photon` survives are intentional external references (e.g. `photon-sol.tinyastro.io`, an unrelated third-party site). Don't reintroduce `photon` in new code.
