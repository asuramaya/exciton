# Security Policy

## Reporting a vulnerability

**Do not file a public GitHub issue for security problems.**

If you find a vulnerability — credential leak, RCE, privilege escalation, anything that lets a stranger spend funds, impersonate the operator, or read private data — report it privately:

- Open a [GitHub Security Advisory](https://github.com/asuramaya/exciton/security/advisories/new) (preferred), **or**
- Email the maintainer via the address listed on the GitHub profile.

Please include: a clear description, reproduction steps, the commit/version affected, and (if applicable) a suggested fix. Expect an acknowledgement within 72 hours.

## Scope

In scope:

- The Rust crate in this repository
- Configuration / environment loading
- The MCP server surface
- The Telegram long-poll surface
- The publisher's git push path

Out of scope:

- Third-party dependencies (report upstream — happy to coordinate)
- The Solana RPC provider you choose
- DexScreener / external APIs
- Bugs in the deployed `madapesai.com` site (it's a sister project — its repo lives separately)

## Operating securely

- Never inline secrets in `config.toml`. Use `${VAR}` references and put the actual values in `.env`. Both `config.toml` and `.env` are gitignored — keep them that way.
- Run with the smallest viable RPC quota. Exciton self-throttles, but a leaked endpoint URL with embedded auth is still a way to exhaust your provider's quota.
- The operator-DM Telegram bot is admin-only via `admin_user_ids`. Setting that list to `[]` makes the bot ignore every command — fail-safe default. Do not run with the public bot's token in `dm_bot_token`.
- `[execution].enabled = false` is the default. Do not flip it on without reading `src/execution.rs` end-to-end and understanding the position-sizing math.

## Disclosure

After a fix lands we'll publish a security advisory describing the issue, affected versions, and credit the reporter (unless they request anonymity).
