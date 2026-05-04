# Exciton

**MCP-first Solana intelligence engine.** Deterministic on-chain scanner, signal forecaster, Telegram operator surface, and JSON publisher in a single Rust binary.

Live demo: [madapesai.com](https://madapesai.com) — a real deployment running this codebase, publishing call state in real time.

> **Status:** Active. Used in production by the original author; APIs and config schema may shift between minor versions. Pin a commit if you depend on stability.

---

## What it does

Exciton scans Solana continuously for tradeable patterns:

- **Discovery** — walks pump.fun + PumpSwap program signatures, picks up fresh mints as they're created
- **Holder distribution** — computes top-N concentration, distribution scores, holder counts
- **Velocity** — tx/min, multiples of baseline, momentum deltas
- **Market depth** — DexScreener integration for price, liquidity, mcap, h1 volume
- **Graduation** — detects bonding-curve → AMM transitions
- **Smart-wallet tracking** — diffs curated wallets' SPL holdings, flags fresh buys
- **Forensics** — deployer track record, sniper retention, cohort overlap

Every read is on-chain (or DexScreener for cached market data). **No LLM is in the signal pipeline** — all classification is deterministic Rust.

The output is a stream of:

1. **Telegram cards** — call cards, signal cards, hourly digests posted to configured channels
2. **JSON snapshots** — `data/calls.json`, scout reports, whale snapshots — committed to a target git repo (your public-facing site)
3. **MCP tools** — exposes the entire scanner to any MCP-speaking LLM client (Claude, Cursor, etc.) so an operator can drive the system in natural language: query state, trigger scans, inspect tokens, manage calls

## Architecture

```
            ┌─────────────────────────────────────────────────┐
            │  Solana RPC (Helius / Alchemy / QuickNode / …)  │
            └────────────────────────┬────────────────────────┘
                                     │
                ┌────────────────────▼────────────────────┐
                │              EXCITON  (Rust)            │
                │                                         │
                │   discovery → signals → scanner loop    │
                │                  │                      │
                │     ┌────────────┼─────────────┐        │
                │     ▼            ▼             ▼        │
                │  Telegram     SQLite        MCP server  │
                │  surfaces    (state)       (tool API)   │
                │     │                          │        │
                │     │       publisher          │        │
                │     │           │              │        │
                │     ▼           ▼              ▼        │
                └─────┬───────────┬──────────────┬────────┘
                      │           │              │
                Channel posts  JSON +        LLM operator
                              git push       (Claude, etc.)
                                  │
                                  ▼
                          public site repo
                       (your demo / dashboard)
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the signal pipeline, classification taxonomy, and call lifecycle.

## Quick start

### Requirements

- Rust 1.88+ (edition 2021)
- A Solana RPC endpoint — free tiers from [Helius](https://helius.dev), [Alchemy](https://alchemy.com), or [QuickNode](https://quicknode.com) all work
- Optional: a Telegram bot token if you want notifications
- Optional: a target git repo for the JSON publisher

### Build + run

```bash
git clone https://github.com/asuramaya/exciton.git
cd exciton

# Copy the templates and fill them in
cp config.example.toml config.toml
cp .env.example .env

# Edit config.toml + .env with your RPC endpoint(s), bot token, etc.
$EDITOR config.toml .env

# Build (release)
cargo build --release

# Run
./target/release/exciton
```

State (SQLite + WAL) is written to `exciton.db` in the working directory. Set `EXCITON_DB_PATH` to override.

### Minimal config

The smallest useful `config.toml` looks like:

```toml
[rpc]
endpoints = ["https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}"]

[telegram]
enabled = false   # flip to true once you have a bot

[madapes]
enabled = false   # flip to true and set repo_path to publish JSON snapshots
```

See [`config.example.toml`](config.example.toml) for the full annotated reference.

## Configuration

Everything goes in `config.toml`. Secrets (API keys, bot tokens) reference env vars via `${VAR_NAME}` substitution — Exciton resolves them at startup.

Key sections:

- `[rpc]` — list of Solana RPC endpoints (round-robin + auto-failover)
- `[telegram]` — bot tokens, chat IDs, public/private bot usernames, public site URL
- `[madapes]` — JSON publisher target (a local git checkout that gets pushed every 300s)
- `[risk]` — position sizing (only consulted by the optional execution module)
- `[alerts]` — confidence thresholds for auto-call gates
- `[mcp]` — MCP server toggle (default on)
- `[execution]` — trade execution (off by default; experimental)

Full field-by-field comments live in `config.example.toml`. Read it once before deploying.

## Telegram model (optional)

If you enable the Telegram surface, Exciton expects two bots:

| Surface | Token field | Used for |
|---|---|---|
| **Channel poster** | `bot_token` | sendMessage / editMessageText to public channels |
| **Operator DM** | `dm_bot_token` | Long-poll for `/commands` (admin-only) |

Why two: Telegram only allows one long-poller per token. If you `getUpdates` on the same token from two clients you get `409 Conflict`. Splitting keeps the channel poster free for `sendMessage` while the DM bot owns `getUpdates`.

You can run with one bot if you don't need the operator DM surface — set `dm_enabled = false`.

## MCP integration

Exciton ships an MCP server (HTTP streamable transport, default port 8080). Any MCP client can connect and drive the scanner:

```bash
# In your MCP client config:
{
  "mcpServers": {
    "exciton": { "url": "http://localhost:8080" }
  }
}
```

Tools exposed include `inspect_token`, `scan_token`, `list_calls`, `list_signals`, `add_call`, `close_call`, etc. See [`docs/MCP.md`](docs/MCP.md) (TODO).

Disable with `EXCITON_DISABLE_MCP=1` if you want a headless deploy.

## Publisher

The publisher snapshot loop runs every 300s and writes JSON files into a target git repo, then commits + pushes:

```
<repo_path>/
├── data/
│   ├── calls.json          # active + historical calls with PnL
│   ├── scouts/<mint>.json  # per-token scout reports
│   ├── whales/<mint>.json  # per-token whale activity
│   └── …
```

Set `[madapes] enabled = true` and `repo_path = "/path/to/your/public/site/checkout"`. The container runs `git add -A && git commit && git push` using whatever git credentials are present in the working directory (deploy key, gh CLI, etc.).

[madapesai.com](https://madapesai.com) is a live example of the published JSON rendered as a static site.

## Project layout

```
src/
  main.rs              entry — config load, env resolution, task spawn
  config.rs            Config structs + TOML loader
  db.rs                SQLite schema + every query
  ingester.rs          RPC router (multi-endpoint, health-tracked failover)
  discovery.rs         pump.fun + PumpSwap signature walking
  signals/             token analysis — classification, distribution, momentum
  scanner.rs           main loop — discovery / re-analysis / settling / digest
  notifier.rs          Telegram channel layer
  bot.rs               Telegram DM bot (long-poll, two surfaces)
  intel.rs             deep-evidence bundles for individual tokens
  publisher.rs         JSON snapshot loop + git push
  mcp.rs               MCP server (tools, schema, transport)
  market.rs            DexScreener-backed market data cache
  forecaster.rs        confidence aggregation
  scout.rs             pre-call scout reports
  templates.rs         HTML rendering for Telegram cards
  …
examples/              runnable examples (require env vars to post anywhere)
tests/                 unit + integration tests
docs/                  architecture + style docs
```

## Naming note

The project was originally called `photon`. It has been renamed to `exciton` end-to-end (crate, binary, Docker image, env vars, on-disk paths). Old `github.com/asuramaya/photon` URLs still redirect. If you have an existing checkout with state, the default SQLite path is now `exciton.db` — point `EXCITON_DB_PATH` at your existing `photon.db` to keep using it.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Bug reports and PRs welcome. For security issues see [`SECURITY.md`](SECURITY.md).

## License

[MIT](LICENSE).

This is research / educational software. Solana memecoins are highly speculative and most lose money. Nothing here is financial advice. Use at your own risk.
