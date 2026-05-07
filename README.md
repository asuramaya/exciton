# Exciton

**OSS crypto-influencer engine for Solana.** A deterministic on-chain scanner + LLM-driven autonomous-trading agent in a single Rust workspace. One codebase, many instances — every operator runs their own ape with their own wallet, persona, Telegram surface, and published site.

[madapesai.com](https://madapesai.com) is the reference deployment, not the product. The product is the engine that lets you spin up a different ape on the same code. The public shell for that reference deployment now lives in this repo under [`cloudflare/pages`](cloudflare/pages).

> **Status:** Active. Used in production by the original author; APIs and config schema may shift between minor versions. Pin a commit if you depend on stability.

---

## Quickstart

```bash
# 1. Pull the image
docker pull ghcr.io/asuramaya/exciton:latest

# 2. Copy + edit the example config
git clone https://github.com/asuramaya/exciton
cp exciton/deploy/config.container.toml.example config.toml
# edit config.toml: wallet, telegram tokens, publisher repo path, RPC endpoints

# 3. Run
docker run -d --name my-ape \
  -v "$(pwd)/state:/data" \
  -v "$(pwd)/config.toml:/etc/exciton/config.toml:ro" \
  -p 127.0.0.1:8082:8082 \
  ghcr.io/asuramaya/exciton:latest

# 4. Authorize claw (the autonomy agent)
docker exec -it my-ape claw login
```

Full walkthrough: [`docs/DEPLOY.md`](docs/DEPLOY.md).

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
2. **JSON snapshots** — `data/calls.json`, scout reports, whale snapshots — written to a local publish dir and shipped to the public site surface
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
                              CF Worker     (Claude, etc.)
                                  │
                                  ▼
                          edge KV + Pages
                       (your demo / dashboard)
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the signal pipeline, classification taxonomy, and call lifecycle.

## Quick start

### Requirements

- Rust 1.88+ (edition 2021)
- A Solana RPC endpoint — free tiers from [Helius](https://helius.dev), [Alchemy](https://alchemy.com), or [QuickNode](https://quicknode.com) all work
- Optional: a Telegram bot token if you want notifications
- Optional: a Cloudflare account (free tier) for the public face — see [`deploy/CLOUDFLARE.md`](deploy/CLOUDFLARE.md)

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
enabled = false   # flip to true to publish snapshots to your CF Worker
# cf_publish_url = "https://your-domain.com/api/admin/publish"
# cf_publish_secret = "${CF_PUBLISH_SECRET}"
```

See [`config.example.toml`](config.example.toml) for the full annotated reference.

## Configuration

Everything goes in `config.toml`. Secrets (API keys, bot tokens) reference env vars via `${VAR_NAME}` substitution — Exciton resolves them at startup.

Key sections:

- `[rpc]` — list of Solana RPC endpoints (round-robin + auto-failover)
- `[telegram]` — bot tokens, chat IDs, public/private bot usernames, public site URL
- `[madapes]` — JSON publisher: stages snapshots to a local dir, ships them via HMAC-signed POST to a Cloudflare Worker every 300s
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

The publisher snapshot loop runs every 300s. Each tick it writes the JSON files into a local staging dir, then ships the consolidated state to a Cloudflare Worker via an HMAC-signed POST:

```
<repo_path>/data/
├── calls.json          # active + historical calls with PnL
├── scout/<mint>.json   # per-token scout reports
├── whales/<mint>.json  # per-token whale activity
└── …
```

The Worker writes each present key (`diary`, `calls`, `strategy`) to KV; the public read endpoints (`/api/diary`, `/api/calls`, `/api/strategy`) serve them with edge cache. The MCP surface stays bound to `localhost` — exposing it publicly would let anyone burn your RPC budget.

To wire it up: follow [`deploy/CLOUDFLARE.md`](deploy/CLOUDFLARE.md) (fits inside Cloudflare's free tier), set `[madapes] enabled = true` with `cf_publish_url` + `cf_publish_secret` in your `config.toml`, restart the engine.

[madapesai.com](https://madapesai.com) is the live reference deployment, sourced from this repo's `cloudflare/pages` tree.

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
  publisher.rs         JSON snapshot loop + Cloudflare Worker push
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
