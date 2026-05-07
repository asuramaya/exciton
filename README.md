# Exciton

<p align="center">
  <img src="docs/exciton.png" alt="Exciton mascot" width="520">
</p>

<p align="center">
  <a href="https://github.com/asuramaya/exciton/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/asuramaya/exciton/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/asuramaya/exciton/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
  <img alt="Rust" src="https://img.shields.io/badge/rust-1.88%2B-orange">
  <img alt="Status" src="https://img.shields.io/badge/status-alpha-orange">
  <img alt="MCP" src="https://img.shields.io/badge/mcp-first-7c3aed">
</p>

<p align="center">
  <a href="https://madapesai.com"><b>Reference deployment</b></a> ·
  <a href="https://github.com/asuramaya/exciton/blob/main/docs/DEPLOY.md">Deploy guide</a> ·
  <a href="https://github.com/asuramaya/exciton/blob/main/docs/ARCHITECTURE.md">Architecture</a> ·
  <a href="https://github.com/asuramaya/exciton/blob/main/deploy/CLOUDFLARE.md">Cloudflare</a>
</p>

> MCP-first Solana intelligence engine. Deterministic on-chain scanning,
> signal scoring, Telegram surfaces, and a public publishing loop in one Rust
> workspace.

`exciton` is the engine. [`madapesai.com`](https://madapesai.com) is the live
reference deployment built from it, and the public shell for that deployment
now lives in this repo under [`cloudflare/pages`](cloudflare/pages).

## What It Does

- **Scans Solana continuously** across pump.fun and PumpSwap, picking up fresh mints, holder concentration, velocity, market depth, graduation state, and smart-wallet movement
- **Keeps the signal path deterministic**: classification and scoring stay in Rust, on-chain and market-data driven, with no LLM in the signal pipeline
- **Exposes multiple operator surfaces**: Telegram cards, a private MCP server, JSON snapshots, diary/state publishing, and a Cloudflare-backed public site
- **Supports many instances from one codebase**: each operator can run their own wallet, persona, Telegram surface, publish target, and control loop

## Quick Start

```bash
# Pull the image
docker pull ghcr.io/asuramaya/exciton:latest

# Clone and copy the container config
git clone https://github.com/asuramaya/exciton
cp exciton/deploy/config.container.toml.example config.toml

# Run the engine
docker run -d --name my-ape \
  -v "$(pwd)/state:/data" \
  -v "$(pwd)/config.toml:/etc/exciton/config.toml:ro" \
  -p 127.0.0.1:8082:8082 \
  ghcr.io/asuramaya/exciton:latest

# Authorize claw if you want the autonomous review surface
docker exec -it my-ape claw login
```

Full walkthrough: [`docs/DEPLOY.md`](docs/DEPLOY.md).

## Surfaces

| Surface | What it is for |
| --- | --- |
| **Scanner** | Discovery, scoring, re-analysis, digests, and token evidence collection |
| **Telegram** | Public call cards plus private operator commands |
| **Publisher** | Snapshot loop writing local state and shipping it to the public site |
| **MCP** | Private tool surface for agents and operators to inspect state and drive the system |
| **Cloudflare shell** | The public `madapesai.com` face under [`cloudflare/pages`](cloudflare/pages) |

## Architecture

```text
Solana RPC / DexScreener
          ↓
       exciton
  scanner · state · signals
      ↙      ↓       ↘
Telegram   JSON      MCP
surface   publish   surface
              ↓
      Cloudflare Worker + Pages
          madapesai.com
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the signal pipeline,
call lifecycle, and internal module split.

## Deployment Model

The intended deployment shape is:

1. `exciton` runs as the long-lived scanner/runtime
2. the publisher stages local JSON output and ships it to the Cloudflare Worker
3. the public shell in [`cloudflare/pages`](cloudflare/pages) reads from the Worker-backed endpoints
4. the MCP surface stays private unless you intentionally expose and secure it

[`madapesai.com`](https://madapesai.com) is the reference deployment, not the
product boundary.

## Configuration

Everything lives in `config.toml`, with secrets referenced through
`${VAR_NAME}` substitution.

Key sections:

- `[rpc]` — Solana RPC endpoints with round-robin and failover
- `[telegram]` — channel poster, DM bot, public URLs, chat IDs
- `[madapes]` — publisher staging path and Cloudflare publish settings
- `[mcp]` — MCP server transport and exposure
- `[execution]` — optional execution module

Start with [`config.example.toml`](config.example.toml) and
[`deploy/config.container.toml.example`](deploy/config.container.toml.example).

## Project Layout

```text
src/              engine, scanner loop, notifier, publisher, MCP server
crates/claw/      autonomous review / tuning agent
cloudflare/       public shell + Worker deployment
deploy/           container and deployment guides
docs/             architecture and operational docs
examples/         runnable examples
tests/            integration and regression tests
```

## Naming Note

The project was originally called `photon`. It has been renamed to `exciton`
across the crate, binary, image, env vars, and deployment docs. Old
`github.com/asuramaya/photon` URLs still redirect.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). For security issues, see
[`SECURITY.md`](SECURITY.md).

## License

[MIT](LICENSE).

This is research / educational software. Solana memecoins are highly
speculative and most lose money. Nothing here is financial advice.
