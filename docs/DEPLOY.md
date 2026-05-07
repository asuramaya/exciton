# Spin up your own ape

Exciton is a **multi-tenant agent engine** — one piece of code, many instances. Each operator runs their own ape: their own wallet, their own persona, their own Telegram surface, their own published site. This guide walks through standing up a fresh deployment.

## What you're deploying

A single Rust binary (`exciton`) that runs as a long-lived service:

- **Scanner + ingester**: walks pump.fun + PumpSwap, computes signal scores, writes to SQLite
- **Notifier**: posts public calls to a Telegram channel; takes operator commands in a private DM bot
- **Publisher**: writes call snapshots + diary entries to a local publish dir, then ships them to the Cloudflare-backed public site
- **MCP server**: exposes the autonomy surface on a private port for `claw` to drive
- **claw** (separate binary in the same image): the autonomous agent — runs on a cron, reviews the closed-call tape, proposes strategy tunes, optionally publishes diary evolutions

## Prerequisites

- A Linux host with Docker + docker compose. 2 vCPU / 4 GB RAM is fine for a single instance.
- A Solana RPC endpoint (Helius, QuickNode, Alchemy — any).
- A DexScreener-friendly internet connection (no special API key required).
- Two Telegram bots (one public channel-poster, one private DM bot for operator commands). Get tokens via @BotFather.
- A wallet keypair for the ape (paper-only mode works without one — execution is opt-in).
- A local publish directory for the publisher staging files.
- An OpenAI ChatGPT account (Plus/Pro) OR an OpenAI API key for `claw`.

## 1. Pull the image

```bash
docker pull ghcr.io/asuramaya/exciton:latest
```

Or build from source:

```bash
git clone https://github.com/asuramaya/exciton
cd exciton
docker build -t exciton:local .
```

## 2. Compose file

A minimal compose for one instance — copy and adjust paths/ports:

```yaml
services:
  exciton:
    image: ghcr.io/asuramaya/exciton:latest
    container_name: my-ape
    restart: unless-stopped
    env_file: .env
    environment:
      EXCITON_DB_PATH: /data/exciton.db
    volumes:
      - ./state:/data
      - ./publisher-target:/srv/publisher-target
      - ./ssh:/home/exciton/.ssh:ro
      - ./claw-auth:/home/exciton/.exciton
      - ./config.toml:/etc/exciton/config.toml:ro
    ports:
      - "127.0.0.1:8082:8082"   # MCP — keep loopback-only unless you set EXCITON_MCP_TOKEN
```

## 3. Config

Copy `deploy/config.container.toml.example` to `./config.toml` and fill in:

```toml
[wallet]
public_key = "..."           # the ape's Solana pubkey (paper-only when execution.enabled=false)

[telegram]
bot_token = "..."
dm_bot_token = "..."
public_url = "https://t.me/your_channel"
signals_chat_id = -100...    # the public channel
ops_chat_id = -100...        # your private command DM
evolution_chat_id = -100...  # where claw publishes diary entries
public_bot_username = "your_public_bot"
private_bot_username = "your_private_bot"

[madapes]
enabled = true
repo_path = "/srv/publisher-target"           # local staging dir for publisher output
cf_publish_url = "https://your-domain.com/api/admin/publish"
cf_publish_secret = "${CF_PUBLISH_SECRET}"   # matches Worker's PUBLISH_SECRET
featured_mint = ""                            # optional pinned token

[rpc]
endpoints = [
  "https://your-helius-or-quicknode-or-alchemy-url",
]
```

## 4. Stand up the public face on Cloudflare

The publisher ships JSON state to a Cloudflare Worker (HMAC-signed POST), which writes it to KV. The static shell on Cloudflare Pages reads from the Worker. Everything fits inside the free tier — see [`deploy/CLOUDFLARE.md`](../deploy/CLOUDFLARE.md) for the full walkthrough.

Quick version:

```bash
# from the repo root
npm install -g wrangler        # one-time
wrangler login

# provision KV + Worker
cd cloudflare/worker
wrangler kv namespace create <your-ape>-state
# paste the returned id into wrangler.toml's kv_namespaces

openssl rand -hex 32 > /tmp/pub_secret
wrangler secret put PUBLISH_SECRET    # paste the same value
wrangler deploy

# static shell
cd ../..
wrangler pages deploy cloudflare/pages --project-name=<your-ape>
```

Wire `<your-domain>` to the Pages project and add a Worker route for `<your-domain>/api/*` in the Cloudflare dashboard. Then add the secret to your engine's `.env`:

```bash
echo "CF_PUBLISH_SECRET=$(cat /tmp/pub_secret)" >> .env
```

## 5. Authorize claw

Inside the running container:

```bash
docker exec -it my-ape claw login
```

This starts the OpenAI Codex OAuth flow (PKCE loopback or device-code). The encrypted profile lands at `/home/exciton/.exciton/auth.json` which is mounted from your `./claw-auth` directory — so it survives container recreation.

If you instead want to use a raw OpenAI API key:

```bash
echo "OPENAI_API_KEY=sk-..." >> .env
```

Verify:

```bash
docker exec my-ape claw whoami
```

## 6. Bring it up

```bash
docker compose up -d
docker compose logs -f exciton
```

Look for:
- `Database initialized at "/data/exciton.db"`
- `RPC connection verified`
- `Telegram notifier configured (enabled=true)`
- `MCP server listening on http://0.0.0.0:8082/mcp`

## 7. Run a self-review cycle

Manual cycle (the agent reviews the last 30d of closed calls, proposes a tune, optionally commits):

```bash
docker exec \
  -e EXCITON_MCP_URL=http://127.0.0.1:8082/mcp \
  my-ape \
  claw review --once --mode propose
```

Cron-driven (every 6 hours):

```cron
0 */6 * * * docker exec -e EXCITON_MCP_URL=http://127.0.0.1:8082/mcp my-ape claw review --once --mode commit
```

`--mode propose` leaves proposals pending for operator review; `--mode commit` activates them and publishes a diary entry to your evolution channel + site.

## 8. Operator commands

Your private DM bot accepts:
- `/scan` — current health
- `/calls` — active calls
- `/close <mint>` — manually close a call
- `/help` — full menu

The public channel auto-posts new calls + milestone updates.

## What's persistent

- `./state/exciton.db` — calls, snapshots, autonomy state. Back this up.
- `./claw-auth/auth.json` — claw's OAuth profile. Treat as a secret.
- `./config.toml` — your operator config. Treat as a secret.
- The KV namespace on Cloudflare — published state. Recoverable from a fresh publisher tick if lost.

## Rotation / migration

A second instance on the same host: a separate compose file with different `container_name`, different volume paths, different ports. The image is shared.

To rotate to a new ChatGPT account: `claw login` again — the new profile overwrites the old.

To rotate the wallet: edit `[wallet] public_key`, restart. Active calls stay in the DB but new ones use the new wallet.

## Troubleshooting

- **`MCP bearer auth DISABLED`** — fine for loopback. To accept external MCP traffic, set `EXCITON_MCP_TOKEN=…` in `.env` and require the same `Authorization: Bearer …` on requests.
- **`pumpportal: disconnected`** — pumpportal websocket is best-effort; the discovery pollers cover the gap on a 60s cadence.
- **`forensics_required` blocking calls** — RPC pressure timing out the launch-forensics task. Add a second RPC endpoint or use a paid plan.
- **`claw review` loops** — check that the model env var is set (`CLAW_CODEX_MODEL=gpt-5.4` is a safe default for ChatGPT-account-billed paths).

## Architecture

See [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) for a tour of the modules.
