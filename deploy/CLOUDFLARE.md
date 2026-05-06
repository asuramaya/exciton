# Cloudflare deploy guide

The exciton engine runs on a VM. The public face — diary feed, recent
calls, current strategy — lives on Cloudflare's free tier. This guide
walks you through standing up the public face for your own ape.

## What you'll deploy

| Component | Service | What it does |
|---|---|---|
| Static shell | Pages | HTML/CSS/JS for `<your-domain>` |
| Read API + admin write | Workers | `/api/diary`, `/api/calls`, `/api/strategy`, `/api/admin/publish` |
| Snapshot storage | KV | Holds the three state blobs the publisher writes |

All three fit comfortably inside Cloudflare's free tier for a
small-to-medium audience: 100k Worker requests/day, 1k KV writes/day,
unlimited Pages bandwidth.

## Prerequisites

- A Cloudflare account (free)
- A domain on that account (you can transfer DNS to CF or use a
  subdomain of an existing zone)
- Node.js v22+ and `wrangler` 4.x: `npm install -g wrangler`
- The exciton repo cloned locally (you're reading this from `deploy/`)

## 1. Authenticate wrangler

```sh
wrangler login
wrangler whoami
```

Confirm the right account is selected if you have more than one.

## 2. Provision the KV namespace

From `cloudflare/worker/`:

```sh
wrangler kv namespace create <your-ape>-state
```

Copy the returned `id` and replace the placeholder in
`cloudflare/worker/wrangler.toml`:

```toml
kv_namespaces = [
  { binding = "STATE", id = "<paste-the-id-here>" }
]
```

## 3. Generate the publish secret

This shared secret lets the engine sign publishes the Worker accepts.
Generate something high-entropy and paste it both into the Worker (as
a Worker secret) and into the engine's `.env` (as `CF_PUBLISH_SECRET`).

```sh
# generate
openssl rand -hex 32

# install on the Worker
wrangler secret put PUBLISH_SECRET
# (paste the value when prompted)
```

In the engine's `.env`:

```
CF_PUBLISH_URL=https://<your-domain>/api/admin/publish
CF_PUBLISH_SECRET=<the-same-hex-string>
```

## 4. Deploy the Worker

From `cloudflare/worker/`:

```sh
wrangler deploy
```

The Worker is published to `<name>.<your-account>.workers.dev` by
default. Hit `/health` to confirm it's live:

```sh
curl https://<name>.<your-account>.workers.dev/health
# {"ok":true,"ts":1746...}
```

## 5. Deploy the static shell to Pages

From the repo root:

```sh
wrangler pages deploy cloudflare/pages --project-name=<your-ape>
```

Or, in the Cloudflare dashboard, create a Pages project pointing at
your fork of the repo, with `cloudflare/pages` as the build root and
no build command (vanilla static).

## 6. Wire the custom domain

In the Cloudflare dashboard:

1. **Pages project** → custom domain → add `<your-domain>`. CF
   provisions a cert and points DNS.
2. **Worker** → Triggers → add a route like
   `<your-domain>/api/*` so the Worker handles the API surface while
   Pages handles everything else. The Worker route takes precedence
   over the Pages catch-all for matching paths.

## 7. Restart the engine

With `CF_PUBLISH_URL` and `CF_PUBLISH_SECRET` in `.env`, restart the
engine (`docker compose up -d` on the VM). The next publisher kick
will POST a consolidated state blob to the Worker; subsequent visits
to `<your-domain>` show the live data.

## Sanity check

```sh
curl https://<your-domain>/api/strategy
# {"key":"strategy","value":{...},"captured_at":1746...}
```

## What's NOT exposed

The MCP surface stays bound to `localhost` on the VM. It is compute-
on-demand over your data — exposing it publicly would let anyone burn
your RPC budget or pin the engine. Operators running their own ape
get the full MCP surface for their own deployment, but it's not a
hosted service.

## Updating

- **Worker code change** → `wrangler deploy` from `cloudflare/worker/`
- **Static shell change** → `wrangler pages deploy cloudflare/pages
  --project-name=<your-ape>`
- **Strategy / diary / calls** → no manual step. The engine's
  publisher pushes them on every kick.
