# Project — photon + MadApes.ai

Read this once at the start of any new session before touching code.
Everything below is either authoritative-current-state or a convention
that's load-bearing. Deviation without reason will break the public
contract the project has with itself.

---

## What this project is

A pipeline that:

1. **photon** (Rust) — scans Solana for patterns in holder distribution,
   velocity, whale flow, and graduation events. Runs in the background on
   this machine, pushes alerts, serves MCP tools, fires Telegram calls,
   publishes live state to a public site. Source of truth for everything
   on-chain.
2. **MadApes.ai** — a public GitHub Pages site (`asuramaya.github.io/MadApes.ai`)
   that reflects the operating wallet's bag, tracks, calls, and notes.
   Static files. No backend. Fed exclusively by photon's publisher.
3. **Telegram** — two channels (Chat + Calls) + `@Claudeinatorbot` for DM.
   Chat gets automatic signal firehose; Calls is rare, hand-curated.

The ape (the persona) is a trader that learns by adding eyes, keeping
every thought public, and writing down every trade. The public face is
playful-ape-voice; the engineering is unsentimental Rust.

---

## Where things live

| What | Path |
|---|---|
| photon repo | `/Users/asuramaya/Code/MadApesAI/photon` |
| photon config | `/Users/asuramaya/Code/MadApesAI/photon/config.toml` (gitignored — secrets here) |
| photon DB | `/Users/asuramaya/Code/MadApesAI/photon/photon.db` (SQLite) |
| photon binary | `./target/release/photon` |
| photon logs | `/tmp/photon.log` |
| MadApes.ai repo | `/Users/asuramaya/Code/MadApesAI/MadApes.ai` |
| MadApes.ai live | `https://asuramaya.github.io/MadApes.ai/` |
| Voice guide (calls) | `docs/claudeinator-voice.md` |
| Voice guide (Telegram HTML) | `docs/telegram-style.md` |

---

## What's running right now

Run `pgrep -fl 'target/release/photon'` to verify. If zero results, see
the restart procedure below.

Inside the photon process:

- **Scanner** — 15s cycle. Watchlist re-analysis + discovery + smart-wallet
  polling. Writes snapshots to `token_snapshots`, alerts to `alerts`.
- **Publisher** — 5min cycle. Pulls wallet state + market + trades, writes
  JSON snapshots to `MadApes.ai/data/*.json`, git commit + push with
  `data:` prefix. Also emits `data/whales/<mint>.json` for every active
  call. Also calls `expire_stale_calls` on every tick.
- **Thought-image processor** — 15min cycle. Scans `MadApes.ai/thoughts/*.md`
  for `<div class="img-placeholder">[IMAGE: caption]</div>` blocks,
  generates missing assets via Recraft, writes WebP files, updates
  `thoughts/assets.json`, commits with `assets:` prefix. Idempotent.
- **Auto-ack loop** — 60s cycle. Acks alerts older than `STALE_SECS` (1800)
  so the UserPromptSubmit hook never surfaces stale rows.
- **Notifier** — triggered per-token, per-scan. Posts/edits Telegram cards;
  auto-registers calls in the `calls` table when qualifying.
- **DM bot** — long-poll. Commands below.
- **MCP server** — stdio transport. Tools below.

---

## How to restart photon

```bash
pkill -f "target/release/photon"
pkill -f "tail -f /dev/null"  # the pty stub that keeps stdio alive
sleep 2
nohup bash -c 'tail -f /dev/null | ./target/release/photon config.toml' \
  > /tmp/photon.log 2>&1 & disown
```

The `tail -f /dev/null | ...` pipe is load-bearing: rmcp's stdio transport
exits on EOF, so we keep stdin open with a noop.

---

## MCP tools (server-side, stdio)

The Claude session connects to photon via MCP. All current tools:

| Tool | Purpose |
|---|---|
| `status` | Wallet balance, RPC health, alert counts |
| `scan` | Top pending alerts by effective confidence, live re-inspects top 3 |
| `inspect(address)` | Full signal-layer breakdown for a mint |
| `present(address, style)` | Render Telegram-ready HTML card |
| `trade(...)` | Currently stubbed — execution not wired |
| `scout(address)` | Website + deployer + socials |
| `deep_scout(address)` | All 6 chain tools + basic scout |
| `pipeline_health` | Every subsystem's last heartbeat |
| `post_note(title, body, [slug])` | Append a new note to MadApes.ai |
| `fire_call(mint, [note], [expires_days])` | Insert call with entry snapshot |
| `close_call(mint, [exit_note])` | Close active call, stamp exit price |
| `active_calls()` | List active calls + pct_from_call |

If the session is warm, MCP tools are already available. Test with
`status()`.

---

## DM bot commands (`@Claudeinatorbot`)

All commands are DM-only; groups are ignored. Admin-gated commands check
`admin_user_ids` in `config.toml`.

**Public:** `/scan`, `/status`, `/inspect`, `/signals`, `/traps`, `/top`,
`/why`, `/safety`, `/watch`, `/unwatch`, `/watchlist`, `/mute`, `/unmute`,
`/muted`, `/nearmisses`, `/scout`, `/whales`, `/lp`, `/deployer`, `/calls`,
`/wallets`.

**Admin:** `/halt`, `/resume`, `/threshold`, `/stats`, `/watch_wallet`,
`/unwatch_wallet`, `/ref_mint`, `/unref_mint`, `/refs`, `/call`,
`/close_call`.

---

## Conventions that are load-bearing

### Voice (public-facing)

- Jungle/ape register. Squid Gambles voice translated to pump.fun.
- Metaphor is welcome. Emoji spam is not.
- Numbers up front. No disclaimers. No "NFA/DYOR."
- No AI-tell phrases ("Based on my analysis", "I have evaluated").
- Photorealistic cinematic images only (2:1, Recraft).

### Notes (`MadApes.ai/thoughts/`)

- **Append-only. Never edit. Never delete.**
- New file per entry: `YYYY-MM-DD_kebab-slug.md`.
- Update `thoughts/index.json` with `{date, file, title}`.
- Image placeholders: `<div class="img-placeholder">[IMAGE: caption]</div>`.
  Image processor picks them up within 15 min.
- Wrong takes get new notes documenting the correction, not edits.

### Calls (`calls` table, `data/calls.json`)

- Entry state frozen at call-time via DB unique partial index. Cannot be
  silently revised.
- Every call gets a 14-day `expires_at` by default. Auto-expires if no
  confirmation.
- Every active call gets a live `data/whales/<mint>.json` snapshot so the
  public can monitor the same triggers the ape is watching.
- Calls that go to zero get a `close_call` with a truthful `exit_note`.
  Never quietly removed.

### Git streams on MadApes.ai

Four commit prefixes braid the log into a narrative:
- `data:` — publisher pulse (5 min cadence)
- `assets:` — image processor pulse (15 min)
- `note:` — hand-written notes (rare)
- (unprefixed) — structural changes committed by me

Never mix streams in one commit.

### Call discipline (before firing)

Read the seven problems I audited myself on (see
`thoughts/2026-04-22_...` or the handoff note):

1. Is there a named catalyst? (not just "clean shape")
2. Are the triggers publicly monitorable?
3. Is short-term flow confirming or contradicting?
4. Are force-rebalance rules a coping mechanism?
5. Is the exit ladder sized to the token's age?
6. Is there a time-based close?
7. Am I forcing because it's the only candidate?

If fewer than 5 of 7 are clean: pass. The Calls channel earns value from
rarity, not throughput.

---

## Config secrets (never commit)

`config.toml` is gitignored. It contains:

- `rpc.endpoints` — Helius (via `${HELIUS_API_KEY}`), Alchemy, QuickNode URLs
- `telegram.bot_token` — `@Claudeinatorbot` token
- `madapes.recraft_api_key` — image generation

Never echo, log, or commit these.

---

## Active DB schema highlights

Key tables (see `src/db.rs` for full shape):

- `tokens` — every mint we've seen + `deployer_address`
- `token_snapshots` — per-scan state including market-data cols
- `alerts` — unacknowledged via `acknowledged=0`, auto-acked past 30min
- `watchlist` — tokens under rolling re-analysis
- `wallet_ledger` — every swap on the operating wallet, idempotent by sig
- `calls` — public commitments, unique partial index on (mint, active)
- `smart_wallets` + `smart_wallet_holdings` — imitation-alpha tracking
- `sniper_cohort` — first-20 holders at discovery time
- `reference_mints` — known-winners list for cohort_overlap

---

## Hot files and modules

| File | What it owns |
|---|---|
| `src/main.rs` | Startup — spawns scanner, notifier, publisher, image proc, auto-ack |
| `src/config.rs` | Config parsing + defaults |
| `src/db.rs` | All SQLite schema + methods |
| `src/ingester.rs` | `RpcRouter` — 3-endpoint round-robin with failover |
| `src/scanner.rs` | Background scan loop |
| `src/signals/mod.rs` | `analyze_token` — full per-token pipeline |
| `src/discovery.rs` | Pump.fun mint discovery via raw RPC |
| `src/market.rs` | DexScreener client |
| `src/scout.rs` | Six chain tools (whale_trace, lp_check, …) |
| `src/notifier.rs` | Telegram card posting + calls auto-hook |
| `src/bot.rs` | DM command dispatcher |
| `src/publisher.rs` | 5-min pulse to MadApes.ai/data |
| `src/thought_images.rs` | 15-min pulse scanning thoughts for placeholders |
| `src/image_gen.rs` | Recraft client |
| `src/mcp.rs` | MCP server — all tools |

---

## Living docs to read alongside this one

- **`docs/BUGS_AND_ROADMAP.md`** — canonical bug list, data-integrity
  issues, TODOs, and roadmap. Update that file when you fix something
  or discover something new; future instances read it before touching
  code.
- **`docs/claudeinator-voice.md`** — call-channel voice guide.
- **`docs/telegram-style.md`** — Telegram HTML card conventions.

## Known limits / gotchas

- **Helius quota-exhausted** — the first endpoint 429s on every call. Router
  rotates automatically; don't remove the endpoint, it'll recover on plan
  reset.
- **Pumpswap LP is program-native** — `lp_check` returns `pool_program_native`
  for pump.fun-graduated tokens because they don't issue transferable LP
  tokens. Not a bug.
- **DexScreener indexing lag** — fresh mints may return no pairs for ~30s
  after creation. First snapshot often has zero market data; next cycle
  fills it in.
- **Image processor is idempotent** — already-generated assets never
  regenerate. Edits to captions after generation don't retrigger. (Fix
  when needed: delete the asset file, it regenerates next cycle.)
- **Nothing is reading or writing to `/Users/asuramaya/Code/MadApesAI/MadApes.ai/.git`
  except photon + the operator.** Don't concurrent-write from a second
  process — it will clash with the publisher's `git commit && git push`.

---

## One-paragraph orientation for the next instance

You are working on a Solana trading intelligence system. photon
(Rust, running as a background process on this machine) scans the chain,
runs six on-chain scout tools, posts alerts to Telegram, and publishes
live state to `asuramaya.github.io/MadApes.ai` — a static site styled
as a "public PnL tracker plus jungle journal." The jungle journal is
append-only markdown with photoreal 2:1 Recraft-generated illustrations.
The Calls channel is deliberately rare. Every call freezes its entry
state immutably and auto-expires in 14 days. The voice is ape-themed,
Squid-Gambles-inspired, LLM-free on the data path. Read the current
MCP tools via `status`; read the current state via `pipeline_health`;
add notes via `post_note`; fire calls via `fire_call`.
