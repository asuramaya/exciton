# Architecture

Exciton is one Rust binary with five concurrent loops. This document explains what each loop does, how they share state, and where to look in the source.

## Process model

```
                          ┌────────────┐
                          │  main.rs   │
                          └─────┬──────┘
              ┌─────────────────┼─────────────────┬───────────┐
              ▼                 ▼                 ▼           ▼
       ┌────────────┐    ┌────────────┐    ┌──────────┐  ┌────────┐
       │  scanner   │    │  publisher │    │   bot    │  │  mcp   │
       │  (15s loop)│    │ (300s loop)│    │ long-poll│  │ server │
       └─────┬──────┘    └─────┬──────┘    └────┬─────┘  └───┬────┘
             │                 │                │            │
             └──────┬──────────┴────────┬───────┴────────────┘
                    │                   │
                    ▼                   ▼
            ┌──────────────┐    ┌──────────────┐
            │  RPC router  │    │  SQLite DB   │
            │ (ingester)   │    │   (db.rs)    │
            └──────────────┘    └──────────────┘
```

All loops share an `Arc<Db>` (SQLite, WAL mode) and an `Arc<RpcRouter>` (round-robin across configured endpoints with health tracking and 3-fail sideline).

## Scanner — `scanner::run_loop` (every 15s)

One cycle = `scan_cycle`:

### Phase 1: Watchlist re-analysis
- Pull up to N candidates from `watchlist WHERE active=1`, oldest `last_checked` first
- Run `signals::analyze_token` on each → fresh `TokenAnalysis` snapshot
- Compare to previous snapshot (`delta`): classification flip, concentration jump, velocity crash → emit alerts
- Apply `should_remove_from_watchlist` (DEAD / CRASHING / ACTIVE_TRAP / UNSAFE / concentrated → deactivate)
- Apply `is_confirmed_unsafe` (UNSAFE\* + has active call → fail the call)
- If passed `SIGNAL_MIN_WATCHLIST_AGE_SECONDS` → `notifier.process_token` (auto-call gate)

### Phase 2: Discover new tokens
- Walk `SIG_BATCH=40` recent pump.fun program signatures
- Fetch up to `MAX_TX_FETCHES=12` full transactions to extract mints
- For each fresh mint: full `analyze_token` → insert into `tokens`
- Emit classification-based discovery alerts

### Phase 2b: Graduation detection
- Walk 30 recent PumpSwap program signatures (cap 8 tx fetches per cycle)
- For each mint already in `tokens` but not on active watchlist: re-analyze
- Catches the moment a bonding-curve token bonds out to PumpSwap
- Cooldown: `GRADUATION_COOLDOWN_SECS=600` per mint

### Phase 3: Smart-wallet tracker
- For each curated wallet: diff current SPL holdings vs stored view
- Newly-seen mint → `smart_wallet_buy` alert

### Phase 4: Re-ingest
- Query `list_stale_discovered_candidates` (age 30min–6h, not on active watchlist)
- One batched DexScreener call (max 30 addrs/req)
- Tokens with `h1_vol > $5k` → full `analyze_token` → gate through watchlist admission

### Phase 5: Digest tick + settling
- Hour-bucketed ops digest (edits the current hour's message; rolls over at :00)
- Self-heals if the digest message was deleted in the channel
- Settling phase walks every active call, applies horizon-aware close rules

## Token classifications

Computed by `signals::analyze_token` from on-chain + market data:

| Class | Meaning |
|---|---|
| `STAIRCASE` | Stepwise upward price action with healthy distribution |
| `GRINDER` | Sustained accumulation — slower than STAIRCASE, recency + distribution bonus |
| `SPRING` | Compression then breakout |
| `SURGE` | Sharp momentum spike |
| `DEVELOPING` | Early-stage — building base, no verdict yet |
| `CRASHING` | Active dump |
| `DEAD` | Stale price, no volume |
| `ACTIVE_TRAP` | Distribution score collapsed (one wallet selling into ladder) |
| `UNSAFE:PERMANENT_DELEGATE` | On-chain confirmed honeypot — never trade |
| `UNSAFE:FREEZE` | Freeze authority present — never trade |
| `UNSAFE:NON_TRANSFERABLE` | Non-transferable extension — never trade |

## Auto-call gate

`notifier::should_signal` requires ALL of:

- `class ∈ {STAIRCASE, GRINDER, SPRING}`
- `effective_confidence ≥ 75` (decays with age)
- `top_holder_pct < 20%`
- `momentum_delta ≥ 0` (not fading)
- `delta.is_some()` (has at least one prior snapshot)
- `liquidity_usd ≥ $20k`
- `volume_24h_usd ≥ $50k`
- `now − first_seen ≥ 1h`

Plus halted/threshold overrides.

## Call lifecycle

```
  active ── settling phase auto-close ────────────────► withdrew / failed / expired
     │
     ├── operator /close_call ─────────────────────────► withdrew
     │
     ├── UNSAFE classification ────────────────────────► failed
     │
     └── administrative cleanup ───────────────────────► voided  (excluded from publisher)
```

The `calls` table has a unique partial index on `(mint) WHERE status='active'` — one active call per mint.

### Settling rules

For each active call: parse horizon from note (default SHORT), fetch current price, compare to entry, apply:

| Horizon | Trigger | Verdict |
|---|---|---|
| SHORT | pct ≥ +100% | `withdrew` (2x done) |
| SHORT | pct ≥ +50% | `withdrew` (took the win) |
| SHORT | pct ≤ −40% | `failed` (thesis broke) |
| SHORT | age ≥ 6h | `expired` (no follow-through) |
| LONG | pct ≤ −70% | `failed` (thesis broke) |
| LONG | age ≥ 30d | `expired` (30d hold complete) |
| LONG | (price OK) | operator settles via `/close_call` |

## Publisher — `publisher::run_once` (every 300s)

1. Fetch SOL balance + price → write wallet snapshot
2. Build positions from `wallet_ledger` cost basis
3. Compute realized + unrealized PnL
4. Build activity stream from `wallet_ledger`
5. `build_calls_file` (filtered by `voided` exclusion) → `data/calls.json`
6. Per-token scout snapshots → `data/scouts/<mint>.json`
7. Per-token whale snapshots → `data/whales/<mint>.json`
8. `git add -A && git commit && git push` to the configured target repo

## Database

`exciton.db` (SQLite, WAL mode, foreign keys on). Key tables:

| Table | What |
|---|---|
| `tokens` | Every mint we've ever seen (`address`, `first_seen`, `safety_score`) |
| `token_snapshots` | Every analysis run — classification, scores, top holder, holders, timestamp |
| `watchlist` | Active candidates for re-analysis (`active=0/1`, `added_at`, `last_checked`) |
| `alerts` | Discovery + classification + concentration + velocity alerts |
| `calls` | Public ledger entries (active/withdrew/failed/expired/voided) |
| `telegram_deliveries` | Every channel post we made — `message_id`, `timeline_json`, status |
| `telegram_digests` | Hour-bucketed digest message IDs |
| `audit_log` | Operator + auto actions for forensics |

## MCP server

`mcp.rs` exposes the scanner's analysis tools over MCP (Model Context Protocol). Default transport is HTTP-streamable on port 8080. Tools include:

- `inspect_token(address)` — full evidence bundle for a mint
- `scan_token(address)` — force a fresh `analyze_token` run
- `list_calls(status?)` — current call ledger
- `list_signals(since?)` — recent classification alerts
- `add_call(mint, horizon, note?)` — manual call (operator only)
- `close_call(mint, note?)` — manual settlement
- `list_watchlist()` — active candidates

Set `EXCITON_DISABLE_MCP=1` to run without the MCP server (headless deploys).

## Telegram surfaces

Two long-poll bots, one per surface:

- **Public** (`bot_token`) — read-only intel commands + per-user state. Per-user 30/min rate limit + global 1/min ceiling on RPC-heavy lookups.
- **Private** (`dm_bot_token`) — operator-only. `/claw`, `/halt`, `/resume`, `/threshold`, `/stats`, `/call`, `/close_call`. Hard-gated by `admin_user_ids`.

Why two tokens: Telegram only allows one long-poller per token (409 Conflict on duplicate `getUpdates`). The channel poster (`bot_token`) is busy with `sendMessage`/`editMessageText`, so the DM long-poll lives on a separate `dm_bot_token`.

## Configuration resolution

`main.rs` loads `config.toml`, then walks the tree and substitutes any `${VAR}` patterns from the process environment. **If you add a new env-var-backed config field, add it to that resolution sweep** — otherwise the literal string `${VAR}` survives into runtime.

Secrets never live in tracked files. `.env` and `config.toml` are gitignored; `.env.example` and `config.example.toml` are the only checked-in templates.
