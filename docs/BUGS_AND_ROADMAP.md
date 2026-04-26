# Bugs, Problems, TODOs, Roadmap

Living document. Update when a new issue is found or a known one is fixed.
Check here before adding a duplicate item.

Severity key: **S0** = broken now, user-visible. **S1** = data wrong or
misleading. **S2** = latent / edge-case. **S3** = cosmetic / cleanup.

---

## 1. Bugs (observed or reproducible)

### photon (Rust)

- **S1 · publisher writes `value_usd=0` on transient RPC failure.** When
  `get_balance` returns 0 lamports (silent RPC error), the publisher
  serializes a 0-valued PnL point. Client currently filters these, but
  the row still bloats the series and costs storage forever. *Fix: skip
  the publish tick entirely when balance read fails.*
- **S2 · `price_usd_at_trade` / `mcap_usd_at_trade` are observation-time,
  not trade-time.** A 12-day-old buy scanned today records today's mcap
  as the "trade-time" mcap. `wallet_ledger` needs a real historical
  source (Birdeye history, or tx-timestamp-based pricing).
- **S2 · `fmt_amount` and `fmt_compact` in `src/publisher.rs` do the
  same thing.** Consolidate.
- **S2 · `get_tx_wallet_summary` doesn't follow WSOL wraps.** A swap that
  routes SOL → WSOL → token produces a 0-SOL delta on the wallet even
  though SOL went out. Only the WSOL account moves. Need to also count
  the wallet's associated-token WSOL balance change.
- **S2 · `sniper_cohort` captured at first-scan is mislabeled when a
  mint is discovered post-launch.** The top-20 at our first scan aren't
  actually snipers if the token was already days old. `retention_pct`
  becomes misleading.
- **S2 · `deployer_address` is the top-holder at first scan.** If the
  real deployer has already sold into the bonding curve, we record the
  wrong wallet. `dev_selling` then flags a random whale, not the dev.
  Needs actual instruction parsing (parse first pump.fun CreateEvent
  for the real deployer).
- **S2 · notifier calls-hook can fire on edit events, not just first
  promotion.** `insert_call` uses `OR IGNORE` so no duplicate active
  row appears, but we waste a bunch of lookups. Guard explicitly on
  `first delivery for this token`.
- **S2 · `get_token_largest_accounts` has no retry on 429.** Unlike
  `check_connection` and `get_recent_signatures`, this path only tries
  once. RPC failures bubble straight up.
- **S3 · RPC endpoint health reset path can thrash.** When all three
  endpoints are genuinely broken, we reset and retry immediately, which
  burns cycles. Add a back-off.
- **S3 · unused legacy `trades` table.** Superseded by `wallet_ledger`
  but still in the schema. Safe to leave, noisy in grep.
- **S3 · compiler warnings for unused fields/methods.** `is_running`,
  `healthy_count`, `weight`, a few more. Not broken, just noise.

### MadApes.ai site

- **S1 · `BOOK_PAGES` loaded once at bootstrap.** New notes added between
  the first load and the 30-sec poll don't appear until the user
  manually reloads. The poll should refresh `thoughts/index.json` too.
- **S1 · ticker percentage on `BAG` misleading.** It's
  `unrealized_pnl_usd / total_value_usd`. With 0 positions,
  `unrealized` is 0 and pct shows `+0.0%` — looks like a measured zero
  when it's really "no data."
- **S1 · chart tab selection doesn't persist across refresh.** Every
  30-sec poll snaps back to 1H. Should honor the user's last choice.
- **S2 · hash-based deep-link flicker on notes.** `#note=foo` triggers a
  re-render on every poll because we call `renderThoughts` again from
  bootstrap (nope — actually called only once; but chart redraw can
  still flicker). Verify.
- **S2 · chart breaks with 1 or 2 series points.** uPlot needs at least
  2 points for a line; when the series is freshly restarted and has
  only 1 real row, the chart renders a dot the user can't see.
- **S2 · `SEEN_STREAM` grows unbounded.** Long sessions accumulate every
  event signature. Minor memory leak; cap at 500.
- **S2 · empty-state message for ticker.** Renders "no live values yet"
  when `health` is null; fine, but once SOL price lands it flickers.
- **S2 · on very narrow viewports (<380px) the PnL grid wraps
  awkwardly.** Test on a real phone.
- **S3 · timezone drift.** All timestamps are UTC in storage but
  rendered in local time for the "HH:MM:SS · refreshed" marker. Not
  wrong, just potentially confusing.
- **S3 · `scrollbar-width: none` not supported in older Safari.**
  Cosmetic; the scrollbar shows anyway.
- **S3 · `fmtTimeAgo` with future timestamps produces `NaNs ago`.**
  Shouldn't happen, but defense-in-depth is cheap.

### MCP tools

- **S1 · `post_note` has no validation.** Garbled markdown or empty
  body commits happily. Should at least reject empty body and
  duplicate same-day title.
- **S2 · `pipeline_health` uses file mtime as a freshness signal.** If
  photon is writing but the mtime doesn't change (rare), we'd report
  stale. Replace with an in-memory last-push timestamp.
- **S2 · `fire_call`/`close_call` error shape is inconsistent.** Some
  errors return `{"error": "..."}`, some return prose strings. Pick
  one.
- **S3 · no `pulse` tool to force a publisher tick.** After calling
  `post_note` or `fire_call` we wait up to 5 min for it to land on the
  site.

### DM bot

- **S2 · `/scout` and `/whales` output can exceed Telegram's 4096-char
  message limit.** Split into multiple messages or truncate.
- **S2 · no backoff on Telegram 429s.** A burst of messages will eat
  our rate limit and we never recover gracefully.

---

## 2. Data-integrity problems

- **PnL series historical points predate some fixes.** We've one-shot
  cleaned two rounds of contamination; any future price-source change
  will likely introduce a third. Document the cleanup in git so
  future-self knows.
- **No cost-basis for trades recorded BEFORE photon was watching.** The
  wallet_ledger back-fills from signatures but uses current SOL price
  as the historical price. Realistic fix: Birdeye historical price
  API, priced at `block_time`.
- **`price_usd_at_trade` under-reports for historical buys.** Token
  prices fluctuate hard; using "now" as proxy for weeks-ago trade
  price materially misstates entries.
- **Sniper retention misleading for older mints.** See S2 bug above.
- **Dev-selling can fire on wrong wallet.** See S2 bug above.
- **`active_call.pct_from_call` is 0% when `entry_price_usd=0`.** That's
  an honest "we don't know" rendering, but the UI still shows `+0.0%`
  which reads as "flat."
- **Auto-acknowledged alerts bloat the `alerts` table indefinitely.**
  Never purged. Run a daily vacuum: `DELETE FROM alerts WHERE
  acknowledged=1 AND timestamp < now-7d`.

---

## 3. Infrastructure problems

- **No process supervision.** photon running under `nohup` — if it
  panics, it stays dead. Needs a `launchd` plist on macOS (or systemd
  on Linux) with `KeepAlive: true`.
- **`/tmp/photon.log` grows unbounded.** `tracing-appender` daily
  rotation would cost ~15 lines.
- **No alerting on photon crash or git-push failure.** Stale data
  banner on the site is our only signal; user has to visit to know.
  A heartbeat-to-healthchecks.io would fix it for free.
- **photon.db is a single point of failure.** Nightly gzipped snapshot
  to a private repo or S3. Trivial.
- **No secret rotation playbook.** If the bot token leaks, we fumble.
  Document the swap procedure.
- **Recraft credits run out silently.** Image generation fails without
  surfacing. Add a threshold alert when Recraft returns low-credits
  response, or expose credit count in `pipeline_health`.
- **DexScreener rate limit (~300/min) not respected.** We hit it
  incidentally when scanning a full watchlist. Needs a simple token
  bucket.
- **GitHub push auth isn't monitored.** If `gh` auth expires or SSH
  keys rotate, every publish would silently fail.

---

## 4. TODOs (sorted by effort, smallest first)

- [ ] Consolidate `fmt_amount` + `fmt_compact` in `src/publisher.rs`.
- [ ] Skip `publish.tick` when `get_balance` fails instead of writing 0.
- [ ] Persist chart-window choice in localStorage.
- [ ] Poll `thoughts/index.json` in the 30-sec refresh cycle.
- [ ] Cap `SEEN_STREAM` size at ~500 entries.
- [ ] Add retry wrapper to `get_token_largest_accounts`.
- [ ] Add vacuum job: purge `alerts` rows older than 7 days.
- [ ] Add `pulse` MCP tool that forces a publisher tick on demand.
- [ ] Add Telegram message-length splitter for long `/scout` output.
- [ ] Log-rotation setup (tracing-appender daily).
- [ ] launchd plist so photon auto-restarts on crash.
- [ ] Replace `pipeline_health`'s mtime check with in-memory
      last-tick timestamp exposed from the publisher.
- [ ] Ticker BAG pct should read "—" when positions are empty, not
      "+0.0%".
- [ ] WSOL-aware `get_tx_wallet_summary` that also checks the wallet's
      WSOL ATA delta.
- [ ] Active-call UI: hide pct when `entry_price_usd=0`.
- [ ] photon.db nightly snapshot cron.

---

## 5. Roadmap

Execution work is intentionally deferred for now. No wallet funding,
real-money trading, or Jupiter swap integration until the intel system,
call flow, and website polish are in a materially better state.

### Near-term (next 1–3 sessions)

1. **Intel correctness pass.** Fix the bugs that make the signal
   misleading: historical trade-time pricing, WSOL-aware wallet deltas,
   sniper-label drift for old mints, and real deployer detection.
2. **Call flow hardening.** Make the promotion path trustworthy:
   notifier should trigger only on first promotion, freeze
   `data/scouts/<mint>.json` at call time, and clean up "unknown"
   render states like `entry_price_usd=0` / empty BAG positions.
3. **Website refresh + state polish.** Refresh `thoughts/index.json`
   during the 30-sec poll, persist chart-window choice, handle sparse
   series cleanly, and remove note/chart flicker on refresh.
4. **Smart-wallet watchlist populated.** Curate 3–5 known sharp Solana
   callers so `smart_wallet_buy` alerts have real signal.
5. **Reference-mints populated.** Pick 10–20 known winner mints so
   `cohort_overlap` yields something interpretable.
6. **First real Calls-channel post.** Ship a call only once the scout →
   evidence → site → Telegram flow reads clean end-to-end, even if the
   trade path stays stubbed.
7. **Process supervision + log rotation.** If photon has to run
   unattended on this laptop, it needs to survive a crash.
8. **Mobile polish.** Real phone testing; PnL grid, stream-panel
   touch-scroll, ticker crowding.

### Medium-term (sessions 4–10)

- **Wallet funding + first real trade.** Still necessary eventually,
  but not before the intel and publishing surfaces are trustworthy.
- **Jupiter swap integration + wallet signing.** Turns `trade` MCP
  tool from stub to real. Requires: private key in OS keychain,
  slippage bounds, preview → confirm flow, Jito tip.
- **Scout-at-call-time receipt.** `data/scouts/<mint>.json` frozen the
  moment a call fires. Public evidence the call was justified, not
  post-rationalized.
- **Auto-drafted call-narrative notes.** When a call fires, a thought
  draft gets generated with the scout data framed in ape voice —
  operator edits before pushing.
- **Historical price back-fill.** Birdeye historical API for trades
  that predate our tracking. Real entry prices for PnL math.
- **Performance page on MadApes.ai.** Win rate, median hold, max
  drawdown, pct of calls > 2x. Gated on having ≥10 closed calls.
- **Second provider rotation.** When Helius free tier runs out,
  auto-swap to a warm backup without config edits.
- **Dev-sell alert → banner on site.** Currently just a stream row;
  when it fires for an active call, it deserves a red banner on the
  call row.
- **Calls channel metrics block.** At the top of the Calls Telegram
  channel, a pinned live message showing rolling win rate + biggest
  wins + biggest misses. Accountability-by-visibility.

### Longer-term (strategic)

- **Self-evaluation loop.** photon tracks its own signal → outcome
  pairs, learns which classifier thresholds correlate with post-call
  performance, adjusts promotion criteria dynamically. Arena-esque
  but single-model: the ape grades itself.
- **Paper-trading mode.** Simulate trades without execution; compare
  hypothetical PnL vs actual-held to stress-test the call criteria.
- **Backtesting rig.** Replay historical `token_snapshots` against
  tuned thresholds to see which tweaks would have materially improved
  hit rate. Required before ever relaxing the signal criteria by
  feel.
- **Smart-wallet auto-discovery.** Instead of hand-curating, train
  the system to recognize sharp wallets by retrospective PnL on their
  historical holdings. A smart-money league table.
- **Cross-chain expansion.** BSC and Base first (Squid Gambles' Hachiko
  was BSC). Largest design lift is abstracting the provider layer
  (RPC + DexScreener + chain-specific parsers).
- **Multi-persona operator mode.** Different wallets, different
  personas, different call cadences. MadApes-the-degen vs
  MadApes-the-patient.
- **Public REST API.** Read-only endpoints for scout, calls, stream —
  lets other builders stand on our primitives.
- **Token-relationship graph.** Which holders hold which tokens, who
  trades against whom. Social-graph intel beyond
  `cohort_overlap`.
- **"Ape council" consensus layer.** Curated wallets privately vote on
  whether to call; when 3 of 5 vote yes, the bot fires. Multi-agent
  confirmation without the cost of running multiple models.
