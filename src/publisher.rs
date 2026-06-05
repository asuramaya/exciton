//! Publisher — periodically snapshots the operating wallet to JSON files
//! in a local staging dir, then ships the consolidated state via an
//! HMAC-signed POST to a Cloudflare Worker (`/api/admin/publish`). The
//! Worker writes each present key into KV; public read endpoints
//! (`/api/diary`, `/api/calls`, `/api/strategy`) serve the snapshots
//! with edge cache.
//!
//! Zero LLM, zero LLM-shaped API keys. All numbers come from on-chain
//! reads + DexScreener; all summaries are templated from raw balance
//! deltas.

use crate::config::PublisherConfig;
use crate::db::Db;
use crate::ingester::RpcRouter;
use crate::wallet_cache::SharedWalletCache;
use crate::market;
use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// ~7 days of 5-minute samples on the chart. Trimmed on each push.
/// Was 288 (24h) — lifted so the "ALL" tab means what the user expects.
const MAX_SERIES_POINTS: usize = 2016;

#[derive(Debug, Serialize)]
struct Health {
    wallet: String,
    sol_balance: f64,
    sol_price_usd: f64,
    last_update: i64,
}

#[derive(Debug, Serialize)]
struct Position {
    mint: String,
    symbol: String,
    balance_ui: f64,
    avg_entry_usd: f64,
    current_price_usd: f64,
    position_usd: f64,
    pnl_pct: f64,
}

#[derive(Debug, Serialize)]
struct PositionsFile {
    positions: Vec<Position>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PnlPoint {
    ts: i64,
    value_usd: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TradeMarker {
    ts: i64,
    side: String,
    symbol: String,
    mint: String,
    token_amount_ui: f64,
    sol_amount_ui: f64,
    signature: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Pnl {
    total_value_usd: f64,
    realized_pnl_usd: f64,
    unrealized_pnl_usd: f64,
    #[serde(default)]
    series: Vec<PnlPoint>,
    #[serde(default)]
    trades: Vec<TradeMarker>,
}

#[derive(Debug, Serialize)]
struct Activity {
    ts: i64,
    summary: String,
    signature: Option<String>,
    mint: Option<String>,
}

#[derive(Debug, Serialize)]
struct ActivityFile {
    activity: Vec<Activity>,
}

/// Featured-token snapshot for the site's pinned section. Carries the
/// operator's chosen featured coin (mint configured via `featured_mint`
/// in the publisher section of config) plus the operator wallet's
/// holding percentage.
#[derive(Debug, Serialize, Default, Clone)]
struct FeaturedFile {
    mint: String,
    symbol: String,
    name: String,
    price_usd: f64,
    mcap_usd: f64,
    liquidity_usd: f64,
    price_change_h1: f64,
    pair_dex: String,
    pair_address: String,
    ape_wallet: String,
    ape_balance: f64,
    ape_holding_pct: f64,
    supply_est: f64,
    buy_url: String,
    pledge: String,
    last_update: i64,
}

/// One entry in the live stream feed — the arena-style commentary scroll.
/// Kinds: `alert` (scanner), `call` (fired/closed), `note` (hand-written),
/// `trade` (wallet activity), `graduation`, `whale_buy`. Timestamp-ordered
/// newest-first when serialized.
#[derive(Debug, Serialize, Clone)]
struct StreamEvent {
    ts: i64,
    kind: String,
    tag: String,
    summary: String,
    mint: Option<String>,
    signature: Option<String>,
}

/// Per-mint metadata bundle attached to the stream file. The client groups
/// events by mint and uses this for the token-card header (symbol, current
/// mcap, 1h change). Avoids per-event duplication and lets the client
/// fold runs of repetitive same-token alerts (e.g. "DEV SELLING" rolling
/// updates) under one collapsible card.
#[derive(Debug, Serialize, Default, Clone)]
struct StreamTokenInfo {
    symbol: Option<String>,
    name: Option<String>,
    mcap_usd: Option<f64>,
    price_usd: Option<f64>,
    price_change_1h: Option<f64>,
    price_change_24h: Option<f64>,
    liquidity_usd: Option<f64>,
}

/// Tokens in the "watching" tier — passed the classifier with non-trivial
/// confidence within the last few minutes but didn't fire a call yet
/// (missed one or more secondary gates: top1, top10, mcap window, etc).
/// Surfaced in the LIVE FEED alongside alerts so the channel sees what
/// the bot is *considering*, not just what it acted on.
#[derive(Debug, Serialize, Clone)]
struct WatchingEntry {
    mint: String,
    symbol: Option<String>,
    classification: String,
    confidence: i32,
    top_holder_pct: f64,
    age_secs: i64,
    /// Which gate kept this from firing — "top_holder" / "top10" / "conf"
    /// / "mcap" / "liq" etc. Empty when the row didn't record a single
    /// blocker (rare).
    gate: String,
    /// Human-readable gap description from the near-miss row.
    gap: String,
    last_seen: i64,
}

#[derive(Debug, Serialize)]
struct StreamFile {
    events: Vec<StreamEvent>,
    /// mint → token info, populated for every distinct mint present in
    /// `events` or `watching`. Empty map when no mints were referenced.
    tokens: std::collections::HashMap<String, StreamTokenInfo>,
    /// Active "watching" candidates — top-N by confidence in the last
    /// few minutes that the gate rejected. Lets the LIVE FEED show
    /// what the bot is currently considering, not just historical
    /// alerts. Empty array when nothing is at the gate.
    watching: Vec<WatchingEntry>,
}

#[derive(Debug, Serialize)]
struct CallSnapshot {
    id: i64,
    mint: String,
    symbol: String,
    classification: String,
    confidence: i32,
    called_at: i64,
    expires_at: Option<i64>,
    note: String,
    source: String,
    status: String,
    /// Human-readable outcome: "active", "withdrew", "failed", "expired".
    /// "closed" (legacy) maps to "withdrew".
    outcome_type: String,
    closed_at: Option<i64>,
    exit_price_usd: Option<f64>,
    exit_note: Option<String>,
    // Entry state (frozen at call-time).
    entry_mcap_usd: f64,
    entry_price_usd: f64,
    entry_liquidity_usd: f64,
    entry_top_holder_pct: f64,
    entry_pair_dex: String,
    // Live mark-to-market (refreshed each publisher tick for active calls).
    current_mcap_usd: f64,
    current_price_usd: f64,
    current_liquidity_usd: f64,
    pct_from_call: Option<f64>,
    // Journey: best and worst price observed between called_at..closed_at
    // (or called_at..now for active calls), derived from token_snapshots.
    // Lets the front end show "peaked at +210%, troughed at -8%" — entry
    // and exit alone don't tell the story.
    peak_pct: Option<f64>,
    peak_at: Option<i64>,
    trough_pct: Option<f64>,
    trough_at: Option<i64>,
    /// Site-relative URL of the long-thesis markdown when the call's
    /// note carries a `thesis=<filename>` tag. Front-end renders a
    /// 📖 link on rows where this is set. None for short pump.fun
    /// auto-calls (no thesis to write).
    thesis_url: Option<String>,
}

/// Per-horizon aggregate stats for the public ledger. Computed over the
/// rows the publisher just emitted — gives the page a track-record
/// summary without the client doing the math.
#[derive(Debug, Serialize, Default)]
struct CallStatsBucket {
    count: usize,
    wins: usize,
    losses: usize,
    expired: usize,
    win_rate: f64,
    avg_winner_pct: f64,
    avg_loser_pct: f64,
    best_pct: f64,
    worst_pct: f64,
}

#[derive(Debug, Serialize, Default)]
struct CallStats {
    short: CallStatsBucket,
    long: CallStatsBucket,
    moonshot: CallStatsBucket,
    scalp: CallStatsBucket,
    overall: CallStatsBucket,
    /// Per-source buckets — track whether the bot's auto-calls
    /// (`source = "notifier"`) outperform operator manual calls
    /// (`source = "dm"` from /call, `source = "mcp"` from claw).
    /// Same bucket shape as the horizon axis. Keys present only
    /// when at least one closed call exists for that source.
    by_source: std::collections::HashMap<String, CallStatsBucket>,
}

#[derive(Debug, Serialize, Default)]
struct CallsFile {
    active: Vec<CallSnapshot>,
    history: Vec<CallSnapshot>,
    stats: CallStats,
}

/// Shared signal that any state-changing component (settling phase,
/// auto-call, manual call/close) fires to ask the publisher to run a
/// snapshot now. Lets the public site update within ~30s of an event
/// instead of waiting for the next 300s tick. Coalesces — multiple
/// notifies during a single publish tick collapse into one extra run.
pub type PublishKick = Arc<tokio::sync::Notify>;

pub struct Publisher {
    cfg: PublisherConfig,
    wallet: String,
    rpc: Arc<RpcRouter>,
    db: Arc<Db>,
    /// Shared wallet snapshot. Refreshed by `wallet_cache::spawn_refresh`
    /// on its own cadence. Publisher reads from here — never blocks on RPC.
    wallet_cache: SharedWalletCache,
}

impl Publisher {
    pub fn new(
        cfg: PublisherConfig,
        wallet: String,
        rpc: Arc<RpcRouter>,
        db: Arc<Db>,
        wallet_cache: SharedWalletCache,
    ) -> Self {
        Self {
            cfg,
            wallet,
            rpc,
            db,
            wallet_cache,
        }
    }

    pub fn spawn(self: Arc<Self>, kick: PublishKick) {
        let interval = self.cfg.interval_seconds.max(60);
        tokio::spawn(async move {
            tracing::info!(
                "Publisher active: pushing to {} (max {}s, push-on-event)",
                self.cfg.repo_path,
                interval
            );
            let mut tick = tokio::time::interval(Duration::from_secs(interval));
            // Skip the immediate first tick (interval fires at t=0 by
            // default). The container has just started — RPCs aren't
            // warm and there's nothing new to publish yet.
            tick.tick().await;
            // Cooldown between successive runs: even if 5 events kick
            // back-to-back, give the prior CF publish room to land.
            // 30s is comfortably below interesting human-perception
            // staleness while bounding RPC load on bursts.
            const MIN_INTERVAL: Duration = Duration::from_secs(30);
            let mut last_run = tokio::time::Instant::now() - MIN_INTERVAL;
            loop {
                tokio::select! {
                    _ = tick.tick() => {},
                    _ = kick.notified() => {},
                }
                // Burst-coalesce: if we just ran, sleep the remainder
                // before honoring this kick. notified-during-sleep
                // wakes us once for the cumulative event burst.
                let since = last_run.elapsed();
                if since < MIN_INTERVAL {
                    tokio::time::sleep(MIN_INTERVAL - since).await;
                }
                last_run = tokio::time::Instant::now();
                tracing::debug!("publisher tick start");
                // Hard 60s budget on the entire tick. The inner per-RPC
                // timeouts (5s/8s) cap individual hot calls, but
                // downstream work (build_calls_file's per-mint market
                // fetches, scout/whale/details per active call, the
                // CF publish) can still accumulate beyond a useful
                // staleness budget. If the tick can't finish in 60s
                // it's not worth the next tick waiting.
                match tokio::time::timeout(Duration::from_secs(60), self.run_once()).await {
                    Ok(Ok(committed)) if committed => {
                        tracing::info!("Publisher: data snapshot pushed")
                    }
                    Ok(Ok(_)) => tracing::info!("Publisher: no data change"),
                    Ok(Err(e)) => tracing::warn!("Publisher failed: {:#}", e),
                    Err(_) => tracing::warn!("Publisher: tick exceeded 60s budget — abandoning"),
                }
            }
        });
    }

    /// Background scout loop. Generates per-call scout receipts, whale
    /// snapshots, and detail JSON on a slow cadence, off the publisher
    /// critical path. Each per-call RPC chain runs without an outer
    /// timeout so degraded RPC fleets only delay the data, never drop
    /// it. The publisher's own tick reads whatever scout files exist
    /// on disk.
    pub fn spawn_scout_loop(self: Arc<Self>) {
        // 5min cadence — long enough that the per-call RPC walks have
        // headroom to complete even when 2/3 endpoints are 429'd.
        let interval = Duration::from_secs(300);
        tokio::spawn(async move {
            tracing::info!(
                "Scout loop active (every {}s, no per-tick timeout)",
                interval.as_secs()
            );
            let repo = std::path::PathBuf::from(&self.cfg.repo_path);
            let data_dir = repo.join("data");
            if let Err(e) = std::fs::create_dir_all(&data_dir) {
                tracing::warn!("scout_loop: create data dir failed: {} — exiting loop", e);
                return;
            }
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First tick fires immediately by default; that's fine here —
            // scout work is idempotent + dedupes by call_id.
            loop {
                tick.tick().await;
                let calls_file = self.build_calls_file().await;
                self.publish_call_scout_snapshots(&calls_file, &data_dir).await;
                self.publish_whale_snapshots(&calls_file, &data_dir).await;
                self.publish_call_details(&calls_file, &data_dir).await;
                tracing::debug!("scout_loop: pass complete");
            }
        });
    }

    pub async fn run_once(&self) -> Result<bool> {
        let repo = PathBuf::from(&self.cfg.repo_path);
        // `repo_path` is the local staging dir for the JSON files that
        // get bundled into the HMAC-signed POST. No git involvement —
        // the engine ships exclusively to a Cloudflare Worker.
        let data_dir = repo.join("data");
        std::fs::create_dir_all(&data_dir).context("create data/ dir")?;
        let now = chrono::Utc::now().timestamp();

        // 1+2. Wallet state — read from `wallet_cache`. RPCs are reserved
        // for the scanner/scout decision loop; this critical path never
        // blocks on a Solana RPC. The cache is refreshed ambient by
        // `wallet_cache::spawn_refresh` on its own cadence; staleness here
        // is bounded by that cadence (default 5min) and is preferable to
        // a publisher tick timing out under RPC degradation.
        let (sol_balance, holdings, wallet_snap_age) = {
            let snap = self.wallet_cache.read().await;
            let age = if snap.last_updated > 0 {
                now - snap.last_updated
            } else {
                -1
            };
            (snap.sol_balance, snap.holdings.clone(), age)
        };
        if wallet_snap_age < 0 {
            tracing::warn!(
                "publisher: wallet_cache not yet populated — first refresh pending"
            );
        } else if wallet_snap_age > 600 {
            tracing::warn!(
                "publisher: wallet_cache is {}s stale — RPC fleet may be saturated",
                wallet_snap_age
            );
        }
        // SOL-price fetch via CoinGecko — wrap in a short timeout so a
        // single slow CG response can't eat the whole tick. fetch_sol_price
        // returns Option<f64> (None on err), so timeout gives
        // Result<Option<f64>, Elapsed>. Fallback covers both branches.
        let sol_price_usd = tokio::time::timeout(
            Duration::from_secs(3),
            fetch_sol_price(),
        )
        .await
        .ok()
        .flatten()
        .unwrap_or(self.cfg.sol_price_fallback_usd);

        // 3. Scan recent wallet signatures, record any detected trades into
        //    the wallet_ledger. Idempotent by signature, so re-scanning the
        //    same window never double-counts. Capped at 8s — capture walks
        //    multiple per-sig RPC calls and can stall the whole tick under
        //    full upstream failure.
        if tokio::time::timeout(
            Duration::from_secs(8),
            self.capture_recent_trades(now),
        )
        .await
        .is_err()
        {
            tracing::warn!("publisher: capture_recent_trades timed out (>8s) — skipping this tick");
        }

        // 4. Build positions, bringing in cost basis from the ledger.
        let cost_basis = self
            .db
            .get_wallet_cost_basis(&self.wallet)
            .unwrap_or_default();
        // Map: mint → (bought_tokens, spent_sol, sold_tokens, received_sol).
        let mut cb_map: std::collections::HashMap<String, (f64, f64, f64, f64)> =
            std::collections::HashMap::new();
        for (mint, bt, bs, st, rs) in cost_basis {
            cb_map.insert(mint, (bt, bs, st, rs));
        }

        let mut positions: Vec<Position> = Vec::new();
        let mut unrealized_total_usd = 0f64;
        for (mint, balance) in &holdings {
            if mint == SOL_MINT {
                continue;
            }
            let market = market::get_market(mint).await.ok().flatten();
            let price = market.as_ref().map(|m| m.price_usd).unwrap_or(0.0);
            let symbol = market
                .as_ref()
                .map(|m| m.symbol.clone())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| short(mint));
            let (bt, bs, _st, _rs) = cb_map.get(mint).copied().unwrap_or((0.0, 0.0, 0.0, 0.0));
            // Weighted average cost in USD = total SOL spent × current SOL price / tokens bought.
            // Using current SOL price here is approximate; a deeper ledger would
            // persist SOL price at each trade too. Good enough for display honesty.
            let avg_entry_usd = if bt > 0.0 {
                (bs * sol_price_usd) / bt
            } else {
                0.0
            };
            let position_usd = balance * price;
            let pnl_pct = if avg_entry_usd > 0.0 && price > 0.0 {
                (price / avg_entry_usd - 1.0) * 100.0
            } else {
                0.0
            };
            unrealized_total_usd += if avg_entry_usd > 0.0 {
                (price - avg_entry_usd) * balance
            } else {
                0.0
            };
            positions.push(Position {
                mint: mint.clone(),
                symbol,
                balance_ui: *balance,
                avg_entry_usd,
                current_price_usd: price,
                position_usd,
                pnl_pct,
            });
        }
        positions.sort_by(|a, b| {
            b.position_usd
                .partial_cmp(&a.position_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // 5. Realized PnL = SOL received from sells - SOL spent on positions
        //    that are fully exited (current balance == 0). For partial exits
        //    we book proportionally: realized = received - (sold_tokens/bought_tokens)*spent.
        let mut realized_sol_net = 0f64;
        for (bt, bs, st, rs) in cb_map.values() {
            if *st <= 0.0 || *bt <= 0.0 {
                continue;
            }
            let proportion = (st / bt).min(1.0);
            realized_sol_net += rs - proportion * bs;
        }
        let realized_pnl_usd = realized_sol_net * sol_price_usd;

        // 6. Totals.
        let positions_total: f64 = positions.iter().map(|p| p.position_usd).sum();
        let total_value = sol_balance * sol_price_usd + positions_total;

        // 7. Activity: template from wallet_ledger (cleaner than re-deriving
        //    from raw signatures every tick — the ledger already stores the
        //    parsed summary).
        let ledger_rows = self
            .db
            .get_wallet_trades_recent(&self.wallet, 20)
            .unwrap_or_default();
        let mut activity: Vec<Activity> = Vec::new();
        let mut chart_trades: Vec<TradeMarker> = Vec::new();
        for (ts, mint, side, sig, tok_ui, sol_ui) in ledger_rows {
            let market = market::get_market(&mint).await.ok().flatten();
            // Real symbol only — never fall back to a short-form mint as
            // a "symbol". That pollutes the ticker and stream with things
            // like `$CHXt…pump` that look linkable but aren't meaningful
            // symbols. Use Option<String>: Some = real symbol, None = no
            // symbol, caller formats accordingly.
            let symbol = market
                .as_ref()
                .map(|m| m.symbol.clone())
                .filter(|s| !s.is_empty());
            let (name_for_summary, mcap_clause) = match &market {
                Some(m) => {
                    let name = match &symbol {
                        Some(s) => format!("${}", s),
                        None => short(&mint),
                    };
                    let mcap = if m.mcap_usd > 0.0 {
                        format!(" at ${:.0}k mcap", m.mcap_usd / 1000.0)
                    } else {
                        String::new()
                    };
                    (name, mcap)
                }
                None => (short(&mint), String::new()),
            };
            let verb = if side == "buy" { "bought" } else { "cut" };
            let sol_clause = if sol_ui > 0.0 {
                format!(" for {:.3} SOL", sol_ui)
            } else {
                String::new()
            };
            let summary = format!(
                "{} {} {}{}{}",
                verb,
                fmt_amount(tok_ui),
                name_for_summary,
                sol_clause,
                mcap_clause
            );
            activity.push(Activity {
                ts,
                summary,
                signature: Some(sig.clone()),
                mint: Some(mint.clone()),
            });
            // TradeMarker.symbol stays empty when unknown; the client-side
            // renderer falls back to the short mint for display rather
            // than having the backend fabricate a symbol.
            chart_trades.push(TradeMarker {
                ts,
                side: side.clone(),
                symbol: symbol.clone().unwrap_or_default(),
                mint: mint.clone(),
                token_amount_ui: tok_ui,
                sol_amount_ui: sol_ui,
                signature: sig,
            });
        }

        // 8. PnL series: read existing, append, trim.
        let pnl_path = data_dir.join("pnl.json");
        let mut pnl: Pnl = read_json(&pnl_path).unwrap_or_default();
        pnl.series.push(PnlPoint {
            ts: now,
            value_usd: total_value,
        });
        if pnl.series.len() > MAX_SERIES_POINTS {
            let drop = pnl.series.len() - MAX_SERIES_POINTS;
            pnl.series.drain(..drop);
        }
        pnl.total_value_usd = total_value;
        pnl.realized_pnl_usd = realized_pnl_usd;
        pnl.unrealized_pnl_usd = unrealized_total_usd;
        pnl.trades = chart_trades;

        // 8b. Expire stale active calls before serializing — a call with
        //     no confirmation in its window gets closed cleanly so the
        //     book doesn't accumulate zombie "active" rows.
        let _ = self.db.expire_stale_calls(now);

        // Each remaining phase below has its own per-phase timeout. Without
        // this, a single slow market::get_market or DexScreener fetch eats
        // the outer 60s tick budget and aborts the whole tick. After the
        // wallet swap this manifested as 100% tick-budget-exceeded failures
        // because the per-call DexScreener fetches in build_calls_file
        // weren't bounded individually. Per-phase timeouts let the tick
        // gracefully degrade — one slow phase doesn't sink the rest.
        const PHASE_BUDGET: Duration = Duration::from_secs(10);

        // 9. Calls: every active call + last N closed calls, each with
        //    live mark-to-market so the site shows pct-from-call honestly.
        let calls_file = match tokio::time::timeout(
            PHASE_BUDGET,
            self.build_calls_file(),
        )
        .await
        {
            Ok(c) => c,
            Err(_) => {
                tracing::warn!(
                    "publisher: build_calls_file timed out (>{}s) — using empty calls",
                    PHASE_BUDGET.as_secs()
                );
                CallsFile::default()
            }
        };

        // 9b/c/d. Scout receipts, whale snapshots, and per-call details
        // were inline RPC blocks here. They've moved to `spawn_scout_loop`
        // so the publisher tick stays pure read-from-state. The publisher
        // will pick up whatever scout files exist on disk and ship them;
        // the scout loop fills them in on its own slow cadence with
        // generous timeouts so RPC degradation doesn't drop ticks.

        let health = Health {
            wallet: self.wallet.clone(),
            sol_balance,
            sol_price_usd,
            last_update: now,
        };
        // 10. Live stream — last 50 events across alerts + calls + wallet
        //     trades. Renders as an arena-inspired side-panel feed on the
        //     main site. Timestamp-sorted newest-first.
        let stream = self.build_stream_file().await;

        // 11. Featured token — the project's own coin (configured via
        //     featured_mint). Independent timeout: a slow DexScreener
        //     fetch on this single mint should not abort the rest of the
        //     publish cycle.
        let featured = match tokio::time::timeout(
            PHASE_BUDGET,
            self.build_featured_file(),
        )
        .await
        {
            Ok(f) => f,
            Err(_) => {
                tracing::warn!("publisher: build_featured_file timed out — skipping featured this tick");
                None
            }
        };

        write_json(&data_dir.join("health.json"), &health)?;
        write_json(&pnl_path, &pnl)?;
        write_json(
            &data_dir.join("positions.json"),
            &PositionsFile { positions },
        )?;
        write_json(&data_dir.join("activity.json"), &ActivityFile { activity })?;
        write_json(&data_dir.join("calls.json"), &calls_file)?;
        write_json(&data_dir.join("stream.json"), &stream)?;
        if let Some(f) = featured {
            write_json(&data_dir.join("featured.json"), &f)?;
        }

        self.post_publish(&data_dir, now).await
    }

    /// Snapshot the featured token (the project's own coin) for the site
    /// banner: live market data + the ape wallet's holding pct + the
    /// "never selling" pledge text. Returns None when no featured_mint
    /// is configured (banner stays hidden client-side).
    async fn build_featured_file(&self) -> Option<FeaturedFile> {
        let mint = if self.cfg.featured_mint.is_empty() {
            return None;
        } else {
            self.cfg.featured_mint.clone()
        };
        // Live market — symbol/name/price/mcap/24h. Cached via market::get_market.
        let market = market::get_market(&mint).await.ok().flatten();
        // Ape wallet's MAAI balance — read from cache, never RPC.
        let ape_balance: f64 = {
            let snap = self.wallet_cache.read().await;
            snap.holdings
                .iter()
                .find(|(m, _)| m == &mint)
                .map(|(_, bal)| *bal)
                .unwrap_or(0.0)
        };
        // Supply: derive from mcap/price (pump.fun = ~1B fixed supply but
        // we don't hardcode that — derive from observable market data).
        let (price_usd, mcap_usd) = market
            .as_ref()
            .map(|m| (m.price_usd, m.mcap_usd))
            .unwrap_or((0.0, 0.0));
        let supply_est = if price_usd > 0.0 {
            mcap_usd / price_usd
        } else {
            0.0
        };
        let holding_pct = if supply_est > 0.0 {
            (ape_balance / supply_est) * 100.0
        } else {
            0.0
        };
        Some(FeaturedFile {
            mint: mint.clone(),
            symbol: market.as_ref().map(|m| m.symbol.clone()).unwrap_or_default(),
            name: market.as_ref().map(|m| m.name.clone()).unwrap_or_default(),
            price_usd,
            mcap_usd,
            liquidity_usd: market.as_ref().map(|m| m.liquidity_usd).unwrap_or(0.0),
            price_change_h1: market.as_ref().map(|m| m.price_change_h1).unwrap_or(0.0),
            pair_dex: market.as_ref().map(|m| m.pair_dex.clone()).unwrap_or_default(),
            pair_address: market.as_ref().map(|m| m.pair_address.clone()).unwrap_or_default(),
            ape_wallet: self.wallet.clone(),
            ape_balance,
            ape_holding_pct: holding_pct,
            supply_est,
            buy_url: self.cfg.featured_buy_url.clone(),
            pledge: "Mad Apes wallet holds this and is never selling.".to_string(),
            last_update: chrono::Utc::now().timestamp(),
        })
    }

    async fn build_stream_file(&self) -> StreamFile {
        // Slot allocation per kind so the global newest-first cap doesn't
        // drown call/trade events under alert volume. Production showed
        // all 23 published events were `kind=alert` because alerts
        // outpace call/trade timestamps (scanner emits alerts every
        // 15s; calls fire ~2-5/hr; trades = 0 in paper mode). Each
        // kind gets its own bucket; final stream is the merged sort.
        const SLOT_ALERTS: usize = 18;
        const SLOT_CALLS: usize = 16;
        const SLOT_TRADES: usize = 10;
        let mut alert_events: Vec<StreamEvent> = Vec::new();
        let mut call_events: Vec<StreamEvent> = Vec::new();
        let mut trade_events: Vec<StreamEvent> = Vec::new();

        // Recent scanner alerts — any with confidence >= 50 gets in. The
        // alert table stores the full mint inline in the message body for
        // Telegram-channel readability; for the stream we prefer the
        // shortened form since the mint already rides along as a structured
        // field that the UI renders as a link.
        //
        // Producer-side dedup: keep at most 3 most-recent per
        // (mint, alert_type). Without this, runs of "DEV SELLING"
        // updates dominate the stream wall — the screenshot showed 11
        // identical rows for one mint. Client-side grouping handles
        // visual presentation but the underlying noise is still in
        // the JSON. 3 lets the trend stay readable (latest, prior,
        // earlier) without flooding.
        const PER_KEY_KEEP: usize = 3;
        if let Ok(alerts) = self.db.get_pending_alerts(40) {
            let mut by_key: std::collections::HashMap<(String, String), Vec<StreamEvent>> =
                std::collections::HashMap::new();
            for a in alerts {
                if a.confidence < 50 {
                    continue;
                }
                let tag = a.alert_type.to_uppercase().replace('_', " ");
                let clean_summary = match a.token_address.as_deref() {
                    Some(mint) => a.message.replace(mint, &mint_short(mint)),
                    None => a.message,
                };
                let key = (
                    a.token_address.clone().unwrap_or_default(),
                    a.alert_type.clone(),
                );
                by_key.entry(key).or_default().push(StreamEvent {
                    ts: a.timestamp,
                    kind: "alert".into(),
                    tag,
                    summary: clean_summary,
                    mint: a.token_address,
                    signature: None,
                });
            }
            for (_, mut bucket) in by_key {
                bucket.sort_by(|a, b| b.ts.cmp(&a.ts));
                bucket.truncate(PER_KEY_KEEP);
                alert_events.extend(bucket);
            }
            alert_events.sort_by(|a, b| b.ts.cmp(&a.ts));
            alert_events.truncate(SLOT_ALERTS);
        }

        // Recent wallet trades — every row the ledger detected.
        let trades = self
            .db
            .get_wallet_trades_recent(&self.wallet, 15)
            .unwrap_or_default();
        for (ts, mint, side, sig, tok, sol) in trades {
            let verb = if side == "buy" { "bought" } else { "cut" };
            trade_events.push(StreamEvent {
                ts,
                kind: "trade".into(),
                tag: side.to_uppercase(),
                summary: format!(
                    "{} {} tokens of {} for {:.3} SOL",
                    verb,
                    fmt_compact(tok),
                    mint_short(&mint),
                    sol,
                ),
                mint: Some(mint),
                signature: Some(sig),
            });
        }
        trade_events.sort_by(|a, b| b.ts.cmp(&a.ts));
        trade_events.truncate(SLOT_TRADES);

        // Calls fired + closed, newest first.
        // Narrative filter: the live feed used to flood with CALL FAILED
        // entries (44% of stream events were failed calls — small fakeout
        // exits that don't tell a story). Filter to keep only the
        // interesting outcomes:
        //   - CALL FIRED: always (we just opened a position — that's news)
        //   - CALL WITHDREW: only if exit_pct >= +25% (real win)
        //   - CALL FAILED: only if exit_pct <= -35% (severe loss only)
        //     AND additionally capped at 3 entries — the feed is a
        //     story, not a graveyard. Without the cap, even a tightened
        //     threshold left half the slot full of red.
        //   - CALL EXPIRED: dropped entirely (timeouts are zero-signal)
        // exit_pct extracted from the leading "+X.X%" / "-X.X%" of
        // exit_note. When extraction fails we keep the event (don't
        // silently swallow data we can't parse).
        const MAX_FAILED_IN_FEED: usize = 3;
        let mut failed_kept = 0usize;
        if let Ok(rows) = self.db.list_calls(false, 25) {
            for c in rows {
                let sym = if c.symbol.is_empty() {
                    mint_short(&c.mint)
                } else {
                    format!("${}", c.symbol)
                };
                match c.status.as_str() {
                    "active" => call_events.push(StreamEvent {
                        ts: c.called_at,
                        kind: "call".into(),
                        tag: "CALL FIRED".into(),
                        summary: format!(
                            "{} at ${:.0}k mcap — entry frozen",
                            sym,
                            c.entry_mcap_usd / 1000.0
                        ),
                        mint: Some(c.mint),
                        signature: None,
                    }),
                    "withdrew" | "failed" | "closed" | "expired" => {
                        if c.status == "expired" {
                            continue;
                        }
                        let Some(closed_ts) = c.closed_at else { continue };
                        let exit_note = c.exit_note.clone().unwrap_or_default();
                        let exit_pct = exit_pct_from_note(&exit_note);
                        let interesting = match c.status.as_str() {
                            "withdrew" | "closed" => exit_pct.is_none_or(|p| p >= 25.0),
                            "failed" => exit_pct.is_none_or(|p| p <= -35.0),
                            _ => true,
                        };
                        if !interesting {
                            continue;
                        }
                        if c.status == "failed" {
                            if failed_kept >= MAX_FAILED_IN_FEED {
                                continue;
                            }
                            failed_kept += 1;
                        }
                        let tag_str = match c.status.as_str() {
                            "withdrew" => "CALL WITHDREW",
                            "failed" => "CALL FAILED",
                            _ => "CALL CLOSED",
                        };
                        call_events.push(StreamEvent {
                            ts: closed_ts,
                            kind: "call".into(),
                            tag: tag_str.into(),
                            summary: format!(
                                "{} — {}",
                                sym,
                                if exit_note.is_empty() { "no note".into() } else { exit_note }
                            ),
                            mint: Some(c.mint),
                            signature: None,
                        });
                    }
                    _ => {}
                }
            }
        }
        call_events.sort_by(|a, b| b.ts.cmp(&a.ts));
        call_events.truncate(SLOT_CALLS);

        // Merge slots, sort by timestamp newest-first, hard cap.
        let mut events: Vec<StreamEvent> = Vec::new();
        events.extend(alert_events);
        events.extend(call_events);
        events.extend(trade_events);
        events.sort_by(|a, b| b.ts.cmp(&a.ts));
        events.truncate(60);

        // Enrich each distinct mint with current market metadata. The
        // client uses this for the token-card header in the live feed —
        // symbol, mcap, 1h change. Cached via market::get_market so this
        // adds at most one DexScreener fetch per distinct mint per tick.
        let mut tokens: std::collections::HashMap<String, StreamTokenInfo> =
            std::collections::HashMap::new();
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for ev in &events {
            if let Some(mint) = &ev.mint {
                if !seen.insert(mint.clone()) {
                    continue;
                }
                if let Ok(Some(m)) = market::get_market(mint).await {
                    tokens.insert(
                        mint.clone(),
                        StreamTokenInfo {
                            symbol: Some(m.symbol.clone()).filter(|s| !s.is_empty()),
                            name: Some(m.name.clone()).filter(|s| !s.is_empty()),
                            mcap_usd: if m.mcap_usd > 0.0 { Some(m.mcap_usd) } else { None },
                            price_usd: Some(m.price_usd).filter(|p| *p > 0.0),
                            price_change_1h: Some(m.price_change_h1).filter(|v| *v != 0.0),
                            price_change_24h: None,
                            liquidity_usd: Some(m.liquidity_usd).filter(|v| *v > 0.0),
                        },
                    );
                }
            }
        }

        // Watching tier — "what the bot is considering right now". Pull
        // recent near-misses, dedupe per mint keeping the most recent
        // observation, filter to last 30min + conf >= 60. Cap at 10
        // entries — beyond that the section becomes scroll-noise.
        //
        // ALSO skip mints that are already present in `events` —
        // otherwise the same token shows up twice (once as a watching
        // candidate, once as a CLASSIFICATION CHANGE / DEV SELLING /
        // etc alert) and the feed feels duplicated. Watching is the
        // "candidates not represented elsewhere" view.
        let now_ts = chrono::Utc::now().timestamp();
        let watch_window = 30 * 60;
        let event_mints: std::collections::HashSet<String> = events
            .iter()
            .filter_map(|e| e.mint.clone())
            .collect();
        let mut watching: Vec<WatchingEntry> = Vec::new();

        // Watching tier source: latest snapshot per mint at conf>=60 in
        // the firing classes (STAIRCASE/GRINDER/SPRING/DEVELOPING) within
        // the last 30min. Replaces the prior near-miss-table source which
        // only logged tokens that ALMOST fired on a single specific gate
        // (production showed `watching: 0` despite 6 active candidates in
        // DB). We still cross-reference the near-miss table to surface a
        // single "blocking gate" hint per row when one exists.
        let near_miss_by_mint: std::collections::HashMap<String, (String, String)> = self
            .db
            .get_recent_near_misses(200)
            .unwrap_or_default()
            .into_iter()
            .filter(|r| r.timestamp >= now_ts - watch_window)
            .map(|r| (r.token_address, (r.gate_that_failed, r.gap)))
            .collect();

        if let Ok(rows) = self
            .db
            .get_active_watching_candidates(now_ts - watch_window, 60, 30)
        {
            for (mint, classification, conf, top1, _top10, ts) in rows {
                if event_mints.contains(&mint) {
                    continue;
                }
                let r_token_address = mint.clone();
                let age = now_ts - ts;
                let (gate, gap) = near_miss_by_mint
                    .get(&mint)
                    .cloned()
                    .unwrap_or_else(|| (String::new(), String::new()));
                let mut entry = WatchingEntry {
                    mint: mint.clone(),
                    symbol: None,
                    classification,
                    confidence: conf,
                    top_holder_pct: top1,
                    age_secs: age,
                    gate,
                    gap,
                    last_seen: ts,
                };
                // Symbol from market cache when available — same enrichment
                // as the events list. Cheap because get_market is cached.
                if !tokens.contains_key(&r_token_address) {
                    if let Ok(Some(m)) = market::get_market(&r_token_address).await {
                        let info = StreamTokenInfo {
                            symbol: Some(m.symbol.clone()).filter(|s| !s.is_empty()),
                            name: Some(m.name.clone()).filter(|s| !s.is_empty()),
                            mcap_usd: if m.mcap_usd > 0.0 { Some(m.mcap_usd) } else { None },
                            price_usd: Some(m.price_usd).filter(|p| *p > 0.0),
                            price_change_1h: Some(m.price_change_h1).filter(|v| *v != 0.0),
                            price_change_24h: None,
                            liquidity_usd: Some(m.liquidity_usd).filter(|v| *v > 0.0),
                        };
                        entry.symbol = info.symbol.clone();
                        tokens.insert(r_token_address.clone(), info);
                    }
                } else if let Some(info) = tokens.get(&r_token_address) {
                    entry.symbol = info.symbol.clone();
                }
                watching.push(entry);
                if watching.len() >= 10 {
                    break;
                }
            }
        }
        // Sort watching by confidence desc, then age asc (newest first
        // among same-conf entries).
        watching.sort_by(|a, b| {
            b.confidence
                .cmp(&a.confidence)
                .then(a.age_secs.cmp(&b.age_secs))
        });

        StreamFile { events, tokens, watching }
    }

    async fn build_calls_file(&self) -> CallsFile {
        let mut active: Vec<CallSnapshot> = Vec::new();
        let mut history: Vec<CallSnapshot> = Vec::new();
        let now = chrono::Utc::now().timestamp();
        let rows = self.db.list_calls(false, 200).unwrap_or_default();
        for row in rows {
            // The public ledger only carries calls with a real lifecycle
            // outcome. `voided` rows (orphan-cleanup, retractions) stay in
            // the DB for forensics but never reach the front end.
            if !matches!(
                row.status.as_str(),
                "active" | "withdrew" | "failed" | "expired" | "closed"
            ) {
                continue;
            }
            let is_active = row.status == "active";
            // Only fetch live market data for active calls — history is fixed at exit price.
            let (current_price, current_mcap, current_liq) = if is_active {
                let market = market::get_market(&row.mint).await.ok().flatten();
                (
                    market.as_ref().map(|m| m.price_usd).unwrap_or(0.0),
                    market.as_ref().map(|m| m.mcap_usd).unwrap_or(0.0),
                    market.as_ref().map(|m| m.liquidity_usd).unwrap_or(0.0),
                )
            } else {
                // Use the locked exit price for closed calls so P&L is stable.
                (row.exit_price_usd.unwrap_or(0.0), 0.0, 0.0)
            };
            let ref_price = if is_active {
                current_price
            } else {
                row.exit_price_usd.unwrap_or(current_price)
            };
            let pct_from_call = if row.entry_price_usd > 0.0 && ref_price > 0.0 {
                Some((ref_price / row.entry_price_usd - 1.0) * 100.0)
            } else {
                None
            };
            // Normalise legacy 'closed' to 'withdrew' so the UI only sees the
            // four canonical outcomes: active, withdrew, failed, expired.
            let outcome_type = match row.status.as_str() {
                "closed" | "withdrew" => "withdrew",
                "failed" => "failed",
                "expired" => "expired",
                _ => "active",
            }
            .to_string();

            // Journey from token_snapshots: peak/trough between call-time
            // and (closed_at or now). Lets the front end show "ran +210%
            // before settling at +50%" — the entry/exit pair alone hides
            // the swing magnitude. Skip when we can't compute a pct.
            //
            // Snapshot-gap floor (2026-05-02): token_snapshots cadence
            // can drop to multi-minute gaps during fast runs (LMEOW had a
            // 46-min gap during a +393% pump and the publisher reported
            // peak +95% from the only snapshots that landed in window).
            // Use the realized exit pct as a lower bound — the trail-stop
            // ladder fires AT or near the run's peak, so exit ≤ peak by
            // definition. Take max(snap_peak, exit_pct) so the journey
            // never undersells the actual high-water mark.
            let (peak_pct, peak_at, trough_pct, trough_at) =
                if row.entry_price_usd > 0.0 {
                    let until = row.closed_at.unwrap_or(now);
                    let snap_extremes = self.db.get_price_extremes(&row.mint, row.called_at, until);
                    let exit_pct_floor = row.exit_price_usd.and_then(|exit| {
                        if exit > 0.0 {
                            Some((exit / row.entry_price_usd - 1.0) * 100.0)
                        } else {
                            None
                        }
                    });
                    match snap_extremes {
                        Ok(Some(((hi, hi_ts), (lo, lo_ts)))) => {
                            let snap_peak_pct = (hi / row.entry_price_usd - 1.0) * 100.0;
                            let snap_trough_pct = (lo / row.entry_price_usd - 1.0) * 100.0;
                            let combined_peak = match exit_pct_floor {
                                Some(f) if f > snap_peak_pct => f,
                                _ => snap_peak_pct,
                            };
                            (
                                Some(combined_peak),
                                Some(hi_ts),
                                Some(snap_trough_pct),
                                Some(lo_ts),
                            )
                        }
                        _ => (
                            exit_pct_floor,
                            row.closed_at,
                            None,
                            None,
                        ),
                    }
                } else {
                    (None, None, None, None)
                };

            let snap = CallSnapshot {
                id: row.id,
                mint: row.mint.clone(),
                symbol: row.symbol.clone(),
                classification: row.classification.clone(),
                confidence: row.confidence,
                called_at: row.called_at,
                expires_at: row.expires_at,
                note: row.note.clone(),
                source: row.source.clone(),
                status: row.status.clone(),
                outcome_type,
                closed_at: row.closed_at,
                exit_price_usd: row.exit_price_usd,
                exit_note: row.exit_note.clone(),
                entry_mcap_usd: row.entry_mcap_usd,
                entry_price_usd: row.entry_price_usd,
                entry_liquidity_usd: row.entry_liquidity_usd,
                entry_top_holder_pct: row.entry_top_holder_pct,
                entry_pair_dex: row.entry_pair_dex.clone(),
                current_mcap_usd: current_mcap,
                current_price_usd: current_price,
                current_liquidity_usd: current_liq,
                pct_from_call,
                peak_pct,
                peak_at,
                trough_pct,
                trough_at,
                thesis_url: crate::horizon::parse_thesis(&row.note)
                    .map(|f| format!("thoughts/{}", f)),
            };
            if is_active {
                active.push(snap);
            } else {
                history.push(snap);
            }
        }
        let stats = compute_call_stats(&history);
        CallsFile { active, history, stats }
    }

    /// For every call (active + closed), emit a one-shot scout receipt
    /// that freezes the evidence bundle as close to call-time as possible.
    /// Iterating history too lets fast-closed calls (settled before the
    /// next publisher tick caught them in `active`) still get a scout
    /// written from current state — better than nothing for the per-call
    /// detail page. Idempotent via the `call_id` field on the existing
    /// JSON: same call_id present → skip. Closed-call scouts are
    /// post-mortem snapshots, not entry-state, so the file's
    /// `captured_at` timestamp tells the reader when it was taken.
    async fn publish_call_scout_snapshots(&self, calls: &CallsFile, data_dir: &Path) {
        if calls.active.is_empty() && calls.history.is_empty() {
            return;
        }
        let scouts_dir = data_dir.join("scouts");
        if let Err(e) = std::fs::create_dir_all(&scouts_dir) {
            tracing::warn!("publisher: create scouts dir: {}", e);
            return;
        }
        for call in calls.active.iter().chain(calls.history.iter()) {
            let path = scouts_dir.join(format!("{}.json", call.mint));
            let existing: serde_json::Value = read_json(&path).unwrap_or_default();
            let existing_call_id = existing.get("call_id").and_then(|v| v.as_i64());
            if existing_call_id == Some(call.id) {
                continue;
            }

            let market = market::get_market(&call.mint).await.ok().flatten();
            let pair_addr = market
                .as_ref()
                .map(|m| m.pair_address.clone())
                .unwrap_or_default();
            let pair_dex = market
                .as_ref()
                .map(|m| m.pair_dex.clone())
                .filter(|dex| !dex.is_empty())
                .unwrap_or_else(|| call.entry_pair_dex.clone());
            let basic = crate::scout::scout(&call.mint, &self.rpc, &self.db)
                .await
                .ok();
            let whales = crate::scout::whale_trace(&call.mint, &self.rpc)
                .await
                .unwrap_or_default();
            let lp = if !pair_addr.is_empty() && !pair_dex.is_empty() {
                crate::scout::lp_check(&pair_addr, &pair_dex, &self.rpc)
                    .await
                    .ok()
            } else {
                None
            };
            let deployer_history = match &basic {
                Some(report) => match &report.deployer {
                    Some(deployer) => {
                        crate::scout::deployer_history(&deployer.deployer_address, &self.db)
                            .await
                            .unwrap_or_default()
                    }
                    None => Vec::new(),
                },
                None => Vec::new(),
            };
            let holder_evolution =
                crate::scout::holder_evolution(&call.mint, 24, &self.db).unwrap_or_default();
            let sniper_cohort = crate::scout::sniper_cohort(&call.mint, &self.rpc, &self.db)
                .await
                .unwrap_or_default();
            let cohort_overlap = crate::scout::cohort_overlap(&call.mint, &self.rpc, &self.db)
                .await
                .unwrap_or_default();

            let payload = serde_json::json!({
                "call_id": call.id,
                "captured_at": chrono::Utc::now().timestamp(),
                "call": {
                    "mint": call.mint,
                    "symbol": call.symbol,
                    "classification": call.classification,
                    "confidence": call.confidence,
                    "called_at": call.called_at,
                    "entry_mcap_usd": call.entry_mcap_usd,
                    "entry_price_usd": call.entry_price_usd,
                    "entry_liquidity_usd": call.entry_liquidity_usd,
                    "entry_top_holder_pct": call.entry_top_holder_pct,
                    "entry_pair_dex": call.entry_pair_dex,
                    "note": call.note,
                    "source": call.source,
                },
                "market": {
                    "symbol": market.as_ref().map(|m| m.symbol.clone()).unwrap_or_default(),
                    "name": market.as_ref().map(|m| m.name.clone()).unwrap_or_default(),
                    "mcap_usd": market.as_ref().map(|m| m.mcap_usd).unwrap_or(0.0),
                    "liquidity_usd": market.as_ref().map(|m| m.liquidity_usd).unwrap_or(0.0),
                    "pair_dex": pair_dex,
                    "pair_address": pair_addr,
                },
                "basic_scout": basic,
                "whales": whales,
                "lp": lp,
                "deployer_history": deployer_history,
                "holder_evolution_24h": holder_evolution,
                "sniper_cohort": sniper_cohort,
                "cohort_overlap": cohort_overlap,
            });
            if let Err(e) = std::fs::write(
                &path,
                serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".into()),
            ) {
                tracing::warn!("publisher: write scout snapshot {}: {}", path.display(), e);
            }
        }
    }

    /// Per-call detail page data. For each call (active + history), write
    /// `data/calls/<mint>.json` containing the full snapshot + the
    /// token_snapshots window between called_at and (closed_at or now).
    /// Drives the front-end's `#call=<mint>` drill-in. Active calls
    /// re-publish each tick (their snapshots advance); closed calls are
    /// frozen (idempotent skip when latest snapshot is unchanged).
    async fn publish_call_details(&self, calls: &CallsFile, data_dir: &Path) {
        if calls.active.is_empty() && calls.history.is_empty() {
            return;
        }
        let calls_dir = data_dir.join("calls");
        if let Err(e) = std::fs::create_dir_all(&calls_dir) {
            tracing::warn!("publisher: create calls dir: {}", e);
            return;
        }
        let now = chrono::Utc::now().timestamp();
        for call in calls.active.iter().chain(calls.history.iter()) {
            // Window for snapshot lookup. Active = called_at..now,
            // closed = called_at..closed_at. The DB helper takes a
            // limit, not a range; we filter post-pull.
            let window_end = call.closed_at.unwrap_or(now);
            // Pull up to 200 snapshots — covers a 30d LONG call at 15s
            // cycles after the watchlist drops to 5min cadence.
            let snaps = self.db.get_snapshot_history(&call.mint, 200).unwrap_or_default();
            // Filter to the window + reverse to chronological. Drop
            // snapshots before called_at (orphan analyses from when
            // the token wasn't on the watchlist as a call yet).
            let mut in_window: Vec<_> = snaps
                .into_iter()
                .filter(|s| s.timestamp >= call.called_at && s.timestamp <= window_end)
                .collect();
            in_window.sort_by_key(|a| a.timestamp);
            // Idempotency: when the latest snapshot ts in the file
            // matches what we'd write, skip. Closed calls converge to
            // a stable file after one write.
            let path = calls_dir.join(format!("{}.json", call.mint));
            let existing: serde_json::Value = read_json(&path).unwrap_or_default();
            let existing_last_ts = existing
                .get("last_snapshot_ts")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let new_last_ts = in_window.last().map(|s| s.timestamp).unwrap_or(0);
            let existing_status = existing
                .get("call")
                .and_then(|c| c.get("status"))
                .and_then(|s| s.as_str())
                .unwrap_or("");
            // Skip when snapshot tip + status are unchanged. Status flip
            // (active → withdrew) always re-publishes so the verdict
            // lands.
            if existing_last_ts == new_last_ts && existing_status == call.status {
                continue;
            }
            let payload = serde_json::json!({
                "call": call,
                "snapshots": in_window,
                "last_snapshot_ts": new_last_ts,
                "captured_at": now,
            });
            if let Err(e) = std::fs::write(
                &path,
                serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".into()),
            ) {
                tracing::warn!("publisher: write call detail {}: {}", path.display(), e);
            }
        }
    }

    /// For each active call, emit a per-mint JSON with the current whale
    /// trace + LP status. Lets the public verify the thesis in the same
    /// primitives we use in-session. Write to data/whales/<mint>.json.
    async fn publish_whale_snapshots(&self, calls: &CallsFile, data_dir: &Path) {
        if calls.active.is_empty() && calls.history.is_empty() {
            return;
        }
        let whales_dir = data_dir.join("whales");
        if let Err(e) = std::fs::create_dir_all(&whales_dir) {
            tracing::warn!("publisher: create whales dir: {}", e);
            return;
        }
        // One-shot receipts, mirror of `publish_call_scout_snapshots`. Walk
        // active + history. Skip when an existing JSON already records this
        // call_id — same idempotency contract. Without this, every tick
        // hammered RPCs for `whale_trace` + `lp_check` per active call,
        // overwrote the file, and closed calls' snapshots were lost.
        for call in calls.active.iter().chain(calls.history.iter()) {
            let path = whales_dir.join(format!("{}.json", call.mint));
            let existing: serde_json::Value = read_json(&path).unwrap_or_default();
            let existing_call_id = existing.get("call_id").and_then(|v| v.as_i64());
            if existing_call_id == Some(call.id) {
                continue;
            }
            let whales = crate::scout::whale_trace(&call.mint, &self.rpc)
                .await
                .unwrap_or_default();
            let lp = if !call.entry_pair_dex.is_empty() {
                let market = market::get_market(&call.mint).await.ok().flatten();
                let pair_addr = market
                    .as_ref()
                    .map(|m| m.pair_address.clone())
                    .unwrap_or_default();
                if !pair_addr.is_empty() {
                    crate::scout::lp_check(&pair_addr, &call.entry_pair_dex, &self.rpc)
                        .await
                        .ok()
                } else {
                    None
                }
            } else {
                None
            };
            let payload = serde_json::json!({
                "call_id": call.id,
                "mint": call.mint,
                "symbol": call.symbol,
                "captured_at": chrono::Utc::now().timestamp(),
                "whales": whales,
                "lp": lp,
            });
            if let Err(e) = std::fs::write(
                &path,
                serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".into()),
            ) {
                tracing::warn!("publisher: write whale snapshot {}: {}", path.display(), e);
            }
        }
    }

    /// Walk the wallet's recent signatures and upsert any detected trades
    /// into the ledger. A trade is: native SOL delta non-zero AND at least
    /// one SPL token delta on our owner. Exiting on the first signature
    /// that's already recorded keeps the incremental cost bounded.
    async fn capture_recent_trades(&self, _now: i64) {
        let sigs = self
            .rpc
            .get_recent_signatures(&self.wallet, 40)
            .await
            .unwrap_or_default();
        // Build the set of known signatures ONCE, outside the loop. The
        // previous code called get_wallet_trades_recent per-signature —
        // 40× DB round-trips fetching 200 rows each, all for a membership
        // check. Now: one query, one HashSet, O(1) lookup per sig.
        let known: std::collections::HashSet<String> = self
            .db
            .get_wallet_trades_recent(&self.wallet, 200)
            .unwrap_or_default()
            .into_iter()
            .map(|(_, _, _, s, _, _)| s)
            .collect();
        for sig in sigs.iter() {
            if sig.err {
                continue;
            }
            // Hit a known sig → ledger is immutable from older onward;
            // stop walking. Returned-newest-first means continuing past
            // a known sig only re-touches older known rows.
            if known.contains(&sig.signature) {
                break;
            }
            let summary = match self
                .rpc
                .get_tx_wallet_summary(&sig.signature, &self.wallet)
                .await
            {
                Ok(s) => s,
                Err(_) => continue,
            };
            // A trade needs SOL moving + at least one non-SOL token moving
            // on our wallet. Pure SOL transfers and pure token transfers
            // (airdrops, self-moves) aren't trades.
            if summary.sol_delta_ui.abs() < 1e-6 {
                continue;
            }
            for (mint, token_delta) in &summary.token_deltas {
                if mint == SOL_MINT {
                    continue;
                }
                let (side, sol_amount) = if *token_delta > 0.0 {
                    ("buy", summary.sol_delta_ui.abs())
                } else {
                    ("sell", summary.sol_delta_ui.abs())
                };
                let market = market::get_market(mint).await.ok().flatten();
                let price = market.as_ref().map(|m| m.price_usd).unwrap_or(0.0);
                let mcap = market.as_ref().map(|m| m.mcap_usd).unwrap_or(0.0);
                let _ = self.db.upsert_wallet_trade(
                    &sig.signature,
                    &self.wallet,
                    mint,
                    side,
                    token_delta.abs(),
                    sol_amount,
                    price,
                    mcap,
                    sig.block_time.unwrap_or(0),
                );
            }
        }
    }

    /// Push the consolidated public state to the Cloudflare Worker as a
    /// single HMAC-signed POST. Reads the JSON files just written into
    /// `data_dir` (so the file-write helpers stay untouched), pulls
    /// the diary feed + override snapshot from DB, signs `<ts>.<body>`
    /// with HMAC-SHA256, and ships the bundle.
    ///
    /// Returns `Ok(true)` on a successful push. The Worker validates
    /// the signature + timestamp skew before writing each present key
    /// into KV; the read endpoints pick up the new state on their next
    /// cache miss.
    async fn post_publish(&self, data_dir: &Path, ts: i64) -> Result<bool> {
        let url = self.cfg.cf_publish_url.as_str();
        if url.is_empty() {
            anyhow::bail!("cf_publish_url is empty — required when publisher.enabled");
        }
        // Secret was env-expanded at startup in main.rs.
        let secret = self.cfg.cf_publish_secret.as_str();
        if secret.is_empty() {
            anyhow::bail!("cf_publish_secret is empty — required when publisher.enabled");
        }

        // calls.json was just written by run_once. Read it back so the
        // POST carries the same shape the legacy GH Pages site
        // consumed; the Worker stores it under KV key `calls`.
        let calls_path = data_dir.join("calls.json");
        let calls = if calls_path.exists() {
            match std::fs::read_to_string(&calls_path) {
                Ok(s) => serde_json::from_str::<serde_json::Value>(&s)
                    .unwrap_or(serde_json::Value::Null),
                Err(e) => {
                    tracing::warn!("publisher: read calls.json: {}", e);
                    serde_json::Value::Null
                }
            }
        } else {
            serde_json::Value::Null
        };

        // Diary feed — last 20 evolution events ordered newest-first.
        // Same shape the static site renders client-side.
        let diary = self
            .db
            .list_evolution_events(None, 20)
            .map(|events| {
                events
                    .into_iter()
                    .map(|e| {
                        serde_json::json!({
                            "id": e.id,
                            "kind": e.kind,
                            "title": e.summary,
                            "summary": e.summary,
                            "body_md": e.body_md,
                            "created_at": e.committed_at,
                            "diary_path": e.diary_path,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Strategy snapshot — the runtime-mutable signal_overrides in
        // effect right now. Empty when the engine is running on
        // defaults (the Worker renders that as "no overrides currently
        // in effect — engine running on defaults").
        let strategy = self
            .db
            .list_signal_overrides()
            .map(|rows| {
                let map: serde_json::Map<String, serde_json::Value> = rows
                    .into_iter()
                    .map(|(field, scope, value, set_at)| {
                        let key = if scope.is_empty() {
                            field
                        } else {
                            format!("{}:{}", field, scope)
                        };
                        (
                            key,
                            serde_json::json!({ "value": value, "set_at": set_at }),
                        )
                    })
                    .collect();
                serde_json::Value::Object(map)
            })
            .unwrap_or(serde_json::json!({}));

        // Rich data feed — every JSON file the live site reads, bundled
        // into one `data: {...}` field so the Worker can fan it out into
        // KV under the `data:<name>` namespace.
        //
        // Top-level snapshots are read from the staging dir verbatim;
        // the per-mint detail dirs (calls/, scouts/, whales/) are
        // collapsed into single objects keyed by mint so that one KV
        // write per detail class covers all calls. The Worker slices on
        // read at /api/data/{calls,scouts,whales}/<mint>.
        // Thoughts assets + index live one level up under thoughts/.
        // image_gen writes assets.json on its own cadence; the publisher
        // ships whatever's there so the page can swap from the static
        // bundled assets.json to the live KV-backed one.
        let thoughts_dir = PathBuf::from(&self.cfg.repo_path).join("thoughts");
        let data = serde_json::json!({
            "health":           read_json_file(&data_dir.join("health.json")),
            "pnl":              read_json_file(&data_dir.join("pnl.json")),
            "positions":        read_json_file(&data_dir.join("positions.json")),
            "activity":         read_json_file(&data_dir.join("activity.json")),
            "calls":            calls.clone(),
            "stream":           read_json_file(&data_dir.join("stream.json")),
            "featured":         read_json_file(&data_dir.join("featured.json")),
            "calls_details":    read_json_dir_as_map(&data_dir.join("calls")),
            "scouts":           read_json_dir_as_map(&data_dir.join("scouts")),
            "whales":           read_json_dir_as_map(&data_dir.join("whales")),
            "thoughts_index":   read_json_file(&thoughts_dir.join("index.json")),
            "thoughts_assets":  read_json_file(&thoughts_dir.join("assets.json")),
        });

        let body = serde_json::json!({
            "calls": calls,
            "diary": diary,
            "strategy": strategy,
            "data": data,
            "captured_at": ts,
        });
        let body_str = serde_json::to_string(&body)?;

        // Signature: HMAC-SHA256 over `<ts>.<body>`. The same scheme
        // the Worker (cloudflare/worker/src/index.js) validates with.
        // Replay protection lives on the Worker side via timestamp
        // skew check.
        let signed = format!("{}.{}", ts, body_str);
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).context("hmac key init")?;
        mac.update(signed.as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent(concat!("exciton-publisher/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("build reqwest client")?;
        let resp = client
            .post(url)
            .header("X-Exciton-Timestamp", ts.to_string())
            .header("X-Exciton-Signature", sig)
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await
            .context("cf publish send")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            anyhow::bail!("cf publish {} -> {}", status, detail);
        }
        tracing::info!("publisher: cf publish ok ts={}", ts);
        Ok(true)
    }
}

/// Live SOL price from DexScreener. We already hit the endpoint for every
/// scanned token, so caching isn't critical here — one extra call per
/// publish cycle. On any failure the caller falls back to the configured
/// constant so publish never bails on a missing price.
async fn fetch_sol_price() -> Option<f64> {
    let m = market::get_market(SOL_MINT).await.ok().flatten()?;
    if m.price_usd > 0.0 {
        Some(m.price_usd)
    } else {
        None
    }
}

/// Aggregate stats for the public history view. Computed in three buckets:
/// short-horizon, long-horizon, overall. Excludes calls without a valid
/// pct (entry_price=0 + no exit). `wins` counts withdrew, `losses` counts
/// failed, `expired` is its own bucket. win_rate is wins / (wins + losses)
/// — expired calls don't count as either since they didn't reach a verdict.
fn compute_call_stats(history: &[CallSnapshot]) -> CallStats {
    let mut short_calls: Vec<f64> = Vec::new();
    let mut long_calls: Vec<f64> = Vec::new();
    let mut moonshot_calls: Vec<f64> = Vec::new();
    let mut scalp_calls: Vec<f64> = Vec::new();
    let mut all_calls: Vec<f64> = Vec::new();
    // bucket_counts[0]=short, [1]=long, [2]=moonshot, [3]=scalp, [4]=overall.
    // Tuple: (count, wins, losses, expired).
    let mut bucket_counts: [(usize, usize, usize, usize); 5] = [(0, 0, 0, 0); 5];

    // Per-source accumulator: source string → (pcts, counts). Built up
    // alongside the horizon axis so we can answer "do operator picks
    // beat the bot?". `source` is the field set at insert_call time:
    // `notifier` for auto-call, `dm` for /call, `mcp` for claw.
    let mut by_source: std::collections::HashMap<String, (Vec<f64>, (usize, usize, usize, usize))> =
        std::collections::HashMap::new();

    for c in history {
        let Some(pct) = c.pct_from_call else { continue };
        let h = crate::horizon::parse(&c.note);
        // Bucket index per horizon. Unknown defaults to "short" for back-
        // compat (legacy rows had no horizon tag and were short-shaped).
        let bucket_idx = match h {
            crate::horizon::Horizon::Long => 1,
            crate::horizon::Horizon::Moonshot => 2,
            crate::horizon::Horizon::Scalp => 3,
            _ => 0,
        };
        // Update both per-horizon and overall (idx 4) buckets.
        for &i in &[bucket_idx, 4] {
            bucket_counts[i].0 += 1;
            match c.outcome_type.as_str() {
                "withdrew" => bucket_counts[i].1 += 1,
                "failed" => bucket_counts[i].2 += 1,
                "expired" => bucket_counts[i].3 += 1,
                _ => {}
            }
        }
        match h {
            crate::horizon::Horizon::Long => long_calls.push(pct),
            crate::horizon::Horizon::Moonshot => moonshot_calls.push(pct),
            crate::horizon::Horizon::Scalp => scalp_calls.push(pct),
            _ => short_calls.push(pct),
        }
        all_calls.push(pct);
        // Bucket by source. Calls from before the source axis was
        // recorded land in a `legacy` key.
        let src_key = if c.source.is_empty() {
            "legacy".to_string()
        } else {
            c.source.clone()
        };
        let entry = by_source.entry(src_key).or_default();
        entry.0.push(pct);
        entry.1 .0 += 1;
        match c.outcome_type.as_str() {
            "withdrew" => entry.1 .1 += 1,
            "failed" => entry.1 .2 += 1,
            "expired" => entry.1 .3 += 1,
            _ => {}
        }
    }

    let mk = |pcts: &[f64], (count, wins, losses, expired): (usize, usize, usize, usize)| -> CallStatsBucket {
        let winners: Vec<f64> = pcts.iter().copied().filter(|p| *p > 0.0).collect();
        let losers: Vec<f64> = pcts.iter().copied().filter(|p| *p < 0.0).collect();
        let avg = |v: &[f64]| -> f64 {
            if v.is_empty() {
                0.0
            } else {
                v.iter().sum::<f64>() / v.len() as f64
            }
        };
        let win_rate = if wins + losses > 0 {
            wins as f64 / (wins + losses) as f64 * 100.0
        } else {
            0.0
        };
        let best = pcts.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let worst = pcts.iter().copied().fold(f64::INFINITY, f64::min);
        CallStatsBucket {
            count,
            wins,
            losses,
            expired,
            win_rate,
            avg_winner_pct: avg(&winners),
            avg_loser_pct: avg(&losers),
            best_pct: if best.is_finite() { best } else { 0.0 },
            worst_pct: if worst.is_finite() { worst } else { 0.0 },
        }
    };

    let by_source_out: std::collections::HashMap<String, CallStatsBucket> = by_source
        .into_iter()
        .map(|(k, (pcts, counts))| (k, mk(&pcts, counts)))
        .collect();

    CallStats {
        short: mk(&short_calls, bucket_counts[0]),
        long: mk(&long_calls, bucket_counts[1]),
        moonshot: mk(&moonshot_calls, bucket_counts[2]),
        scalp: mk(&scalp_calls, bucket_counts[3]),
        overall: mk(&all_calls, bucket_counts[4]),
        by_source: by_source_out,
    }
}

fn short(s: &str) -> String {
    if s.len() < 10 {
        s.to_string()
    } else {
        format!("{}…{}", &s[..4], &s[s.len() - 4..])
    }
}

fn mint_short(m: &str) -> String {
    short(m)
}

/// Parse the leading "+X.X%" or "-X.X%" from an exit_note like
/// "+250.4% · moonshot 3.5x" or "-30.2% · scalp stop". Returns None
/// when the note doesn't start with a percentage (legacy rows, manual
/// closes). Used by the live-feed filter to decide which call closes
/// are worth surfacing — small-magnitude exits get hidden so the feed
/// reads like a story instead of a graveyard.
fn exit_pct_from_note(note: &str) -> Option<f64> {
    let trimmed = note.trim_start();
    let (sign, rest) = match trimmed.chars().next()? {
        '+' => (1.0, &trimmed[1..]),
        '-' => (-1.0, &trimmed[1..]),
        c if c.is_ascii_digit() => (1.0, trimmed),
        _ => return None,
    };
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    let num: f64 = rest[..end].parse().ok()?;
    Some(sign * num)
}

fn fmt_number_human(n: f64) -> String {
    let abs = n.abs();
    if abs >= 1_000_000.0 {
        format!("{:.2}M", n / 1_000_000.0)
    } else if abs >= 1_000.0 {
        format!("{:.1}k", n / 1_000.0)
    } else {
        format!("{:.2}", n)
    }
}

fn fmt_compact(n: f64) -> String {
    fmt_number_human(n)
}

/// Format a token amount with k/M suffixes so the activity log reads like
/// a human wrote it, not a calculator ("2.3M" beats "2341893.25").
fn fmt_amount(n: f64) -> String {
    fmt_number_human(n)
}

fn read_json<T: for<'de> Deserialize<'de> + Default>(path: &Path) -> Result<T> {
    if !path.exists() {
        return Ok(T::default());
    }
    let s = std::fs::read_to_string(path).with_context(|| format!("read {:?}", path))?;
    Ok(serde_json::from_str(&s).unwrap_or_default())
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let s = serde_json::to_string_pretty(value).context("serialize json")?;
    std::fs::write(path, s).with_context(|| format!("write {:?}", path))?;
    Ok(())
}

/// Read a JSON file off the staging dir into a `serde_json::Value`
/// without forcing a schema. Missing/unreadable/unparseable files
/// resolve to `null` so the publisher POST body always has the same
/// shape regardless of which side-effect files made it to disk.
fn read_json_file(path: &Path) -> serde_json::Value {
    if !path.exists() {
        return serde_json::Value::Null;
    }
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::Null),
        Err(_) => serde_json::Value::Null,
    }
}

/// Read every `*.json` file in a directory into an object keyed by the
/// file stem. Used for the per-mint detail dirs (data/calls,
/// data/scouts, data/whales) which the engine writes one file per call
/// but the Worker stores as a single map for cheap KV writes. Missing
/// directory yields an empty object.
fn read_json_dir_as_map(dir: &Path) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return serde_json::Value::Object(map),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let stem = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_owned(),
            None => continue,
        };
        if let Ok(s) = std::fs::read_to_string(&path) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) {
                map.insert(stem, v);
            }
        }
    }
    serde_json::Value::Object(map)
}
