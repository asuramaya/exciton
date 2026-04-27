//! MadApes.ai publisher — periodically snapshots the operating wallet to
//! JSON files inside the MadApes.ai repo checkout, then commits and pushes.
//!
//! Zero LLM, zero API keys. All numbers come from on-chain reads + DexScreener;
//! all summaries are templated from raw balance deltas. The site is the bag +
//! the tracks + the thoughts — this module owns the first two. Thoughts are
//! append-only markdown and never touched by the publisher.
//!
//! Pacing:
//!   - runs on a fixed tokio interval (default 5 min)
//!   - commits only touch data/ — thoughts/ is append-only and manual
//!   - commit prefix `data:` so the git log stays legible next to `note:`
//!     commits from hand-written reads

use crate::config::MadapesConfig;
use crate::db::Db;
use crate::ingester::RpcRouter;
use crate::market;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;
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

#[derive(Debug, Serialize)]
struct StreamFile {
    events: Vec<StreamEvent>,
    /// mint → token info, populated for every distinct mint present in
    /// `events`. Empty map when no mints were referenced.
    tokens: std::collections::HashMap<String, StreamTokenInfo>,
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
    overall: CallStatsBucket,
}

#[derive(Debug, Serialize)]
struct CallsFile {
    active: Vec<CallSnapshot>,
    history: Vec<CallSnapshot>,
    stats: CallStats,
}

pub struct Publisher {
    cfg: MadapesConfig,
    wallet: String,
    rpc: Arc<RpcRouter>,
    db: Arc<Db>,
}

impl Publisher {
    pub fn new(cfg: MadapesConfig, wallet: String, rpc: Arc<RpcRouter>, db: Arc<Db>) -> Self {
        Self {
            cfg,
            wallet,
            rpc,
            db,
        }
    }

    pub fn spawn(self: Arc<Self>) {
        let interval = self.cfg.interval_seconds.max(60);
        tokio::spawn(async move {
            tracing::info!(
                "MadApes publisher active: pushing to {} every {}s",
                self.cfg.repo_path,
                interval
            );
            let mut tick = tokio::time::interval(Duration::from_secs(interval));
            loop {
                tick.tick().await;
                match self.run_once().await {
                    Ok(committed) if committed => {
                        tracing::info!("MadApes publish: data snapshot pushed")
                    }
                    Ok(_) => tracing::debug!("MadApes publish: no data change"),
                    Err(e) => tracing::warn!("MadApes publish failed: {}", e),
                }
            }
        });
    }

    pub async fn run_once(&self) -> Result<bool> {
        let repo = PathBuf::from(&self.cfg.repo_path);
        if !repo.join(".git").exists() {
            anyhow::bail!("repo_path {:?} is not a git checkout", repo);
        }
        let data_dir = repo.join("data");
        std::fs::create_dir_all(&data_dir).context("create data/ dir")?;
        let now = chrono::Utc::now().timestamp();

        // 1. Wallet state + live SOL price. RPC failure here used to abort
        // the entire tick, which froze the public ledger whenever both RPCs
        // were 429'd — calls.json, scout receipts, and whale snapshots all
        // stopped publishing for unrelated reasons. Now: log + degrade to 0
        // so the rest of the pipeline still ships fresh data.
        let sol_balance = match self.rpc.get_balance(&self.wallet).await {
            Ok(lamports) => lamports as f64 / 1e9,
            Err(e) => {
                tracing::warn!(
                    "publisher: get_balance failed ({}) — degrading wallet snapshot, continuing",
                    e
                );
                0.0
            }
        };
        let sol_price_usd = fetch_sol_price()
            .await
            .unwrap_or(self.cfg.sol_price_fallback_usd);

        // 2. Current holdings — used for positions + mark-to-market PnL.
        let holdings = self
            .rpc
            .get_wallet_token_holdings(&self.wallet)
            .await
            .unwrap_or_default();

        // 3. Scan recent wallet signatures, record any detected trades into
        //    the wallet_ledger. Idempotent by signature, so re-scanning the
        //    same window never double-counts.
        self.capture_recent_trades(now).await;

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
        for (_mint, (bt, bs, st, rs)) in &cb_map {
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

        // 9. Calls: every active call + last N closed calls, each with
        //    live mark-to-market so the site shows pct-from-call honestly.
        let calls_file = self.build_calls_file().await;

        // 9b. One-shot scout receipts per active call — captures the
        //     evidence bundle close to call-time and keeps it public.
        self.publish_call_scout_snapshots(&calls_file, &data_dir)
            .await;

        // 9c. Per-call whale snapshots — makes trigger monitoring public.
        //     A visitor can now load data/whales/<mint>.json and see
        //     exactly the same top-10 flow we're watching in-session.
        self.publish_whale_snapshots(&calls_file, &data_dir).await;

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

        write_json(&data_dir.join("health.json"), &health)?;
        write_json(&pnl_path, &pnl)?;
        write_json(
            &data_dir.join("positions.json"),
            &PositionsFile { positions },
        )?;
        write_json(&data_dir.join("activity.json"), &ActivityFile { activity })?;
        write_json(&data_dir.join("calls.json"), &calls_file)?;
        write_json(&data_dir.join("stream.json"), &stream)?;

        self.commit_and_push(&repo, now)
    }

    async fn build_stream_file(&self) -> StreamFile {
        let mut events: Vec<StreamEvent> = Vec::new();

        // Recent scanner alerts — any with confidence >= 50 gets in. The
        // alert table stores the full mint inline in the message body for
        // Telegram-channel readability; for the stream we prefer the
        // shortened form since the mint already rides along as a structured
        // field that the UI renders as a link.
        if let Ok(alerts) = self.db.get_pending_alerts(40) {
            for a in alerts {
                if a.confidence < 50 {
                    continue;
                }
                let tag = a.alert_type.to_uppercase().replace('_', " ");
                let clean_summary = match a.token_address.as_deref() {
                    Some(mint) => a.message.replace(mint, &mint_short(mint)),
                    None => a.message,
                };
                events.push(StreamEvent {
                    ts: a.timestamp,
                    kind: "alert".into(),
                    tag,
                    summary: clean_summary,
                    mint: a.token_address,
                    signature: None,
                });
            }
        }

        // Recent wallet trades — every row the ledger detected.
        let trades = self
            .db
            .get_wallet_trades_recent(&self.wallet, 15)
            .unwrap_or_default();
        for (ts, mint, side, sig, tok, sol) in trades {
            let verb = if side == "buy" { "bought" } else { "cut" };
            events.push(StreamEvent {
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

        // Calls fired + closed, newest first.
        if let Ok(rows) = self.db.list_calls(false, 25) {
            for c in rows {
                let sym = if c.symbol.is_empty() {
                    mint_short(&c.mint)
                } else {
                    format!("${}", c.symbol)
                };
                match c.status.as_str() {
                    "active" => events.push(StreamEvent {
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
                    "closed" | "expired" => {
                        if let Some(closed_ts) = c.closed_at {
                            let tag_str = if c.status == "expired" {
                                "CALL EXPIRED"
                            } else {
                                "CALL CLOSED"
                            };
                            events.push(StreamEvent {
                                ts: closed_ts,
                                kind: "call".into(),
                                tag: tag_str.into(),
                                summary: format!(
                                    "{} — {}",
                                    sym,
                                    c.exit_note.unwrap_or_else(|| "no note".into())
                                ),
                                mint: Some(c.mint),
                                signature: None,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }

        // Sort newest first, cap at 50.
        events.sort_by(|a, b| b.ts.cmp(&a.ts));
        events.truncate(50);

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

        StreamFile { events, tokens }
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
            let (peak_pct, peak_at, trough_pct, trough_at) =
                if row.entry_price_usd > 0.0 {
                    let until = row.closed_at.unwrap_or(now);
                    match self.db.get_price_extremes(&row.mint, row.called_at, until) {
                        Ok(Some(((hi, hi_ts), (lo, lo_ts)))) => (
                            Some((hi / row.entry_price_usd - 1.0) * 100.0),
                            Some(hi_ts),
                            Some((lo / row.entry_price_usd - 1.0) * 100.0),
                            Some(lo_ts),
                        ),
                        _ => (None, None, None, None),
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

    /// For each active call, emit a one-shot scout receipt that freezes the
    /// evidence bundle near call-time. Overwrites only when the active call
    /// row changes, so a reopened mint gets a fresh receipt.
    async fn publish_call_scout_snapshots(&self, calls: &CallsFile, data_dir: &Path) {
        if calls.active.is_empty() {
            return;
        }
        let scouts_dir = data_dir.join("scouts");
        if let Err(e) = std::fs::create_dir_all(&scouts_dir) {
            tracing::warn!("publisher: create scouts dir: {}", e);
            return;
        }
        for call in &calls.active {
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

    /// For each active call, emit a per-mint JSON with the current whale
    /// trace + LP status. Lets the public verify the thesis in the same
    /// primitives we use in-session. Write to data/whales/<mint>.json.
    async fn publish_whale_snapshots(&self, calls: &CallsFile, data_dir: &Path) {
        if calls.active.is_empty() {
            return;
        }
        let whales_dir = data_dir.join("whales");
        if let Err(e) = std::fs::create_dir_all(&whales_dir) {
            tracing::warn!("publisher: create whales dir: {}", e);
            return;
        }
        for call in &calls.active {
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
                "mint": call.mint,
                "symbol": call.symbol,
                "snapshot_ts": chrono::Utc::now().timestamp(),
                "whales": whales,
                "lp": lp,
            });
            let path = whales_dir.join(format!("{}.json", call.mint));
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
        for sig in sigs.iter() {
            if sig.err {
                continue;
            }
            // Once we hit a signature we already stored, stop — the ledger
            // is immutable from older-than-that onward.
            let already = self
                .db
                .get_wallet_trades_recent(&self.wallet, 200)
                .unwrap_or_default()
                .into_iter()
                .any(|(_, _, _, s, _, _)| s == sig.signature);
            if already {
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

    fn commit_and_push(&self, repo: &Path, ts: i64) -> Result<bool> {
        let status = Command::new("git")
            .args(["-C", repo.to_str().unwrap_or(".")])
            .args(["status", "--porcelain", "--", "data/"])
            .output()
            .context("git status")?;
        if status.stdout.is_empty() {
            return Ok(false);
        }

        Command::new("git")
            .args(["-C", repo.to_str().unwrap_or(".")])
            .args(["add", "data/"])
            .output()
            .context("git add")?;

        let msg = format!("data: snapshot {}", ts);
        let commit = Command::new("git")
            .args(["-C", repo.to_str().unwrap_or(".")])
            .args(["commit", "-m", &msg])
            .output()
            .context("git commit")?;
        if !commit.status.success() {
            anyhow::bail!(
                "git commit failed: {}",
                String::from_utf8_lossy(&commit.stderr)
            );
        }

        let push = Command::new("git")
            .args(["-C", repo.to_str().unwrap_or(".")])
            .args(["push", "--quiet"])
            .output()
            .context("git push")?;
        if !push.status.success() {
            anyhow::bail!("git push failed: {}", String::from_utf8_lossy(&push.stderr));
        }
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
    let mut all_calls: Vec<f64> = Vec::new();
    let mut bucket_counts: [(usize, usize, usize, usize); 3] = [(0, 0, 0, 0); 3];
    // bucket_counts[0]=short, [1]=long, [2]=overall.
    // Tuple: (count, wins, losses, expired).

    for c in history {
        let Some(pct) = c.pct_from_call else { continue };
        let is_long = c.note.contains("horizon=LONG");
        let bucket_idx = if is_long { 1 } else { 0 };
        // Update both per-horizon and overall buckets.
        for &i in &[bucket_idx, 2] {
            bucket_counts[i].0 += 1;
            match c.outcome_type.as_str() {
                "withdrew" => bucket_counts[i].1 += 1,
                "failed" => bucket_counts[i].2 += 1,
                "expired" => bucket_counts[i].3 += 1,
                _ => {}
            }
        }
        if is_long {
            long_calls.push(pct);
        } else {
            short_calls.push(pct);
        }
        all_calls.push(pct);
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

    CallStats {
        short: mk(&short_calls, bucket_counts[0]),
        long: mk(&long_calls, bucket_counts[1]),
        overall: mk(&all_calls, bucket_counts[2]),
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
