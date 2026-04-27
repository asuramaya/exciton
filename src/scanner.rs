use crate::db::Db;
use crate::discovery;
use crate::horizon;
use crate::ingester::RpcRouter;
use crate::notifier::Notifier;
use crate::signals;
use crate::signals::TokenAnalysis;
use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

const WATCHLIST_RECHECK_SECONDS: i64 = 15;
const WATCHLIST_STALE_SECONDS: i64 = 60 * 60;
const WATCHLIST_TARGET_REVISIT_SECONDS: u64 = 5 * 60;
const WATCHLIST_MIN_RESCAN_BATCH: usize = 6;
const SIGNAL_MIN_WATCHLIST_AGE_SECONDS: i64 = 120;

/// Background scanner that continuously discovers and analyzes tokens
pub struct BackgroundScanner {
    db: Arc<Db>,
    rpc: Arc<RpcRouter>,
    interval: Duration,
    alert_threshold: i32,
    max_active_tokens: usize,
    watchlist_rescan_limit: usize,
    running: Arc<AtomicBool>,
    notifier: Option<Arc<Notifier>>,
}

impl BackgroundScanner {
    pub fn new(
        db: Arc<Db>,
        rpc: Arc<RpcRouter>,
        interval_seconds: u64,
        alert_threshold: i32,
        max_active_tokens: usize,
    ) -> Self {
        let bounded_active_tokens = max_active_tokens.max(1);
        Self {
            db,
            rpc,
            interval: Duration::from_secs(interval_seconds),
            alert_threshold,
            max_active_tokens: bounded_active_tokens,
            watchlist_rescan_limit: compute_watchlist_rescan_limit(
                bounded_active_tokens,
                interval_seconds,
            ),
            running: Arc::new(AtomicBool::new(false)),
            notifier: None,
        }
    }

    /// Attach a telegram notifier. When set, the scanner will route qualifying
    /// analyses to the notifier after each classification.
    pub fn with_notifier(mut self, notifier: Arc<Notifier>) -> Self {
        self.notifier = Some(notifier);
        self
    }

    /// Route a fresh analysis to the notifier. Errors are logged but never
    /// interrupt the scan cycle — a broken notifier must not break discovery.
    async fn notify(&self, analysis: &TokenAnalysis) {
        let Some(n) = &self.notifier else {
            return;
        };
        if let Err(e) = n.process_token(analysis, analysis.confidence.total).await {
            tracing::warn!("notifier.process_token failed: {}", e);
        }
    }

    /// At startup, log any active call whose watchlist entry is inactive.
    /// We deliberately don't auto-fail or auto-void these: most watchlist
    /// deactivations are non-UNSAFE (DEAD/CRASHING/concentrated) which
    /// runtime intentionally leaves open for operator review (see scanner
    /// `should_remove_from_watchlist` vs `is_confirmed_unsafe` split). The
    /// 14-day `expires_at` handles abandoned rows; everything else needs an
    /// explicit operator `/close_call` to write a verdict.
    async fn cleanup_orphaned_calls(&self) {
        let orphans = match self.db.list_orphaned_calls() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("list_orphaned_calls failed: {}", e);
                return;
            }
        };
        if orphans.is_empty() {
            return;
        }
        tracing::info!(
            "startup: {} active call(s) with inactive watchlist — left open for operator review",
            orphans.len()
        );
        for mint in &orphans {
            tracing::info!("orphan call (left active): {}", mint);
        }
    }

    /// One-shot startup pass that re-renders TG cards for terminal calls
    /// whose deliveries are demoted with stale content. Two cohorts:
    ///   - Voided calls from the orphan-cleanup migration (header still
    ///     reads SIGNAL · active or FAILED with no verdict line).
    ///   - Closed calls from before the caller-voice + settling rewrite
    ///     (header reads FAILED but with old robot-voice body).
    /// `force_update_card` is idempotent: when the card already shows
    /// the right state Telegram returns 400 "message is not modified"
    /// which the notifier treats as success.
    async fn backfill_terminal_deliveries(&self) {
        let Some(notifier) = &self.notifier else {
            return;
        };
        let calls = match self.db.list_calls(false, 200) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("backfill: list_calls failed: {}", e);
                return;
            }
        };
        let mut count = 0usize;
        for c in calls {
            // Skip active calls — those cards are managed by the live loop.
            if c.status == "active" {
                continue;
            }
            // Map to canonical outcome string + a verdict that reads
            // sensibly even if exit_note is missing or stale.
            let (outcome, exit_note): (&str, String) = match c.status.as_str() {
                "withdrew" | "closed" => (
                    "withdrew",
                    c.exit_note
                        .clone()
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "closed".to_string()),
                ),
                "failed" => (
                    "failed",
                    c.exit_note
                        .clone()
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "thesis broke".to_string()),
                ),
                "expired" => (
                    "expired",
                    c.exit_note
                        .clone()
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "expired".to_string()),
                ),
                "voided" => (
                    "voided",
                    "administrative cleanup — not a market verdict".to_string(),
                ),
                _ => continue,
            };
            // Skip when the call has no delivery row at all (manual call
            // that never reached the channel, or a row pre-dating the
            // delivery system).
            let has_delivery = self
                .db
                .get_active_delivery(&c.mint, "winners")
                .map(|o| o.is_some())
                .unwrap_or(false);
            if !has_delivery {
                continue;
            }
            match notifier
                .force_update_card(&c.mint, outcome, None, &exit_note)
                .await
            {
                Ok(_) => count += 1,
                Err(e) => tracing::warn!("backfill: force_update_card {} failed: {}", c.mint, e),
            }
            // Light pacing — Telegram caps at 30 edits/sec/bot. 200ms
            // keeps us well under, and the backfill only runs once.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if count > 0 {
            tracing::info!("backfill: re-rendered {} terminal card(s)", count);
        }
    }

    /// Start the background scan loop. Returns a handle to stop it.
    pub fn start(self) -> ScannerHandle {
        let running = self.running.clone();
        running.store(true, AtomicOrdering::SeqCst);

        let handle_running = running.clone();
        tokio::spawn(async move {
            self.run_loop().await;
        });

        ScannerHandle {
            running: handle_running,
        }
    }

    async fn run_loop(self) {
        tracing::info!(
            "Background scanner started: interval={}s, alert_threshold={}, max_active_tokens={}, watchlist_batch={}",
            self.interval.as_secs(),
            self.alert_threshold,
            self.max_active_tokens,
            self.watchlist_rescan_limit,
        );

        let _ = self
            .db
            .audit_log("scanner", "start", "Background scanner started");

        // One-time startup cleanup: auto-fail any active call whose watchlist
        // entry is already inactive. These orphans arise when the service
        // restarted after a watchlist deactivation but before the auto-fail
        // hook ran. Without this, they sit open forever.
        self.cleanup_orphaned_calls().await;

        // One-shot card backfill: walk closed calls whose TG delivery never
        // got a proper outcome edit (voided cards from orphan-cleanup, old
        // demote/FAILED flips from the should_fail data-glitch era), and
        // re-render with the current caller-voice format + correct verdict
        // header. Idempotent — re-runs are no-ops once the channel is clean.
        self.backfill_terminal_deliveries().await;

        let mut cycle = 0u64;
        while self.running.load(AtomicOrdering::SeqCst) {
            cycle += 1;
            tracing::debug!("Scanner cycle {}", cycle);

            match self.scan_cycle().await {
                Ok(found) => {
                    if found > 0 {
                        tracing::info!("Scanner cycle {}: {} alerts generated", cycle, found);
                    }
                }
                Err(e) => {
                    tracing::warn!("Scanner cycle {} failed: {}", cycle, e);
                    // Don't spam on persistent errors — back off
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }

            tokio::time::sleep(self.interval).await;
        }

        tracing::info!("Background scanner stopped");
    }

    async fn scan_cycle(&self) -> anyhow::Result<usize> {
        // Phase 1: Re-analyze a bounded, scored watchlist shortlist.
        let watchlist = self.prepare_watchlist_batch()?;

        // Pre-fetch DexScreener market data for all watchlist tokens in one HTTP
        // call instead of one per token. Falls back to per-token fetch on error.
        let wl_mints: Vec<&str> = watchlist.iter().map(|(addr, _)| addr.as_str()).collect();
        let mut market_cache = crate::market::get_market_batch(&wl_mints)
            .await
            .unwrap_or_default();

        for (addr, added_at) in &watchlist {
            match signals::analyze_token(&self.rpc, addr, Some(&self.db), market_cache.remove(addr)).await {
                Ok(analysis) => {
                    let class = &analysis.confidence.classification;
                    self.db.update_watchlist_checked(addr, class)?;

                    // Check delta for alerts
                    if let Some(ref delta) = analysis.delta {
                        // Classification changed — always alert
                        if delta.classification_changed {
                            self.db.insert_alert(
                                "classification_change",
                                Some(addr),
                                &format!(
                                    "CLASSIFICATION_CHANGE {} · {}→{} · top {:+.2}%",
                                    addr,
                                    delta.previous.classification,
                                    analysis.confidence.classification,
                                    delta.top_holder_delta
                                ),
                                analysis.confidence.total,
                            )?;
                        }

                        // Concentration direction alert — raw facts only
                        if delta.concentration_direction == "concentrating" {
                            self.db.insert_alert(
                                "concentrating",
                                Some(addr),
                                &format!(
                                    "CONCENTRATING {} · top {:.2}%→{:.2}% ({:+.2}%) · {}",
                                    addr,
                                    delta.previous.top_holder_pct,
                                    delta.current.top_holder_pct,
                                    delta.top_holder_delta,
                                    analysis.confidence.classification,
                                ),
                                analysis.confidence.total,
                            )?;
                        }

                        // Velocity exit — raw velocity numbers, no editorial
                        if delta.current.velocity < 0.5 && delta.previous.velocity > 1.0 {
                            self.db.insert_alert(
                                "velocity_crash",
                                Some(addr),
                                &format!(
                                    "VELOCITY_CRASH {} · vel {:.2}x→{:.2}x · tpm {:.1}",
                                    addr,
                                    delta.previous.velocity,
                                    delta.current.velocity,
                                    analysis.tx_rate,
                                ),
                                analysis.confidence.total,
                            )?;
                        }
                    }

                    // Drop dead/trap/concentrated tokens from the watchlist so
                    // the shortlist keeps cycling through viable candidates.
                    // Auto-failing a call is a separate, stricter gate: only
                    // on-chain confirmed safety violations (UNSAFE:*) justify
                    // writing a loss to the public ledger automatically.
                    // DEAD/CRASHING/ACTIVE_TRAP may be transient or data-quality
                    // false positives — leave those calls open for manual review.
                    let has_active_call = self.db.has_active_call(addr).unwrap_or(false);
                    if should_remove_from_watchlist(&analysis) {
                        // Don't pull active-call mints off the watchlist. The
                        // settling phase needs ongoing analysis to apply
                        // horizon-aware close rules — once we deactivate, the
                        // token disappears from re-analysis and the call
                        // either drifts forever or sails past a real exit.
                        // The watchlist row is the runtime tracker for live
                        // calls, not just for unknown candidates.
                        if !has_active_call {
                            self.db.deactivate_watchlist(addr)?;
                        }
                        if has_active_call && is_confirmed_unsafe(&analysis) {
                            let exit_price = crate::metadata::fetch(addr)
                                .await
                                .ok()
                                .flatten()
                                .and_then(|m| m.price_usd)
                                .unwrap_or(0.0);
                            let exit_note = format!(
                                "auto-failed: {} · top {:.1}% · conf {}",
                                analysis.confidence.classification,
                                analysis.top_holder_pct,
                                analysis.confidence.total,
                            );
                            let _ = self.db.fail_call(addr, exit_price, &exit_note);
                            tracing::info!(
                                "call auto-failed (unsafe): {} class={} top={:.1}%",
                                addr, analysis.confidence.classification, analysis.top_holder_pct
                            );
                            // Update Telegram delivery if one exists.
                            if let Some(ref n) = self.notifier {
                                let addr_owned = addr.clone();
                                let n = n.clone();
                                let note = exit_note.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = n.update_call_outcome(&addr_owned, "failed", None, &note).await {
                                        tracing::warn!("auto-fail update_call_outcome failed: {}", e);
                                    }
                                });
                            }
                        }
                    }

                    // Notify: lets the notifier edit/demote an existing winner card
                    // when the watchlist re-analysis shows the token has moved.
                    // Gate behind minimum watchlist age to prevent first-sight signals.
                    let now_ts = chrono::Utc::now().timestamp();
                    if now_ts - added_at >= SIGNAL_MIN_WATCHLIST_AGE_SECONDS {
                        self.notify(&analysis).await;
                    }
                }
                Err(e) => {
                    tracing::warn!("Watchlist re-analysis failed for {}: {}", addr, e);
                }
            }
        }

        // Phase 2: Discover new tokens (limit 3 per cycle to leave budget for watchlist)
        let analyses = discovery::discover_new_tokens(&self.db, &self.rpc, 3).await?;

        let mut alert_count = 0;

        for analysis in &analyses {
            let class = &analysis.confidence.classification;

            // UNSAFE tokens (PermanentDelegate / frozen / non-transferable) never
            // generate alerts regardless of scoring — they're vetoed outright.
            if class.starts_with("UNSAFE") {
                continue;
            }

            // Raw-fact alert messages — no narrative suffixes. Every number
            // here is a real measurement; the reader interprets.
            match class.as_str() {
                "CRASHING" => {
                    self.db.insert_alert(
                        "crashing",
                        Some(&analysis.address),
                        &format!(
                            "CRASHING {} · mom {} · top {:.2}% · tpm {:.1}",
                            &analysis.address,
                            analysis.confidence.momentum,
                            analysis.top_holder_pct,
                            analysis.tx_rate
                        ),
                        analysis.confidence.total,
                    )?;
                    alert_count += 1;
                }
                "GRINDER" => {
                    self.db.insert_alert(
                        "grinder",
                        Some(&analysis.address),
                        &format!(
                            "GRINDER {} · top {:.2}% · dist {} · tpm {:.1}",
                            &analysis.address,
                            analysis.top_holder_pct,
                            analysis.confidence.distribution,
                            analysis.tx_rate
                        ),
                        analysis.confidence.total,
                    )?;
                    alert_count += 1;
                }
                "SPRING" => {
                    self.db.insert_alert(
                        "spring",
                        Some(&analysis.address),
                        &format!(
                            "SPRING {} · top {:.2}% · spring {} · mom {}",
                            &analysis.address,
                            analysis.top_holder_pct,
                            analysis.confidence.spring,
                            analysis.confidence.momentum
                        ),
                        analysis.confidence.total,
                    )?;
                    alert_count += 1;
                }
                "STAIRCASE" => {
                    self.db.insert_alert(
                        "staircase",
                        Some(&analysis.address),
                        &format!(
                            "STAIRCASE {} · mom {} · dist {} · top {:.2}%",
                            &analysis.address,
                            analysis.confidence.momentum,
                            analysis.confidence.distribution,
                            analysis.top_holder_pct
                        ),
                        analysis.confidence.total,
                    )?;
                    alert_count += 1;
                }
                "SURGE" => {
                    if analysis.confidence.total >= self.alert_threshold {
                        self.db.insert_alert(
                            "surge",
                            Some(&analysis.address),
                            &format!(
                                "SURGE {} · mom {} · top {:.2}%",
                                &analysis.address,
                                analysis.confidence.momentum,
                                analysis.top_holder_pct
                            ),
                            analysis.confidence.total,
                        )?;
                        alert_count += 1;
                    }
                }
                "ACTIVE_TRAP" => {
                    if analysis.top_holder_pct < 60.0
                        && analysis.holder_count >= 10
                        && analysis.confidence.momentum > 70
                    {
                        self.db.insert_alert(
                            "active_trap",
                            Some(&analysis.address),
                            &format!(
                                "ACTIVE_TRAP {} · mom {} · top {:.2}% · {} holders",
                                &analysis.address,
                                analysis.confidence.momentum,
                                analysis.top_holder_pct,
                                analysis.holder_count
                            ),
                            analysis.confidence.total,
                        )?;
                        alert_count += 1;
                    }
                }
                _ => {
                    if analysis.confidence.classification == "DEVELOPING"
                        && analysis.holder_count >= 10
                        && analysis.top_holder_pct < 50.0
                    {
                        self.db.insert_alert(
                            "developing",
                            Some(&analysis.address),
                            &format!(
                                "DEVELOPING {} · mom {} · top {:.2}% · {} holders",
                                &analysis.address,
                                analysis.confidence.momentum,
                                analysis.top_holder_pct,
                                analysis.holder_count
                            ),
                            analysis.confidence.total,
                        )?;
                        alert_count += 1;
                    }
                }
            }

            // Add only shortlist-worthy tokens to the re-analysis queue.
            // This keeps the watchlist dense with candidates that can earn
            // enough snapshots to confirm or fail quickly.
            // Phase 2 never calls notify() directly — tokens added here will
            // signal from Phase 1 once they have aged past SIGNAL_MIN_WATCHLIST_AGE_SECONDS.
            if should_track_on_watchlist(analysis, self.alert_threshold) {
                let _ = self
                    .db
                    .add_to_watchlist(&analysis.address, &analysis.confidence.classification);
            }
        }

        // Phase 2b: Graduation detection — walk recent pumpswap AMM signatures to
        // catch pump.fun tokens that just bonded-curve-graduated. These are already
        // in our `tokens` table but not on the watchlist. Re-analyze immediately
        // instead of waiting for Phase 4's polling cycle.
        match discovery::check_graduated_tokens(&self.db, &self.rpc, 3).await {
            Ok(graduated) => {
                for analysis in &graduated {
                    if should_track_on_watchlist(analysis, self.alert_threshold) {
                        let _ = self
                            .db
                            .add_to_watchlist(&analysis.address, &analysis.confidence.classification);
                        tracing::info!(
                            "graduation: watchlisted {} ({})",
                            analysis.address,
                            analysis.confidence.classification
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!("check_graduated_tokens failed: {}", e);
            }
        }

        // Phase 3: Smart-wallet tracker — for each user-curated wallet, diff
        // the current SPL holdings against our stored view; every newly-seen
        // mint becomes a `smart_wallet_buy` alert. Imitation alpha: when a
        // proven caller's wallet buys something, we want to know immediately.
        self.scan_smart_wallets().await;

        // Phase 4: Re-ingest. Every cycle, batch-query DexScreener for recently-
        // discovered tokens that failed the initial watchlist gate (too concentrated,
        // too few holders, or no pair yet). One HTTP call for 30 candidates per cycle;
        // full RPC analysis only fires when a token crosses the volume floor.
        self.run_reingest_phase().await;

        // Phase 5: Settling — apply horizon-aware close rules to every active
        // call. This is the lifecycle manager: SHORT calls book wins/losses
        // fast (+50%/+100% withdrew, -40% failed, 6h timeout); LONG calls hold
        // through normal volatility (-70% catastrophic fail only, 30d timeout).
        // Without this, calls drift indefinitely and TIME MACHINE's +126% sat
        // open while mOK's -98% never resolved.
        self.settle_calls().await;

        // Tick the hourly digest — notifier dedups by hour bucket internally,
        // so calling once per cycle is safe and self-pacing.
        if let Some(n) = &self.notifier {
            if let Err(e) = n.tick_digest_now().await {
                tracing::warn!("notifier.tick_digest_now failed: {}", e);
            }
        }

        // Demote stale signal deliveries whose token is no longer tracked
        // and has no active call — keeps /signals count honest.
        match self.db.demote_orphaned_deliveries() {
            Ok(n) if n > 0 => tracing::info!("demoted {} orphaned deliveries", n),
            Ok(_) => {}
            Err(e) => tracing::warn!("demote_orphaned_deliveries failed: {}", e),
        }

        Ok(alert_count)
    }

    /// Phase 5: Settling — apply horizon-aware lifecycle rules to active
    /// calls. Reads `horizon=SHORT` / `horizon=LONG` from the call's note
    /// (defaults to SHORT — auto-calls all use the short profile). Compares
    /// current price to entry, age to horizon window, and either closes the
    /// call cleanly or leaves it active.
    ///
    /// Settle outcomes (writes to DB + edits the channel card):
    ///   SHORT  ≥+100%  → withdrew · "2x done"
    ///   SHORT  ≥+50%   → withdrew · "took the win"
    ///   SHORT  ≤-40%   → failed   · "thesis broke"
    ///   SHORT  age≥6h  → expired  · "no follow-through"
    ///   LONG   ≤-70%   → failed   · "thesis broke"
    ///   LONG   age≥30d → expired  · "30d hold complete"
    ///
    /// Velocity-collapse settle for SHORT is intentionally deferred: it
    /// needs the entry tx_rate persisted on the call row, which it isn't
    /// today. The +50% / -40% / 6h envelope already settles the noise; an
    /// explicit velocity rule is a refinement.
    async fn settle_calls(&self) {
        let active = match self.db.list_calls(true, 200) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("settle: list_calls failed: {}", e);
                return;
            }
        };
        if active.is_empty() {
            return;
        }
        let now = chrono::Utc::now().timestamp();
        for call in active {
            // Horizon parsing via the shared module. Default-on-Unknown
            // is SHORT (the auto-call default; manual operator calls
            // always tag explicitly).
            let is_long = horizon::parse(&call.note).is_long();
            let market = crate::market::get_market(&call.mint).await.ok().flatten();
            let current_price = market.as_ref().map(|m| m.price_usd).unwrap_or(0.0);
            // No reliable price → nothing to settle on. Could happen during
            // a DexScreener outage; skip and try next cycle.
            if call.entry_price_usd <= 0.0 || current_price <= 0.0 {
                continue;
            }
            let pct = (current_price / call.entry_price_usd - 1.0) * 100.0;
            let age = now - call.called_at;

            // Outcome decision: returns (action, status_str, exit_note).
            // Action determines which DB call + which TG outcome string.
            let outcome: Option<(&'static str, String)> = if is_long {
                if pct <= -70.0 {
                    Some(("failed", format!("{:+.1}% · thesis broke", pct)))
                } else if age >= 30 * 86_400 {
                    Some(("expired", format!("{:+.1}% · 30d hold complete", pct)))
                } else {
                    None
                }
            } else if pct >= 100.0 {
                Some(("withdrew", format!("{:+.1}% · 2x done", pct)))
            } else if pct >= 50.0 {
                Some(("withdrew", format!("{:+.1}% · took the win", pct)))
            } else if age <= 30 * 60 && pct <= -25.0 {
                // Fast-fail: SHORT calls that drop ≥25% within the first 30
                // minutes are dead. Memecoins don't recover from a -25% in
                // half an hour — waiting for the regular -40% / 6h envelope
                // just publishes a more catastrophic loss. mOK lost -98%
                // because we waited; this stops at -25% with the runway
                // intact.
                Some(("failed", format!("{:+.1}% · early collapse", pct)))
            } else if pct <= -40.0 {
                Some(("failed", format!("{:+.1}% · thesis broke", pct)))
            } else if age >= 6 * 3600 {
                Some(("expired", format!("{:+.1}% · no follow-through", pct)))
            } else {
                None
            };

            let Some((status, exit_note)) = outcome else {
                continue;
            };

            // Apply DB write per outcome. Each helper is idempotent on the
            // (mint, status='active') unique partial index — safe under any
            // double-fire race with another scan cycle.
            let db_ok = match status {
                "withdrew" => self.db.close_call(&call.mint, current_price, &exit_note),
                "failed" => self.db.fail_call(&call.mint, current_price, &exit_note),
                "expired" => self.db.expire_call(&call.mint, current_price, &exit_note),
                _ => Ok(false),
            };
            match db_ok {
                Ok(true) => tracing::info!(
                    "settle: {} {} ({}={:+.1}%, age={}m, horizon={})",
                    status,
                    call.symbol,
                    if is_long { "long" } else { "short" },
                    pct,
                    age / 60,
                    if is_long { "LONG" } else { "SHORT" }
                ),
                Ok(false) => continue,
                Err(e) => {
                    tracing::warn!("settle: {} {} failed: {}", status, call.symbol, e);
                    continue;
                }
            }

            // Flip the TG channel card to the terminal state. update_call_outcome
            // handles all four canonical outcomes (active/withdrew/failed/expired);
            // we pass the same status verbatim so the card header matches the DB.
            // Same task also DMs the operator(s) a one-line notification via
            // Claudeinatorbot — channel is for the audience, DM is for the human
            // running the system.
            if let Some(ref n) = self.notifier {
                let mint = call.mint.clone();
                let symbol = call.symbol.clone();
                let n = n.clone();
                let note = exit_note.clone();
                let status_owned = status.to_string();
                let exit_pct = Some(pct);
                tokio::spawn(async move {
                    if let Err(e) = n
                        .update_call_outcome(&mint, &status_owned, exit_pct, &note)
                        .await
                    {
                        tracing::warn!("settle: update_call_outcome failed for {}: {}", mint, e);
                    }
                    let icon = match status_owned.as_str() {
                        "withdrew" => "🟢",
                        "failed" => "🔴",
                        "expired" => "⏰",
                        _ => "·",
                    };
                    let label = match status_owned.as_str() {
                        "withdrew" => "banked",
                        "failed" => "failed",
                        "expired" => "expired",
                        _ => status_owned.as_str(),
                    };
                    let sym_for_dm = if symbol.is_empty() {
                        format!("{}…{}", &mint[..mint.len().min(4)], &mint[mint.len().saturating_sub(4)..])
                    } else {
                        format!("${}", symbol)
                    };
                    let dm = format!("{} <b>{}</b> {} {}", icon, sym_for_dm, label, note);
                    n.dm_admins(&dm).await;
                });
            }
        }
    }

    /// Phase 4: Re-evaluate tokens that were discovered but not watchlisted.
    /// Pump.fun tokens often launch with concentrated ownership and no DexScreener
    /// pair; they mature within 30-120 minutes and become legitimate signals.
    /// We check recently-discovered, non-watchlisted tokens via a DexScreener batch
    /// call and fully re-analyze any showing meaningful volume.
    async fn run_reingest_phase(&self) {
        // Candidates: discovered 30min–48h ago, no active watchlist entry,
        // no snapshot in the last 30 minutes (prevents repeated hammering).
        let candidates = match self
            .db
            .list_stale_discovered_candidates(30 * 60, 6 * 3600, 30 * 60, 30)
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("reingest: list_stale_discovered_candidates failed: {}", e);
                return;
            }
        };
        tracing::debug!("reingest: {} candidates queued for DexScreener check", candidates.len());
        if candidates.is_empty() {
            return;
        }

        // Batch DexScreener lookup — one HTTP call for up to 40 mints.
        let mint_refs: Vec<&str> = candidates.iter().map(|s| s.as_str()).collect();
        let markets = match crate::market::get_market_batch(&mint_refs).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("reingest: DexScreener batch failed: {}", e);
                return;
            }
        };
        tracing::debug!("reingest: DexScreener returned {} pairs for {} candidates", markets.len(), candidates.len());

        // Rank by h1 volume descending; only proceed with the top N that
        // show meaningful market activity.
        const REINGEST_VOLUME_FLOOR: f64 = 5_000.0;
        const REINGEST_MAX_FULL_ANALYSES: usize = 3;

        let mut ranked: Vec<(&str, f64)> = candidates
            .iter()
            .filter_map(|mint| {
                let m = markets.get(mint.as_str())?;
                let vol = m.volume_h1_usd;
                if vol >= REINGEST_VOLUME_FLOOR {
                    Some((mint.as_str(), vol))
                } else {
                    None
                }
            })
            .collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(REINGEST_MAX_FULL_ANALYSES);

        if ranked.is_empty() {
            return;
        }

        tracing::info!(
            "reingest: {} candidates, {} above volume floor, re-analyzing",
            candidates.len(),
            ranked.len()
        );

        for (mint, vol) in ranked {
            match signals::analyze_token(&self.rpc, mint, Some(&self.db), None).await {
                Ok(analysis) => {
                    // Always write a snapshot so the recent_snapshot_cutoff
                    // prevents us from re-checking the same token next cycle.
                    let _ = self.db.insert_token(mint, analysis.confidence.total);
                    if should_track_on_watchlist(&analysis, self.alert_threshold) {
                        let _ = self.db.add_to_watchlist(
                            mint,
                            &analysis.confidence.classification,
                        );
                        tracing::info!(
                            "reingest: added {} to watchlist · class={} conf={} holders={} top={:.1}% vol_h1=${:.0}",
                            mint,
                            analysis.confidence.classification,
                            analysis.confidence.total,
                            analysis.holder_count,
                            analysis.top_holder_pct,
                            vol,
                        );
                    } else {
                        tracing::debug!(
                            "reingest: {} still below threshold · class={} conf={} holders={} top={:.1}% vol_h1=${:.0}",
                            mint,
                            analysis.confidence.classification,
                            analysis.confidence.total,
                            analysis.holder_count,
                            analysis.top_holder_pct,
                            vol,
                        );
                    }
                }
                Err(e) => {
                    tracing::debug!("reingest: analyze_token {} failed: {}", mint, e);
                }
            }
        }
    }

    async fn scan_smart_wallets(&self) {
        let wallets = match self.db.list_active_smart_wallets() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("list_active_smart_wallets failed: {}", e);
                return;
            }
        };
        if wallets.is_empty() {
            return;
        }
        for (wallet, label) in wallets {
            let holdings = match self.rpc.get_wallet_token_holdings(&wallet).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::debug!("smart-wallet {} fetch failed: {}", wallet, e);
                    continue;
                }
            };
            let known: std::collections::HashSet<String> = self
                .db
                .get_smart_wallet_mints(&wallet)
                .unwrap_or_default()
                .into_iter()
                .collect();
            for (mint, balance) in &holdings {
                if !known.contains(mint) {
                    // Brand new holding — alert before recording so we don't
                    // miss it on a crashed upsert.
                    let label_tag = if label.is_empty() {
                        String::new()
                    } else {
                        format!(" [{}]", label)
                    };
                    let _ = self.db.insert_alert(
                        "smart_wallet_buy",
                        Some(mint),
                        &format!(
                            "SMART_WALLET_BUY {} bought {} (bal {:.2}){}",
                            wallet, mint, balance, label_tag,
                        ),
                        80,
                    );
                }
                let _ = self.db.upsert_smart_wallet_holding(&wallet, mint, *balance);
            }
            let _ = self.db.touch_smart_wallet(&wallet);
        }
    }

    fn prepare_watchlist_batch(&self) -> anyhow::Result<Vec<(String, i64)>> {
        let now = chrono::Utc::now().timestamp();
        let mut candidates = self.db.list_active_watchlist_candidates()?;
        let mut to_deactivate: Vec<String> = Vec::new();

        candidates.retain(|candidate| {
            if should_evict_watchlist_candidate(candidate, now) {
                to_deactivate.push(candidate.token_address.clone());
                false
            } else {
                true
            }
        });

        candidates.sort_by(|a, b| {
            let score_a = watchlist_priority(a, now);
            let score_b = watchlist_priority(b, now);
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.last_checked.cmp(&b.last_checked))
                .then_with(|| a.added_at.cmp(&b.added_at))
        });

        if candidates.len() > self.max_active_tokens {
            for candidate in candidates.iter().skip(self.max_active_tokens) {
                to_deactivate.push(candidate.token_address.clone());
            }
            candidates.truncate(self.max_active_tokens);
        }

        if !to_deactivate.is_empty() {
            to_deactivate.sort();
            to_deactivate.dedup();
            let removed = self.db.deactivate_watchlist_many(&to_deactivate)?;
            if removed > 0 {
                tracing::info!(
                    "watchlist rebalanced: active={} removed={} batch={}",
                    candidates.len(),
                    removed,
                    self.watchlist_rescan_limit,
                );
            }
        }

        let cutoff = now - WATCHLIST_RECHECK_SECONDS;
        let due = candidates
            .into_iter()
            .filter(|candidate| candidate.last_checked < cutoff)
            .take(self.watchlist_rescan_limit)
            .map(|candidate| (candidate.token_address, candidate.added_at))
            .collect();
        Ok(due)
    }
}

fn compute_watchlist_rescan_limit(max_active_tokens: usize, interval_seconds: u64) -> usize {
    let capped_tokens = max_active_tokens.max(1) as u64;
    let target_revisit = WATCHLIST_TARGET_REVISIT_SECONDS.max(interval_seconds);
    let per_cycle = capped_tokens
        .saturating_mul(interval_seconds)
        .div_ceil(target_revisit);
    per_cycle
        .max(WATCHLIST_MIN_RESCAN_BATCH as u64)
        .min(capped_tokens) as usize
}

fn effective_watchlist_class(candidate: &crate::db::WatchlistCandidate) -> &str {
    candidate
        .snapshot_classification
        .as_deref()
        .unwrap_or(&candidate.watch_classification)
}

fn has_favorable_watchlist_profile(candidate: &crate::db::WatchlistCandidate) -> bool {
    let class = effective_watchlist_class(candidate);
    let confidence = candidate.snapshot_confidence.unwrap_or(0);
    let holders = candidate.snapshot_holder_count.unwrap_or(0);
    let top_holder_pct = candidate.snapshot_top_holder_pct.unwrap_or(100.0);
    let momentum = candidate.snapshot_momentum.unwrap_or(0);
    let distribution = candidate.snapshot_distribution.unwrap_or(0);

    matches!(class, "STAIRCASE" | "GRINDER" | "SPRING")
        || (class == "SURGE" && confidence >= 80 && holders >= 30 && top_holder_pct <= 22.0)
        || (class == "DEVELOPING"
            && confidence >= 70
            && holders >= 30
            && top_holder_pct <= 25.0
            && momentum >= 50
            && distribution >= 60)
}

fn should_evict_watchlist_candidate(candidate: &crate::db::WatchlistCandidate, now: i64) -> bool {
    let class = effective_watchlist_class(candidate);
    let holders = candidate.snapshot_holder_count.unwrap_or(0);
    let top_holder_pct = candidate.snapshot_top_holder_pct.unwrap_or(100.0);

    if matches!(class, "DEAD" | "CRASHING" | "ACTIVE_TRAP") || class.starts_with("UNSAFE") {
        return true;
    }
    if holders > 0 && holders < 25 && top_holder_pct >= 33.0 {
        return true;
    }
    now - candidate.added_at > WATCHLIST_STALE_SECONDS
        && !has_favorable_watchlist_profile(candidate)
}

fn watchlist_priority(candidate: &crate::db::WatchlistCandidate, now: i64) -> f64 {
    let class = effective_watchlist_class(candidate);
    let confidence = candidate.snapshot_confidence.unwrap_or(0) as f64;
    let momentum = candidate.snapshot_momentum.unwrap_or(0) as f64;
    let distribution = candidate.snapshot_distribution.unwrap_or(0) as f64;
    let holders = candidate.snapshot_holder_count.unwrap_or(0);
    let top_holder_pct = candidate.snapshot_top_holder_pct.unwrap_or(100.0);
    let snapshot_age_seconds = candidate
        .snapshot_timestamp
        .map(|ts| (now - ts).max(0))
        .unwrap_or(WATCHLIST_STALE_SECONDS * 2);

    let class_base = match class {
        "STAIRCASE" => 120.0,
        "GRINDER" => 110.0,
        "SPRING" => 100.0,
        "SURGE" => 70.0,
        "DEVELOPING" => 45.0,
        "ACTIVE_TRAP" => 5.0,
        "CRASHING" => -30.0,
        "DEAD" => -60.0,
        c if c.starts_with("UNSAFE") => -80.0,
        _ => 0.0,
    };
    let holder_bonus = if holders >= 50 {
        20.0
    } else if holders >= 30 {
        12.0
    } else if holders >= 20 {
        6.0
    } else {
        -12.0
    };
    let concentration_penalty = if top_holder_pct >= 40.0 {
        45.0
    } else if top_holder_pct >= 35.0 {
        30.0
    } else if top_holder_pct >= 30.0 {
        18.0
    } else if top_holder_pct >= 25.0 {
        8.0
    } else {
        0.0
    };
    let freshness_bonus = if snapshot_age_seconds <= 10 * 60 {
        10.0
    } else if snapshot_age_seconds <= 30 * 60 {
        5.0
    } else {
        0.0
    };
    let stale_penalty = if snapshot_age_seconds > 30 * 60 {
        ((snapshot_age_seconds - 30 * 60) / 300).min(20) as f64
    } else {
        0.0
    };
    let aged_without_step_penalty = if now - candidate.added_at > WATCHLIST_STALE_SECONDS
        && !has_favorable_watchlist_profile(candidate)
    {
        30.0
    } else {
        0.0
    };

    class_base + confidence + momentum / 3.0 + distribution / 4.0 + holder_bonus + freshness_bonus
        - concentration_penalty
        - stale_penalty
        - aged_without_step_penalty
}

fn should_track_on_watchlist(analysis: &TokenAnalysis, alert_threshold: i32) -> bool {
    let class = analysis.confidence.classification.as_str();
    // Hard floors that apply to everything: never observe a confirmed
    // honeypot/freeze/permanent-delegate token.
    if class.starts_with("UNSAFE") {
        return false;
    }

    // Strong-signal tokens: standard post-graduation thresholds. These are
    // tokens trading on a real DEX with diverse holder bases — the gate is
    // tuned for "candidate worth re-analyzing every 15s".
    let strong = match class {
        "STAIRCASE" | "GRINDER" | "SPRING" => {
            analysis.holder_count >= 20 && analysis.top_holder_pct <= 30.0
        }
        "SURGE" => {
            analysis.confidence.total >= alert_threshold
                && analysis.holder_count >= 30
                && analysis.top_holder_pct <= 22.0
        }
        "DEVELOPING" => {
            analysis.confidence.total >= 65
                && analysis.confidence.distribution >= 60
                && analysis.confidence.momentum >= 50
                && analysis.holder_count >= 30
                && analysis.top_holder_pct <= 25.0
        }
        _ => false,
    };
    if strong {
        return true;
    }

    // Observation track: pump.fun launches start with the bonding curve
    // account holding ~100% of supply, so the strong-signal gate above
    // rejects every fresh launch by design. We watch them anyway when
    // they show real activity (momentum + multiple holders), so we
    // accumulate the snapshots needed for `should_signal` to ever fire.
    // No alerts come out of this — the watchlist is observational; the
    // call gate (signal_threshold + classification + delta history) is
    // unchanged downstream.
    let observable = matches!(
        class,
        "DEVELOPING" | "GRINDER" | "STAIRCASE" | "SPRING" | "SURGE"
    );
    observable
        && analysis.confidence.momentum >= 55
        && analysis.holder_count >= 10
}

fn should_remove_from_watchlist(analysis: &TokenAnalysis) -> bool {
    let class = analysis.confidence.classification.as_str();
    // DEAD requires a confirmed liquidity_depth signal — that signal is only
    // added when DexScreener returns liquidity_usd > 0. Missing market data
    // (fetch failure returning 0 or no response) must not trigger removal.
    let has_market_signal = analysis
        .scores
        .iter()
        .any(|s| s.signal_type == "liquidity_depth");
    (class == "DEAD" && has_market_signal)
        || matches!(class, "CRASHING" | "ACTIVE_TRAP")
        || class.starts_with("UNSAFE")
        || (analysis.holder_count < 25 && analysis.top_holder_pct >= 33.0)
}

// Only auto-close a public call for on-chain confirmed safety violations.
// Classification-based signals (DEAD, CRASHING, ACTIVE_TRAP) can be transient
// or driven by stale market data — writing a public loss on that alone risks
// reputation damage from false positives.
fn is_confirmed_unsafe(analysis: &TokenAnalysis) -> bool {
    analysis.confidence.classification.starts_with("UNSAFE")
}

pub struct ScannerHandle {
    running: Arc<AtomicBool>,
}

impl ScannerHandle {
    pub fn stop(&self) {
        self.running.store(false, AtomicOrdering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(AtomicOrdering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::WatchlistCandidate;

    #[test]
    fn test_compute_watchlist_rescan_limit_targets_multi_snapshot_flow() {
        assert_eq!(compute_watchlist_rescan_limit(200, 15), 10);
        assert_eq!(compute_watchlist_rescan_limit(50, 15), 6);
    }

    #[test]
    fn test_watchlist_priority_prefers_mature_staircase() {
        let now = 1_000_000;
        let strong = WatchlistCandidate {
            token_address: "strong".into(),
            watch_classification: "STAIRCASE".into(),
            added_at: now - 600,
            last_checked: 0,
            snapshot_classification: Some("STAIRCASE".into()),
            snapshot_confidence: Some(78),
            snapshot_holder_count: Some(42),
            snapshot_top_holder_pct: Some(18.0),
            snapshot_momentum: Some(82),
            snapshot_distribution: Some(72),
            snapshot_timestamp: Some(now - 120),
        };
        let weak = WatchlistCandidate {
            token_address: "weak".into(),
            watch_classification: "DEVELOPING".into(),
            added_at: now - 600,
            last_checked: 0,
            snapshot_classification: Some("DEVELOPING".into()),
            snapshot_confidence: Some(59),
            snapshot_holder_count: Some(20),
            snapshot_top_holder_pct: Some(37.0),
            snapshot_momentum: Some(54),
            snapshot_distribution: Some(48),
            snapshot_timestamp: Some(now - 120),
        };

        assert!(watchlist_priority(&strong, now) > watchlist_priority(&weak, now));
        assert!(should_evict_watchlist_candidate(&weak, now));
    }
}
