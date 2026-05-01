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
#[derive(Clone)]
pub struct BackgroundScanner {
    db: Arc<Db>,
    rpc: Arc<RpcRouter>,
    interval: Duration,
    alert_threshold: i32,
    max_active_tokens: usize,
    watchlist_rescan_limit: usize,
    running: Arc<AtomicBool>,
    notifier: Option<Arc<Notifier>>,
    /// PumpPortal health handle. When set + fresh, Phase 2 / 2b skip
    /// their RPC sig-walks (PumpPortal pushes new tokens + migrations
    /// straight to the DB). When stale, the sig-walks resume as
    /// fallback. Connectivity is the gate; no separate flag.
    pumpportal_health: Option<Arc<crate::pumpportal::PumpPortalHealth>>,
    /// Optional execution context. Some(_) only when PHOTON_PRIVATE_KEY is
    /// loaded AND [execution].enabled=true. None = settle is paper-only.
    /// Sells fire BEFORE the call row's status flips (`withdrew`/`failed`/
    /// `expired`) so the on-chain action precedes the public verdict.
    executor: Option<Arc<crate::execution::ExecutionCtx>>,
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
            pumpportal_health: None,
            executor: None,
        }
    }

    /// Attach trade-execution capability. Sells will fire on settle-decided
    /// outcomes when this is set AND ctx.cfg.enabled is true. Idempotent
    /// per-call via DB single-flight; safe under restart.
    pub fn with_executor(mut self, ctx: Arc<crate::execution::ExecutionCtx>) -> Self {
        self.executor = Some(ctx);
        self
    }

    pub fn with_pumpportal_health(
        mut self,
        health: Arc<crate::pumpportal::PumpPortalHealth>,
    ) -> Self {
        self.pumpportal_health = Some(health);
        self
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
            // Pacing — Telegram channel-edit rate limit is the bottleneck
            // (~1 edit/sec/channel; bursts trigger 429 with 30s+ retry_after).
            // 2s/card = ~30 cards/min, comfortably under any per-channel
            // budget. The backfill only runs once at startup so total
            // wall-time isn't critical.
            tokio::time::sleep(Duration::from_millis(2000)).await;
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

        // One-time startup cleanup + card backfill — spawned as a sibling
        // task so the main scan loop can begin immediately. Backfill walks
        // up to 200 closed calls editing TG cards at 2s/card pacing (~6+
        // minutes) — used to block the scan loop on every restart, blanking
        // the live ledger for 6 min just to re-render history that's already
        // correct. Now both run concurrently with scanning.
        {
            let me = self.clone();
            tokio::spawn(async move {
                me.cleanup_orphaned_calls().await;
                me.backfill_terminal_deliveries().await;
            });
        }

        // Hourly retention GC. Without this the DB grows unbounded between
        // container restarts; the init-time GC alone left curve_snapshots
        // accumulating to 50k+ rows.
        {
            let db = self.db.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(3600));
                tick.tick().await; // skip the immediate first fire
                loop {
                    tick.tick().await;
                    match db.run_periodic_gc() {
                        Ok(n) if n > 0 => tracing::info!("gc: pruned {} rows", n),
                        Ok(_) => {}
                        Err(e) => tracing::warn!("gc: failed: {}", e),
                    }
                }
            });
        }

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
                    // Update last_checked even on failure so a persistently-
                    // failing token (e.g. all RPCs returning errors) doesn't
                    // block the queue. Without this, the same token gets
                    // retried every cycle, starving newer items in the
                    // backlog.
                    let _ = self
                        .db
                        .update_watchlist_last_checked_only(addr);
                }
            }
        }

        // Phase 2: Discover new tokens. PumpPortal pushes new-token events
        // straight to the DB via the sink task — when the WS is fresh
        // we skip this RPC sig-walk entirely. Falls back to sig-walking
        // when PumpPortal is stale (connection lost, server outage, or
        // first 30s after startup).
        const PP_FRESH_SECS: i64 = 30;
        let pp_fresh = self
            .pumpportal_health
            .as_ref()
            .map(|h| h.fresh(PP_FRESH_SECS))
            .unwrap_or(false);
        let analyses: Vec<TokenAnalysis> = if pp_fresh {
            // PumpPortal is feeding tokens directly. Phase 2's "alert
            // generation per analysis" loop below still runs, just over
            // an empty list — alerts come from Phase 1 re-analysis.
            tracing::trace!("Phase 2: PumpPortal fresh, skipping RPC sig-walk");
            Vec::new()
        } else {
            discovery::discover_new_tokens(&self.db, &self.rpc, 3).await?
        };

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

        // Phase 2b: Graduation detection. PumpPortal's subscribeMigration
        // confirmed working (see docs/pumpportal_migration_shape.md) —
        // events arrive within seconds of the on-chain migration tx,
        // and the sink already adds graduated mints to the watchlist
        // directly. Skip the RPC sig-walk when the WS is fresh; fall
        // back to walking when the WS is stale (disconnected or the
        // server stops pushing).
        if !pp_fresh {
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
        } else {
            tracing::trace!("Phase 2b: PumpPortal fresh, skipping pumpswap sig-walk");
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
        // with a -50% catastrophic stop (tightened from -70 after backtest).
        // Both horizons also exit on dev_selling alerts and STAIRCASE→GRINDER
        // class regression — leading indicators that price hasn't yet printed.
        self.settle_calls().await;

        // Phase 6: Bonding-curve observation. For pump.fun mints in their
        // first hour of life (pre-graduation), poll the curve PDA, persist
        // virtual/real reserves to curve_snapshots. Cheap: one batched
        // getMultipleAccounts call covers up to 50 curves per cycle. Drives
        // pre-graduation calling that DexScreener can't see (the 0→$1M
        // ride). Drops out automatically once curve.complete=true.
        self.observe_bonding_curves().await;

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
    ///   ANY    dev_selling alert in last 30m → failed · "dev selling"
    ///   ANY    classification regression on a red call → failed · "structure broke"
    ///   SHORT  ≥+100%  → withdrew · "2x done"
    ///   SHORT  ≥+50%   → withdrew · "took the win"
    ///   SHORT  ≤-25% in 30m → failed · "early collapse"
    ///   SHORT  ≤-40%   → failed   · "thesis broke"
    ///   SHORT  age≥6h  → expired  · "no follow-through"
    ///   LONG   ≥+150%  → withdrew · "2.5x done"
    ///   LONG   ≤-50%   → failed   · "thesis broke"
    ///   LONG   age≥30d → expired  · "30d hold complete"
    ///
    /// Velocity-collapse settle for SHORT is intentionally deferred: it
    /// needs the entry tx_rate persisted on the call row, which it isn't
    /// today. The +50% / -40% / 6h envelope already settles the noise; an
    /// explicit velocity rule is a refinement.
    /// Phase 6: poll bonding-curve PDAs for newly-discovered pump.fun
    /// tokens. Persists each observation to `curve_snapshots`. The
    /// candidate query already excludes graduated mints (curve.complete=1),
    /// so this naturally drops the curve once it bonds out and the
    /// post-grad pipeline (Phase 1+2b) takes over. Batch-fetches up to
    /// 50 curves per cycle via getMultipleAccounts — bounded RPC cost.
    async fn observe_bonding_curves(&self) {
        // Token must be young enough to plausibly still be on the curve.
        // 90min is generous; most pump.fun runs graduate within ~30min
        // when they're going to graduate at all.
        const MAX_AGE_SECS: i64 = 90 * 60;
        const PER_CYCLE_LIMIT: usize = 50;
        let candidates = match self
            .db
            .list_curve_observation_candidates(MAX_AGE_SECS, PER_CYCLE_LIMIT)
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("curve-observe: list candidates failed: {}", e);
                return;
            }
        };
        if candidates.is_empty() {
            return;
        }
        let states = match crate::bonding_curve::fetch_curves_batch(&candidates, &self.rpc).await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("curve-observe: batch fetch failed: {}", e);
                return;
            }
        };
        let now = chrono::Utc::now().timestamp();
        let mut written = 0usize;
        let mut graduated = 0usize;
        for (mint, state_opt) in candidates.iter().zip(states.iter()) {
            let Some(state) = state_opt else { continue };
            if state.complete {
                graduated += 1;
            }
            if let Err(e) = self.db.insert_curve_snapshot(
                mint,
                now,
                state.virtual_sol_reserves,
                state.virtual_token_reserves,
                state.real_sol_reserves,
                state.real_token_reserves,
                state.price_sol(),
                state.fill_pct(),
                state.complete,
            ) {
                tracing::warn!("curve-observe: insert {} failed: {}", mint, e);
                continue;
            }
            written += 1;

            // 6.4: curve-stage auto-call DISABLED.
            //
            // The original design fired calls during the bonding-curve
            // phase using `virtual_sol_reserves * total_supply * sol_price`
            // as entry mcap. That number doesn't compare meaningfully to
            // post-graduation DexScreener prices — the AMM that gets
            // created at graduation has only ~$12k of injected liquidity,
            // so the realized AMM price is typically an order of magnitude
            // below the virtual curve price. Result: every curve call was
            // guaranteed to settle ~-90% the moment DexScreener got a
            // post-grad pair price, regardless of what the token was
            // actually doing on chain.
            //
            // Symptom in production: 19 CURVE-source calls fired and
            // were "early-collapsed" within 1-3 minutes each, all in the
            // -85% to -99% band. That's the math, not the market.
            //
            // The right call-trigger is post-graduation, against real
            // DexScreener pricing — the existing watchlist + settling
            // path. We still OBSERVE curves (snapshots persist for
            // analysis + future research), and we still hand off on
            // graduation so the post-grad pipeline picks up the mint.
            if state.complete {
                self.handle_graduation(mint).await;
            }
            // Curve-stage auto-call removed (commit 919b1c1+ trail). The
            // virtual-reserves entry-mcap math vs post-grad AMM price
            // produced systematic -90% phantom losses. Observation kept;
            // calling resumes post-grad via Phase 1's DexScreener path.
        }
        // Log only when something graduated. The "X snapshots, 0 graduated"
        // line every 15s was 4k+ lines/hour of pure noise — worse, it
        // crowded out actually-actionable scanner logs.
        if graduated > 0 {
            tracing::info!(
                "curve-observe: {} snapshots ({} graduated this batch)",
                written, graduated
            );
        }
    }

    /// Graduation handoff. The token just flipped complete=true on its
    /// bonding curve. If we have an active call from the curve phase,
    /// add the mint to the watchlist so Phase 1's re-analysis loop
    /// (DexScreener-driven) takes over the lifecycle. The call row
    /// stays continuous; only the data source changes from on-chain
    /// curve reads to off-chain DEX feed.
    async fn handle_graduation(&self, mint: &str) {
        if !self.db.has_active_call(mint).unwrap_or(false) {
            return;
        }
        // Best-effort watchlist add. Existing schema uses
        // (token_address, classification, added_at, last_checked, active).
        // Use STAIRCASE as a placeholder class — Phase 1 re-analysis
        // overwrites with real classification on its first read.
        let _ = self.db.add_to_watchlist(mint, "STAIRCASE");
        tracing::info!("curve-grad: {} graduated, handed off to watchlist", mint);
    }

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
        // Batch DexScreener fetch — one HTTP request for all active mints
        // instead of N serial per-call requests. Saves N-1 round-trips
        // every 15s; at 5 active calls that's 4 fewer HTTP calls per
        // settle cycle. DexScreener supports comma-joined mints natively.
        let active_mints: Vec<&str> = active.iter().map(|c| c.mint.as_str()).collect();
        let market_cache = crate::market::get_market_batch(&active_mints)
            .await
            .unwrap_or_default();
        let now = chrono::Utc::now().timestamp();
        for call in active {
            // Horizon parsing via the shared module. Default-on-Unknown
            // is SHORT (the auto-call default; manual operator calls
            // always tag explicitly).
            let h = horizon::parse(&call.note);
            let is_long = h.is_long();
            let is_scalp = h.is_scalp();
            let is_moonshot = h.is_moonshot();
            // Try batch cache first; fall back to per-token fetch when
            // DexScreener doesn't return the mint (unindexed fresh-grad
            // edge case). Maintains correctness while keeping the fast path.
            let market = match market_cache.get(&call.mint) {
                Some(m) => Some(m.clone()),
                None => crate::market::get_market(&call.mint).await.ok().flatten(),
            };
            let current_price = market.as_ref().map(|m| m.price_usd).unwrap_or(0.0);
            // No reliable price → nothing to settle on. Could happen during
            // a DexScreener outage; skip and try next cycle.
            if call.entry_price_usd <= 0.0 || current_price <= 0.0 {
                continue;
            }
            let pct = (current_price / call.entry_price_usd - 1.0) * 100.0;
            let age = now - call.called_at;

            // Take-profit pct: use the higher of (market price, latest on-chain
            // snapshot price) — DexScreener feeds lag during fast spikes by
            // 30-90s, but analyze_token writes on-chain price into snapshots
            // every cycle. HSBC peaked +31.3% on-chain but DexScreener never
            // showed it before the dump → "scalp +30 done" never fired and we
            // ate -79%. Stop-loss still uses market pct (be conservative on
            // stops, liberal on takes).
            let take_pct = self
                .db
                .get_latest_snapshot(&call.mint)
                .ok()
                .flatten()
                .and_then(|s| {
                    if s.price_usd > 0.0 && call.entry_price_usd > 0.0 {
                        Some((s.price_usd / call.entry_price_usd - 1.0) * 100.0)
                    } else {
                        None
                    }
                })
                .map(|snap_pct| pct.max(snap_pct))
                .unwrap_or(pct);

            // Event-driven exits — final calibration after live observation
            // 2026-04-29:
            //   - dev_selling base rate is +10.5% / 30min (NOT bearish)
            //   - class_regression (soft, e.g. STAIRCASE→GRINDER) is +7.4%
            //     (NOT bearish either)
            //   - BUT chadhouse (call 52, -45% loss) went STAIRCASE→ACTIVE_TRAP
            //     in 11min, a structural collapse not captured by soft-
            //     regression analysis. Terminal classifications (ACTIVE_TRAP,
            //     CRASHING, DEAD, UNSAFE_*) are categorically different.
            //
            // Three-trigger event exit: severe dev exit (alert conf >= 90,
            // deployer drop >= 40%), terminal classification, OR multiple
            // adverse signals firing within the same 30min window.
            let event_window_secs = 30 * 60i64;
            let event_since = now - event_window_secs;
            let severe_dev_selling = self
                .db
                .has_recent_severe_alert(&call.mint, "dev_selling", event_since, 90)
                .unwrap_or(false);
            let terminal_class = self
                .db
                .get_latest_snapshot(&call.mint)
                .ok()
                .flatten()
                .map(|s| {
                    let c = s.classification.as_str();
                    c == "ACTIVE_TRAP"
                        || c == "CRASHING"
                        || c == "DEAD"
                        || c.starts_with("UNSAFE")
                })
                .unwrap_or(false);

            // Event-exit gating is horizon-aware. Goblin (LONG, called
            // 2026-05-01) closed at -3.6% in 17min only because classification
            // flipped — peak +0.5%, trough -5.4%. LONG horizon is supposed to
            // be patient; firing on classification flip alone defeats it.
            // Wish (LONG) banked +49.8% via "structural collapse" — right
            // outcome for the wrong reason; could have been -50% just as
            // easily. MOONSHOT same patience — DEV→STAIRCASE flip is GOOD
            // (the moonshot maturing); only UNSAFE/freeze/permdelegate +
            // confirming price drop is fatal.
            let event_exit: Option<(&'static str, String)> = if is_long || is_moonshot {
                // LONG + MOONSHOT: only severe dev exit (rug-detect) and
                // UNSAFE-class flips with confirming price drop ≥-20% can
                // close. Lets the take/stop ladder own normal volatility.
                if severe_dev_selling && pct <= -20.0 {
                    Some(("failed", format!("{:+.1}% · severe dev exit", pct)))
                } else if terminal_class && pct <= -20.0 {
                    Some(("failed", format!("{:+.1}% · structural collapse", pct)))
                } else {
                    None
                }
            } else if severe_dev_selling {
                Some(("failed", format!("{:+.1}% · severe dev exit", pct)))
            } else if terminal_class {
                if is_scalp {
                    Some(("failed", format!("{:+.1}% · structural collapse", pct)))
                } else if pct <= -10.0 {
                    Some(("failed", format!("{:+.1}% · structural collapse", pct)))
                } else {
                    None
                }
            } else {
                None
            };

            // Universal trailing-stop ladder. The single biggest EV leak in
            // the prior settle was holding peaked positions through their
            // dump back through a fixed stop. $Pets peaked +251% → exited
            // -21%. $S&L peaked +169% → exited -62%. $Fartbuckle peaked
            // +73% → exited -67%. The fix: once a position has run, the
            // stop ratchets up so it can never lose more than what was
            // banked. "Enter for free, ride the profit."
            //
            // Tiers (peak observed since entry → stop floor):
            //   peak ≥ +400% → floor +200%   (lock 4x of the 5x)
            //   peak ≥ +200% → floor +100%
            //   peak ≥ +100% → floor +50%
            //   peak ≥  +50% → floor +25%
            //   peak ≥  +20% → floor   0%   (breakeven — entered for free)
            //   else        → floor = default_stop_for_horizon
            //
            // The take ladder still owns the upside (e.g. moonshot's +250%
            // take fires before the +200% trail tier). The trail floor
            // only kicks when the position has peaked but is now retracing.
            // Time-expire is gated to peak<+20% so we never time-out a
            // winning position arbitrarily — it rides until the trail
            // catches it. peak comes from snapshots OR current take_pct
            // (whichever is larger) — the on-chain snapshot path captures
            // momentary spikes that DexScreener missed.
            let default_stop_for_horizon = if is_moonshot { -25.0 }
                else if is_scalp { -30.0 }
                else if is_long { -50.0 }
                else { -40.0 };
            let snapshot_peak = self
                .db
                .get_peak_pct_since(&call.mint, call.called_at, call.entry_price_usd)
                .unwrap_or(0.0);
            let peak_observed = snapshot_peak.max(take_pct).max(pct);
            let trail_floor = trailing_stop_floor(peak_observed, default_stop_for_horizon);
            // Trail-stop trigger. When floor ≥ 0 we're locking a profit
            // and the verdict is "withdrew" (a win, not a failure). When
            // floor < 0 we're using the bucket's default_stop and the
            // verdict is the bucket-specific failure label.
            let trail_exit: Option<(&'static str, String)> = if pct <= trail_floor {
                if trail_floor >= 0.0 {
                    Some((
                        "withdrew",
                        format!("{:+.1}% · trailing stop @ +{:.0}% (peak {:+.0}%)", pct, trail_floor, peak_observed),
                    ))
                } else {
                    None // let the per-horizon block produce its own label
                }
            } else {
                None
            };

            // Outcome decision: returns (action, status_str, exit_note).
            // Action determines which DB call + which TG outcome string.
            let outcome: Option<(&'static str, String)> = if let Some(e) = event_exit {
                Some(e)
            } else if let Some(e) = trail_exit {
                Some(e)
            } else if is_scalp {
                // SCALP bucket exit ladder. Recalibrated 2026-04-29 against
                // the 15-call cohort screenshot:
                //   - +60 take never fired (MINIBELKA peaked +85 but settle
                //     captured at +51.9 → +30 rule fired). Lowered to +50 so
                //     SIR-style flash peaks (peak +63.8 → settle missed +60)
                //     would have caught at +50 → exit ~+50 vs realized -75.7.
                //   - Added "stale-no-pump": if held >= 30min AND never reached
                //     +15% AND currently red, exit. NICETRUMP (held 39min,
                //     never +20, exited -97.6) was the textbook miss.
                if take_pct >= 50.0 {
                    Some(("withdrew", format!("{:+.1}% · scalp 1.5x", take_pct)))
                } else if take_pct >= 30.0 {
                    Some(("withdrew", format!("{:+.1}% · scalp +30 done", take_pct)))
                } else if pct <= trail_floor {
                    // Default-stop case (peak hasn't activated trail tier).
                    // trail_exit covered the lock-profit case earlier.
                    Some(("failed", format!("{:+.1}% · scalp stop", pct)))
                } else if age >= 30 * 60 && pct < 0.0 && peak_observed <= 15.0 {
                    // No pump after 30min — peak never crossed the trail-stop
                    // activation. Bleeds slowly past the floor while never
                    // going green; close at current.
                    Some(("failed", format!("{:+.1}% · scalp no-pump", pct)))
                } else if age >= 4 * 3600 && peak_observed < 20.0 {
                    // Time-expire only when the position never even hit
                    // breakeven trail activation. Winning positions ride
                    // until the trailing stop catches them.
                    Some(("expired", format!("{:+.1}% · scalp timeout", pct)))
                } else {
                    None
                }
            } else if is_moonshot {
                // Bucket B v2 — retuned 2026-05-01 after 1150-token universe
                // analysis. Old +500/-60 ladder had +21% sim EV but bled in
                // live (0/11 -61%) due to settle latency widening realized
                // stop fills. New +250/-25 ladder simulates +14.5% EV under
                // realistic execution and +2.2% under pessimistic — robust
                // across slippage regimes. Tighter stop caps per-fire loss
                // at -25% trigger; lower take threshold doubles realized
                // capture rate (11.3% of cohort hit ≥+200% peak vs 5.7%
                // hitting ≥+500%).
                if pct <= trail_floor {
                    Some(("failed", format!("{:+.1}% · moonshot stop", pct)))
                } else if take_pct >= 250.0 {
                    Some(("withdrew", format!("{:+.1}% · moonshot 3.5x", take_pct)))
                } else if age >= 72 * 3600 && peak_observed < 20.0 {
                    // 72h timeout fires only when the moonshot never even
                    // hit breakeven trail activation. Winning positions
                    // ride until the trailing stop catches them.
                    Some(("expired", format!("{:+.1}% · moonshot timeout", pct)))
                } else {
                    None
                }
            } else if is_long {
                // LONG ladder now actually tiered. PsyopAnime peaked +68.9%,
                // closed -20.1% — pure unrealized profit deleted. The +40/+80
                // tiers were claimed in a comment but never implemented. Each
                // tier just closes the call (we don't track partials at the
                // call-row level today); operator-discretionary scaling is
                // outside the auto-settle path.
                if pct <= trail_floor {
                    Some(("failed", format!("{:+.1}% · thesis broke", pct)))
                } else if take_pct >= 150.0 {
                    Some(("withdrew", format!("{:+.1}% · 2.5x done", take_pct)))
                } else if take_pct >= 80.0 {
                    Some(("withdrew", format!("{:+.1}% · long second take", take_pct)))
                } else if take_pct >= 40.0 {
                    Some(("withdrew", format!("{:+.1}% · long first take", take_pct)))
                } else if age >= 30 * 86_400 && peak_observed < 20.0 {
                    Some(("expired", format!("{:+.1}% · 30d hold complete", pct)))
                } else {
                    None
                }
            } else if take_pct >= 100.0 {
                Some(("withdrew", format!("{:+.1}% · 2x done", take_pct)))
            } else if take_pct >= 50.0 {
                Some(("withdrew", format!("{:+.1}% · took the win", take_pct)))
            } else if age <= 30 * 60 && pct <= -25.0 {
                // Fast-fail: SHORT calls that drop ≥25% within the first 30
                // minutes are dead. Memecoins don't recover from a -25% in
                // half an hour. Faster than the trail floor's default -40%.
                Some(("failed", format!("{:+.1}% · early collapse", pct)))
            } else if pct <= trail_floor {
                Some(("failed", format!("{:+.1}% · thesis broke", pct)))
            } else if call.entry_tx_rate > 0.0
                && self.detect_volume_collapse(&call.mint, call.entry_tx_rate)
            {
                // Volume-collapse rule: two consecutive snapshots showing
                // tx_rate ≤ 10% of entry. The token is silently dying —
                // price hasn't moved -40% yet but flow is gone. Better
                // close at break-even-ish than wait 6h for the bleed.
                Some(("withdrew", format!("{:+.1}% · energy gone", pct)))
            } else if age >= 6 * 3600 && peak_observed < 20.0 {
                Some(("expired", format!("{:+.1}% · no follow-through", pct)))
            } else {
                None
            };

            let Some((status, exit_note)) = outcome else {
                continue;
            };

            // Real-money sell gating. If the call has a buy_signature
            // (real position open) and execution is enabled, fire the
            // sell first, BEFORE flipping the call's status to a paper
            // verdict. The sell either succeeds (proceed to status flip)
            // or fails this cycle (skip status flip, settle retries
            // next cycle until sell_attempt_count hits cap).
            let mut paper_only = true;
            if let Some(exec) = self.executor.as_ref() {
                if exec.cfg.enabled
                    && self.db.call_has_buy(call.id).unwrap_or(false)
                    && !self.db.call_has_sell(call.id).unwrap_or(false)
                {
                    paper_only = false;
                    let attempts = self.db.get_sell_attempt_count(call.id).unwrap_or(0);
                    if attempts >= exec.cfg.sell_retry_max {
                        // Retry cap exhausted — flip the row to failed
                        // with a stuck-position note for manual exit. The
                        // sell will not be retried automatically.
                        let stuck_note = format!(
                            "{:+.1}% · stuck position — manual exit (sells failed {}× / cap {})",
                            pct, attempts, exec.cfg.sell_retry_max
                        );
                        let _ = self.db.fail_call(&call.mint, current_price, &stuck_note);
                        tracing::warn!(
                            "settle: stuck position on call {} ({}) after {} sell attempts — manual exit required",
                            call.id, call.symbol, attempts
                        );
                        continue;
                    }
                    // Pull the buy-time token amount as the sell quantity.
                    // Survives RPC weather (no live SPL balance fetch needed).
                    let token_amount = self.db.get_buy_token_amount(call.id).unwrap_or(None).unwrap_or(0.0);
                    if token_amount <= 0.0 {
                        tracing::warn!(
                            "settle: call {} has buy_signature but buy_token_received is 0 — skipping sell",
                            call.id
                        );
                        continue;
                    }
                    let mcap_for_sell = call.entry_mcap_usd; // for ledger record only
                    let sell_result = crate::execution::execute_sell_for_call(
                        &exec.http,
                        &exec.rpc,
                        &self.db,
                        &exec.keypair,
                        call.id,
                        &call.mint,
                        token_amount,
                        exec.cfg.slippage_bps,
                        exec.priority_fee_lamports,
                        exec.jito_tip_lamports,
                        current_price,
                        mcap_for_sell,
                    )
                    .await;
                    if sell_result.is_err() {
                        // Failure was already logged + record_sell_failure
                        // bumped the attempt counter inside execute_sell_for_call.
                        // Leave the call active so the next cycle retries.
                        tracing::info!(
                            "settle: sell deferred for call {} — will retry next cycle (attempt {})",
                            call.id, attempts + 1
                        );
                        continue;
                    }
                }
            }

            // Apply DB write per outcome. Each helper is idempotent on the
            // (mint, status='active') unique partial index — safe under any
            // double-fire race with another scan cycle. paper_only=true
            // means there was no real position to sell; status flip stands
            // alone. paper_only=false + we got here means the sell already
            // confirmed and we proceed to mirror the outcome publicly.
            let _ = paper_only; // silence: kept for future telemetry
            let db_ok = match status {
                "withdrew" => self.db.close_call(&call.mint, current_price, &exit_note),
                "failed" => self.db.fail_call(&call.mint, current_price, &exit_note),
                "expired" => self.db.expire_call(&call.mint, current_price, &exit_note),
                _ => Ok(false),
            };
            let horizon_label = if is_scalp {
                "SCALP"
            } else if is_long {
                "LONG"
            } else if is_moonshot {
                "MOONSHOT"
            } else {
                "SHORT"
            };
            match db_ok {
                Ok(true) => tracing::info!(
                    "settle: {} {} ({}={:+.1}%, age={}m, horizon={})",
                    status,
                    call.symbol,
                    horizon_label.to_lowercase(),
                    pct,
                    age / 60,
                    horizon_label
                ),
                Ok(false) => continue,
                Err(e) => {
                    tracing::warn!("settle: {} {} failed: {}", status, call.symbol, e);
                    continue;
                }
            }
            // Kick the publisher so the public ledger reflects the new
            // outcome within ~30s instead of waiting for the next 300s
            // tick. Done before the spawned TG-edit task — that task
            // races independently and shouldn't gate the publish.
            if let Some(ref n) = self.notifier {
                n.kick_publisher();
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

    /// Two consecutive snapshots whose tx_rate ≤ 10% of `entry_tx_rate`
    /// → the token is dying silently. Price-based settling alone misses
    /// this: a token at +5% with no flow is dead, not still working.
    /// Stateless via DB; resets on restart since "consecutive" is read
    /// from `token_snapshots`, not in-memory state.
    fn detect_volume_collapse(&self, mint: &str, entry_tx_rate: f64) -> bool {
        let threshold = entry_tx_rate * 0.10;
        match self.db.get_snapshot_history(mint, 2) {
            Ok(snaps) if snaps.len() >= 2 => {
                snaps.iter().all(|s| s.tx_rate <= threshold)
            }
            _ => false,
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

/// Trailing-stop floor — given the peak return observed since entry and the
/// horizon's default stop, return the stop floor that should fire the exit.
/// Once a position has run, the floor ratchets upward so a previously
/// profitable trade can never be closed at a loss. "Enter for free, ride
/// the profit." Tiers tuned against the live cohort: most peaked positions
/// retraced 30-50% off peak before recovering or rugging, so each tier
/// captures roughly half of the prior tier's gain.
fn trailing_stop_floor(peak_pct: f64, default_stop: f64) -> f64 {
    if peak_pct >= 400.0 {
        200.0
    } else if peak_pct >= 200.0 {
        100.0
    } else if peak_pct >= 100.0 {
        50.0
    } else if peak_pct >= 50.0 {
        25.0
    } else if peak_pct >= 20.0 {
        // Breakeven floor — once the position has gained 20% it can never
        // be closed below entry. The "free ride" threshold.
        0.0
    } else {
        default_stop
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

    // Holder thresholds dropped 30 → 15: getTokenLargestAccounts caps the
    // holder count at 20 in our sampling, so 30 was unreachable. Result:
    // every token that drifted to DEVELOPING got evicted at 1h regardless
    // of structural quality. NOHOUSE (call 64) was the canonical loss —
    // entered SCALP at GRINDER, drifted to DEVELOPING, evicted before
    // recovering. 15 is the realistic ceiling under RPC cap.
    matches!(class, "STAIRCASE" | "GRINDER" | "SPRING")
        || (class == "SURGE" && confidence >= 80 && holders >= 15 && top_holder_pct <= 22.0)
        || (class == "DEVELOPING"
            && confidence >= 70
            && holders >= 15
            && top_holder_pct <= 25.0
            && momentum >= 50
            && distribution >= 60)
}

fn should_evict_watchlist_candidate(candidate: &crate::db::WatchlistCandidate, now: i64) -> bool {
    let class = effective_watchlist_class(candidate);
    let holders = candidate.snapshot_holder_count.unwrap_or(0);
    // CRITICAL: default top_holder_pct to 0.0 when NO snapshot exists yet
    // (fresh watchlist additions). Previous default of 100.0 immediately
    // tripped the >=50 concentration eviction on every fresh add — every
    // newly-added token got instantly deactivated before its first
    // analysis cycle could even run. Result: watchlist permanently
    // empty + 0 calls fire ever. Use 0.0 as 'unknown', let the first
    // analysis cycle populate the real value, then re-evaluate.
    let top_holder_pct = candidate.snapshot_top_holder_pct.unwrap_or(0.0);
    let has_snapshot = candidate.snapshot_classification.is_some();

    if matches!(class, "DEAD" | "CRASHING" | "ACTIVE_TRAP") || class.starts_with("UNSAFE") {
        return true;
    }
    // top1>=50 concentration cut applies only when we HAVE measured data.
    // No snapshot yet = give the token a chance to be analyzed first.
    let _ = holders;
    if has_snapshot && top_holder_pct >= 50.0 {
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
    // Default top_holder_pct to 0.0 (unknown) instead of 100.0. The 100.0
    // default was treating fresh items as maximally-concentrated and
    // applying the +45 concentration penalty — pushing freshly-added
    // tokens to the BACK of the queue and starving them of analysis.
    // Fresh items should sort by class+age, then get analyzed to populate
    // real top_holder_pct. Only THEN does the concentration penalty matter.
    let top_holder_pct = candidate.snapshot_top_holder_pct.unwrap_or(0.0);
    let has_snapshot = candidate.snapshot_classification.is_some();
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
    // Concentration penalty only applies to MEASURED data. Fresh items
    // (no snapshot) get 0 penalty so they sort by class/age, then get
    // analyzed first to populate real values.
    let concentration_penalty = if !has_snapshot {
        0.0
    } else if top_holder_pct >= 40.0 {
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
    // First-snapshot boost: items with no snapshot yet get a small bump
    // so they're not at the bottom of the queue. After their first
    // analysis, real metrics determine priority.
    let first_snapshot_boost = if !has_snapshot { 15.0 } else { 0.0 };
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

    class_base
        + confidence
        + momentum / 3.0
        + distribution / 4.0
        + holder_bonus
        + freshness_bonus
        + first_snapshot_boost
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
    // Concentration trigger: rather than holders<25, use top1>=50%. The
    // holder count is RPC-capped at 20; a 25-floor was always-true and the
    // top-holder threshold was the real signal anyway. Tokens with one
    // wallet holding >=50% are unambiguously concentrated.
    (class == "DEAD" && has_market_signal)
        || matches!(class, "CRASHING" | "ACTIVE_TRAP")
        || class.starts_with("UNSAFE")
        || analysis.top_holder_pct >= 50.0
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
            // top1 raised to 55 to trigger the concentration eviction
            // (>=50 cut after the 2026-04-29 watchlist tuning).
            snapshot_top_holder_pct: Some(55.0),
            snapshot_momentum: Some(54),
            snapshot_distribution: Some(48),
            snapshot_timestamp: Some(now - 120),
        };

        assert!(watchlist_priority(&strong, now) > watchlist_priority(&weak, now));
        assert!(should_evict_watchlist_candidate(&weak, now));
    }
}
