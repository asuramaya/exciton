use crate::db::Db;
use crate::discovery;
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

    /// At startup, auto-fail any call that has an active status in the `calls`
    /// table but whose watchlist entry is already inactive. This handles the
    /// race where the service restarted between watchlist deactivation and the
    /// auto-fail hook firing.
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
        tracing::info!("cleanup: {} orphaned call(s) to auto-fail", orphans.len());
        for mint in &orphans {
            let exit_price = crate::metadata::fetch(mint)
                .await
                .ok()
                .flatten()
                .and_then(|m| m.price_usd)
                .unwrap_or(0.0);
            let note = "auto-failed: orphaned call (watchlist inactive at startup)";
            match self.db.fail_call(mint, exit_price, note) {
                Ok(_) => {
                    tracing::info!("orphan auto-failed: {}", mint);
                    if let Some(ref n) = self.notifier {
                        let mint_owned = mint.clone();
                        let n = n.clone();
                        tokio::spawn(async move {
                            if let Err(e) = n
                                .update_call_outcome(&mint_owned, "failed", None, note)
                                .await
                            {
                                tracing::warn!("orphan update_call_outcome failed: {}", e);
                            }
                        });
                    }
                }
                Err(e) => tracing::warn!("fail_call for orphan {} failed: {}", mint, e),
            }
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

                    // Drop dead/trap/concentrated tokens quickly so the
                    // shortlist keeps cycling through viable candidates.
                    // When an active call exists for this token, also auto-close
                    // it as 'failed' so the public ledger shows a clean exit
                    // rather than an orphaned open position.
                    if should_remove_from_watchlist(&analysis) {
                        self.db.deactivate_watchlist(addr)?;
                        if self.db.has_active_call(addr).unwrap_or(false) {
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
                                "call auto-failed: {} class={} top={:.1}%",
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

        // Phase 3: Smart-wallet tracker — for each user-curated wallet, diff
        // the current SPL holdings against our stored view; every newly-seen
        // mint becomes a `smart_wallet_buy` alert. Imitation alpha: when a
        // proven caller's wallet buys something, we want to know immediately.
        self.scan_smart_wallets().await;

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
    match class {
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
    }
}

fn should_remove_from_watchlist(analysis: &TokenAnalysis) -> bool {
    let class = analysis.confidence.classification.as_str();
    // DEAD requires a confirmed liquidity_depth signal — that signal is only
    // added when DexScreener returns liquidity_usd > 0. Missing market data
    // (fetch failure returning 0 or no response) must not auto-fail calls.
    let has_market_signal = analysis
        .scores
        .iter()
        .any(|s| s.signal_type == "liquidity_depth");
    (class == "DEAD" && has_market_signal)
        || matches!(class, "CRASHING" | "ACTIVE_TRAP")
        || class.starts_with("UNSAFE")
        || (analysis.holder_count < 25 && analysis.top_holder_pct >= 33.0)
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
