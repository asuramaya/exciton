use crate::db::Db;
use crate::discovery;
use crate::ingester::RpcRouter;
use crate::signals;
use crate::signals::SignalLayer;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Background scanner that continuously discovers and analyzes tokens
pub struct BackgroundScanner {
    db: Arc<Db>,
    rpc: Arc<RpcRouter>,
    helius_url: String,
    interval: Duration,
    alert_threshold: i32,
    running: Arc<AtomicBool>,
}

impl BackgroundScanner {
    pub fn new(
        db: Arc<Db>,
        rpc: Arc<RpcRouter>,
        helius_url: String,
        interval_seconds: u64,
        alert_threshold: i32,
    ) -> Self {
        Self {
            db,
            rpc,
            helius_url,
            interval: Duration::from_secs(interval_seconds),
            alert_threshold,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the background scan loop. Returns a handle to stop it.
    pub fn start(self) -> ScannerHandle {
        let running = self.running.clone();
        running.store(true, Ordering::SeqCst);

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
            "Background scanner started: interval={}s, alert_threshold={}",
            self.interval.as_secs(),
            self.alert_threshold
        );

        let _ = self.db.audit_log("scanner", "start", "Background scanner started");

        let mut cycle = 0u64;
        while self.running.load(Ordering::SeqCst) {
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
        // Phase 1: Re-analyze watchlist tokens (up to 3 per cycle)
        let watchlist = self.db.get_watchlist_due(15, 3)?; // re-check every 15s
        for addr in &watchlist {
            match signals::analyze_token(&self.rpc, addr, Some(&self.db)).await {
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
                                    "CLASSIFICATION CHANGE {} — {} → {}, top holder delta {:+.1}%",
                                    addr, delta.previous.classification, analysis.confidence.classification,
                                    delta.top_holder_delta
                                ),
                                analysis.confidence.total,
                            )?;
                        }

                        // Concentration direction alert
                        if delta.concentration_direction == "concentrating" {
                            self.db.insert_alert(
                                "concentrating",
                                Some(addr),
                                &format!(
                                    "CONCENTRATING {} — top holder {:+.1}% ({:.1}% → {:.1}%), {} — EXIT SIGNAL",
                                    addr, delta.top_holder_delta,
                                    delta.previous.top_holder_pct, delta.current.top_holder_pct,
                                    analysis.confidence.classification,
                                ),
                                analysis.confidence.total,
                            )?;
                        }

                        // Velocity exit warning
                        if delta.current.velocity < 0.5 && delta.previous.velocity > 1.0 {
                            self.db.insert_alert(
                                "velocity_crash",
                                Some(addr),
                                &format!(
                                    "VELOCITY CRASH {} — {:.1}x → {:.1}x, momentum dying",
                                    addr, delta.previous.velocity, delta.current.velocity,
                                ),
                                analysis.confidence.total,
                            )?;
                        }
                    }

                    // Deactivate DEAD tokens from watchlist
                    if analysis.confidence.classification == "DEAD" {
                        self.db.deactivate_watchlist(addr)?;
                    }
                }
                Err(e) => {
                    tracing::warn!("Watchlist re-analysis failed for {}: {}", addr, e);
                }
            }
        }

        // Phase 2: Discover new tokens (limit 3 per cycle to leave budget for watchlist)
        let analyses =
            discovery::discover_new_tokens(&self.helius_url, &self.db, &self.rpc, 3).await?;

        let mut alert_count = 0;

        for analysis in &analyses {
            let class = &analysis.confidence.classification;

            match class.as_str() {
                // GRINDER: escaped velocity — deep distribution + sustained demand congestion
                "GRINDER" => {
                    self.db.insert_alert(
                        "grinder",
                        Some(&analysis.address),
                        &format!(
                            "GRINDER {} — top holder {:.1}%, demand congestion on deep base",
                            &analysis.address,
                            analysis.top_holder_pct,
                        ),
                        analysis.confidence.total,
                    )?;
                    alert_count += 1;
                }

                // SPRING: distributed and quiet — loaded potential. Always alert.
                "SPRING" => {
                    self.db.insert_alert(
                        "spring",
                        Some(&analysis.address),
                        &format!(
                            "SPRING {} — distributed (top holder {:.1}%), quiet, spring score {}, waiting for ignition",
                            &analysis.address,
                            analysis.top_holder_pct,
                            analysis.confidence.spring,
                        ),
                        analysis.confidence.total,
                    )?;
                    alert_count += 1;
                }

                // STAIRCASE: active with deep distribution — the best pattern. Always alert.
                "STAIRCASE" => {
                    self.db.insert_alert(
                        "staircase",
                        Some(&analysis.address),
                        &format!(
                            "STAIRCASE {} — momentum {}, distribution {}, multi-wave potential",
                            &analysis.address,
                            analysis.confidence.momentum,
                            analysis.confidence.distribution,
                        ),
                        analysis.confidence.total,
                    )?;
                    alert_count += 1;
                }

                // SURGE: explosive on concentrated token — short window. Alert if high confidence.
                "SURGE" => {
                    if analysis.confidence.total >= self.alert_threshold {
                        self.db.insert_alert(
                            "surge",
                            Some(&analysis.address),
                            &format!(
                                "SURGE {} — momentum {}, top holder {:.1}%, window may be open",
                                &analysis.address,
                                analysis.confidence.momentum,
                                analysis.top_holder_pct,
                            ),
                            analysis.confidence.total,
                        )?;
                        alert_count += 1;
                    }
                }

                // ACTIVE_TRAP: only alert if top holder < 60% AND has real holder depth
                // 100% top holder with momentum 93 is just a bot pinging a shell
                "ACTIVE_TRAP" => {
                    if analysis.top_holder_pct < 60.0
                        && analysis.holder_count >= 10
                        && analysis.confidence.momentum > 70
                    {
                        self.db.insert_alert(
                            "active_trap",
                            Some(&analysis.address),
                            &format!(
                                "ACTIVE TRAP {} — momentum {}, top holder {:.1}%, {} holders — distribution starting?",
                                &analysis.address,
                                analysis.confidence.momentum,
                                analysis.top_holder_pct,
                                analysis.holder_count,
                            ),
                            analysis.confidence.total,
                        )?;
                        alert_count += 1;
                    }
                }

                // DEAD or DEVELOPING: don't alert on every concentrated token.
                // Only alert on DEVELOPING tokens that have real distribution
                // potential (>10 holders, top holder dropping)
                _ => {
                    if analysis.confidence.classification == "DEVELOPING"
                        && analysis.holder_count >= 10
                        && analysis.top_holder_pct < 50.0
                    {
                        self.db.insert_alert(
                            "developing",
                            Some(&analysis.address),
                            &format!(
                                "DEVELOPING {} — top holder {:.1}%, {} holders, momentum {}, watch for transition",
                                &analysis.address,
                                analysis.top_holder_pct,
                                analysis.holder_count,
                                analysis.confidence.momentum,
                            ),
                            analysis.confidence.total,
                        )?;
                        alert_count += 1;
                    }
                    // DEAD tokens with 90%+ concentration are noise — don't alert
                }
            }

            // Add interesting tokens to watchlist for trajectory tracking
            let dominated_class = &analysis.confidence.classification;
            match dominated_class.as_str() {
                "STAIRCASE" | "SPRING" | "SURGE" | "ACTIVE_TRAP" => {
                    let _ = self.db.add_to_watchlist(&analysis.address, dominated_class);
                }
                "DEVELOPING" if analysis.confidence.distribution > 40 => {
                    let _ = self.db.add_to_watchlist(&analysis.address, dominated_class);
                }
                _ => {}
            }
        }

        Ok(alert_count)
    }
}

pub struct ScannerHandle {
    running: Arc<AtomicBool>,
}

impl ScannerHandle {
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}
