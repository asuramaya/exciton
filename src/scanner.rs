use crate::db::Db;
use crate::discovery;
use crate::ingester::RpcRouter;
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
        // Discover new tokens (limit 5 per cycle to respect rate limits)
        let analyses =
            discovery::discover_new_tokens(&self.helius_url, &self.db, &self.rpc, 5).await?;

        let mut alert_count = 0;

        for analysis in &analyses {
            let class = &analysis.confidence.classification;

            match class.as_str() {
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

                // ACTIVE_TRAP: activity but concentrated — watch for distribution starting
                "ACTIVE_TRAP" => {
                    if analysis.confidence.momentum > 70 {
                        self.db.insert_alert(
                            "active_trap",
                            Some(&analysis.address),
                            &format!(
                                "ACTIVE TRAP {} — momentum {} but top holder {:.1}%, watch for distribution",
                                &analysis.address,
                                analysis.confidence.momentum,
                                analysis.top_holder_pct,
                            ),
                            analysis.confidence.total,
                        )?;
                        alert_count += 1;
                    }
                }

                // DEAD or DEVELOPING: only alert on danger signals
                _ => {
                    let has_danger = analysis.scores.iter().any(|s| {
                        s.layer == SignalLayer::Safety && s.score <= 10
                    });
                    if has_danger {
                        let dangers: Vec<String> = analysis
                            .scores
                            .iter()
                            .filter(|s| s.layer == SignalLayer::Safety && s.score <= 10)
                            .map(|s| s.details.clone())
                            .collect();

                        self.db.insert_alert(
                            "danger",
                            Some(&analysis.address),
                            &format!("DANGER {}: {}", &analysis.address, dangers.join("; ")),
                            analysis.confidence.total,
                        )?;
                        alert_count += 1;
                    }
                }
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
