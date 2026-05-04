//! Free-tier discovery pollers — supplements PumpPortal WS by rotating
//! through public DexScreener endpoints to catch tokens the WS feed
//! missed (lossy events) and tokens from non-pump.fun launchpads
//! (Raydium-direct, Moonshot, etc.) that PumpPortal doesn't cover at all.
//!
//! Three endpoints, round-robin one per 60s tick. All free, no auth,
//! no key. DexScreener's documented public API:
//!   - /token-profiles/latest/v1   — recently profiled tokens
//!   - /token-boosts/latest/v1     — recently boosted (paid promos)
//!   - /token-boosts/top/v1        — top boosted right now
//!
//! Each tick fetches one source, filters to chainId="solana", and adds
//! any mint not already in `tokens` to the watchlist. The downstream
//! pipeline (analyze_token + gates) decides what to do with them.
//!
//! Cost: 3 HTTPS calls per minute total, ~5 MB/day of bandwidth.

use crate::db::Db;
use anyhow::Result;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

const ENDPOINTS: &[&str] = &[
    "https://api.dexscreener.com/token-profiles/latest/v1",
    "https://api.dexscreener.com/token-boosts/latest/v1",
    "https://api.dexscreener.com/token-boosts/top/v1",
];

const POLL_INTERVAL: Duration = Duration::from_secs(60);
const HTTP_TIMEOUT: Duration = Duration::from_secs(8);

pub struct DiscoveryPoller {
    db: Arc<Db>,
    http: reqwest::Client,
}

impl DiscoveryPoller {
    pub fn new(db: Arc<Db>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .user_agent("exciton/0.1 (free-tier discovery poller)")
            .build()
            .expect("reqwest client init");
        Self { db, http }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            // Skip the immediate first tick — let the rest of the
            // pipeline warm up before we start adding fresh discovery
            // load on top.
            let mut tick = tokio::time::interval(POLL_INTERVAL);
            tick.tick().await;
            tracing::info!(
                "discovery pollers started ({} sources, {}s round-robin)",
                ENDPOINTS.len(),
                POLL_INTERVAL.as_secs()
            );
            let mut idx = 0usize;
            loop {
                tick.tick().await;
                let url = ENDPOINTS[idx % ENDPOINTS.len()];
                idx = idx.wrapping_add(1);
                match self.poll_one(url).await {
                    Ok(added) => {
                        // Always log so the poller's heartbeat is visible.
                        // PumpPortal firehose typically adds the same mints
                        // first, so 0-adds is the normal steady state — but
                        // distinguishing "0 because already known" from
                        // "0 because the poller died silently" matters.
                        tracing::info!(
                            "discovery poll: +{} new mints from {}",
                            added,
                            short_url(url)
                        );
                    }
                    Err(e) => {
                        tracing::warn!("discovery poll {} failed: {}", short_url(url), e);
                    }
                }
            }
        });
    }

    async fn poll_one(&self, url: &str) -> Result<usize> {
        let resp = self.http.get(url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("status {}", resp.status());
        }
        let items: Vec<DsTokenRef> = resp.json().await?;
        let source_tag = short_url(url);
        let is_boost_feed = source_tag.starts_with("ds:boosts");
        let mut added = 0;
        for item in items {
            if item.chain_id.as_deref() != Some("solana") {
                continue;
            }
            let Some(mint) = item.token_address else {
                continue;
            };
            // Boost capture: any item from ds:boosts-* with a positive
            // amount becomes a token_boosts row. Used by the signal
            // gates as an early conviction marker — a project paying
            // for promotion is real-money intent, even if our standard
            // pattern gates haven't qualified the token yet.
            // boosts-latest reports incremental `amount`; boosts-top
            // reports only cumulative `totalAmount`. Prefer amount,
            // fall back to total so both feeds land rows.
            if is_boost_feed {
                let boost = item.amount.or(item.total_amount).unwrap_or(0);
                if boost > 0 {
                    if let Err(e) = self.db.record_token_boost(&mint, boost, source_tag) {
                        tracing::debug!("discovery poll: record_boost {} failed: {}", mint, e);
                    }
                }
            }
            // Already known — leave it alone. The watchlist scanner
            // owns reanalysis cadence; we only seed new mints.
            if self
                .db
                .get_token(&mint)
                .ok()
                .flatten()
                .is_some()
            {
                continue;
            }
            if let Err(e) = self.db.insert_token(&mint, 0) {
                tracing::debug!("discovery poll: insert_token {} failed: {}", mint, e);
                continue;
            }
            // Seed with a placeholder classification — analyze_token
            // overwrites with real reading on the first scan.
            if let Err(e) = self.db.add_to_watchlist(&mint, "DEVELOPING") {
                tracing::debug!("discovery poll: add_to_watchlist {} failed: {}", mint, e);
                continue;
            }
            added += 1;
        }
        Ok(added)
    }
}

#[derive(Deserialize)]
struct DsTokenRef {
    #[serde(rename = "chainId")]
    chain_id: Option<String>,
    #[serde(rename = "tokenAddress")]
    token_address: Option<String>,
    /// Per-event boost delta on `ds:boosts-latest`. Absent on `ds:boosts-top`
    /// (which only reports cumulative `totalAmount`) and on `ds:profiles`.
    #[serde(default)]
    amount: Option<i64>,
    /// Cumulative boost spend on `ds:boosts-top` and `ds:boosts-latest`.
    /// Absent on `ds:profiles`. Used as a fallback when `amount` is missing
    /// so boosts-top actually records signal — without it, the table only
    /// captured boosts-latest and we lost half the boost firehose.
    #[serde(rename = "totalAmount", default)]
    total_amount: Option<i64>,
}

fn short_url(u: &str) -> &str {
    u.rsplit_once('/').map(|(_, t)| t).unwrap_or(u);
    // The endpoints all share the /v1 suffix; pick a more useful slice.
    if u.contains("token-profiles") {
        "ds:profiles"
    } else if u.contains("token-boosts/latest") {
        "ds:boosts-latest"
    } else if u.contains("token-boosts/top") {
        "ds:boosts-top"
    } else {
        "ds:?"
    }
}
