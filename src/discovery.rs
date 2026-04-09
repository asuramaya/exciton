use crate::db::Db;
use crate::ingester::RpcRouter;
use crate::signals::{self, TokenAnalysis};
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::Arc;

const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Response from Helius enhanced transaction API
#[derive(Debug, Deserialize)]
struct HeliusTx {
    #[serde(default)]
    description: String,
    #[serde(rename = "type", default)]
    tx_type: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    signature: String,
    #[serde(default)]
    timestamp: i64,
    #[serde(rename = "tokenTransfers", default)]
    token_transfers: Vec<HeliusTokenTransfer>,
}

#[derive(Debug, Deserialize)]
struct HeliusTokenTransfer {
    #[serde(default)]
    mint: String,
    #[serde(rename = "tokenAmount", default)]
    token_amount: f64,
}

/// Discover new tokens from Pump.fun activity via Helius enhanced API
pub async fn discover_new_tokens(
    helius_endpoint: &str,
    db: &Arc<Db>,
    rpc: &Arc<RpcRouter>,
    limit: usize,
) -> Result<Vec<TokenAnalysis>> {
    // Extract API key from the RPC endpoint URL
    let api_key = extract_api_key(helius_endpoint)
        .ok_or_else(|| anyhow::anyhow!("Could not extract Helius API key from endpoint URL"))?;

    // Fetch recent Pump.fun transactions
    let url = format!(
        "https://api.helius.xyz/v0/addresses/{}/transactions?api-key={}&limit=30",
        PUMPFUN_PROGRAM, api_key
    );

    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("Helius API returned status {}", resp.status());
    }

    let txs: Vec<HeliusTx> = resp.json().await?;

    // Extract unique token mints (excluding SOL)
    let mut seen_mints: HashSet<String> = HashSet::new();
    let mut new_mints: Vec<String> = Vec::new();

    for tx in &txs {
        for transfer in &tx.token_transfers {
            if !transfer.mint.is_empty()
                && transfer.mint != SOL_MINT
                && !seen_mints.contains(&transfer.mint)
            {
                seen_mints.insert(transfer.mint.clone());
                new_mints.push(transfer.mint.clone());
            }
        }
    }

    tracing::info!(
        "Discovery: found {} unique tokens in {} Pump.fun transactions",
        new_mints.len(),
        txs.len()
    );

    // Filter out tokens we've already analyzed recently
    let fresh_mints: Vec<String> = new_mints
        .into_iter()
        .filter(|m| {
            // Check if token is already in DB with a recent score
            match db.get_token(m) {
                Ok(Some(_)) => false, // Already analyzed
                _ => true,            // New — analyze it
            }
        })
        .take(limit)
        .collect();

    tracing::info!(
        "Discovery: {} tokens are new, analyzing up to {}",
        fresh_mints.len(),
        limit
    );

    // Analyze each new token
    let mut analyses: Vec<TokenAnalysis> = Vec::new();

    for mint in &fresh_mints {
        match signals::analyze_token(rpc, mint).await {
            Ok(analysis) => {
                // Store in DB
                let _ = db.insert_token(&analysis.address, analysis.confidence.total);
                let _ = db.audit_log(
                    "discovery",
                    "new_token",
                    &format!(
                        "{} confidence={} safety_avg={}",
                        mint,
                        analysis.confidence.total,
                        analysis
                            .scores
                            .iter()
                            .filter(|s| s.layer == signals::SignalLayer::Safety)
                            .map(|s| s.score)
                            .sum::<i32>()
                            / analysis
                                .scores
                                .iter()
                                .filter(|s| s.layer == signals::SignalLayer::Safety)
                                .count()
                                .max(1) as i32
                    ),
                );
                analyses.push(analysis);
            }
            Err(e) => {
                tracing::warn!("Failed to analyze {}: {}", mint, e);
            }
        }
    }

    // Sort by confidence, highest first
    analyses.sort_by(|a, b| b.confidence.total.cmp(&a.confidence.total));

    Ok(analyses)
}

/// Extract API key from a Helius endpoint URL
fn extract_api_key(url: &str) -> Option<String> {
    if let Some(idx) = url.find("api-key=") {
        let rest = &url[idx + 8..];
        let end = rest.find('&').unwrap_or(rest.len());
        Some(rest[..end].to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_api_key() {
        let url = "https://mainnet.helius-rpc.com/?api-key=abc123";
        assert_eq!(extract_api_key(url), Some("abc123".to_string()));
    }

    #[test]
    fn test_extract_api_key_with_params() {
        let url = "https://mainnet.helius-rpc.com/?api-key=abc123&other=val";
        assert_eq!(extract_api_key(url), Some("abc123".to_string()));
    }

    #[test]
    fn test_extract_api_key_missing() {
        let url = "https://api.mainnet-beta.solana.com";
        assert_eq!(extract_api_key(url), None);
    }
}
