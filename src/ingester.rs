use anyhow::{Context, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

/// A single RPC endpoint with health tracking
struct Endpoint {
    url: String,
    client: RpcClient,
    request_count: AtomicU64,
    error_count: AtomicU64,
    healthy: AtomicBool,
}

impl Endpoint {
    fn new(url: &str) -> Self {
        let client = RpcClient::new_with_commitment(
            url.to_string(),
            CommitmentConfig::confirmed(),
        );
        Self {
            url: url.to_string(),
            client,
            request_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
        }
    }

    fn record_success(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.healthy.store(true, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.error_count.fetch_add(1, Ordering::Relaxed);
        // Mark unhealthy after 3 consecutive errors
        if self.error_count.load(Ordering::Relaxed) > 3 {
            self.healthy.store(false, Ordering::Relaxed);
        }
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn reset_errors(&self) {
        self.error_count.store(0, Ordering::Relaxed);
        self.healthy.store(true, Ordering::Relaxed);
    }
}

/// Multi-endpoint RPC router with round-robin and failover
pub struct RpcRouter {
    endpoints: Vec<Endpoint>,
    current: AtomicUsize,
}

impl RpcRouter {
    pub fn new(urls: &[String]) -> Result<Self> {
        if urls.is_empty() {
            anyhow::bail!("At least one RPC endpoint is required");
        }
        let endpoints: Vec<Endpoint> = urls.iter().map(|u| Endpoint::new(u)).collect();
        tracing::info!("RPC router initialized with {} endpoints", endpoints.len());
        for (i, ep) in endpoints.iter().enumerate() {
            // Mask API keys in log output
            let masked = mask_url(&ep.url);
            tracing::info!("  [{}] {}", i, masked);
        }
        Ok(Self {
            endpoints,
            current: AtomicUsize::new(0),
        })
    }

    /// Get the next healthy endpoint, round-robin with failover
    fn next_client(&self) -> Option<&RpcClient> {
        let len = self.endpoints.len();
        let start = self.current.fetch_add(1, Ordering::Relaxed) % len;

        // Try from current position, wrapping around
        for i in 0..len {
            let idx = (start + i) % len;
            if self.endpoints[idx].is_healthy() {
                return Some(&self.endpoints[idx].client);
            }
        }

        // All unhealthy — reset all and try first
        tracing::warn!("All RPC endpoints unhealthy, resetting");
        for ep in &self.endpoints {
            ep.reset_errors();
        }
        Some(&self.endpoints[start % len].client)
    }

    fn record_result(&self, idx: usize, success: bool) {
        if let Some(ep) = self.endpoints.get(idx) {
            if success {
                ep.record_success();
            } else {
                ep.record_error();
            }
        }
    }

    fn current_index(&self) -> usize {
        self.current.load(Ordering::Relaxed).wrapping_sub(1) % self.endpoints.len()
    }

    pub fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    pub fn healthy_count(&self) -> usize {
        self.endpoints.iter().filter(|e| e.is_healthy()).count()
    }

    /// Get balance for a wallet
    pub async fn get_balance(&self, pubkey: &str) -> Result<u64> {
        let pk = Pubkey::from_str(pubkey).context("Invalid public key")?;
        let client = self.next_client().context("No RPC endpoints available")?;
        let idx = self.current_index();

        match client.get_balance(&pk).await {
            Ok(balance) => {
                self.record_result(idx, true);
                Ok(balance)
            }
            Err(e) => {
                self.record_result(idx, false);
                // Try one more endpoint on failure
                if let Some(retry_client) = self.next_client() {
                    let retry_idx = self.current_index();
                    match retry_client.get_balance(&pk).await {
                        Ok(balance) => {
                            self.record_result(retry_idx, true);
                            Ok(balance)
                        }
                        Err(e2) => {
                            self.record_result(retry_idx, false);
                            Err(e2.into())
                        }
                    }
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Get recent blockhash to verify connectivity
    pub async fn check_connection(&self) -> Result<bool> {
        let client = self.next_client().context("No RPC endpoints available")?;
        let idx = self.current_index();

        match client.get_latest_blockhash().await {
            Ok(_) => {
                self.record_result(idx, true);
                Ok(true)
            }
            Err(e) => {
                self.record_result(idx, false);
                tracing::warn!("RPC connection check failed: {}", e);
                Ok(false)
            }
        }
    }

    /// Get slot to check data freshness
    pub async fn get_slot(&self) -> Result<u64> {
        let client = self.next_client().context("No RPC endpoints available")?;
        let idx = self.current_index();

        match client.get_slot().await {
            Ok(slot) => {
                self.record_result(idx, true);
                Ok(slot)
            }
            Err(e) => {
                self.record_result(idx, false);
                Err(e.into())
            }
        }
    }

    /// Get token account balance
    pub async fn get_token_account_balance(&self, token_account: &str) -> Result<u64> {
        let pk = Pubkey::from_str(token_account).context("Invalid token account")?;
        let client = self.next_client().context("No RPC endpoints available")?;
        let idx = self.current_index();

        match client.get_token_account_balance(&pk).await {
            Ok(balance) => {
                self.record_result(idx, true);
                let amount = balance
                    .amount
                    .parse::<u64>()
                    .unwrap_or(0);
                Ok(amount)
            }
            Err(e) => {
                self.record_result(idx, false);
                Err(e.into())
            }
        }
    }
}

/// Mask API keys in URLs for safe logging
fn mask_url(url: &str) -> String {
    // Mask query params like ?api-key=xxx
    if let Some(idx) = url.find("api-key=") {
        let prefix = &url[..idx + 8];
        format!("{}****", prefix)
    }
    // Mask path tokens (QuickNode style: .../token/)
    else if url.contains("quiknode.pro/") {
        if let Some(idx) = url.find("quiknode.pro/") {
            let prefix = &url[..idx + 13]; // "quiknode.pro/"
            format!("{}****", prefix)
        } else {
            url.to_string()
        }
    }
    // Mask path tokens (Alchemy style: /v2/key)
    else if url.contains("alchemy.com/v2/") {
        if let Some(idx) = url.find("/v2/") {
            let prefix = &url[..idx + 4]; // "/v2/"
            format!("{}****", prefix)
        } else {
            url.to_string()
        }
    } else {
        url.to_string()
    }
}

/// Resolve endpoint URLs, substituting environment variables
/// Supports ${VAR_NAME} syntax in endpoint strings
pub fn resolve_endpoints(endpoints: &[String]) -> Vec<String> {
    endpoints
        .iter()
        .map(|ep| resolve_env_vars(ep))
        .collect()
}

fn resolve_env_vars(s: &str) -> String {
    let mut result = s.to_string();
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            let var_name = &result[start + 2..start + end];
            let value = std::env::var(var_name).unwrap_or_default();
            result = format!("{}{}{}", &result[..start], value, &result[start + end + 1..]);
        } else {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_url_helius() {
        let url = "https://mainnet.helius-rpc.com/?api-key=abc123secret";
        assert_eq!(
            mask_url(url),
            "https://mainnet.helius-rpc.com/?api-key=****"
        );
    }

    #[test]
    fn test_mask_url_quicknode() {
        let url = "https://my-ep.solana-mainnet.quiknode.pro/secret_token/";
        let masked = mask_url(url);
        assert!(masked.contains("****"));
        assert!(!masked.contains("secret_token"));
    }

    #[test]
    fn test_mask_url_plain() {
        let url = "https://api.mainnet-beta.solana.com";
        assert_eq!(mask_url(url), url);
    }

    #[test]
    fn test_resolve_env_vars() {
        std::env::set_var("TEST_PHOTON_KEY", "my_secret_key");
        let result = resolve_env_vars("https://mainnet.helius-rpc.com/?api-key=${TEST_PHOTON_KEY}");
        assert_eq!(
            result,
            "https://mainnet.helius-rpc.com/?api-key=my_secret_key"
        );
        std::env::remove_var("TEST_PHOTON_KEY");
    }

    #[test]
    fn test_resolve_env_vars_missing() {
        let result = resolve_env_vars("https://example.com/?key=${NONEXISTENT_VAR_12345}");
        assert_eq!(result, "https://example.com/?key=");
    }

    #[test]
    fn test_resolve_multiple_vars() {
        std::env::set_var("TEST_HOST", "mainnet");
        std::env::set_var("TEST_KEY", "abc");
        let result = resolve_env_vars("https://${TEST_HOST}.example.com/?key=${TEST_KEY}");
        assert_eq!(result, "https://mainnet.example.com/?key=abc");
        std::env::remove_var("TEST_HOST");
        std::env::remove_var("TEST_KEY");
    }

    #[test]
    fn test_rpc_router_creation() {
        let urls = vec!["https://api.mainnet-beta.solana.com".to_string()];
        let router = RpcRouter::new(&urls).unwrap();
        assert_eq!(router.endpoint_count(), 1);
        assert_eq!(router.healthy_count(), 1);
    }

    #[test]
    fn test_rpc_router_requires_endpoints() {
        let urls: Vec<String> = vec![];
        assert!(RpcRouter::new(&urls).is_err());
    }
}
