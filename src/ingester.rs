use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Per-RPC-call HTTP timeout. Without this, a stalled provider can hang the
/// whole publisher loop indefinitely (observed 2026-04-29: publisher froze
/// for 1+ hour with zero log lines after a cascade of 429/403s left every
/// RPC call awaiting a response that never came).
const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A single RPC endpoint with health tracking
/// Sidelined endpoints get re-evaluated after this cooldown — without it
/// a single rate-limit hour permanently retires a provider since
/// sidelined endpoints see no traffic and never get a chance to call
/// record_success(). 5 min matches typical free-tier rate-limit windows.
const SIDELINE_COOLDOWN_SECS: u64 = 300;

struct Endpoint {
    url: String,
    client: RpcClient,
    request_count: AtomicU64,
    error_count: AtomicU64,
    /// Lifetime success count — does not reset on transient errors.
    success_total: AtomicU64,
    /// Lifetime failure count — does not reset on transient successes.
    failure_total: AtomicU64,
    healthy: AtomicBool,
    /// Unix-second timestamp when this endpoint was last marked unhealthy.
    /// Compared against SIDELINE_COOLDOWN_SECS to time-bound the sideline.
    sidelined_at: AtomicU64,
}

impl Endpoint {
    fn new(url: &str) -> Self {
        let client = RpcClient::new_with_timeout_and_commitment(
            url.to_string(),
            RPC_CALL_TIMEOUT,
            CommitmentConfig::confirmed(),
        );
        Self {
            url: url.to_string(),
            client,
            request_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            success_total: AtomicU64::new(0),
            failure_total: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
            sidelined_at: AtomicU64::new(0),
        }
    }

    fn record_success(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.success_total.fetch_add(1, Ordering::Relaxed);
        self.healthy.store(true, Ordering::Relaxed);
        self.sidelined_at.store(0, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.error_count.fetch_add(1, Ordering::Relaxed);
        self.failure_total.fetch_add(1, Ordering::Relaxed);
        if self.error_count.load(Ordering::Relaxed) > 3 {
            if self.healthy.swap(false, Ordering::Relaxed) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                self.sidelined_at.store(now, Ordering::Relaxed);
            }
        }
    }

    /// Health check with auto-rehab. Endpoints sidelined more than
    /// SIDELINE_COOLDOWN_SECS ago get a probationary reset — error
    /// counter cleared, healthy flag flipped on. If the endpoint is
    /// genuinely still down the next request will fail and re-sideline
    /// it; if it's recovered we get capacity back without operator
    /// intervention.
    fn is_healthy(&self) -> bool {
        if self.healthy.load(Ordering::Relaxed) {
            return true;
        }
        let sidelined_at = self.sidelined_at.load(Ordering::Relaxed);
        if sidelined_at == 0 {
            return false;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(sidelined_at) >= SIDELINE_COOLDOWN_SECS {
            self.error_count.store(0, Ordering::Relaxed);
            self.sidelined_at.store(0, Ordering::Relaxed);
            self.healthy.store(true, Ordering::Relaxed);
            tracing::info!(
                "RPC endpoint {} cooldown elapsed — back in rotation",
                mask_url(&self.url)
            );
            return true;
        }
        false
    }

    fn reset_errors(&self) {
        self.error_count.store(0, Ordering::Relaxed);
        self.healthy.store(true, Ordering::Relaxed);
        self.sidelined_at.store(0, Ordering::Relaxed);
    }
}

/// Per-endpoint health snapshot for diagnostics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EndpointStats {
    pub url: String,
    pub healthy: bool,
    pub success_total: u64,
    pub failure_total: u64,
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

    /// Get the next healthy endpoint, round-robin with failover.
    /// Returns (endpoint_index, client) so callers can attribute health records
    /// to the actual endpoint used, not the one `current` happened to point to.
    fn next_client(&self) -> Option<(usize, &RpcClient)> {
        let len = self.endpoints.len();
        let start = self.current.fetch_add(1, Ordering::Relaxed) % len;

        for i in 0..len {
            let idx = (start + i) % len;
            if self.endpoints[idx].is_healthy() {
                return Some((idx, &self.endpoints[idx].client));
            }
        }

        // All unhealthy — reset all and try first
        tracing::warn!("All RPC endpoints unhealthy, resetting");
        for ep in &self.endpoints {
            ep.reset_errors();
        }
        let idx = start % len;
        Some((idx, &self.endpoints[idx].client))
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

    fn record_failure(&self, idx: usize, error: &str) {
        if let Some(ep) = self.endpoints.get(idx) {
            ep.record_error();
            if is_rate_limited(error) {
                ep.error_count.store(4, Ordering::Relaxed);
                if ep.healthy.swap(false, Ordering::Relaxed) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    ep.sidelined_at.store(now, Ordering::Relaxed);
                }
                tracing::warn!(
                    "RPC endpoint rate-limited, sidelining provider: {}",
                    mask_url(&ep.url)
                );
            }
        }
    }

    pub fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    pub fn healthy_count(&self) -> usize {
        self.endpoints.iter().filter(|e| e.is_healthy()).count()
    }

    pub fn endpoint_stats(&self) -> Vec<EndpointStats> {
        self.endpoints
            .iter()
            .map(|e| EndpointStats {
                url: mask_url(&e.url),
                healthy: e.is_healthy(),
                success_total: e.success_total.load(Ordering::Relaxed),
                failure_total: e.failure_total.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Send a signed VersionedTransaction to the network via the best available endpoint.
    pub async fn send_versioned_transaction(
        &self,
        tx: &solana_sdk::transaction::VersionedTransaction,
    ) -> Result<Signature> {
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.send_transaction(tx).await {
                Ok(sig) => {
                    self.record_result(idx, true);
                    return Ok(sig);
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Get balance for a wallet
    pub async fn get_balance(&self, pubkey: &str) -> Result<u64> {
        let pk = Pubkey::from_str(pubkey).context("Invalid public key")?;
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_balance(&pk).await {
                Ok(balance) => {
                    self.record_result(idx, true);
                    return Ok(balance);
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    tracing::warn!(
                        "getBalance failed on endpoint {}: {}",
                        mask_url(&self.endpoints[idx].url),
                        err
                    );
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Get recent blockhash to verify connectivity
    pub async fn check_connection(&self) -> Result<bool> {
        let attempts = self.endpoints.len();
        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_latest_blockhash().await {
                Ok(_) => {
                    self.record_result(idx, true);
                    return Ok(true);
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    tracing::warn!(
                        "RPC connection check failed on endpoint {}: {}",
                        mask_url(&self.endpoints[idx].url),
                        err
                    );
                }
            }
        }
        Ok(false)
    }

    /// Get slot to check data freshness
    pub async fn get_slot(&self) -> Result<u64> {
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_slot().await {
                Ok(slot) => {
                    self.record_result(idx, true);
                    return Ok(slot);
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Get raw account info for a mint address — used to read authorities and extensions
    pub async fn get_account_info(&self, address: &str) -> Result<Option<Account>> {
        let pk = Pubkey::from_str(address).context("Invalid address")?;
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_account(&pk).await {
                Ok(account) => {
                    self.record_result(idx, true);
                    return Ok(Some(account));
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("AccountNotFound")
                        || err_str.contains("could not find account")
                    {
                        self.record_result(idx, true);
                        return Ok(None);
                    }
                    self.record_failure(idx, &err_str);
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Get token supply for a mint
    pub async fn get_token_supply(&self, mint: &str) -> Result<TokenSupplyInfo> {
        let pk = Pubkey::from_str(mint).context("Invalid mint address")?;
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_token_supply(&pk).await {
                Ok(supply) => {
                    self.record_result(idx, true);
                    return Ok(TokenSupplyInfo {
                        amount: supply.amount.parse::<u64>().unwrap_or(0),
                        decimals: supply.decimals,
                        ui_amount: supply.ui_amount.unwrap_or(0.0),
                    });
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Get largest token holders — critical for concentration analysis.
    /// Retries across endpoints on failure so a quota-exhausted provider
    /// (e.g. Helius 429) doesn't kill a scan when healthier peers exist.
    pub async fn get_token_largest_accounts(&self, mint: &str) -> Result<Vec<TokenHolderInfo>> {
        let pk = Pubkey::from_str(mint).context("Invalid mint address")?;
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_token_largest_accounts(&pk).await {
                Ok(accounts) => {
                    self.record_result(idx, true);
                    let holders: Vec<TokenHolderInfo> = accounts
                        .iter()
                        .map(|a| TokenHolderInfo {
                            address: a.address.clone(),
                            amount: a.amount.amount.parse::<u64>().unwrap_or(0),
                            ui_amount: a.amount.ui_amount.unwrap_or(0.0),
                            decimals: a.amount.decimals,
                        })
                        .collect();
                    return Ok(holders);
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    tracing::warn!(
                        "getTokenLargestAccounts failed on endpoint {}: {}",
                        mask_url(&self.endpoints[idx].url),
                        err
                    );
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Get recent transaction signatures for an address.
    /// Retries across endpoints on failure — same rationale as
    /// `get_token_largest_accounts`.
    pub async fn get_recent_signatures(
        &self,
        address: &str,
        limit: usize,
    ) -> Result<Vec<SignatureInfo>> {
        let pk = Pubkey::from_str(address).context("Invalid address")?;
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            let config = solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                limit: Some(limit),
                ..Default::default()
            };
            match client
                .get_signatures_for_address_with_config(&pk, config)
                .await
            {
                Ok(sigs) => {
                    self.record_result(idx, true);
                    let infos: Vec<SignatureInfo> = sigs
                        .iter()
                        .map(|s| SignatureInfo {
                            signature: s.signature.clone(),
                            slot: s.slot,
                            block_time: s.block_time,
                            err: s.err.is_some(),
                        })
                        .collect();
                    return Ok(infos);
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    tracing::warn!(
                        "getSignaturesForAddress failed on endpoint {}: {}",
                        mask_url(&self.endpoints[idx].url),
                        err
                    );
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Return the SPL token holdings (mint, ui_balance) of a wallet. Used by
    /// the smart-wallet tracker to detect when a watched address picks up a
    /// new mint. Filters to the SPL Token program; silently drops accounts
    /// whose parsed shape is unexpected.
    pub async fn get_wallet_token_holdings(&self, owner: &str) -> Result<Vec<(String, f64)>> {
        use solana_client::rpc_request::TokenAccountsFilter;
        const TOKEN_PROGRAM_IDS: [&str; 2] = [
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        ];

        let owner_pk = Pubkey::from_str(owner).context("invalid owner address")?;
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            let mut holdings_by_mint: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            let mut endpoint_ok = true;

            for program_id in TOKEN_PROGRAM_IDS {
                let program_pk =
                    Pubkey::from_str(program_id).expect("constant token program id is valid");
                let filter = TokenAccountsFilter::ProgramId(program_pk);
                match client.get_token_accounts_by_owner(&owner_pk, filter).await {
                    Ok(accounts) => {
                        for acc in accounts {
                            let Ok(v) = serde_json::to_value(&acc.account.data) else {
                                continue;
                            };
                            let info = &v["parsed"]["info"];
                            let mint = info["mint"].as_str().unwrap_or("").to_string();
                            let ui = info["tokenAmount"]["uiAmount"].as_f64().unwrap_or(0.0);
                            if !mint.is_empty() && ui > 0.0 {
                                *holdings_by_mint.entry(mint).or_insert(0.0) += ui;
                            }
                        }
                    }
                    Err(e) => {
                        let err = e.to_string();
                        self.record_failure(idx, &err);
                        tracing::warn!(
                            "getTokenAccountsByOwner failed on endpoint {} for program {}: {}",
                            mask_url(&self.endpoints[idx].url),
                            program_id,
                            err
                        );
                        last_err = Some(e.into());
                        endpoint_ok = false;
                    }
                }
            }

            if endpoint_ok {
                // Endpoint responded without error — empty map is valid (wallet holds nothing).
                self.record_result(idx, true);
                let mut holdings: Vec<(String, f64)> = holdings_by_mint.into_iter().collect();
                holdings.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                return Ok(holdings);
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Return the owner wallet for a given SPL token account. `getTokenLargestAccounts`
    /// gives us token-account addresses, not the wallets behind them — we need
    /// this resolution step before we can trace a holder's activity.
    pub async fn get_token_account_owner(&self, token_account: &str) -> Result<String> {
        let pk = Pubkey::from_str(token_account).context("invalid token account")?;
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_account(&pk).await {
                Ok(acc) => {
                    self.record_result(idx, true);
                    // SPL token account layout: mint[32] | owner[32] | ...
                    if acc.data.len() < 64 {
                        return Err(anyhow::anyhow!("token account data too short"));
                    }
                    let owner_bytes: [u8; 32] = acc.data[32..64]
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("owner slice conversion failed"))?;
                    return Ok(Pubkey::new_from_array(owner_bytes).to_string());
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Batch-resolve owner wallets for multiple SPL token accounts in a single
    /// `getMultipleAccounts` RPC call instead of N sequential `getAccountInfo`
    /// calls. Returns one `Option<String>` per input address — `None` when an
    /// account is missing or its layout is too short to parse. Chunks at 100
    /// to stay within standard RPC server limits.
    pub async fn get_multiple_token_account_owners(
        &self,
        token_accounts: &[String],
    ) -> Result<Vec<Option<String>>> {
        if token_accounts.is_empty() {
            return Ok(Vec::new());
        }
        let pks: Vec<Pubkey> = token_accounts
            .iter()
            .map(|a| Pubkey::from_str(a).context("invalid token account"))
            .collect::<Result<Vec<_>>>()?;

        let mut all_owners: Vec<Option<String>> = Vec::with_capacity(pks.len());
        for chunk in pks.chunks(100) {
            let owners = self.resolve_owner_chunk(chunk).await?;
            all_owners.extend(owners);
        }
        Ok(all_owners)
    }

    /// Generic batched account-info fetch. Same chunk + failover pattern
    /// as the SPL-owner specialization, but returns raw Account so callers
    /// can parse arbitrary on-chain layouts (bonding-curve PDAs, etc.).
    pub async fn get_multiple_accounts(
        &self,
        addresses: &[String],
    ) -> Result<Vec<Option<Account>>> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }
        let pks: Vec<Pubkey> = addresses
            .iter()
            .map(|a| Pubkey::from_str(a).context("invalid address"))
            .collect::<Result<Vec<_>>>()?;

        let mut all: Vec<Option<Account>> = Vec::with_capacity(pks.len());
        for chunk in pks.chunks(100) {
            let attempts = self.endpoints.len();
            let mut last_err: Option<anyhow::Error> = None;
            let mut got: Option<Vec<Option<Account>>> = None;
            for _ in 0..attempts {
                let (idx, client) = self.next_client().context("No RPC endpoints available")?;
                match client.get_multiple_accounts(chunk).await {
                    Ok(accounts) => {
                        self.record_result(idx, true);
                        got = Some(accounts);
                        break;
                    }
                    Err(e) => {
                        let err = e.to_string();
                        self.record_failure(idx, &err);
                        last_err = Some(e.into());
                    }
                }
            }
            match got {
                Some(v) => all.extend(v),
                None => return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed"))),
            }
        }
        Ok(all)
    }

    async fn resolve_owner_chunk(&self, pks: &[Pubkey]) -> Result<Vec<Option<String>>> {
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_multiple_accounts(pks).await {
                Ok(accounts) => {
                    self.record_result(idx, true);
                    return Ok(accounts
                        .into_iter()
                        .map(|opt| {
                            let acc = opt?;
                            if acc.data.len() < 64 {
                                return None;
                            }
                            // SPL token account layout: mint[32] | owner[32] | ...
                            let bytes: [u8; 32] = acc.data[32..64].try_into().ok()?;
                            Some(Pubkey::new_from_array(bytes).to_string())
                        })
                        .collect());
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Return the net SPL token balance changes for a transaction — one entry
    /// per (owner, mint) pair that had a pre/post delta. Used by `whale_trace`
    /// and `sniper_cohort` to attribute buys vs sells from raw chain data
    /// without needing an enhanced-API wrapper.
    pub async fn get_tx_balance_changes(&self, signature: &str) -> Result<Vec<TokenBalanceChange>> {
        let sig = Signature::from_str(signature).context("invalid signature")?;
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_transaction_with_config(&sig, cfg).await {
                Ok(tx) => {
                    self.record_result(idx, true);
                    let Some(meta) = tx.transaction.meta else {
                        return Ok(Vec::new());
                    };
                    let pre = match meta.pre_token_balances {
                        OptionSerializer::Some(v) => v,
                        _ => Vec::new(),
                    };
                    let post = match meta.post_token_balances {
                        OptionSerializer::Some(v) => v,
                        _ => Vec::new(),
                    };
                    // Key balances by (owner, mint) so we can diff post vs pre.
                    let mut map: std::collections::HashMap<(String, String), (f64, f64)> =
                        std::collections::HashMap::new();
                    for b in &pre {
                        let owner = match &b.owner {
                            OptionSerializer::Some(o) => o.clone(),
                            _ => continue,
                        };
                        let ui = b.ui_token_amount.ui_amount.unwrap_or(0.0);
                        map.entry((owner, b.mint.clone()))
                            .and_modify(|v| v.0 = ui)
                            .or_insert((ui, 0.0));
                    }
                    for b in &post {
                        let owner = match &b.owner {
                            OptionSerializer::Some(o) => o.clone(),
                            _ => continue,
                        };
                        let ui = b.ui_token_amount.ui_amount.unwrap_or(0.0);
                        map.entry((owner, b.mint.clone()))
                            .and_modify(|v| v.1 = ui)
                            .or_insert((0.0, ui));
                    }
                    let mut out = Vec::new();
                    for ((owner, mint), (pre_ui, post_ui)) in map {
                        let delta = post_ui - pre_ui;
                        if delta.abs() < f64::EPSILON {
                            continue;
                        }
                        out.push(TokenBalanceChange {
                            owner,
                            mint,
                            pre_ui_amount: pre_ui,
                            post_ui_amount: post_ui,
                            delta_ui: delta,
                        });
                    }
                    return Ok(out);
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Combined wallet-centric summary of a transaction — both the native
    /// SOL delta for the owner's account AND every SPL token delta touching
    /// that owner. Used by the publisher to derive trade cost basis from
    /// chain data (SOL out + token in = buy; the inverse = sell).
    pub async fn get_tx_wallet_summary(
        &self,
        signature: &str,
        owner: &str,
    ) -> Result<WalletTxSummary> {
        let sig = Signature::from_str(signature).context("invalid signature")?;
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_transaction_with_config(&sig, cfg).await {
                Ok(tx) => {
                    self.record_result(idx, true);

                    // Account keys are needed to map owner → index for the
                    // native SOL balance vectors. Raw (UiMessage::Raw)
                    // carries them as strings; Parsed wraps them.
                    let account_keys: Vec<String> = match &tx.transaction.transaction {
                        solana_transaction_status::EncodedTransaction::Json(ui_tx) => {
                            match &ui_tx.message {
                                solana_transaction_status::UiMessage::Raw(raw) => {
                                    raw.account_keys.clone()
                                }
                                solana_transaction_status::UiMessage::Parsed(p) => {
                                    p.account_keys.iter().map(|a| a.pubkey.clone()).collect()
                                }
                            }
                        }
                        _ => Vec::new(),
                    };

                    let Some(meta) = tx.transaction.meta else {
                        return Ok(WalletTxSummary::default());
                    };

                    let owner_idx = account_keys.iter().position(|k| k == owner);
                    let sol_delta_ui = match owner_idx {
                        Some(i) => {
                            let pre = meta.pre_balances.get(i).copied().unwrap_or(0) as i64;
                            let post = meta.post_balances.get(i).copied().unwrap_or(0) as i64;
                            (post - pre) as f64 / 1e9
                        }
                        None => 0.0,
                    };

                    // Token deltas, filtered to our owner.
                    let pre_tok = match meta.pre_token_balances {
                        OptionSerializer::Some(v) => v,
                        _ => Vec::new(),
                    };
                    let post_tok = match meta.post_token_balances {
                        OptionSerializer::Some(v) => v,
                        _ => Vec::new(),
                    };
                    let mut map: std::collections::HashMap<String, (f64, f64)> =
                        std::collections::HashMap::new();
                    for b in &pre_tok {
                        let o = match &b.owner {
                            OptionSerializer::Some(o) => o.as_str(),
                            _ => continue,
                        };
                        if o != owner {
                            continue;
                        }
                        let ui = b.ui_token_amount.ui_amount.unwrap_or(0.0);
                        map.entry(b.mint.clone())
                            .and_modify(|v| v.0 = ui)
                            .or_insert((ui, 0.0));
                    }
                    for b in &post_tok {
                        let o = match &b.owner {
                            OptionSerializer::Some(o) => o.as_str(),
                            _ => continue,
                        };
                        if o != owner {
                            continue;
                        }
                        let ui = b.ui_token_amount.ui_amount.unwrap_or(0.0);
                        map.entry(b.mint.clone())
                            .and_modify(|v| v.1 = ui)
                            .or_insert((0.0, ui));
                    }
                    let mut token_deltas = Vec::new();
                    for (mint, (pre_ui, post_ui)) in map {
                        let delta = post_ui - pre_ui;
                        if delta.abs() < f64::EPSILON {
                            continue;
                        }
                        token_deltas.push((mint, delta));
                    }
                    let fee_payer = account_keys.first().cloned().unwrap_or_default();
                    return Ok(WalletTxSummary {
                        sol_delta_ui,
                        token_deltas,
                        fee_payer,
                    });
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }

    /// Return the unique SPL mint addresses referenced by a transaction's
    /// token balance changes. Used by discovery to extract tokens involved
    /// in a program's recent activity without needing a provider-specific
    /// enhanced API — works against any standard Solana RPC.
    pub async fn get_transaction_mints(&self, signature: &str) -> Result<Vec<String>> {
        let sig = Signature::from_str(signature).context("invalid signature")?;
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        let attempts = self.endpoints.len();
        let mut last_err: Option<anyhow::Error> = None;

        for _ in 0..attempts {
            let (idx, client) = self.next_client().context("No RPC endpoints available")?;
            match client.get_transaction_with_config(&sig, cfg).await {
                Ok(tx) => {
                    self.record_result(idx, true);
                    let mut mints: Vec<String> = Vec::new();
                    if let Some(meta) = tx.transaction.meta {
                        if let OptionSerializer::Some(balances) = meta.post_token_balances {
                            for b in balances {
                                if !mints.contains(&b.mint) {
                                    mints.push(b.mint);
                                }
                            }
                        }
                        if let OptionSerializer::Some(balances) = meta.pre_token_balances {
                            for b in balances {
                                if !mints.contains(&b.mint) {
                                    mints.push(b.mint);
                                }
                            }
                        }
                    }
                    return Ok(mints);
                }
                Err(e) => {
                    let err = e.to_string();
                    self.record_failure(idx, &err);
                    tracing::debug!(
                        "getTransaction failed on endpoint {}: {}",
                        mask_url(&self.endpoints[idx].url),
                        err
                    );
                    last_err = Some(e.into());
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all endpoints failed")))
    }
}

// -- Domain types for chain data --

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenSupplyInfo {
    pub amount: u64,
    pub decimals: u8,
    pub ui_amount: f64,
}

/// Wallet-centric view of a single transaction — the native SOL delta for
/// the queried owner plus every SPL token delta touching that owner. Used
/// by the publisher to derive trade cost basis (SOL out + token in = buy).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WalletTxSummary {
    pub sol_delta_ui: f64,
    pub token_deltas: Vec<(String, f64)>,
    /// First account key of the transaction — by SVM convention this is the
    /// fee payer. For a fresh wallet's oldest tx (account creation), this is
    /// the wallet that funded it. Used by `launch_forensics::compute_insider_pct`.
    pub fee_payer: String,
}

/// A single (owner, mint) balance delta inside a transaction — the raw
/// primitive that lets `whale_trace` attribute net buys vs net sells from
/// chain data alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBalanceChange {
    pub owner: String,
    pub mint: String,
    pub pre_ui_amount: f64,
    pub post_ui_amount: f64,
    pub delta_ui: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenHolderInfo {
    pub address: String,
    pub amount: u64,
    pub ui_amount: f64,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignatureInfo {
    pub signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub err: bool,
}

/// Parsed mint account data — extracted from raw account bytes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintInfo {
    pub address: String,
    pub supply: u64,
    pub decimals: u8,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
    pub is_token_2022: bool,
    /// Token-2022 extensions discovered on the mint — empty for classic SPL.
    /// Critical ones are surfaced on dedicated fields below; this list keeps
    /// everything for display/debugging.
    #[serde(default)]
    pub extensions: Vec<String>,
    /// PermanentDelegate authority — can move/burn ANY account's tokens. Veto.
    #[serde(default)]
    pub permanent_delegate: Option<String>,
    /// TransferHook program — runs custom code on every transfer. Flag.
    #[serde(default)]
    pub transfer_hook_program: Option<String>,
    /// TransferFeeConfig — skims on every transfer. Flag with basis points.
    #[serde(default)]
    pub transfer_fee_bps: Option<u16>,
    /// Mints that freeze new accounts by default are honeypots.
    #[serde(default)]
    pub default_frozen: bool,
    #[serde(default)]
    pub non_transferable: bool,
}

// Token-2022 extension type IDs (stable from spl-token-2022)
const EXT_UNINITIALIZED: u16 = 0;
const EXT_TRANSFER_FEE_CONFIG: u16 = 1;
const EXT_MINT_CLOSE_AUTHORITY: u16 = 3;
const EXT_DEFAULT_ACCOUNT_STATE: u16 = 6;
const EXT_NON_TRANSFERABLE: u16 = 9;
const EXT_INTEREST_BEARING: u16 = 10;
const EXT_PERMANENT_DELEGATE: u16 = 12;
const EXT_TRANSFER_HOOK: u16 = 14;
const EXT_METADATA_POINTER: u16 = 18;
const EXT_TOKEN_METADATA: u16 = 19;
const EXT_GROUP_POINTER: u16 = 20;

fn ext_name(id: u16) -> &'static str {
    match id {
        EXT_TRANSFER_FEE_CONFIG => "TransferFeeConfig",
        EXT_MINT_CLOSE_AUTHORITY => "MintCloseAuthority",
        EXT_DEFAULT_ACCOUNT_STATE => "DefaultAccountState",
        EXT_NON_TRANSFERABLE => "NonTransferable",
        EXT_INTEREST_BEARING => "InterestBearingConfig",
        EXT_PERMANENT_DELEGATE => "PermanentDelegate",
        EXT_TRANSFER_HOOK => "TransferHook",
        EXT_METADATA_POINTER => "MetadataPointer",
        EXT_TOKEN_METADATA => "TokenMetadata",
        EXT_GROUP_POINTER => "GroupPointer",
        _ => "Unknown",
    }
}

/// Token-2022 mint account layout:
/// bytes 0..82   — base mint data (same as SPL)
/// bytes 82..165 — padding to the account-data-length boundary
/// byte 165      — AccountType discriminator (1 = Mint)
/// bytes 166..   — TLV extensions (type u16 LE, length u16 LE, data)
const T22_ACCOUNT_TYPE_OFFSET: usize = 165;
const T22_EXTENSIONS_OFFSET: usize = 166;

/// Parse a Solana SPL Token mint account from raw bytes
pub fn parse_mint_account(address: &str, data: &[u8], owner: &Pubkey) -> Option<MintInfo> {
    // SPL Token program ID
    let spl_token = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").ok()?;
    // Token-2022 program ID
    let token_2022 = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").ok()?;

    let is_token_2022 = *owner == token_2022;
    let is_spl_token = *owner == spl_token;

    if !is_spl_token && !is_token_2022 {
        return None;
    }

    // Mint account layout (first 82 bytes are the same for both):
    // [0..4]   - mint_authority COption (4 bytes tag + 32 bytes pubkey)
    // [36..44] - supply (u64 LE)
    // [44]     - decimals (u8)
    // [45]     - is_initialized (bool)
    // [46..82] - freeze_authority COption (4 bytes tag + 32 bytes pubkey)
    if data.len() < 82 {
        return None;
    }

    let mint_authority_tag = u32::from_le_bytes(data[0..4].try_into().ok()?);
    let mint_authority = if mint_authority_tag == 1 {
        let key = Pubkey::try_from(&data[4..36]).ok()?;
        Some(key.to_string())
    } else {
        None
    };

    let supply = u64::from_le_bytes(data[36..44].try_into().ok()?);
    let decimals = data[44];
    let _is_initialized = data[45] != 0;

    let freeze_authority_tag = u32::from_le_bytes(data[46..50].try_into().ok()?);
    let freeze_authority = if freeze_authority_tag == 1 {
        let key = Pubkey::try_from(&data[50..82]).ok()?;
        Some(key.to_string())
    } else {
        None
    };

    let mut info = MintInfo {
        address: address.to_string(),
        supply,
        decimals,
        mint_authority,
        freeze_authority,
        is_token_2022,
        extensions: Vec::new(),
        permanent_delegate: None,
        transfer_hook_program: None,
        transfer_fee_bps: None,
        default_frozen: false,
        non_transferable: false,
    };

    if is_token_2022 && data.len() > T22_EXTENSIONS_OFFSET {
        let account_type = data[T22_ACCOUNT_TYPE_OFFSET];
        // AccountType 1 = Mint
        if account_type == 1 {
            let mut cursor = T22_EXTENSIONS_OFFSET;
            while cursor + 4 <= data.len() {
                let ext_type = u16::from_le_bytes([data[cursor], data[cursor + 1]]);
                let ext_len = u16::from_le_bytes([data[cursor + 2], data[cursor + 3]]) as usize;
                cursor += 4;
                if ext_type == EXT_UNINITIALIZED {
                    break;
                }
                if cursor + ext_len > data.len() {
                    break;
                }
                let slice = &data[cursor..cursor + ext_len];
                info.extensions.push(ext_name(ext_type).to_string());
                match ext_type {
                    EXT_PERMANENT_DELEGATE => {
                        if slice.len() == 32 {
                            if let Ok(pk) = Pubkey::try_from(slice) {
                                info.permanent_delegate = Some(pk.to_string());
                            }
                        }
                    }
                    EXT_TRANSFER_HOOK => {
                        // layout: authority (32) + program_id (32)
                        if slice.len() >= 64 {
                            if let Ok(pk) = Pubkey::try_from(&slice[32..64]) {
                                // All-zero pubkey means hook disabled
                                let all_zero = slice[32..64].iter().all(|b| *b == 0);
                                if !all_zero {
                                    info.transfer_hook_program = Some(pk.to_string());
                                }
                            }
                        }
                    }
                    EXT_TRANSFER_FEE_CONFIG => {
                        // TransferFeeConfig layout includes current fee basis_points at offset:
                        //   transfer_fee_config_authority (32) + withdraw_authority (32) +
                        //   withheld_amount (8) + older_fee (TransferFee = 24 bytes) +
                        //   newer_fee (TransferFee). TransferFee = { epoch(8), maximum_fee(8), basis_points(2) }.
                        // We read the newer_fee basis_points if buffer is big enough.
                        if slice.len() >= 108 {
                            let bps = u16::from_le_bytes([slice[106], slice[107]]);
                            info.transfer_fee_bps = Some(bps);
                        }
                    }
                    EXT_DEFAULT_ACCOUNT_STATE => {
                        // 1 byte: 0 = Uninitialized, 1 = Initialized, 2 = Frozen
                        if !slice.is_empty() && slice[0] == 2 {
                            info.default_frozen = true;
                        }
                    }
                    EXT_NON_TRANSFERABLE => {
                        info.non_transferable = true;
                    }
                    _ => {}
                }
                cursor += ext_len;
            }
        }
    }

    Some(info)
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

/// Should this error sideline the endpoint immediately, instead of waiting
/// for the 3-strike error_count path? Catches both transient throttles
/// and structural breakage (expired keys, plan-restricted methods, dead
/// TLS subscriptions) — anything where retrying the same endpoint inside
/// the same cycle is just wasted RTT.
fn is_rate_limited(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    error.contains("429")
        || error.contains("403")
        || lowered.contains("too many requests")
        || lowered.contains("rate limit")
        || lowered.contains("monthly capacity")
        || lowered.contains("plan upgrade")
        || lowered.contains("tlsv1 alert")
        || lowered.contains("ssl routines")
        || lowered.contains("forbidden")
}

/// Resolve endpoint URLs, substituting environment variables
/// Supports ${VAR_NAME} syntax in endpoint strings
pub fn resolve_endpoints(endpoints: &[String]) -> Vec<String> {
    endpoints.iter().map(|ep| resolve_env_vars(ep)).collect()
}

pub fn resolve_env_vars(s: &str) -> String {
    let mut result = s.to_string();
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            let var_name = &result[start + 2..start + end];
            let value = std::env::var(var_name).unwrap_or_default();
            result = format!(
                "{}{}{}",
                &result[..start],
                value,
                &result[start + end + 1..]
            );
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
