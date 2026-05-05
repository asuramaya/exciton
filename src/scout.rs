//! Scout tools — pure data extractors.
//!
//! The intelligence lives upstream of this module (in the operator or the
//! conversational LLM that calls us via MCP). Our job is deterministic:
//! pull structured facts from on-chain + DexScreener + the project's own
//! website, and return them as JSON-serializable structs. No LLM calls,
//! no heuristics that smuggle a thesis, no marketing fluff.

use crate::db::Db;
use crate::ingester::RpcRouter;
use crate::market;
use anyhow::Result;
use once_cell::sync::Lazy;
use serde::Serialize;
use std::sync::Arc;

/// Shared client for project-website fetches. Browser UA + 5-redirect
/// limit + 8s timeout so we behave like a normal user agent on the
/// operator pages we audit.
static WEBSITE_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 \
             (KHTML, like Gecko) Photon/1.0",
        )
        .timeout(std::time::Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .expect("scout WEBSITE_CLIENT init")
});

// Cap on bytes of website text we return — enough for a human or LLM reader
// to extract a thesis, short enough that one scout response fits in a single
// Telegram message and doesn't balloon the MCP payload.
const WEBSITE_TEXT_CAP: usize = 6000;

/// Deployer-wallet profile for a single mint. Populated from on-chain data
/// only — we already stamp the deployer at first-scan in `tokens.deployer_*`,
/// so here we just refresh their current balance and recent activity.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DeployerProfile {
    pub deployer_address: String,
    pub initial_balance_ui: f64,
    pub current_balance_ui: f64,
    /// Percentage of the launch bag sold. Negative means they added
    /// (e.g., buying back from the market). 0 if initial is unknown.
    pub pct_sold: f64,
    pub tx_count_24h: i32,
    pub tx_count_7d: i32,
    /// Seconds since their most recent signature. None if we couldn't
    /// find any activity at all.
    pub seconds_since_last_tx: Option<i64>,
    /// `true` when there was at least one signature in the last 48 hours.
    pub active: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WebsiteFetch {
    pub url: String,
    pub status: u16,
    /// Stripped, whitespace-collapsed readable text. Capped at
    /// `WEBSITE_TEXT_CAP` bytes. Empty when fetch failed or the page was
    /// unparseable — the `status` field carries the HTTP code for diagnosis.
    pub text: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScoutReport {
    pub mint: String,
    pub symbol: String,
    pub name: String,
    pub mcap_usd: f64,
    pub liquidity_usd: f64,
    pub pair_dex: String,
    pub deployer: Option<DeployerProfile>,
    pub website: Option<WebsiteFetch>,
    /// Verbatim social URLs DexScreener has on file. We don't try to scrape
    /// them here — the operator reads them manually when deciding a call.
    pub socials: Vec<(String, String)>,
}

/// Pull the deployer profile for a mint. Returns `Ok(None)` when we've
/// never recorded a deployer for this token (fresh mint, not yet scanned).
pub async fn scout_deployer(
    mint: &str,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
) -> Result<Option<DeployerProfile>> {
    let Some((deployer, initial)) = db.get_deployer(mint)? else {
        return Ok(None);
    };
    if deployer.is_empty() {
        return Ok(None);
    }

    // Current balance: the deployer's SPL holdings include every mint they
    // hold. Pull all holdings once, find ours.
    let holdings = rpc
        .get_wallet_token_holdings(&deployer)
        .await
        .unwrap_or_default();
    let current = holdings
        .iter()
        .find(|(m, _)| m == mint)
        .map(|(_, b)| *b)
        .unwrap_or(0.0);

    let pct_sold = if initial > 0.0 {
        (1.0 - current / initial) * 100.0
    } else {
        0.0
    };

    // Recent signatures for the deployer wallet. 100 is a pragmatic cap —
    // enough to cover 7d of activity for a typical trader, small enough to
    // stay fast.
    let sigs = rpc
        .get_recent_signatures(&deployer, 100)
        .await
        .unwrap_or_default();

    let now = chrono::Utc::now().timestamp();
    let mut tx_24h = 0i32;
    let mut tx_7d = 0i32;
    let mut last_ts: Option<i64> = None;
    for s in &sigs {
        let Some(ts) = s.block_time else { continue };
        if last_ts.is_none_or(|prev| ts > prev) {
            last_ts = Some(ts);
        }
        let age = now - ts;
        if age <= 86_400 {
            tx_24h += 1;
        }
        if age <= 86_400 * 7 {
            tx_7d += 1;
        }
    }

    let seconds_since_last_tx = last_ts.map(|ts| now - ts);
    let active = seconds_since_last_tx.is_some_and(|s| s <= 86_400 * 2);

    Ok(Some(DeployerProfile {
        deployer_address: deployer,
        initial_balance_ui: initial,
        current_balance_ui: current,
        pct_sold,
        tx_count_24h: tx_24h,
        tx_count_7d: tx_7d,
        seconds_since_last_tx,
        active,
    }))
}

/// Fetch the project website (first URL on DexScreener `info.websites`),
/// strip HTML to readable text, truncate. Returns `Ok(None)` if no website
/// is registered. Never errors on a network failure — we report the
/// HTTP status so the caller can reason about dead sites.
pub async fn scout_website(url: &str) -> Result<WebsiteFetch> {
    let resp = WEBSITE_CLIENT.get(url).send().await;
    match resp {
        Ok(r) => {
            let status = r.status().as_u16();
            let body = r.text().await.unwrap_or_default();
            let stripped = strip_html(&body);
            let (text, truncated) = if stripped.len() > WEBSITE_TEXT_CAP {
                (stripped[..WEBSITE_TEXT_CAP].to_string(), true)
            } else {
                (stripped, false)
            };
            Ok(WebsiteFetch {
                url: url.to_string(),
                status,
                text,
                truncated,
            })
        }
        Err(e) => Ok(WebsiteFetch {
            url: url.to_string(),
            status: 0,
            text: format!("fetch failed: {}", e),
            truncated: false,
        }),
    }
}

/// Compose the full scout report for a mint — DexScreener market + deployer
/// profile + first-website fetch. Socials come through as raw URLs.
pub async fn scout(mint: &str, rpc: &Arc<RpcRouter>, db: &Arc<Db>) -> Result<ScoutReport> {
    let market_data = market::get_market(mint).await.ok().flatten();
    let deployer = scout_deployer(mint, rpc, db).await.ok().flatten();
    let website = if let Some(m) = &market_data {
        if let Some(url) = m.websites.first() {
            scout_website(url).await.ok()
        } else {
            None
        }
    } else {
        None
    };

    let (symbol, name, mcap, liq, dex, socials) = if let Some(m) = market_data {
        (
            m.symbol,
            m.name,
            m.mcap_usd,
            m.liquidity_usd,
            m.pair_dex,
            m.socials,
        )
    } else {
        Default::default()
    };

    Ok(ScoutReport {
        mint: mint.to_string(),
        symbol,
        name,
        mcap_usd: mcap,
        liquidity_usd: liq,
        pair_dex: dex,
        deployer,
        website,
        socials,
    })
}

/// Very small HTML-to-text: drop script/style blocks, replace tags with
/// spaces, decode a handful of common entities, collapse whitespace.
/// Good enough for the long-form narrative pages that crypto projects ship.
/// Intentionally not a full HTML parser — the input is adversarial and
/// sometimes malformed; we want something that never panics.
fn strip_html(html: &str) -> String {
    // Drop <script>…</script> and <style>…</style>.
    let mut cleaned = String::with_capacity(html.len());
    let mut depth_script = 0i32;
    let mut depth_style = 0i32;
    let mut i = 0;
    let bytes = html.as_bytes();
    while i < bytes.len() {
        if depth_script == 0 && starts_with_ci(bytes, i, b"<script") {
            depth_script = 1;
            i += 7;
            continue;
        }
        if depth_script > 0 && starts_with_ci(bytes, i, b"</script>") {
            depth_script = 0;
            i += 9;
            continue;
        }
        if depth_style == 0 && starts_with_ci(bytes, i, b"<style") {
            depth_style = 1;
            i += 6;
            continue;
        }
        if depth_style > 0 && starts_with_ci(bytes, i, b"</style>") {
            depth_style = 0;
            i += 8;
            continue;
        }
        if depth_script == 0 && depth_style == 0 {
            cleaned.push(bytes[i] as char);
        }
        i += 1;
    }

    // Replace tags with spaces.
    let tag_stripped: String = {
        let mut out = String::with_capacity(cleaned.len());
        let mut in_tag = false;
        for c in cleaned.chars() {
            if c == '<' {
                in_tag = true;
                out.push(' ');
            } else if c == '>' {
                in_tag = false;
                out.push(' ');
            } else if !in_tag {
                out.push(c);
            }
        }
        out
    };

    // Decode the few entities worth the effort.
    let decoded = tag_stripped
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'");

    // Collapse runs of whitespace to single spaces, preserving line breaks
    // so the reader can see paragraph structure.
    let mut out = String::with_capacity(decoded.len());
    let mut last_ws = true;
    let mut last_nl = false;
    for c in decoded.chars() {
        if c == '\n' || c == '\r' {
            if !last_nl {
                out.push('\n');
                last_nl = true;
                last_ws = true;
            }
        } else if c.is_whitespace() {
            if !last_ws {
                out.push(' ');
                last_ws = true;
            }
        } else {
            out.push(c);
            last_ws = false;
            last_nl = false;
        }
    }
    out.trim().to_string()
}

fn starts_with_ci(bytes: &[u8], pos: usize, needle: &[u8]) -> bool {
    if pos + needle.len() > bytes.len() {
        return false;
    }
    for (i, &nb) in needle.iter().enumerate() {
        let b = bytes[pos + i];
        if !b.eq_ignore_ascii_case(&nb) {
            return false;
        }
    }
    true
}

// =======================================================================
// Deep on-chain tools — whale_trace, lp_check, deployer_history,
// holder_evolution, sniper_cohort, cohort_overlap.
//
// All pure RPC + DB. No LLM. Each returns a structured JSON-serializable
// report so the caller (operator or conversational LLM) can synthesize.
// =======================================================================

/// One entry in the whale-behavior report — a top-N holder of the target
/// mint and what their recent activity says they're doing with the bag.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WhaleMove {
    pub token_account: String,
    pub owner: String,
    pub balance_ui: f64,
    pub pct_of_supply: f64,
    /// Net position change (ui units) in the last 24h, derived from
    /// diffing pre/post token balances across every relevant transaction.
    /// Positive = accumulating, negative = distributing.
    pub net_change_24h_ui: f64,
    pub net_change_7d_ui: f64,
    pub tx_count_24h: i32,
    pub tx_count_7d: i32,
    /// One of: `accumulating`, `distributing`, `idle`, `owner_unresolved`.
    pub action: String,
}

const SUPPLY_FALLBACK: f64 = 1_000_000_000.0;

/// For each of the top-10 token-account holders of a mint, resolve the
/// wallet owner, fetch their recent signatures, diff per-tx balance changes
/// for this specific mint, and classify them as accumulating / distributing
/// / idle. Cap: 10 holders × 50 sigs each × ~up to 5 relevant tx → ~350
/// RPC calls in the worst case, gated behind on-demand invocation.
pub async fn whale_trace(mint: &str, rpc: &Arc<RpcRouter>) -> Result<Vec<WhaleMove>> {
    let holders = rpc.get_token_largest_accounts(mint).await?;
    let supply_ui = rpc
        .get_token_supply(mint)
        .await
        .map(|s| s.ui_amount)
        .unwrap_or(SUPPLY_FALLBACK);

    let now = chrono::Utc::now().timestamp();
    let cutoff_7d = now - 7 * 86_400;

    // Batch-resolve all top-10 holder owners in one RPC call.
    let top10: Vec<_> = holders.iter().take(10).collect();
    let top10_addrs: Vec<String> = top10.iter().map(|h| h.address.clone()).collect();
    let owner_batch = rpc
        .get_multiple_token_account_owners(&top10_addrs)
        .await
        .unwrap_or_else(|_| vec![None; top10.len()]);

    let mut moves: Vec<WhaleMove> = Vec::new();
    for (h, owner_opt) in top10.iter().zip(owner_batch) {
        let pct_of_supply = if supply_ui > 0.0 {
            h.ui_amount / supply_ui * 100.0
        } else {
            0.0
        };

        let owner = match owner_opt {
            Some(o) => o,
            None => {
                moves.push(WhaleMove {
                    token_account: h.address.clone(),
                    owner: String::new(),
                    balance_ui: h.ui_amount,
                    pct_of_supply,
                    action: "owner_unresolved".into(),
                    ..Default::default()
                });
                continue;
            }
        };

        let sigs = rpc
            .get_recent_signatures(&owner, 50)
            .await
            .unwrap_or_default();

        let mut net_24h = 0f64;
        let mut net_7d = 0f64;
        let mut n_24 = 0i32;
        let mut n_7 = 0i32;

        for sig in &sigs {
            let Some(ts) = sig.block_time else { continue };
            if ts < cutoff_7d {
                break;
            }
            let Ok(changes) = rpc.get_tx_balance_changes(&sig.signature).await else {
                continue;
            };
            // Sum deltas matching our (owner, mint) pair — there's usually at
            // most one, but pool routing can produce multiple entries.
            let delta: f64 = changes
                .iter()
                .filter(|c| c.owner == owner && c.mint == mint)
                .map(|c| c.delta_ui)
                .sum();
            if delta == 0.0 {
                continue;
            }
            net_7d += delta;
            n_7 += 1;
            if ts >= now - 86_400 {
                net_24h += delta;
                n_24 += 1;
            }
        }

        let action = if n_7 == 0 {
            "idle"
        } else if net_7d > 0.0 {
            "accumulating"
        } else {
            "distributing"
        };

        moves.push(WhaleMove {
            token_account: h.address.clone(),
            owner,
            balance_ui: h.ui_amount,
            pct_of_supply,
            net_change_24h_ui: net_24h,
            net_change_7d_ui: net_7d,
            tx_count_24h: n_24,
            tx_count_7d: n_7,
            action: action.to_string(),
        });
    }
    Ok(moves)
}

/// Binary-ish rug-risk filter: take the DexScreener pair address, derive
/// the LP mint by pattern-matching known pool-account layouts, then read
/// who holds the LP supply. Burnt (11…111) or held by a lock program =
/// liquidity cannot walk. Held by a wallet = the team can rug.
#[derive(Debug, Clone, Default, Serialize)]
pub struct LpStatus {
    pub pair_address: String,
    pub pair_dex: String,
    pub lp_mint: String,
    pub lp_supply_ui: f64,
    pub top_lp_holder: String,
    pub top_lp_holder_pct: f64,
    /// One of: `burnt`, `locked`, `held_by_wallet`, `unknown_layout`, `error`.
    pub verdict: String,
    pub detail: String,
}

const BURN_ADDRESS: &str = "1nc1nerator11111111111111111111111111111111";
const NULL_ADDRESS: &str = "11111111111111111111111111111111";
// Heuristic: common Solana programs that hold LP tokens as time-locked
// escrow — Streamflow, Jupiter-lock, Token-2022 non-transferable, etc.
// If the top LP holder is one of these program-owned accounts, LP is locked.
const KNOWN_LOCK_PROGRAMS: &[&str] = &[
    "strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m", // Streamflow
    "LocktDzaV1W2Bm9DeZeiyz4J9zs4fRqNiYndQoaDk2w", // Token-2022 lock
    "FLockeraLqZCTRgApZuJYRp13EsDWMCVU4EpkS3zYWTP9", // Flocker lock
];

pub async fn lp_check(
    pair_address: &str,
    pair_dex: &str,
    rpc: &Arc<RpcRouter>,
) -> Result<LpStatus> {
    let mut out = LpStatus {
        pair_address: pair_address.to_string(),
        pair_dex: pair_dex.to_string(),
        ..Default::default()
    };

    let pair_acc = match rpc.get_account_info(pair_address).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            out.verdict = "error".into();
            out.detail = "pair account not found".into();
            return Ok(out);
        }
        Err(e) => {
            out.verdict = "error".into();
            out.detail = format!("fetch pair account failed: {}", e);
            return Ok(out);
        }
    };

    // LP mint location varies by dex. Offsets verified empirically against
    // live pool accounts; when a dex ships a new layout we fall through to
    // `unknown_layout` rather than mis-parse.
    //   pumpswap pool layout: [disc:8][pool_bump:1][index:2][creator:32]
    //     [base_mint:32][quote_mint:32][lp_mint:32] → lp_mint at 107
    let lp_mint_offset: Option<usize> = match pair_dex {
        "raydium" => Some(192),
        "pumpswap" | "pumpfun" => Some(107),
        _ => None,
    };
    let Some(offset) = lp_mint_offset else {
        out.verdict = "unknown_layout".into();
        out.detail = format!("lp-mint offset not known for dex `{}`", pair_dex);
        return Ok(out);
    };
    if pair_acc.data.len() < offset + 32 {
        out.verdict = "unknown_layout".into();
        out.detail = format!(
            "pair account data length {} < expected offset {}+32",
            pair_acc.data.len(),
            offset
        );
        return Ok(out);
    }
    let lp_mint_bytes: [u8; 32] = pair_acc.data[offset..offset + 32]
        .try_into()
        .map_err(|_| anyhow::anyhow!("slice conversion failed"))?;
    let lp_mint = solana_sdk::pubkey::Pubkey::new_from_array(lp_mint_bytes).to_string();
    out.lp_mint = lp_mint.clone();

    // Now inspect LP mint supply + largest holders.
    let supply = rpc.get_token_supply(&lp_mint).await.ok();
    out.lp_supply_ui = supply.as_ref().map(|s| s.ui_amount).unwrap_or(0.0);

    // Pumpswap and some newer AMMs don't issue tradeable LP tokens — the
    // LP mint exists but supply is 0 because liquidity is tracked as
    // program-internal state, not as a transferable SPL token. That means
    // the classic "team dumps LP tokens" rug vector doesn't apply here;
    // the relevant risk is whether the pool program permits admin-initiated
    // withdrawal. That's a code-level question outside on-chain scope.
    if out.lp_supply_ui == 0.0 {
        out.verdict = "pool_program_native".into();
        out.detail = format!(
            "LP mint has zero supply — {} uses program-internal liquidity, \
             no transferable LP tokens exist",
            pair_dex
        );
        return Ok(out);
    }

    let lp_holders = rpc
        .get_token_largest_accounts(&lp_mint)
        .await
        .unwrap_or_default();
    let Some(top) = lp_holders.first() else {
        out.verdict = "unknown_layout".into();
        out.detail = "no LP holders found".into();
        return Ok(out);
    };
    let top_pct = if out.lp_supply_ui > 0.0 {
        top.ui_amount / out.lp_supply_ui * 100.0
    } else {
        0.0
    };
    out.top_lp_holder = top.address.clone();
    out.top_lp_holder_pct = top_pct;

    // Classify by resolving the top LP holder's owner.
    let owner = rpc
        .get_token_account_owner(&top.address)
        .await
        .unwrap_or_default();

    if owner == BURN_ADDRESS || owner == NULL_ADDRESS {
        out.verdict = "burnt".into();
        out.detail = format!("top LP holder ({:.1}%) owned by burn address", top_pct);
    } else if KNOWN_LOCK_PROGRAMS.contains(&owner.as_str()) {
        out.verdict = "locked".into();
        out.detail = format!("top LP holder owned by lock program {}", owner);
    } else {
        out.verdict = "held_by_wallet".into();
        out.detail = format!("top LP holder ({:.1}%) owned by wallet {}", top_pct, owner);
    }
    Ok(out)
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PastLaunch {
    pub mint: String,
    pub first_seen: i64,
    pub current_mcap_usd: f64,
    pub current_liquidity_usd: f64,
    pub current_pair_dex: String,
    pub symbol: String,
}

/// Track record for a deployer wallet, assembled from our own `tokens`
/// table. Only captures launches exciton has observed (post-tracking), not
/// their full career — honest limit, but enough to see a pattern once a
/// deployer has multiple tokens in our DB.
pub async fn deployer_history(deployer: &str, db: &Arc<Db>) -> Result<Vec<PastLaunch>> {
    let mints = db.get_tokens_by_deployer(deployer)?;
    let mut out = Vec::new();
    for (mint, first_seen) in mints {
        let mut entry = PastLaunch {
            mint: mint.clone(),
            first_seen,
            ..Default::default()
        };
        if let Ok(Some(m)) = crate::market::get_market(&mint).await {
            entry.current_mcap_usd = m.mcap_usd;
            entry.current_liquidity_usd = m.liquidity_usd;
            entry.current_pair_dex = m.pair_dex;
            entry.symbol = m.symbol;
        }
        out.push(entry);
    }
    // Newest first — most recent launches are most informative about
    // whether the deployer is actively shipping.
    out.sort_by(|a, b| b.first_seen.cmp(&a.first_seen));
    Ok(out)
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HolderEvolution {
    pub mint: String,
    pub window_hours: i64,
    pub prev_snapshot_ts: i64,
    pub curr_snapshot_ts: i64,
    pub entrants: Vec<(String, f64)>,
    pub exits: Vec<(String, f64)>,
    pub accumulators: Vec<(String, f64, f64)>, // (addr, prev_ui, curr_ui)
    pub distributors: Vec<(String, f64, f64)>,
    pub available: bool,
    pub note: String,
}

/// Compare the most-recent top-20 holder snapshot for a mint against the
/// oldest snapshot within the window. Needs the `top_holders_json` column
/// to have been populated by the scanner — for tokens scanned before that
/// column existed, we return `available: false`.
pub fn holder_evolution(mint: &str, window_hours: i64, db: &Arc<Db>) -> Result<HolderEvolution> {
    let mut out = HolderEvolution {
        mint: mint.to_string(),
        window_hours,
        ..Default::default()
    };
    let Some((curr_ts, curr_json)) = db.get_latest_top_holders(mint)? else {
        out.note = "no top-holder snapshots for this mint".into();
        return Ok(out);
    };
    out.curr_snapshot_ts = curr_ts;
    let cutoff = curr_ts - window_hours * 3600;
    let Some((prev_ts, prev_json)) = db.get_top_holders_at_or_before(mint, cutoff)? else {
        out.note = format!(
            "no snapshot ≥{}h before the latest — token too new for this window",
            window_hours
        );
        return Ok(out);
    };
    out.prev_snapshot_ts = prev_ts;

    let curr_map = parse_top_holders(&curr_json);
    let prev_map = parse_top_holders(&prev_json);
    if curr_map.is_empty() || prev_map.is_empty() {
        out.note = "stored snapshot was empty/unparseable".into();
        return Ok(out);
    }

    for (addr, &curr_ui) in &curr_map {
        match prev_map.get(addr) {
            None => out.entrants.push((addr.clone(), curr_ui)),
            Some(&prev_ui) => {
                if curr_ui > prev_ui {
                    out.accumulators.push((addr.clone(), prev_ui, curr_ui));
                } else if curr_ui < prev_ui {
                    out.distributors.push((addr.clone(), prev_ui, curr_ui));
                }
            }
        }
    }
    for (addr, &prev_ui) in &prev_map {
        if !curr_map.contains_key(addr) {
            out.exits.push((addr.clone(), prev_ui));
        }
    }
    out.available = true;
    Ok(out)
}

fn parse_top_holders(json: &str) -> std::collections::HashMap<String, f64> {
    let v: serde_json::Value = serde_json::from_str(json).unwrap_or(serde_json::Value::Null);
    let mut map = std::collections::HashMap::new();
    if let Some(arr) = v.as_array() {
        for item in arr {
            let addr = item
                .get("address")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            let ui = item
                .get("ui_amount")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);
            if !addr.is_empty() {
                map.insert(addr, ui);
            }
        }
    }
    map
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SniperCohort {
    pub mint: String,
    pub sniper_count: i32,
    pub still_in_top_100: i32,
    pub retention_pct: f64,
    pub available: bool,
    pub note: String,
}

/// Returns the early-buyer retention picture for a mint. Uses the
/// `sniper_cohort` table which gets populated on discovery — so only
/// tokens that entered our pipeline from their first hour have data.
/// Older tokens return `available: false`.
pub async fn sniper_cohort(mint: &str, rpc: &Arc<RpcRouter>, db: &Arc<Db>) -> Result<SniperCohort> {
    let mut out = SniperCohort {
        mint: mint.to_string(),
        ..Default::default()
    };
    let snipers = db.get_sniper_cohort(mint)?;
    if snipers.is_empty() {
        out.note = "no sniper cohort captured (mint predates tracking)".into();
        return Ok(out);
    }
    out.sniper_count = snipers.len() as i32;

    // Current top-100 holder owners. We resolve owners so the comparison
    // is wallet-vs-wallet, not token-account-vs-wallet.
    let top = rpc.get_token_largest_accounts(mint).await?;
    let top_addrs: Vec<String> = top.iter().take(100).map(|h| h.address.clone()).collect();
    let mut top_owners: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Ok(batch) = rpc.get_multiple_token_account_owners(&top_addrs).await {
        for owner in batch.into_iter().flatten() {
            top_owners.insert(owner);
        }
    }
    let still_in = snipers
        .iter()
        .filter(|s| top_owners.contains(s.as_str()))
        .count() as i32;
    out.still_in_top_100 = still_in;
    out.retention_pct = if out.sniper_count > 0 {
        still_in as f64 / out.sniper_count as f64 * 100.0
    } else {
        0.0
    };
    out.available = true;
    Ok(out)
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CohortOverlap {
    pub mint: String,
    pub reference_count: i32,
    /// For each top-20 holder, the reference mints they also hold.
    /// Keyed by holder owner address.
    pub holder_overlaps: Vec<HolderOverlap>,
    /// How many top-20 holders hit at least one reference mint.
    pub holders_with_any_overlap: i32,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct HolderOverlap {
    pub owner: String,
    pub mint_balance_ui: f64,
    pub overlapping_refs: Vec<String>,
}

/// For each top-20 holder of the target mint, look at their SPL portfolio
/// and report which of the user-curated reference mints they also hold.
/// Cheap: one `getTokenAccountsByOwner` per holder (20 calls) plus owner
/// resolution (another 20). No signatures fetched.
pub async fn cohort_overlap(
    mint: &str,
    rpc: &Arc<RpcRouter>,
    db: &Arc<Db>,
) -> Result<CohortOverlap> {
    let refs: std::collections::HashSet<String> = db.list_reference_mints()?.into_iter().collect();
    let mut out = CohortOverlap {
        mint: mint.to_string(),
        reference_count: refs.len() as i32,
        ..Default::default()
    };
    if refs.is_empty() {
        return Ok(out);
    }
    let holders = rpc.get_token_largest_accounts(mint).await?;
    let holder_addrs: Vec<String> = holders.iter().take(20).map(|h| h.address.clone()).collect();
    let owner_batch = rpc
        .get_multiple_token_account_owners(&holder_addrs)
        .await
        .unwrap_or_else(|_| vec![None; holder_addrs.len()]);
    for (h, owner_opt) in holders.iter().take(20).zip(owner_batch) {
        let owner = match owner_opt {
            Some(o) => o,
            None => continue,
        };
        let holdings = rpc
            .get_wallet_token_holdings(&owner)
            .await
            .unwrap_or_default();
        let overlaps: Vec<String> = holdings
            .iter()
            .filter(|(m, _)| refs.contains(m))
            .map(|(m, _)| m.clone())
            .collect();
        if !overlaps.is_empty() {
            out.holders_with_any_overlap += 1;
        }
        out.holder_overlaps.push(HolderOverlap {
            owner,
            mint_balance_ui: h.ui_amount,
            overlapping_refs: overlaps,
        });
    }
    Ok(out)
}
