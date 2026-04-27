//! Pump.fun bonding-curve coverage. Lets photon observe the 0→graduation
//! ride that's invisible to DexScreener.
//!
//! The pump.fun program (6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P)
//! creates one bonding-curve PDA per mint. Trades against the curve
//! atomically update its `virtual_*_reserves` (constant-product pricing)
//! and `real_*_reserves` (actual SOL collected, used to detect
//! graduation). At ~85 SOL collected the curve flips `complete = true`
//! and a PumpSwap pair is created; from there the token trades on
//! standard infrastructure DexScreener can index.
//!
//! The on-chain account layout (Anchor):
//! ```
//!   discriminator: [u8; 8]            // anchor discriminator
//!   virtual_token_reserves: u64 (LE)  // tokens still in the curve
//!   virtual_sol_reserves: u64 (LE)    // virtual SOL — drives price
//!   real_token_reserves: u64 (LE)     // tokens transferred out
//!   real_sol_reserves: u64 (LE)       // SOL accumulated
//!   token_total_supply: u64 (LE)      // total mint supply
//!   complete: bool                    // graduated flag (1 byte)
//! ```
//!
//! Derived signals:
//!   price_sol = virtual_sol / virtual_tok            (current curve price)
//!   fill_pct  = real_sol / GRADUATION_TARGET_SOL     (how full the curve is)
//!
//! Phase 6 uses these to observe early-stage pump.fun tokens before they
//! graduate, so a token like HENRY can be called near $35k mcap rather
//! than after the 27x run.

use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

/// Pump.fun program id. Owner of every bonding-curve PDA we care about.
pub const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

/// SOL-cap a curve must collect before it graduates to PumpSwap. Used
/// for `fill_pct`; the actual graduation threshold is enforced by the
/// program (the `complete` flag flips). Documented at ~85 SOL = ~$69k
/// at SOL ≈ $810. Treated as a soft display number, not a gate.
pub const GRADUATION_TARGET_SOL: f64 = 85.0;

/// Lamports per SOL — for `real_sol_reserves` lamport→SOL conversion.
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

/// Parsed bonding-curve account. All numeric fields are post-conversion
/// (SOL, not lamports; UI tokens, not raw atoms — assumes 6 decimals,
/// the pump.fun default).
#[derive(Debug, Clone)]
pub struct CurveState {
    /// Bonding-curve PDA address.
    pub curve: Pubkey,
    /// Mint this curve serves.
    pub mint: Pubkey,
    /// Virtual token reserves (UI units, ~6 decimals).
    pub virtual_token_reserves: f64,
    /// Virtual SOL reserves (SOL units).
    pub virtual_sol_reserves: f64,
    /// Real token reserves (UI units) — tokens transferred out of the curve.
    pub real_token_reserves: f64,
    /// Real SOL collected (SOL units) — drives the graduation condition.
    pub real_sol_reserves: f64,
    /// Total token supply (UI units) — pump.fun mints fix this at 1B.
    pub token_total_supply: f64,
    /// True once the curve graduated to PumpSwap. Once set, photon's
    /// curve observation drops the token; post-grad pipeline takes over.
    pub complete: bool,
}

impl CurveState {
    /// Current curve price in SOL per token: virtual_sol / virtual_tok.
    /// `0.0` when reserves are 0 (pre-init or post-drain — shouldn't
    /// normally happen for a live token, but we tolerate it).
    pub fn price_sol(&self) -> f64 {
        if self.virtual_token_reserves > 0.0 {
            self.virtual_sol_reserves / self.virtual_token_reserves
        } else {
            0.0
        }
    }

    /// Fill ratio against the graduation target. >1.0 means the curve
    /// has already crossed the SOL-cap (the `complete` flag may lag by
    /// a slot). Display only; the gate is `complete`.
    pub fn fill_pct(&self) -> f64 {
        if GRADUATION_TARGET_SOL > 0.0 {
            self.real_sol_reserves / GRADUATION_TARGET_SOL * 100.0
        } else {
            0.0
        }
    }

    /// Implied USD market cap from curve price + total supply + an
    /// externally-supplied SOL price. Pre-graduation tokens have no
    /// DexScreener mcap, so this is the only number we can quote.
    pub fn mcap_usd(&self, sol_price_usd: f64) -> f64 {
        self.price_sol() * self.token_total_supply * sol_price_usd
    }
}

/// Derive the bonding-curve PDA for a given mint. Anchor seeds:
///   [b"bonding-curve", mint.as_ref()]
/// Owner: PUMPFUN_PROGRAM.
pub fn curve_pda(mint: &Pubkey) -> Result<Pubkey> {
    let program = Pubkey::from_str(PUMPFUN_PROGRAM).context("invalid pump.fun program id")?;
    let (pda, _bump) = Pubkey::find_program_address(&[b"bonding-curve", mint.as_ref()], &program);
    Ok(pda)
}

/// Fetch + parse the bonding-curve account for a mint. Returns `None`
/// when the account doesn't exist (mint isn't pump.fun-issued, or the
/// curve was already swept after graduation in some edge cases).
pub async fn fetch_curve(mint: &str, rpc: &Arc<crate::ingester::RpcRouter>) -> Result<Option<CurveState>> {
    let mint_pk = Pubkey::from_str(mint).context("invalid mint")?;
    let curve = curve_pda(&mint_pk)?;
    let account = match rpc.get_account_info(&curve.to_string()).await? {
        Some(a) => a,
        None => return Ok(None),
    };
    parse_curve_account(mint_pk, curve, &account.data).map(Some)
}

/// Batched curve fetch — derives PDAs for every mint, makes one
/// `getMultipleAccounts` call per chunk of 100, returns one
/// `Option<CurveState>` per input mint. Lets the observation track
/// poll a few hundred curves per minute on a single RPC call instead
/// of hammering get_account_info N times.
pub async fn fetch_curves_batch(
    mints: &[String],
    rpc: &Arc<crate::ingester::RpcRouter>,
) -> Result<Vec<Option<CurveState>>> {
    if mints.is_empty() {
        return Ok(Vec::new());
    }
    // Build parallel arrays of mint PDAs + their derived curve PDAs.
    let mut mint_pks: Vec<Pubkey> = Vec::with_capacity(mints.len());
    let mut curve_addrs: Vec<String> = Vec::with_capacity(mints.len());
    for m in mints {
        let mp = Pubkey::from_str(m).context("invalid mint")?;
        let curve = curve_pda(&mp)?;
        curve_addrs.push(curve.to_string());
        mint_pks.push(mp);
    }
    let accounts = rpc.get_multiple_accounts(&curve_addrs).await?;
    let curve_pks: Vec<Pubkey> = curve_addrs
        .iter()
        .map(|s| Pubkey::from_str(s).expect("derived above"))
        .collect();
    let mut out = Vec::with_capacity(mints.len());
    for (i, opt) in accounts.into_iter().enumerate() {
        match opt {
            Some(acc) => match parse_curve_account(mint_pks[i], curve_pks[i], &acc.data) {
                Ok(s) => out.push(Some(s)),
                Err(_) => out.push(None),
            },
            None => out.push(None),
        }
    }
    Ok(out)
}

/// Parse the on-chain bytes of a bonding-curve account into a CurveState.
/// Mint + curve PDAs come from the caller since this layer doesn't know
/// the address it parsed.
fn parse_curve_account(mint: Pubkey, curve: Pubkey, data: &[u8]) -> Result<CurveState> {
    // Discriminator (8) + 5 × u64 (40) + bool (1) = 49 bytes minimum.
    if data.len() < 49 {
        anyhow::bail!(
            "curve account too small: got {} bytes, need ≥49",
            data.len()
        );
    }
    let read_u64 = |offset: usize| -> u64 {
        u64::from_le_bytes(data[offset..offset + 8].try_into().expect("len-checked above"))
    };
    let virtual_token_raw = read_u64(8);
    let virtual_sol_raw = read_u64(16);
    let real_token_raw = read_u64(24);
    let real_sol_raw = read_u64(32);
    let total_supply_raw = read_u64(40);
    let complete = data[48] != 0;
    // Pump.fun token decimals = 6, fixed for every mint.
    let tok_div = 1_000_000.0;
    Ok(CurveState {
        curve,
        mint,
        virtual_token_reserves: virtual_token_raw as f64 / tok_div,
        virtual_sol_reserves: virtual_sol_raw as f64 / LAMPORTS_PER_SOL,
        real_token_reserves: real_token_raw as f64 / tok_div,
        real_sol_reserves: real_sol_raw as f64 / LAMPORTS_PER_SOL,
        token_total_supply: total_supply_raw as f64 / tok_div,
        complete,
    })
}

/// Velocity-based gate signal computed from a curve's recent snapshot
/// stream. Inputs are tuples (timestamp_secs, real_sol, fill_pct) —
/// what `db.get_recent_curve_snapshots` returns. Higher score = more
/// momentum building on the curve. The auto-call gate fires when
/// `passes_gate(score)` holds and the curve hasn't graduated yet.
#[derive(Debug, Clone)]
pub struct CurveMomentum {
    /// SOL/min inflow over the most-recent observation window.
    pub velocity_sol_per_min: f64,
    /// Current fill percent (real_sol / 85 SOL).
    pub fill_pct: f64,
    /// Combined gate score, 0.0–100.0. Velocity-weighted, fill-modulated.
    pub alpha_score: f64,
    /// Snapshot count used for the calc. 0 means "not enough data".
    pub samples: usize,
}

/// Compute momentum from the latest snapshots. `snaps` is newest-first
/// (matches `db.get_recent_curve_snapshots` ordering). Returns
/// zero-valued CurveMomentum when there aren't enough samples or the
/// curve already graduated.
pub fn compute_momentum(
    snaps: &[(i64, f64, f64, f64, bool)], // (ts, real_sol, price_sol, fill_pct, complete)
) -> CurveMomentum {
    if snaps.len() < 2 {
        return CurveMomentum {
            velocity_sol_per_min: 0.0,
            fill_pct: snaps.first().map(|s| s.3).unwrap_or(0.0),
            alpha_score: 0.0,
            samples: snaps.len(),
        };
    }
    let newest = &snaps[0];
    let oldest = snaps.last().expect("len >= 2");
    let dt = (newest.0 - oldest.0).max(1) as f64;
    let dsol = newest.1 - oldest.1;
    let velocity_sol_per_min = (dsol / dt) * 60.0;
    let fill_pct = newest.3;
    // Score: velocity is the primary driver; fill range modulates.
    // Sweet spot is 5–50% filled (early enough to ride, mature enough
    // that price action isn't pure noise). Outside the band the score
    // is dampened.
    let velocity_component = (velocity_sol_per_min * 20.0).clamp(0.0, 80.0);
    let fill_band = if (5.0..=50.0).contains(&fill_pct) {
        20.0
    } else if (1.0..=70.0).contains(&fill_pct) {
        10.0
    } else {
        0.0
    };
    let alpha_score = if newest.4 { 0.0 } else { velocity_component + fill_band };
    CurveMomentum {
        velocity_sol_per_min,
        fill_pct,
        alpha_score,
        samples: snaps.len(),
    }
}

/// Curve-stage auto-call gate. True iff momentum is strong enough to
/// commit a SHORT call during the bonding-curve phase. Composed of:
///   - score ≥ 50.0 (combines velocity ≥ 1.5 SOL/min + fill in band)
///   - fill_pct in (5%, 50%) — pre-graduation sweet spot
///   - samples ≥ 3 — evidence the velocity is sustained
pub fn passes_gate(m: &CurveMomentum) -> bool {
    m.alpha_score >= 50.0
        && m.samples >= 3
        && m.fill_pct > 5.0
        && m.fill_pct < 50.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pda_is_deterministic() {
        // Just check it derives without panic — the actual PDA value
        // changes per mint, no fixed expectation.
        let mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        let pda = curve_pda(&mint).unwrap();
        // PDAs are 32 bytes; sanity check.
        assert_eq!(pda.to_bytes().len(), 32);
    }

    #[test]
    fn price_sol_handles_zero() {
        let s = CurveState {
            curve: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            virtual_token_reserves: 0.0,
            virtual_sol_reserves: 0.0,
            real_token_reserves: 0.0,
            real_sol_reserves: 0.0,
            token_total_supply: 1_000_000_000.0,
            complete: false,
        };
        assert_eq!(s.price_sol(), 0.0);
    }

    #[test]
    fn price_sol_basic() {
        let s = CurveState {
            curve: Pubkey::new_unique(),
            mint: Pubkey::new_unique(),
            virtual_token_reserves: 1_000_000_000.0,
            virtual_sol_reserves: 30.0,
            real_token_reserves: 200_000_000.0,
            real_sol_reserves: 20.0,
            token_total_supply: 1_000_000_000.0,
            complete: false,
        };
        // 30 SOL / 1B tokens = 30e-9 SOL per token
        assert!((s.price_sol() - 3e-8).abs() < 1e-12);
        // mcap at $810/SOL = 30 * 810 = 24300 (since price * total_supply = virtual_sol)
        assert!((s.mcap_usd(810.0) - 24_300.0).abs() < 1.0);
        // fill pct: 20 / 85 = 23.5%
        assert!((s.fill_pct() - (20.0 / 85.0 * 100.0)).abs() < 0.01);
    }
}
