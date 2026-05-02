//! Telegram notifier — autonomous channel posting for the scanner.
//!
//! Runs as a background task alongside `BackgroundScanner`. Reads recent
//! analyses and decides whether to:
//!   - Post a new WINNER card to the winners channel (rare)
//!   - Edit an existing WINNER card with new timeline entries (material change)
//!   - Demote a WINNER card when it fades
//!   - Open / edit the hourly ops digest in the ops chat
//!
//! All posting goes through Telegram's editMessageText pattern: each card is
//! one message that grows a timeline over its lifetime, preserving history.

use crate::config::TelegramConfig;
use crate::db::Db;
use crate::metadata::{self, TokenMeta};
use crate::signals::TokenAnalysis;
use crate::templates;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

// =============================================================================
// FRESHNESS DECAY — shared by scan() and the notifier. A single source of truth.
// Alerts under FRESH_SECS carry full weight. Between FRESH and STALE they decay
// linearly to 0.5. Past STALE they're suppressed (effective_confidence = 0).
// =============================================================================

pub const FRESH_SECS: i64 = 300;
pub const STALE_SECS: i64 = 1800;

pub fn decay_factor(age_seconds: i64) -> f64 {
    if age_seconds <= FRESH_SECS {
        1.0
    } else if age_seconds < STALE_SECS {
        1.0 - 0.5 * (age_seconds - FRESH_SECS) as f64 / (STALE_SECS - FRESH_SECS) as f64
    } else {
        0.0
    }
}

pub fn effective_confidence(raw: i32, age_seconds: i64) -> i32 {
    (raw as f64 * decay_factor(age_seconds)) as i32
}

// =============================================================================
// SIGNAL CRITERIA — locked with operator. A "signal" is a call that the token
// will make money; it is *not* a claim that it already did. The verdict evolves
// in-card: initial SIGNAL, optional update lines, FAILED if it collapses.
// Every one of these gates must hold to open a signal.
// =============================================================================

// Gates calibrated against the 31-call closed-call backtest (2026-04-22..28),
// recalibrated via gate-sweep analysis (2026-04-29), AGAIN lowered to
// match live distribution (2026-04-30), and ONCE MORE relaxed (2026-05-02)
// after the trailing-stop ladder shipped — the safety net is now strong
// enough that more permissive entries don't tank EV. Loss-side variance
// is bounded by the trail; the question is winner-side capture.
//
// 6h DB sample 2026-04-30:
//   conf >=80: 0 snapshots
//   conf 76-79: 0
//   conf 70-75: 19
//   conf 60-69: 37
//   conf <60:   38
//
// 70-floor was producing slow-fire pace and excluding the bulk of the
// healthy-classification mass (conf 60-69, 37/94 snapshots). Trailing
// stop catches the losers; raising the catch rate on the borderline
// 65-69 cohort is the lever. Drop to 65: ~50% more fires across the
// observed conf distribution. If realized EV degrades materially over
// the next 7d, raise back to 70.
pub const SIGNAL_MIN_EFFECTIVE_CONFIDENCE: i32 = 65;
// 2026-05-01 Bucket A relaxation: top1 gate bumped 6 → 18. Backtest against
// the live token_snapshots universe (n=223 entries passing
// STAIRCASE/GRINDER/SPRING + conf≥70 + top1<22 + liq≥15k) showed +10.8%
// mean realized EV with a 50@+50/25@+100/25@+250 ladder. The old 6% floor
// was so tight it was producing 0 fires for hours and excluding ~95% of
// the historical winner shape. 18 is two points tighter than the backtest
// threshold for a safety margin while still ~3x more permissive than the
// prior gate.
pub const SIGNAL_MAX_TOP_HOLDER_PCT: f64 = 18.0;
// Top-10 gate. 2026-05-02: bumped 30 → 33 after silence-window trace
// surfaced multiple near-passers at top10 31-34% (notably $3h4F4V8v…
// missed standard gate by 1-3pp on top10 across consecutive STAIRCASE
// snapshots while passing every other criterion). 33 still cuts the
// insider-bundler shape (loser cohort sat at 38-50%).
pub const SIGNAL_MAX_TOP10_PCT: f64 = 33.0;
pub const SIGNAL_REQUIRED_CLASSES: &[&str] = &["STAIRCASE", "GRINDER", "SPRING"];
// 2026-05-01 Bucket A: liquidity floor 50k → 20k. Backtest universe used
// 15k floor; 20k adds a 33% safety margin while still capturing ~85% of
// historical 5x+ runners (median entry liq $25-30k for that cohort).
pub const SIGNAL_MIN_LIQUIDITY_USD: f64 = 20_000.0;
pub const SIGNAL_MIN_VOLUME_24H_USD: f64 = 50_000.0;
// 2026-05-01 Bucket A: mcap floor 500k → 30k, ceiling added at 1M. The
// 500k floor was excluding the entire post-grad / mid-cap pump shape
// where median 5x+ runner enters. Ceiling at 1M cuts mature-tape entries
// where remaining upside is small.
pub const SIGNAL_MIN_MCAP_USD: f64 = 30_000.0;
pub const SIGNAL_MAX_MCAP_USD: f64 = 1_000_000.0;
pub const SIGNAL_MIN_TX_RATE_PER_MIN: f64 = 5.0;
// Holder growth gate disabled by setting to 0. holder_count is RPC-capped at
// 20 (`getTokenLargestAccounts` returns ≤20 accounts), making growth-rate
// computation noisy. The forensics gates do the concentration job better.
// Set non-zero to re-enable when a reliable holder-count source is wired.
pub const SIGNAL_MIN_HOLDER_GROWTH_PER_HOUR: f64 = 0.0;
// Launch-forensics ceilings — block calls when the measured concentration
// signals a bundle / sniper-cohort / insider-network risk. Each metric is
// 0.0 when unmeasured (fresh token), so the gate only fires above the
// threshold; absence of data does NOT block (the auto-refresh will catch
// up on the next analysis cycle).
pub const SIGNAL_MAX_BUNDLE_PCT: f64 = 30.0;
pub const SIGNAL_MAX_SNIPER_PCT: f64 = 30.0;
pub const SIGNAL_MAX_INSIDER_PCT: f64 = 25.0;

// Buy/sell ratio gates. 2026-04-30: relaxed 1.10-1.30 → 1.05-1.40 to fire
// at all (tighter band caught zero candidates). 2026-05-02: relaxed upper
// bound 1.40 → 3.0 after live observation showed STAIRCASE conf-75
// runners ($WINNING / 5U7yW5CRQa…, ran ~1000% h1) blocked at b/s 3.2.
// The 1.40-3.0 band was untested by the original cohort analysis — the
// historical losers sat at 0.82/0.93 (dumping) and 3.48/3.70/3.77 (FOMO
// peak). 3.0 keeps both failure modes filtered while letting through the
// runners-mid-rip zone that was the actual false-negative source.
//
// Trade-off: late-FOMO entries between 1.40-3.0 will sometimes slip; the
// trailing-stop ladder is the safety net for those.
pub const SIGNAL_MIN_BUY_SELL_RATIO: f64 = 1.05;
pub const SIGNAL_MAX_BUY_SELL_RATIO: f64 = 3.0;
// Minimum sample size — below this the ratio is noise.
pub const SIGNAL_MIN_HOUR_TXNS: i32 = 100;

// =============================================================================
// MOONSHOT GATES — DEVELOPING-class right-tail capture bucket.
// =============================================================================
// 2026-05-01: shipped. Backtest n=397 entries gave +29.7% mean realized EV
// against hold-to-stop strategy (-60% stop, 72h timeout, no upper take).
// 4.3% of entries hit peak ≥+1000% (best +3597%). 7.8% hit +300-1000%.
// 15.4% hit +100-300%. The shape inverse to SCALP/SHORT — high-concentration
// + low-confidence + sub-$80k mcap is the SIGNAL of organic accumulation,
// not a threat.
//
// Position sizing critical because median is negative (~30% of entries lose
// the full -60% stop). Right-tail carries the EV. Operator should size
// MOONSHOT calls at 1/10th the position of Bucket A — the right tail
// pays for the variance.
//
// =============================================================================
// MOONSHOT v2 — 2026-05-01. Re-enabled with two new discriminators after
// dataset analysis on 1150 historical DEVELOPING-class tokens (last 14d):
//
//   The 11-call live cohort that bled (0/11, mean -61%) was NOT
//   representative of the bucket's underlying distribution. Universe
//   shows 11.3% peak ≥ +200% rate at +25.5% mean realized EV under a
//   +500/-30 ladder. The single-day cohort hit a perfect storm: low-tpm
//   tokens (60-150/min) AND already-falling pre-DEV trajectory.
//
//   Two filters distinguish wins from rugs across the 14d universe:
//
//     (1) tpm ≥ 200/min — winners cluster at high activity. Below 200
//         had 9.0% peak ≥+200% rate; at ≥200 it's 12.7%. tpm 50-200
//         was the bucket's drag.
//
//     (2) pre-DEV slope ≥ 0% — looking at the oldest snapshot price in
//         the 30-min window before first DEV classification. Negative
//         pre-slope tokens are catching the back of a pump (already
//         fading). Tokens with NULL pre-slope (genuinely fresh, no
//         observable history) keep base rate. Tokens with confirmed
//         POSITIVE pre-slope keep the edge.
//
// Re-validated against the 14d universe under a +250%/-25% ladder:
//   Ideal exec:       +26.9% EV/trade
//   Realistic (15% slip): +14.5% EV/trade
//   Pessimistic (30% slip): +2.2% EV/trade
//
// Critically: applying these filters to the 11 May 1 losing calls
// rejects ALL of them (low tpm or confirmed-down pre-slope). The bucket
// fires on a fundamentally different shape going forward.
//
// Settle ladder retuned in scanner.rs: +250 take, -25 stop, 72h expire.
// Lower take threshold improves realized capture (28→56 of cohort
// hit ≥+200% vs ≥+500%); tighter stop caps per-fire loss.
// =============================================================================
pub const MOONSHOT_ENABLED: bool = true;
pub const MOONSHOT_REQUIRED_CLASS: &str = "DEVELOPING";
pub const MOONSHOT_MIN_MCAP_USD: f64 = 5_000.0;
pub const MOONSHOT_MAX_MCAP_USD: f64 = 80_000.0;
// Top1 ceiling at 60% — the moonshot signal is concentrated holders
// (early accumulation pattern), so we explicitly allow what SCALP/Bucket A
// reject. Above 60% is honeypot territory (single wallet can dump).
pub const MOONSHOT_MAX_TOP_HOLDER_PCT: f64 = 60.0;
pub const MOONSHOT_MIN_HOLDER_COUNT: i32 = 15;
pub const MOONSHOT_MAX_HOLDER_COUNT: i32 = 60;
// Lifted from 50/min to 200/min after universe analysis: tpm 50-200
// dragged hit-rate down. Above 200 tokens have enough genuine flow to
// support a +200% leg before the dump.
pub const MOONSHOT_MIN_TX_RATE_PER_MIN: f64 = 200.0;
// Moonshot-specific age floor. The shared SIGNAL_MIN_TOKEN_AGE_SECS (900s)
// excludes the entire moonshot opportunity window — DEVELOPING-class
// snapshots typically last 15-60 seconds before classification rotates.
// 3-min floor still requires distribution to settle past the very-first
// fills (creator + initial 5 bonding-curve buyers) but doesn't kill the
// fresh-DEV play. 2026-05-02: shipped after silence-window trace showed
// 11/13 passing-mints rejected purely on age.
pub const MOONSHOT_MIN_TOKEN_AGE_SECS: i64 = 180;
// Pre-DEV trajectory: look back 30min for the oldest snapshot price.
// Reject only when we have CONFIRMED downward slope (price now < pre-price).
// NULL data (genuinely fresh) passes — base rate still positive.
pub const MOONSHOT_PRE_LOOKBACK_SECS: i64 = 1800;
pub const MOONSHOT_MIN_PRE_PCT: f64 = 0.0;
// Forensics ceilings — even moonshots block on confirmed bundle/sniper
// concentration. The shape we want is human-driven accumulation, not
// programmatic launch-bot fills. Ceilings are looser than SHORT because
// DEV-class tokens are pre-stabilization.
pub const MOONSHOT_MAX_BUNDLE_PCT: f64 = 50.0;
pub const MOONSHOT_MAX_SNIPER_PCT: f64 = 70.0;
pub const MOONSHOT_MAX_INSIDER_PCT: f64 = 40.0;

// =============================================================================
// SCALP GATES — looser bucket for shallow tokens that just printed a 1h+ move.
// Recalibrated 2026-04-29 from the 31-call backtest. Trump (+45.1, mcap 371k,
// top1 9.3), ALEXCOIN (+44.8, mcap 115k, top1 11.7), BLIMP (+41.3, mcap 82k,
// top1 13.4), Archangel (+16.5, mcap 174k) — these were the biggest absolute
// winners and were ALL excluded by the original strict SCALP gate. Also Kiss
// (+15.1) and DUMBMONEY (+6.2) live here.
//
// New mcap range $80k-$500k. New top1<14 / top10<40. Holder count gate
// effectively disabled (RPC-capped at 20). Forensics gates retained — same
// thresholds as SHORT — to filter pure bot rugs.
// =============================================================================
// SCALP REVIVE — 2026-05-01. Re-enabled with a hardened gate after slicing
// the 18-call disabled cohort (ids 52-70) by every dimension we record:
//
//   Wins (n=6):    mcap 87k–477k, tpm 27.5–3000, h1 +147–+426%, b/s 1.19–1.49
//   Losses (n=12): mcap 73k–368k, tpm 18–3000,  h1 -7–+1061%, b/s 0.82–3.77
//
// Three discriminators emerged:
//   (1) mcap floor 87k         (KINDNESS 73k, NICETRUMP 86k, chadhouse 80k all rugged)
//   (2) h1 corridor 100–300%   (HSBC +1061, SIR +544, scam +364 → rugged tops;
//                               FOODBANK -7, NOHOUSE +65 → no-momentum)
//   (3) tpm floor 25/min       (wiffy tpm=18 → -99% rug; below 25 had no wins)
//
// Re-validating the same cohort under the new gate yields 5 wins / 3 losses
// (62.5% win rate). With the existing +30 take / -30 stop ladder that's
// roughly +10% mean realized EV per trade — positive vs the current -24%.
// The one win sacrificed is TOK (h1 +426% — outside the new ceiling); the
// 6 loss filters that fire are chadhouse, NICETRUMP, HSBC, SIR, FOODBANK,
// NOHOUSE — together about half the cohort's catastrophic loss burden.
// =============================================================================
pub const SCALP_ENABLED: bool = true;
// Floor at $87k. Below this every entry in the live cohort lost (-32% to -99%).
// MINIBELKA at $87k is the tightest winner — that's the lower edge of the
// shape that holds together. Tokens below that mcap don't have enough holder
// breadth for a clean +30% leg before the dump.
pub const SCALP_MIN_MCAP_USD: f64 = 87_000.0;
pub const SCALP_MAX_MCAP_USD: f64 = 500_000.0;
pub const SCALP_MAX_TOP_HOLDER_PCT: f64 = 14.0;
pub const SCALP_MAX_TOP10_PCT: f64 = 40.0;
// Floor lifted from +50% to +100%. Below +100% pc1h the cohort had zero wins
// (FOODBANK -7%, NOHOUSE +65%, KINDNESS +107% borderline) — the move hasn't
// established yet and we're catching the wrong side of accumulation.
pub const SCALP_MIN_PRICE_CHANGE_1H_PCT: f64 = 100.0;
// Ceiling tightened from +350% to +300%. HSBC (+1061), SIR (+544), scam
// (+364) all fired above and rugged immediately. TOK (+426 winner) is the
// one false positive sacrificed — net win.
pub const SCALP_MAX_PRICE_CHANGE_1H_PCT: f64 = 300.0;
pub const SCALP_MAX_AGE_SECS: i64 = 4 * 3600;
pub const SCALP_MIN_LIQUIDITY_USD: f64 = 20_000.0;
// Floor lifted from 5/min to 25/min. wiffy at tpm=18 was the one outlier
// below the win range and rugged -99%. Wins clustered at tpm ≥ 27.5.
pub const SCALP_MIN_TX_RATE_PER_MIN: f64 = 25.0;
// 15 because RPC caps at 20 — anything above 20 is rare. Set to 0 to disable.
pub const SCALP_MIN_HOLDER_COUNT: i32 = 15;
// Token must be at least this old to auto-call. Original 1h was excluding
// the entire fresh-grad rip cycle (peak typically 10-30min post-grad).
// 2ssMotVbTUfR at 14min old, GRINDER conf 71, top1=3.9, mcap $1.5M, bsr
// 1.17 — textbook fire candidate, blocked by the 1h floor. Lowered to
// 15min: enough time for the bsr ratio + holder distribution to be
// meaningful, but not so much that the move is over before we fire.
//
// The bot-rug protection that the 1h floor was meant to provide now
// comes from the forensics layer (bundle/sniper/insider %) and the
// tightened bsr ratio band [1.10, 1.30].
pub const SIGNAL_MIN_TOKEN_AGE_SECS: i64 = 900;

// =============================================================================
// MATERIAL CHANGE THRESHOLDS — only edit the card when something notable moves.
// Otherwise noise. Editing on every scan would rate-limit the bot and spam the
// channel's edit history.
// =============================================================================

pub const MATERIAL_CONFIDENCE_DELTA: i32 = 5;
pub const MATERIAL_PRICE_DELTA_PCT: f64 = 10.0;
pub const MATERIAL_TOP_HOLDER_DELTA_PCT: f64 = 3.0;

// =============================================================================
// FAIL TRIGGERS — when to mark a signal's verdict as collapsed. The card
// stays visible (with timeline) but the header flips to FAILED so the channel
// shows exactly which calls didn't pan out.
// =============================================================================

pub const FAIL_CLASSES: &[&str] = &["ACTIVE_TRAP", "CRASHING", "DEAD"];
pub const FAIL_MIN_EFFECTIVE_CONFIDENCE: i32 = 50;

// -- Timeline / state storage ------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEntry {
    pub ts: i64,
    pub kind: String, // 'called' | 'update' | 'failed'
    pub line: String, // pre-rendered human line
}

/// One token's collapse as DB-sourced evidence — no fabrication, no narrative.
struct TrapCandidate {
    address: String,
    peak: crate::db::TokenSnapshot,
    current: crate::db::TokenSnapshot,
    severity: f64,
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Extract `retry_after` seconds from a Telegram 429 error body. The body
/// shape is `{"description":"Too Many Requests: retry after N","error_code":429,..."parameters":{"retry_after":N}}`.
/// We do a substring scan rather than a JSON parse since the error has
/// already been stringified by anyhow by the time we see it.
fn parse_retry_after(err_text: &str) -> Option<u64> {
    let key = "\"retry_after\":";
    let pos = err_text.find(key)?;
    let after = &err_text[pos + key.len()..];
    let end = after
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after.len());
    after[..end].parse::<u64>().ok()
}

/// Extract horizon display string + cleaned note. Thin wrapper around
/// the shared `crate::horizon` module — kept here only to preserve the
/// `(Option<&'static str>, String)` shape the existing call-card
/// renderer expects.
fn parse_horizon_from_note(note: &str) -> (Option<&'static str>, String) {
    let (h, clean) = crate::horizon::parse_with_clean(note);
    (h.display(), clean)
}

fn compact_usd(v: f64) -> String {
    let a = v.abs();
    if a >= 1_000_000_000.0 {
        format!("${:.1}B", v / 1e9)
    } else if a >= 1_000_000.0 {
        format!("${:.1}M", v / 1e6)
    } else if a >= 1_000.0 {
        format!("${:.1}k", v / 1e3)
    } else {
        format!("${:.0}", v)
    }
}

/// Parse the leading signed-percentage off a settle exit_note.
/// Format examples: "+22.1% · trailing stop @ +25% (peak +75%)",
/// "-66.8% · moonshot stop", "+30.5% · scalp +30 done". Returns None
/// when the note doesn't start with a +/- pct (manual close strings).
fn parse_pct_prefix(s: &str) -> Option<f64> {
    let trimmed = s.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.is_empty() || (bytes[0] != b'+' && bytes[0] != b'-') {
        return None;
    }
    let pct_pos = trimmed.find('%')?;
    trimmed[..pct_pos].parse::<f64>().ok()
}

/// Parse the peak-pct annotation when present. Format: "(peak +75%)".
/// Returns None when absent — the brag composer falls back to exit pct
/// in that case so it still produces a well-formed line.
fn parse_peak_pct(s: &str) -> Option<f64> {
    let needle = "peak ";
    let pos = s.find(needle)?;
    let after = &s[pos + needle.len()..];
    let pct_pos = after.find('%')?;
    let val = &after[..pct_pos];
    let val = val.trim_start_matches('+');
    val.parse::<f64>().ok()
}

/// Compose the entry narrative in the ape's own voice — first person,
/// terse, no data dump. The reader sees the chart preview from the
/// link, the price line below, and one paragraph of "here's why I aped
/// and what I'm doing". Heavy data lives in the lounge mirror.
fn compose_ape_entry(
    a: &TokenAnalysis,
    horizon: crate::horizon::Horizon,
    mcap_usd: f64,
) -> String {
    let mc = compact_usd(mcap_usd);

    let tape = if a.tx_rate >= 500.0 {
        "tape's screaming"
    } else if a.tx_rate >= 200.0 {
        "flow stacking"
    } else if a.tx_rate >= 100.0 {
        "flow's there"
    } else {
        "thin tape, watching"
    };

    let warn = if a.bundle_pct >= 25.0 || a.sniper_pct >= 40.0 || a.insider_pct >= 20.0 {
        " holders look weird but the tape is the tape."
    } else {
        ""
    };

    let plan = match horizon {
        crate::horizon::Horizon::Moonshot => "lottery shot — out at 3.5x or -25.",
        crate::horizon::Horizon::Scalp    => "quick scalp — +30 take, -30 stop, 90min max.",
        crate::horizon::Horizon::Long     => "thesis trade — laddering takes from +40, stop -50.",
        crate::horizon::Horizon::Short    => "swing — +50/+100 ladder, stop -40.",
        crate::horizon::Horizon::Unknown  => "riding the trail.",
    };

    let opener = match horizon {
        crate::horizon::Horizon::Moonshot => format!(
            "fresh DEV at {mc}. top1 {top:.0}%, {tape}.",
            mc = mc, top = a.top_holder_pct, tape = tape,
        ),
        crate::horizon::Horizon::Scalp => format!(
            "in at {mc} mc. {tape}, top holder under {top:.0}%.",
            mc = mc, tape = tape, top = a.top_holder_pct.ceil(),
        ),
        crate::horizon::Horizon::Long => format!(
            "graduated at {mc}, structure held. {tape}.",
            mc = mc, tape = tape,
        ),
        crate::horizon::Horizon::Short => format!(
            "in at {mc} mc, distribution clean. {tape}.",
            mc = mc, tape = tape,
        ),
        crate::horizon::Horizon::Unknown => format!("aped at {mc} mc. {tape}.", mc = mc, tape = tape),
    };

    format!("{opener}{warn} {plan}").trim().to_string()
}

/// Compose the brag line for a winning close. Multiplier-first so the
/// channel reads "we hit 3.5x" not "+251.4%". Mentions peak only when it
/// significantly exceeded the realized exit (the trail caught a runner
/// retrace and we want the peak in the brag).
fn compose_ape_brag(pct: f64, peak_pct: f64, horizon: crate::horizon::Horizon) -> String {
    let mult = 1.0 + pct / 100.0;
    let mult_str = if mult >= 2.0 {
        format!("{:.1}x", mult)
    } else {
        // {:+} sign-formats directly. Concatenating "+" with a negative
        // float yields "+-4%" — caller path passes pct unsigned but the
        // composer must defend against either.
        format!("{:+.0}%", pct)
    };
    let peak_str = if peak_pct > pct + 25.0 && peak_pct >= 50.0 {
        let peak_mult = 1.0 + peak_pct / 100.0;
        if peak_mult >= 2.0 {
            format!(" (peaked {:.1}x — trail caught the retrace)", peak_mult)
        } else {
            format!(" (peaked +{:.0}%)", peak_pct)
        }
    } else {
        String::new()
    };
    let phrase = if mult >= 5.0 {
        "ate good"
    } else if mult >= 3.0 {
        "won this one"
    } else if mult >= 2.0 {
        "took the 2x"
    } else if mult >= 1.3 {
        "banked the run"
    } else {
        "small green, took it"
    };
    match horizon {
        crate::horizon::Horizon::Moonshot => format!("{phrase} on a moonshot. {mult_str}{peak_str}."),
        crate::horizon::Horizon::Scalp    => format!("{phrase}. quick scalp, {mult_str}{peak_str}."),
        crate::horizon::Horizon::Long     => format!("{phrase}. thesis paid, {mult_str}{peak_str}."),
        _                                  => format!("{phrase}. {mult_str}{peak_str}."),
    }
}

/// Compose the loss/expire line. Honest, no whining, mentions the runner
/// regret only when the trail let go of real green (peak > 30%, exit red).
fn compose_ape_loss(pct: f64, peak_pct: f64, status: &str) -> String {
    if peak_pct >= 30.0 && pct < peak_pct - 50.0 {
        format!("had it green at +{:.0}%, gave it back. out {:+.0}%.", peak_pct, pct)
    } else if status == "expired" {
        format!("90min flat, didn't move. out {:+.0}%.", pct)
    } else if pct <= -40.0 {
        "stop hit hard. next.".to_string()
    } else {
        format!("out {:+.0}%. next.", pct)
    }
}

// -- Notifier core -----------------------------------------------------------

/// Internal DB key for the calls-channel delivery row. Stable string —
/// renaming would orphan every existing row in production.
const CALLS_CHANNEL: &str = "winners";
/// Internal DB key for the lounge-mirror delivery row. New as of the
/// dual-destination split. Tokens fired before this change have no
/// lounge row; lookups must tolerate None.
const LOUNGE_CHANNEL: &str = "lounge";

pub struct Notifier {
    cfg: TelegramConfig,
    db: Arc<Db>,
    http: reqwest::Client,
    halted: Arc<AtomicBool>,
    signal_threshold_override: Arc<AtomicI32>,
    /// Optional handle that lets state-changing operations (auto-call
    /// fire, settle close, manual call/close) kick the publisher to
    /// run a snapshot now instead of waiting for the next 300s tick.
    /// `None` when the publisher isn't configured.
    publish_kick: Option<crate::publisher::PublishKick>,
    /// Optional trade-execution context. Some(_) only when
    /// PHOTON_PRIVATE_KEY env var is set AND [execution] config block
    /// has enabled=true. None = paper-only mode, the auto-call path
    /// inserts rows + posts cards but never signs trades.
    executor: Option<Arc<crate::execution::ExecutionCtx>>,
    /// Helius API key extracted from rpc.endpoints. Powers wallet_observer
    /// (Layer 1 of the smart-wallet curation system). Empty when no
    /// Helius endpoint is configured — the observer no-ops silently.
    helius_api_key: String,
}

impl Notifier {
    pub fn new(
        cfg: TelegramConfig,
        db: Arc<Db>,
        publish_kick: Option<crate::publisher::PublishKick>,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        Ok(Self {
            cfg,
            db,
            http,
            halted: Arc::new(AtomicBool::new(false)),
            signal_threshold_override: Arc::new(AtomicI32::new(0)),
            publish_kick,
            executor: None,
            helius_api_key: String::new(),
        })
    }

    /// Attach the Helius API key used by wallet_observer for buyer-trace.
    /// When unset (no Helius endpoint configured), wallet_observer no-ops
    /// silently and Layer 1 stops collecting — the rest of the system
    /// stays correct.
    pub fn with_helius_api_key(mut self, key: String) -> Self {
        self.helius_api_key = key;
        self
    }

    /// Attach trade-execution capability. Call once at boot when
    /// PHOTON_PRIVATE_KEY + [execution] config are both wired. Mutates
    /// in place because Notifier is instantiated, then optionally
    /// upgraded — same pattern as `with_notifier` on the scanner.
    pub fn with_executor(mut self, ctx: Arc<crate::execution::ExecutionCtx>) -> Self {
        self.executor = Some(ctx);
        self
    }

    pub fn executor(&self) -> Option<&Arc<crate::execution::ExecutionCtx>> {
        self.executor.as_ref()
    }

    /// Wake the publisher to run an immediate snapshot. Coalesces with
    /// any in-flight or near-future tick — multiple kicks during a run
    /// collapse into one extra cycle. Cheap; safe to call from hot paths.
    pub fn kick_publisher(&self) {
        if let Some(k) = &self.publish_kick {
            k.notify_one();
        }
    }

    pub fn halted(&self) -> bool {
        self.halted.load(Ordering::SeqCst)
    }

    pub fn set_halted(&self, halted: bool) {
        self.halted.store(halted, Ordering::SeqCst);
    }

    pub fn signal_threshold(&self) -> i32 {
        let override_value = self.signal_threshold_override.load(Ordering::SeqCst);
        if override_value > 0 {
            override_value
        } else {
            SIGNAL_MIN_EFFECTIVE_CONFIDENCE
        }
    }

    pub fn set_signal_threshold_override(&self, value: i32) {
        self.signal_threshold_override
            .store(value.max(0), Ordering::SeqCst);
    }

    pub fn signal_threshold_override(&self) -> i32 {
        self.signal_threshold_override.load(Ordering::SeqCst)
    }

    // -- Promotion / material-change predicates -----------------------------

    /// The single source of truth for opening a signal. Change this only after
    /// aligning with the operator — it defines what gets pushed to the signals
    /// channel. Reads thresholds from the module-level SIGNAL_* constants.
    /// Classify near-misses — tokens that failed ONE specific signal gate by a
    /// small margin. Truth-telling for gate tuning: you see exactly what we
    /// almost fired on, and why. Gap thresholds intentionally narrow so the
    /// log isn't flooded with tokens that weren't realistically close.
    ///
    /// Returns (gate_name, gap_description) if a near-miss, else None.
    /// Classify near-misses across the FULL gate vector. Logs the first gate
    /// that fails (in priority order) so the operator can see exactly which
    /// gate is biting on which token. Without this, gate tuning flies blind:
    /// before this expansion, only conf/top1/momentum/sell-pressure were
    /// tracked, leaving 8 of 12 gates unmonitored.
    ///
    /// Returns (gate_name, gap_description) if the token passed the
    /// classification + age + history checks but failed at least one gate.
    /// Returns None when the token is a structural miss (wrong class) or
    /// when it would have fired (no near-miss).
    pub fn classify_near_miss(
        &self,
        a: &TokenAnalysis,
        effective_conf: i32,
        meta: Option<&TokenMeta>,
        first_seen: Option<i64>,
    ) -> Option<(&'static str, String)> {
        let class = a.confidence.classification.as_str();
        let class_ok = SIGNAL_REQUIRED_CLASSES.iter().any(|c| *c == class);
        if !class_ok {
            return None; // structural miss
        }
        // Walk gates in priority order; first failing gate wins.
        if effective_conf < SIGNAL_MIN_EFFECTIVE_CONFIDENCE {
            // Only count "close" misses (>=60 confidence) so the log isn't
            // flooded with random low-conf snapshots.
            if effective_conf >= 60 {
                return Some((
                    "conf",
                    format!(
                        "conf {} < {} (short by {})",
                        effective_conf,
                        SIGNAL_MIN_EFFECTIVE_CONFIDENCE,
                        SIGNAL_MIN_EFFECTIVE_CONFIDENCE - effective_conf
                    ),
                ));
            }
            return None;
        }
        if a.top_holder_pct >= SIGNAL_MAX_TOP_HOLDER_PCT {
            return Some((
                "top_holder",
                format!(
                    "top {:.2}% >= {:.2}% (over by {:.2}pp)",
                    a.top_holder_pct,
                    SIGNAL_MAX_TOP_HOLDER_PCT,
                    a.top_holder_pct - SIGNAL_MAX_TOP_HOLDER_PCT
                ),
            ));
        }
        if a.top10_pct >= SIGNAL_MAX_TOP10_PCT {
            return Some((
                "top10",
                format!(
                    "top10 {:.1}% >= {:.1}% (over by {:.1}pp)",
                    a.top10_pct,
                    SIGNAL_MAX_TOP10_PCT,
                    a.top10_pct - SIGNAL_MAX_TOP10_PCT
                ),
            ));
        }
        if a.tx_rate < SIGNAL_MIN_TX_RATE_PER_MIN {
            return Some((
                "tx_rate",
                format!(
                    "txr {:.1}/min < {:.1} (short by {:.1})",
                    a.tx_rate,
                    SIGNAL_MIN_TX_RATE_PER_MIN,
                    SIGNAL_MIN_TX_RATE_PER_MIN - a.tx_rate
                ),
            ));
        }
        if a.bundle_pct >= SIGNAL_MAX_BUNDLE_PCT {
            return Some(("bundle", format!("bundle {:.1}% >= {:.1}%", a.bundle_pct, SIGNAL_MAX_BUNDLE_PCT)));
        }
        if a.sniper_pct >= SIGNAL_MAX_SNIPER_PCT {
            return Some(("sniper", format!("sniper {:.1}% >= {:.1}%", a.sniper_pct, SIGNAL_MAX_SNIPER_PCT)));
        }
        if a.insider_pct >= SIGNAL_MAX_INSIDER_PCT {
            return Some(("insider", format!("insider {:.1}% >= {:.1}%", a.insider_pct, SIGNAL_MAX_INSIDER_PCT)));
        }
        let total_h1 = a.buys_h1 + a.sells_h1;
        if total_h1 >= SIGNAL_MIN_HOUR_TXNS {
            let bsr = if a.sells_h1 > 0 {
                a.buys_h1 as f64 / a.sells_h1 as f64
            } else {
                1.2
            };
            if bsr < SIGNAL_MIN_BUY_SELL_RATIO {
                return Some(("buy_sell_low", format!("b/s {:.2} < {:.2} (dumping)", bsr, SIGNAL_MIN_BUY_SELL_RATIO)));
            }
            if bsr > SIGNAL_MAX_BUY_SELL_RATIO {
                return Some(("buy_sell_high", format!("b/s {:.2} > {:.2} (FOMO peak)", bsr, SIGNAL_MAX_BUY_SELL_RATIO)));
            }
        }
        if a.delta.as_ref().map_or(true, |d| d.momentum_delta < 0) {
            if let Some(d) = a.delta.as_ref() {
                return Some((
                    "momentum_delta",
                    format!("mom_delta {} < 0", d.momentum_delta),
                ));
            }
            return Some(("history", "no prior snapshot".into()));
        }
        // Meta-dependent gates — without these tracked we can't see why
        // otherwise-clean candidates fail to fire.
        let liq = meta.and_then(|m| m.liquidity_usd);
        if liq.map_or(true, |v| v < SIGNAL_MIN_LIQUIDITY_USD) {
            return Some(("liquidity", format!(
                "liq ${:.0} < ${:.0}",
                liq.unwrap_or(0.0), SIGNAL_MIN_LIQUIDITY_USD
            )));
        }
        let vol = meta.and_then(|m| m.volume_24h_usd);
        if vol.map_or(true, |v| v < SIGNAL_MIN_VOLUME_24H_USD) {
            return Some(("volume24", format!(
                "vol24 ${:.0} < ${:.0}",
                vol.unwrap_or(0.0), SIGNAL_MIN_VOLUME_24H_USD
            )));
        }
        let mcap = meta.and_then(|m| m.market_cap_usd.or(m.fdv_usd));
        if mcap.map_or(true, |v| v < SIGNAL_MIN_MCAP_USD) {
            return Some(("mcap", format!(
                "mcap ${:.0} < ${:.0}",
                mcap.unwrap_or(0.0), SIGNAL_MIN_MCAP_USD
            )));
        }
        if mcap.map_or(false, |v| v > SIGNAL_MAX_MCAP_USD) {
            return Some(("mcap_high", format!(
                "mcap ${:.0} > ${:.0} (mature-tape, low remaining upside)",
                mcap.unwrap_or(0.0), SIGNAL_MAX_MCAP_USD
            )));
        }
        let now = chrono::Utc::now().timestamp();
        let age = first_seen.map(|fs| now - fs).unwrap_or(0);
        if age < SIGNAL_MIN_TOKEN_AGE_SECS {
            return Some(("age", format!(
                "age {}s < {}s",
                age, SIGNAL_MIN_TOKEN_AGE_SECS
            )));
        }
        if let Some(d) = a.delta.as_ref() {
            if d.time_elapsed_seconds > 0 {
                let per_hour =
                    d.holder_count_delta as f64 * 3600.0 / d.time_elapsed_seconds as f64;
                if per_hour < SIGNAL_MIN_HOLDER_GROWTH_PER_HOUR {
                    return Some(("holder_growth", format!(
                        "holders/h {:.1} < {:.1}",
                        per_hour, SIGNAL_MIN_HOLDER_GROWTH_PER_HOUR
                    )));
                }
            }
        }
        None
    }

    pub fn should_signal(
        &self,
        a: &TokenAnalysis,
        effective_conf: i32,
        meta: Option<&TokenMeta>,
        first_seen: Option<i64>,
    ) -> bool {
        if self.halted() {
            return false;
        }
        let class = a.confidence.classification.as_str();
        let class_ok = SIGNAL_REQUIRED_CLASSES.iter().any(|c| *c == class);
        let conf_ok = effective_conf >= self.signal_threshold();
        let holder_ok = a.top_holder_pct < SIGNAL_MAX_TOP_HOLDER_PCT;
        // Insider-network gate: even when top1 looks fine, bundlers that
        // split 30-40% across 20+ wallets show up in top10 aggregate.
        let top10_ok = a.top10_pct < SIGNAL_MAX_TOP10_PCT;
        // momentum_delta ≥ 0 means not fading. Missing delta (first-sight tokens)
        // counts as neutral — allowed through.
        let momentum_ok = a.delta.as_ref().map_or(true, |d| d.momentum_delta >= 0);
        // Require at least one prior snapshot — prevents first-sight signals.
        let history_ok = a.delta.is_some();
        // Market-data floors: prove the token has tradeable depth and is
        // actually trading. Missing meta (DexScreener fetch failed) means
        // the token isn't on any DEX — block.
        let liq_ok = meta
            .and_then(|m| m.liquidity_usd)
            .map_or(false, |v| v >= SIGNAL_MIN_LIQUIDITY_USD);
        let vol_ok = meta
            .and_then(|m| m.volume_24h_usd)
            .map_or(false, |v| v >= SIGNAL_MIN_VOLUME_24H_USD);
        let mcap_ok = meta
            .and_then(|m| m.market_cap_usd.or(m.fdv_usd))
            .map_or(false, |v| v >= SIGNAL_MIN_MCAP_USD && v <= SIGNAL_MAX_MCAP_USD);
        // Velocity gate: trading-velocity is the dominant graduation predictor
        // (arxiv 2602.14860). Post-grad we use it to filter dead books.
        let tx_rate_ok = a.tx_rate >= SIGNAL_MIN_TX_RATE_PER_MIN;
        // Holder growth: convert delta over elapsed seconds → holders/hour.
        let holder_growth_ok = a.delta.as_ref().map_or(false, |d| {
            if d.time_elapsed_seconds <= 0 {
                false
            } else {
                let per_hour =
                    d.holder_count_delta as f64 * 3600.0 / d.time_elapsed_seconds as f64;
                per_hour >= SIGNAL_MIN_HOLDER_GROWTH_PER_HOUR
            }
        });
        // Launch-forensics: blocked when measured > threshold. The
        // "forensics_required" gate (added 2026-04-30) was correct in
        // theory but caused 0-fire deadlock in production: forensics
        // tasks timing out at 180s under the degraded public-RPC
        // environment (429 cascade) → fail-closed sentinel writes 100%
        // → all forensics gates fail → no calls fire ever.
        //
        // Reverted to soft gate: a 0 (unmeasured) value passes through.
        // Sentinel-100 (timeout) still blocks correctly. This is the
        // intended design — measured-clean tokens fire, measured-bad
        // are blocked, unmeasured pass through and the 1h refresh
        // tightens the gate retroactively as data arrives.
        //
        // The 11/15-fired-on-zeros problem the gate was meant to solve
        // is mitigated by the b/s ratio gate + pc1h ceiling that came
        // in the same audit cycle — those filter the actual rugs.
        let bundle_ok = a.bundle_pct < SIGNAL_MAX_BUNDLE_PCT;
        let sniper_ok = a.sniper_pct < SIGNAL_MAX_SNIPER_PCT;
        let insider_ok = a.insider_pct < SIGNAL_MAX_INSIDER_PCT;
        // Buy/sell pressure gate: organic accumulation lives in 0.9-1.6.
        // Below = already dumping. Above = late-stage FOMO peak.
        let total_txns = a.buys_h1 + a.sells_h1;
        let bs_ratio = if a.sells_h1 > 0 {
            a.buys_h1 as f64 / a.sells_h1 as f64
        } else {
            // No sells = pure buying = either real signal or measurement
            // gap. Permissive: pass through and let other gates filter.
            1.2
        };
        let bs_ok = total_txns < SIGNAL_MIN_HOUR_TXNS  // sample too small to gate
            || (bs_ratio >= SIGNAL_MIN_BUY_SELL_RATIO
                && bs_ratio <= SIGNAL_MAX_BUY_SELL_RATIO);
        // Age floor: token must have existed long enough that the holder
        // base reflects organic distribution, not creator + initial 5
        // bonding-curve buyers.
        let now = chrono::Utc::now().timestamp();
        let age_ok = first_seen.map_or(false, |fs| now - fs >= SIGNAL_MIN_TOKEN_AGE_SECS);
        class_ok
            && conf_ok
            && holder_ok
            && top10_ok
            && momentum_ok
            && history_ok
            && liq_ok
            && vol_ok
            && mcap_ok
            && tx_rate_ok
            && holder_growth_ok
            && age_ok
            && bundle_ok
            && sniper_ok
            && insider_ok
            && bs_ok
    }

    /// Scalp gate — fires on shallow-mcap tokens that just printed a 1h+
    /// move. Looser mcap/age/holder bars than `should_signal`, but inherits
    /// the same forensics gates (bundle/sniper/insider) since we don't
    /// scalp pure bot rugs. The settle ladder for SCALP calls is tighter:
    /// +30/+60 take, -30 hard stop, 4h timeout. DEV_SELLING and class
    /// regression are the primary exits, handled by the global event-exit
    /// path in scanner::settle_calls.
    #[allow(unreachable_code, dead_code, clippy::let_and_return)]
    pub fn should_scalp_signal(
        &self,
        a: &TokenAnalysis,
        meta: Option<&TokenMeta>,
        first_seen: Option<i64>,
    ) -> bool {
        if !SCALP_ENABLED {
            return false;
        }
        if self.halted() {
            return false;
        }
        let class = a.confidence.classification.as_str();
        let class_ok = SIGNAL_REQUIRED_CLASSES.iter().any(|c| *c == class);
        // Mcap window — the shallow zone, $80k-$500k. Bigger tokens go SHORT.
        let mcap_val = meta
            .and_then(|m| m.market_cap_usd.or(m.fdv_usd))
            .unwrap_or(0.0);
        let mcap_ok = mcap_val >= SCALP_MIN_MCAP_USD && mcap_val < SCALP_MAX_MCAP_USD;
        // Recent run — token must be moving NOW, not stale. Two-sided gate:
        // floor at +50% (must have run) AND ceiling at +350% (must not be at
        // exhaustion peak). The ceiling is the critical addition that catches
        // the pre-recoil FOMO band where most rugs happen.
        let pc1h = meta.and_then(|m| m.price_change_1h).unwrap_or(0.0);
        let pc_ok = pc1h >= SCALP_MIN_PRICE_CHANGE_1H_PCT
            && pc1h <= SCALP_MAX_PRICE_CHANGE_1H_PCT;
        let tx_rate_ok = a.tx_rate >= SCALP_MIN_TX_RATE_PER_MIN;
        let holders_ok = (a.holder_count as i32) >= SCALP_MIN_HOLDER_COUNT;
        let now = chrono::Utc::now().timestamp();
        let age_ok = first_seen.map_or(false, |fs| now - fs <= SCALP_MAX_AGE_SECS);
        let liq_ok = meta
            .and_then(|m| m.liquidity_usd)
            .map_or(false, |v| v >= SCALP_MIN_LIQUIDITY_USD);
        // Concentration ceilings — shallow tokens have higher natural top1
        // (RPC top-20 dominate by accounting math). Trump/ALEXCOIN/BLIMP
        // ranged 9.3-13.4% top1, 28.7-36.1% top10.
        let top1_ok = a.top_holder_pct < SCALP_MAX_TOP_HOLDER_PCT;
        let top10_ok = a.top10_pct < SCALP_MAX_TOP10_PCT;
        // Forensics ceilings — same as SHORT. Soft gate: unmeasured passes,
        // measured-bad blocks. See should_signal for the full rationale.
        let bundle_ok = a.bundle_pct < SIGNAL_MAX_BUNDLE_PCT;
        let sniper_ok = a.sniper_pct < SIGNAL_MAX_SNIPER_PCT;
        let insider_ok = a.insider_pct < SIGNAL_MAX_INSIDER_PCT;
        // Buy/sell pressure gate — the strongest single signal in the
        // 11-call live SCALP backtest. Inherited from SHORT.
        let total_txns = a.buys_h1 + a.sells_h1;
        let bs_ratio = if a.sells_h1 > 0 {
            a.buys_h1 as f64 / a.sells_h1 as f64
        } else {
            1.2
        };
        let bs_ok = total_txns < SIGNAL_MIN_HOUR_TXNS
            || (bs_ratio >= SIGNAL_MIN_BUY_SELL_RATIO
                && bs_ratio <= SIGNAL_MAX_BUY_SELL_RATIO);

        class_ok
            && mcap_ok
            && pc_ok
            && tx_rate_ok
            && holders_ok
            && age_ok
            && liq_ok
            && top1_ok
            && top10_ok
            && bundle_ok
            && sniper_ok
            && insider_ok
            && bs_ok
    }

    /// Moonshot gate — Bucket B. DEVELOPING-class entries at sub-$80k mcap
    /// where the SCALP/SHORT signal-shape is inverted: high concentration +
    /// low confidence is the SIGNAL of organic accumulation, not the threat.
    /// Backtest n=397 entries, +29.7% mean realized EV with hold-to-stop
    /// (-60% / 72h, no upper take). Right-tail driven: 4.3% of entries hit
    /// peak ≥+1000%, that subset carries the EV.
    ///
    /// Distinct gate path because the standard gate's holder/conf/class
    /// constraints are inverse to what's wanted here.
    pub fn should_moonshot_signal(
        &self,
        a: &TokenAnalysis,
        meta: Option<&TokenMeta>,
        first_seen: Option<i64>,
        current_price: f64,
    ) -> bool {
        if !MOONSHOT_ENABLED {
            return false;
        }
        if self.halted() {
            return false;
        }
        let class = a.confidence.classification.as_str();
        if class != MOONSHOT_REQUIRED_CLASS {
            return false;
        }
        // Mcap window — the bonding-curve / fresh-grad zone where moonshots
        // start. Above 80k they're already mid-pump; below 5k DexScreener
        // hasn't indexed liquidity reliably.
        let mcap_val = meta
            .and_then(|m| m.market_cap_usd.or(m.fdv_usd))
            .unwrap_or(0.0);
        let mcap_ok = mcap_val >= MOONSHOT_MIN_MCAP_USD && mcap_val <= MOONSHOT_MAX_MCAP_USD;
        // Holders 15-60 — under 15 is too thin to read the distribution,
        // over 60 means the token already broke into a wider holder base
        // and the next leg up is incremental, not exponential.
        let holders_ok = (a.holder_count as i32) >= MOONSHOT_MIN_HOLDER_COUNT
            && (a.holder_count as i32) <= MOONSHOT_MAX_HOLDER_COUNT;
        // Top1 ceiling at 60% — explicit allowance for the concentrated-
        // accumulation shape that SCALP/Bucket A reject. Above 60% is
        // honeypot (single wallet can dump the whole supply).
        let top1_ok = a.top_holder_pct < MOONSHOT_MAX_TOP_HOLDER_PCT;
        // Velocity floor lifted to 200/min after universe audit. Tokens at
        // 50-200 tpm dragged hit-rate; above 200 they have enough flow to
        // sustain a +200% leg.
        let tx_rate_ok = a.tx_rate >= MOONSHOT_MIN_TX_RATE_PER_MIN;
        // Forensics — looser than SHORT but still block confirmed bot-fill
        // patterns (bundle/sniper/insider). Soft gate: unmeasured passes.
        let bundle_ok = a.bundle_pct < MOONSHOT_MAX_BUNDLE_PCT;
        let sniper_ok = a.sniper_pct < MOONSHOT_MAX_SNIPER_PCT;
        let insider_ok = a.insider_pct < MOONSHOT_MAX_INSIDER_PCT;
        // Moonshot age floor — looser (3min) than the standard gate's
        // 15min. Standard floor was killing every DEVELOPING-class entry
        // because the classification window only lasts 15-60 seconds.
        let now = chrono::Utc::now().timestamp();
        let age_ok = first_seen.map_or(false, |fs| now - fs >= MOONSHOT_MIN_TOKEN_AGE_SECS);

        // Pre-DEV slope filter. Look back MOONSHOT_PRE_LOOKBACK_SECS
        // (30min) and find the oldest snapshot price for this token. If
        // the current price has dropped vs that pre-price, we're catching
        // the back of a pump — reject. NULL pre-data (genuinely fresh,
        // no observable history) passes — base rate is positive on those.
        let pre_ok = if current_price > 0.0 {
            let until = now - 60; // exclude last minute (the call's own snapshot)
            let since = now - MOONSHOT_PRE_LOOKBACK_SECS;
            match self.db.get_oldest_price_in_window(&a.address, since, until) {
                Ok(Some(pre_price)) if pre_price > 0.0 => {
                    let pre_pct = (current_price - pre_price) / pre_price * 100.0;
                    pre_pct >= MOONSHOT_MIN_PRE_PCT
                }
                _ => true, // no pre-data → don't reject
            }
        } else {
            true // unknown current price → fall back to other gates
        };

        mcap_ok
            && holders_ok
            && top1_ok
            && tx_rate_ok
            && bundle_ok
            && sniper_ok
            && insider_ok
            && age_ok
            && pre_ok
    }

    /// Decides when an open signal's verdict has collapsed.
    pub fn should_fail(&self, a: &TokenAnalysis, effective_conf: i32) -> bool {
        let class = a.confidence.classification.as_str();
        if class.starts_with("UNSAFE") {
            return true;
        }
        FAIL_CLASSES.iter().any(|c| *c == class) || effective_conf < FAIL_MIN_EFFECTIVE_CONFIDENCE
    }

    /// Whether a winner-state change is worth an in-place edit.
    fn is_material_change(
        &self,
        prev_conf: i32,
        prev_class: &str,
        prev_price: Option<f64>,
        prev_top_holder: Option<f64>,
        curr_conf: i32,
        curr_class: &str,
        curr_price: Option<f64>,
        curr_top_holder: Option<f64>,
    ) -> bool {
        if prev_class != curr_class {
            return true;
        }
        if (curr_conf - prev_conf).abs() >= MATERIAL_CONFIDENCE_DELTA {
            return true;
        }
        if let (Some(p), Some(c)) = (prev_price, curr_price) {
            if p > 0.0 {
                let pct = ((c - p) / p).abs() * 100.0;
                if pct >= MATERIAL_PRICE_DELTA_PCT {
                    return true;
                }
            }
        }
        if let (Some(p), Some(c)) = (prev_top_holder, curr_top_holder) {
            if (c - p).abs() >= MATERIAL_TOP_HOLDER_DELTA_PCT {
                return true;
            }
        }
        false
    }

    // -- Telegram API wrappers ----------------------------------------------

    /// Inline keyboard JSON for a token card: Chart · Solscan · Copy Address.
    /// Stateless (URL + copy_text only) so it works in broadcast channels.
    fn token_keyboard(&self, address: &str, pair_url: Option<&str>) -> String {
        let chart_url = pair_url
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("https://dexscreener.com/solana/{}", address));
        serde_json::json!({
            "inline_keyboard": [[
                { "text": "📊 Chart", "url": chart_url },
                { "text": "🔍 Solscan", "url": format!("https://solscan.io/token/{}", address) },
                { "text": "📋 Addr", "copy_text": { "text": address } }
            ]]
        })
        .to_string()
    }

    async fn send_message(&self, chat_id: &str, text: &str) -> Result<i64> {
        self.send_message_ex(chat_id, text, None).await
    }

    /// Send a one-line operator-facing notification to every configured
    /// admin user via the DM bot (Claudeinatorbot). Falls back to the
    /// channel bot token when no dedicated DM token is set. Used by the
    /// settling phase to push BANKED/FAILED/EXPIRED outcomes into the
    /// operator's personal stream — the channel is public, this is for
    /// the human running the system.
    pub async fn dm_admins(&self, text: &str) {
        if !self.cfg.enabled {
            return;
        }
        if self.cfg.admin_user_ids.is_empty() {
            return;
        }
        let token = if self.cfg.dm_bot_token.is_empty() {
            &self.cfg.bot_token
        } else {
            &self.cfg.dm_bot_token
        };
        let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
        for uid in &self.cfg.admin_user_ids {
            let form = vec![
                ("chat_id", uid.to_string()),
                ("text", text.to_string()),
                ("parse_mode", "HTML".to_string()),
                (
                    "link_preview_options",
                    r#"{"is_disabled":true}"#.to_string(),
                ),
            ];
            // Best-effort: a failed admin DM (user blocked the bot,
            // hasn't started a chat with it, etc.) shouldn't propagate
            // back into the settling phase. Log + continue.
            match self.http.post(&url).form(&form).send().await {
                Ok(r) => {
                    if !r.status().is_success() {
                        let s = r.text().await.unwrap_or_default();
                        tracing::warn!("dm_admins: uid {} returned {}", uid, s);
                    }
                }
                Err(e) => tracing::warn!("dm_admins: uid {} send failed: {}", uid, e),
            }
        }
    }

    async fn send_message_ex(
        &self,
        chat_id: &str,
        text: &str,
        reply_markup: Option<&str>,
    ) -> Result<i64> {
        self.send_message_full(chat_id, text, reply_markup, None).await
    }

    /// sendPhoto with a multipart-uploaded PNG buffer + HTML caption.
    /// Returns the new message_id. Caption max 1024 chars (TG limit).
    /// Used for the calls-channel ape card so each new call lands with
    /// a real chart screenshot above it. Edits go through editMessageCaption.
    async fn send_photo(
        &self,
        chat_id: &str,
        png: Vec<u8>,
        caption: &str,
        reply_markup: Option<&str>,
    ) -> Result<i64> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendPhoto",
            self.cfg.bot_token
        );
        let part = reqwest::multipart::Part::bytes(png)
            .file_name("chart.png")
            .mime_str("image/png")?;
        let mut form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .text("parse_mode", "HTML".to_string())
            .text("caption", caption.to_string())
            .part("photo", part);
        if let Some(kb) = reply_markup {
            form = form.text("reply_markup", kb.to_string());
        }
        let resp = self.http.post(&url).multipart(form).send().await?;
        let body: serde_json::Value = resp.json().await?;
        if body["ok"].as_bool() != Some(true) {
            return Err(anyhow!("telegram sendPhoto failed: {}", body));
        }
        Ok(body["result"]["message_id"]
            .as_i64()
            .ok_or_else(|| anyhow!("missing message_id"))?)
    }

    /// editMessageCaption — used when a calls-channel sendPhoto card
    /// closes (settle path). Replaces the caption text + keyboard while
    /// leaving the chart photo unchanged. The original entry chart still
    /// gives readers the visual context of where we entered.
    async fn edit_photo_caption(
        &self,
        chat_id: &str,
        message_id: i64,
        caption: &str,
        reply_markup: Option<&str>,
    ) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/editMessageCaption",
            self.cfg.bot_token
        );
        let mut form = vec![
            ("chat_id", chat_id.to_string()),
            ("message_id", message_id.to_string()),
            ("caption", caption.to_string()),
            ("parse_mode", "HTML".to_string()),
        ];
        if let Some(kb) = reply_markup {
            form.push(("reply_markup", kb.to_string()));
        }
        let resp = self.http.post(&url).form(&form).send().await?;
        let body: serde_json::Value = resp.json().await?;
        if body["ok"].as_bool() != Some(true) {
            let desc = body["description"].as_str().unwrap_or("");
            if desc.contains("not modified") {
                return Ok(());
            }
            return Err(anyhow!("telegram editMessageCaption failed: {}", body));
        }
        Ok(())
    }

    /// Ask zeroclaw to author a one-line close verdict in ape voice.
    /// Routes through `http://zeroclaw:42617/webhook` (ChatGPT OAuth via
    /// zeroclaw — no API key in photon). 4s timeout; on error/timeout
    /// returns Err and the caller falls back to the deterministic
    /// compose_ape_brag/loss line.
    ///
    /// We deliberately use a tiny isolated prompt (NOT the giant
    /// CLAW_SYSTEM_PROMPT used by /claw) — this is a styling task, not
    /// a reasoning task, and the smaller prompt steers GPT toward
    /// actually following the format/length constraints.
    async fn claw_verdict_line(
        &self,
        ticker: &str,
        horizon: crate::horizon::Horizon,
        exit_pct: f64,
        peak_pct: f64,
        reason: &str,
        is_win: bool,
    ) -> anyhow::Result<String> {
        let horizon_label = match horizon {
            crate::horizon::Horizon::Scalp    => "SCALP (quick flip)",
            crate::horizon::Horizon::Short    => "SHORT (swing)",
            crate::horizon::Horizon::Long     => "LONG (thesis hold)",
            crate::horizon::Horizon::Moonshot => "MOONSHOT (lottery shot)",
            crate::horizon::Horizon::Unknown  => "memecoin punt",
        };
        let _ = is_win; // bucket already encoded by exit pct in prompt
        let mult = 1.0 + exit_pct / 100.0;
        let peak_clause = if peak_pct > exit_pct + 25.0 && peak_pct >= 50.0 {
            format!(" · peak {:+.0}% (trail caught a retrace)", peak_pct)
        } else {
            String::new()
        };
        let prompt = format!(
            "You write one-line trade-close commentary for a Telegram trading channel. \
             Voice: human trader, factual, action-oriented. First person, lowercase, \
             no emojis, no hashtags, no exclamation marks, no metaphors, no similes, \
             no poetry, no \"like a ___\" comparisons. Just what happened: where you \
             got in, what the price did, why you exited. State numbers when they matter. \
             100-160 chars. Vary your phrasing across calls.\n\n\
             Context:\n\
             - ticker: ${ticker}\n\
             - horizon: {horizon}\n\
             - exit: {pct:+.0}% ({mult:.2}x){peak}\n\
             - reason: {reason}\n\n\
             Examples of the voice (don't copy phrasing, just shape):\n\
             - aped {ticker_eg1} at 40k, ran to +120, trailing stop took me out at +85.\n\
             - in {ticker_eg2} at 12k, never moved, moonshot stop hit at -27.\n\
             - scalp filled on {ticker_eg3}, +30 clean, walked.\n\
             - {ticker_eg4} peaked +47 then bled, out -8 on the retrace.\n\
             - stopped on {ticker_eg5} at -42, flow died at entry, no setup.\n\n\
             Reply with the line only. Nothing else.",
            ticker = ticker,
            horizon = horizon_label,
            pct = exit_pct,
            mult = mult,
            peak = peak_clause,
            reason = reason,
            ticker_eg1 = "$EG1", ticker_eg2 = "$EG2", ticker_eg3 = "$EG3",
            ticker_eg4 = "$EG4", ticker_eg5 = "$EG5",
        );
        let body = serde_json::json!({ "message": prompt });

        // Up to 5 attempts with exponential backoff (0.5s, 1s, 2s, 4s, 8s).
        // The 6/43 fallbacks in the prior backfill all hit "error sending
        // request for url" repeatedly — sporadic connection issues at the
        // zeroclaw side, not sustained outage. 500ms × 3 attempts wasn't
        // enough; longer windows ride out whatever's happening. Stateless
        // per-call session via unique X-Session-Id (without it each request
        // inherits the running ChatGPT conversation's full prior context).
        const MAX_ATTEMPTS: u32 = 5;
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=MAX_ATTEMPTS {
            let session_id = format!(
                "photon-verdict-{}-{}-{}",
                ticker,
                attempt,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            );
            let send_result = self
                .http
                .post("http://zeroclaw:42617/webhook")
                .header("X-Session-Id", session_id)
                .timeout(std::time::Duration::from_secs(12))
                .json(&body)
                .send()
                .await;
            let resp = match send_result {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(anyhow!("attempt {}: {}", attempt, e));
                    if attempt < MAX_ATTEMPTS {
                        // Exponential backoff: 500ms × 2^(attempt-1).
                        // attempts 1..4 sleep 0.5s, 1s, 2s, 4s before retry.
                        let delay_ms = 500u64 * (1u64 << (attempt - 1));
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    break;
                }
            };
            if !resp.status().is_success() {
                last_err = Some(anyhow!("attempt {}: zeroclaw {}", attempt, resp.status()));
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                break;
            }
            let data: serde_json::Value = match resp.json().await {
                Ok(d) => d,
                Err(e) => {
                    last_err = Some(anyhow!("attempt {}: parse {}", attempt, e));
                    if attempt < MAX_ATTEMPTS {
                        // Exponential backoff: 500ms × 2^(attempt-1).
                        // attempts 1..4 sleep 0.5s, 1s, 2s, 4s before retry.
                        let delay_ms = 500u64 * (1u64 << (attempt - 1));
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    break;
                }
            };
            let raw = data["response"].as_str().unwrap_or("").trim().to_string();
            let cleaned = raw
                .trim_matches(|c: char| c == '"' || c == '`' || c == '\'' || c.is_whitespace())
                .to_string();
            if cleaned.is_empty() {
                last_err = Some(anyhow!("attempt {}: empty response", attempt));
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                break;
            }
            let bounded = if cleaned.len() > 200 {
                format!("{}…", &cleaned[..199])
            } else {
                cleaned
            };
            return Ok(bounded);
        }
        Err(last_err.unwrap_or_else(|| anyhow!("zeroclaw verdict failed all {} attempts", MAX_ATTEMPTS)))
    }

    /// deleteMessage — Telegram bot API. Used by the lounge-anchor bump
    /// to drop the prior copy. Soft-fails on missing/already-deleted
    /// messages so the bump is idempotent across restarts.
    async fn delete_message(&self, chat_id: &str, message_id: i64) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/deleteMessage",
            self.cfg.bot_token
        );
        let form = vec![
            ("chat_id", chat_id.to_string()),
            ("message_id", message_id.to_string()),
        ];
        let resp = self.http.post(&url).form(&form).send().await?;
        let body: serde_json::Value = resp.json().await?;
        if body["ok"].as_bool() != Some(true) {
            return Err(anyhow!("deleteMessage failed: {}", body));
        }
        Ok(())
    }

    /// forwardMessage — preserves the source's inline keyboard (e.g.
    /// Safeguard's "tap to verify" URL button), unlike copyMessage which
    /// drops reply_markup. Adds a small "Forwarded from <chat>" header
    /// in the destination, which is fine when the source IS the
    /// destination channel (the most common anchor case).
    async fn forward_message(
        &self,
        chat_id: &str,
        from_chat_id: &str,
        message_id: i64,
    ) -> Result<i64> {
        let url = format!(
            "https://api.telegram.org/bot{}/forwardMessage",
            self.cfg.bot_token
        );
        let form = vec![
            ("chat_id", chat_id.to_string()),
            ("from_chat_id", from_chat_id.to_string()),
            ("message_id", message_id.to_string()),
        ];
        let resp = self.http.post(&url).form(&form).send().await?;
        let body: serde_json::Value = resp.json().await?;
        if body["ok"].as_bool() != Some(true) {
            return Err(anyhow!("forwardMessage failed: {}", body));
        }
        Ok(body["result"]["message_id"]
            .as_i64()
            .ok_or_else(|| anyhow!("forwardMessage missing result.message_id"))?)
    }

    /// "Always at the bottom" anchor in the lounge. Telegram pins go to
    /// the TOP of a chat — the magic trick to keep a message at the
    /// BOTTOM is: every time a new message arrives in the lounge, delete
    /// the previous copy of the anchor and copyMessage from the source
    /// fresh, which puts the new copy at the bottom of the channel.
    /// Source defaults to the lounge itself when `lounge_anchor_chat` is
    /// empty (the most common case — anchor is just a pinned-style
    /// message that lives in the same channel).
    ///
    /// Caller spawns this in a detached task so the lounge-send hot path
    /// doesn't block on the delete + copy round-trip. Failures are
    /// logged and the next bump retries from scratch.
    pub async fn bump_lounge_anchor(&self) {
        if !self.cfg.enabled {
            return;
        }
        let lounge_chat = self.cfg.lounge_chat_id.clone();
        let anchor_msg = self.cfg.lounge_anchor_msg_id;
        if lounge_chat.is_empty() || anchor_msg <= 0 {
            return;
        }
        let source_chat = if self.cfg.lounge_anchor_chat.is_empty() {
            lounge_chat.clone()
        } else {
            self.cfg.lounge_anchor_chat.clone()
        };
        // Drop the prior copy first so the new copy is unambiguously the
        // bottom message. Soft-fail: if the prior copy was already
        // deleted by the operator (or never existed) the delete errors
        // and we proceed to the fresh copy.
        let prev = self.db.get_lounge_anchor_msg_id().unwrap_or(0);
        if prev > 0 {
            if let Err(e) = self.delete_message(&lounge_chat, prev).await {
                let s = format!("{}", e);
                if !s.contains("message to delete not found")
                    && !s.contains("message can't be deleted")
                {
                    tracing::debug!("anchor-bump: delete prev {} failed: {}", prev, e);
                }
            }
        }
        // forwardMessage instead of copyMessage — preserves the
        // source's inline keyboard (Safeguard's "tap to verify" URL
        // button is the primary use case). copyMessage drops reply_markup.
        match self.forward_message(&lounge_chat, &source_chat, anchor_msg).await {
            Ok(new_id) => {
                let _ = self.db.set_lounge_anchor_msg_id(new_id);
                tracing::debug!("anchor-bump: forwarded source {} → {} (prev was {})", anchor_msg, new_id, prev);
            }
            Err(e) => {
                tracing::warn!(
                    "anchor-bump: forward {} from {} → {} failed: {}",
                    anchor_msg, source_chat, lounge_chat, e
                );
            }
        }
    }

    /// Build the calls-channel chart for a token. Looks up the recent
    /// price history (since the call's first_seen — bounded to 6h to
    /// keep the sparkline meaningful for moonshot windows). Falls back
    /// to a placeholder when no history exists yet.
    fn build_call_chart(
        &self,
        address: &str,
        symbol: &str,
        entry_price: f64,
        since_ts: Option<i64>,
    ) -> Option<Vec<u8>> {
        let lookback = since_ts.unwrap_or_else(|| chrono::Utc::now().timestamp() - 6 * 3600);
        let series = self.db.get_price_history(address, lookback).ok()?;
        crate::chart::render_sparkline(&series, entry_price, symbol).ok()
    }

    /// sendMessage with optional preview URL. When `preview_url` is
    /// Some, Telegram fetches that page's OG image and renders it above
    /// the card (DexScreener pair URLs serve a chart screenshot as their
    /// OG image — that's the call-card chart). When None, link previews
    /// are suppressed.
    async fn send_message_full(
        &self,
        chat_id: &str,
        text: &str,
        reply_markup: Option<&str>,
        preview_url: Option<&str>,
    ) -> Result<i64> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.cfg.bot_token
        );
        let preview = match preview_url {
            Some(u) => format!(
                r#"{{"url":"{}","prefer_large_media":true,"show_above_text":true}}"#,
                u.replace('"', "\\\"")
            ),
            None => r#"{"is_disabled":true}"#.to_string(),
        };
        let mut form = vec![
            ("chat_id", chat_id.to_string()),
            ("text", text.to_string()),
            ("parse_mode", "HTML".to_string()),
            ("link_preview_options", preview),
        ];
        if let Some(kb) = reply_markup {
            form.push(("reply_markup", kb.to_string()));
        }
        let resp = self.http.post(&url).form(&form).send().await?;
        let body: serde_json::Value = resp.json().await?;
        if body["ok"].as_bool() != Some(true) {
            return Err(anyhow!("telegram sendMessage failed: {}", body));
        }
        Ok(body["result"]["message_id"]
            .as_i64()
            .ok_or_else(|| anyhow!("missing message_id"))?)
    }

    async fn edit_message(&self, chat_id: &str, message_id: i64, text: &str) -> Result<()> {
        self.edit_message_ex(chat_id, message_id, text, None).await
    }

    async fn edit_message_ex(
        &self,
        chat_id: &str,
        message_id: i64,
        text: &str,
        reply_markup: Option<&str>,
    ) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/editMessageText",
            self.cfg.bot_token
        );
        let mut form = vec![
            ("chat_id", chat_id.to_string()),
            ("message_id", message_id.to_string()),
            ("text", text.to_string()),
            ("parse_mode", "HTML".to_string()),
            (
                "link_preview_options",
                r#"{"is_disabled":true}"#.to_string(),
            ),
        ];
        if let Some(kb) = reply_markup {
            form.push(("reply_markup", kb.to_string()));
        }
        let resp = self.http.post(&url).form(&form).send().await?;
        let body: serde_json::Value = resp.json().await?;
        if body["ok"].as_bool() != Some(true) {
            let desc = body["description"].as_str().unwrap_or("");
            if desc.contains("not modified") {
                return Ok(());
            }
            return Err(anyhow!("telegram editMessageText failed: {}", body));
        }
        Ok(())
    }

    // -- Card rendering -----------------------------------------------------

    /// Render the public call-channel card — ape voice, no data dump.
    /// Layout: header (status + ticker + multiplier) → ape paragraph →
    /// tiny price line → track link. Telegram pulls a chart screenshot
    /// above this from the DexScreener pair URL via link preview
    /// (callers must enable it via send_message_full preview_url).
    fn render_call_card(
        &self,
        address: &str,
        meta: Option<&crate::metadata::TokenMeta>,
        _timeline: &[TimelineEntry],
        status: &str,  // "active" | "withdrew" | "failed" | "expired"
        entry_note: &str,   // call.note — entry narrative (already ape-voiced for auto-fires)
        exit_note: &str,    // settle verdict ("-22.1% · moonshot stop") or empty
    ) -> String {
        self.render_call_card_with_body(address, meta, status, entry_note, exit_note, None)
    }

    /// Variant that lets the caller supply a pre-composed body line —
    /// used by apply_outcome_card to inject a claw-authored verdict.
    /// When `override_body` is None, falls back to the deterministic
    /// compose_ape_brag/loss output. The override is responsible for
    /// being safe to html_escape (the renderer escapes it).
    fn render_call_card_with_body(
        &self,
        address: &str,
        meta: Option<&crate::metadata::TokenMeta>,
        status: &str,
        entry_note: &str,
        exit_note: &str,
        override_body: Option<&str>,
    ) -> String {
        let ticker_name = match meta {
            Some(m) => format!("<b>${}</b>", html_escape(&m.symbol)),
            None => {
                let end = address.len().saturating_sub(5);
                format!("<code>{}…{}</code>", &address[..6.min(address.len())], &address[end..])
            }
        };
        let (horizon, narrative) = crate::horizon::parse_with_clean(entry_note);
        let exit_pct = parse_pct_prefix(exit_note);
        let peak_pct = parse_peak_pct(exit_note);
        let pct_signed = exit_pct.unwrap_or(0.0);
        let peak_signed = peak_pct.unwrap_or(pct_signed.max(0.0));

        // Realized-pct bucket — source of truth for header + body voice.
        // Mirrors the site's pctBucket(): scanner.status is a lifecycle
        // tag (which exit branch fired), realized pct is the user-visible
        // truth. A position tagged "withdrew" by the trail-at-breakeven
        // trigger but exited at -97% is a loss, not a win. Flat is
        // realized-flat regardless of peak.
        let is_terminal = matches!(status, "withdrew" | "closed" | "failed" | "expired" | "voided");
        let bucket: &str = if !is_terminal {
            "active"
        } else if exit_pct.is_none() {
            "unknown"
        } else if pct_signed >= 5.0 {
            "won"
        } else if pct_signed <= -5.0 {
            "loss"
        } else {
            "flat"
        };

        let header = match (bucket, status) {
            ("active", _) => format!("📣 {} — new call", ticker_name),
            (_, "voided") => format!("⚪ {} — voided", ticker_name),
            ("won", _) => {
                let mult_label = exit_pct
                    .map(|p| {
                        let m = 1.0 + p / 100.0;
                        if m >= 2.0 { format!(" — {:.1}x ✊", m) }
                        else { format!(" — {:+.0}%", p) }
                    })
                    .unwrap_or_default();
                format!("🟢 {}{}", ticker_name, mult_label)
            }
            ("loss", _) => {
                let label = exit_pct.map(|p| format!(" — {:+.0}%", p)).unwrap_or_default();
                format!("🔴 {}{}", ticker_name, label)
            }
            ("flat", _) => format!("⚪ {} — flat", ticker_name),
            // expired or unknown bucket
            (_, "expired") => format!("⏰ {} — no follow-through", ticker_name),
            _ => format!("· {}", ticker_name),
        };

        // Body voice. Override (claw-authored) wins when present. Otherwise
        // the deterministic composer maps from realized-pct bucket.
        let body = if let Some(b) = override_body {
            b.to_string()
        } else {
            match bucket {
                "won"  => compose_ape_brag(pct_signed, peak_signed, horizon),
                "loss" => compose_ape_loss(pct_signed, peak_signed, status),
                "flat" => "trail caught it at breakeven. flat exit.".to_string(),
                "active" => narrative.trim().to_string(),
                _ => narrative.trim().to_string(),
            }
        };

        // Tiny price/mc line — single row, no liq/vol clutter.
        let price_line = match meta {
            Some(m) => {
                let mc = m.market_cap_usd.or(m.fdv_usd)
                    .map(|v| format!("mc {}", compact_usd(v)))
                    .unwrap_or_default();
                let px = m.price_usd
                    .map(|v| format!("px ${:.6}", v))
                    .unwrap_or_default();
                let parts: Vec<&str> = [&px as &str, &mc].iter()
                    .filter(|s| !s.is_empty())
                    .copied()
                    .collect();
                if parts.is_empty() { String::new() } else { format!("\n\n{}", parts.join(" · ")) }
            }
            None => String::new(),
        };

        let track_line = format!(
            "\n\n<a href=\"https://madapesai.com/#call={}\">track live on madapesai.com</a>",
            address
        );

        let body_block = if body.is_empty() { String::new() } else { format!("\n{}", html_escape(&body)) };
        format!("{header}{body}{px}{track}",
            header = header,
            body = body_block,
            px = price_line,
            track = track_line,
        )
    }

    /// Render the lounge mirror — heavy data card. Includes the full
    /// numbers block (mom/dist/spring/tpm, forensics, h1 tape) when the
    /// caller passes an analysis snapshot, falling back to the entry
    /// narrative on close (when `a` is not in scope). Always includes
    /// market line with liq/vol and the full history blockquote.
    fn render_lounge_card(
        &self,
        address: &str,
        meta: Option<&crate::metadata::TokenMeta>,
        analysis: Option<(&TokenAnalysis, i32)>,
        timeline: &[TimelineEntry],
        status: &str,
        entry_note: &str,
        exit_note: &str,
    ) -> String {
        let ticker_name = match meta {
            Some(m) => format!("<b>${}</b>", html_escape(&m.symbol)),
            None => {
                let end = address.len().saturating_sub(5);
                format!("<code>{}…{}</code>", &address[..6.min(address.len())], &address[end..])
            }
        };
        let (horizon_badge, narrative) = parse_horizon_from_note(entry_note);
        let term_label = horizon_badge.map(|h| format!(" · <b>{}</b>", h)).unwrap_or_default();
        let header = match status {
            "withdrew" | "closed" => format!("🟢 <b>BANKED</b>{} · {}", term_label, ticker_name),
            "failed"   => format!("🔴 <b>FAILED</b>{} · {}", term_label, ticker_name),
            "expired"  => format!("⏰ <b>EXPIRED</b>{} · {}", term_label, ticker_name),
            "voided"   => format!("⚪ <b>VOIDED</b>{} · {}", term_label, ticker_name),
            _          => format!("📣 <b>SIGNAL</b>{} · {}", term_label, ticker_name),
        };
        let verdict_line = if !exit_note.trim().is_empty() && status != "active" {
            format!("\n<b>{}</b>", html_escape(exit_note))
        } else {
            String::new()
        };
        // Body: when we have a live analysis snapshot, use the rich
        // signals-card paragraph + numbers block (mom/dist/spring/tpm,
        // forensics, 1h tape). Otherwise fall back to the entry note
        // narrative (close path, no `a` in scope).
        let body = match analysis {
            Some((a, conf)) => {
                let para = templates::caller_paragraph(a, meta, None);
                let nums = templates::numbers_block(a, meta, conf);
                format!("\n\n{}\n\n{}", html_escape(&para), nums)
            }
            None => {
                if narrative.is_empty() {
                    String::new()
                } else {
                    format!("\n\n{}", html_escape(&narrative))
                }
            }
        };
        let market_line = match meta {
            Some(m) => {
                let mc  = m.market_cap_usd.or(m.fdv_usd).map(|v| format!("mc {}", compact_usd(v))).unwrap_or_else(|| "mc ?".to_string());
                let liq = m.liquidity_usd.map(|v| format!("liq {}", compact_usd(v))).unwrap_or_default();
                let vol = m.volume_24h_usd.map(|v| format!("24h {}", compact_usd(v))).unwrap_or_default();
                let px  = m.price_usd.map(|v| format!("px ${:.6}", v)).unwrap_or_else(|| "px ?".to_string());
                let parts: Vec<&str> = [&px as &str, &mc, &liq, &vol]
                    .iter()
                    .filter(|s| !s.is_empty())
                    .copied()
                    .collect();
                format!("\n\n{}", parts.join(" · "))
            }
            None => String::new(),
        };
        let track_line = format!(
            "\n\n📊 <a href=\"https://madapesai.com/#call={}\">track live on madapesai.com</a>",
            address
        );
        let mut html = format!(
            "{header}{verdict}{body}{market}{track}",
            header = header,
            verdict = verdict_line,
            body = body,
            market = market_line,
            track = track_line,
        );
        if !timeline.is_empty() {
            html.push_str("\n\n<blockquote expandable>— history ——");
            for e in timeline {
                let t = chrono::DateTime::from_timestamp(e.ts, 0)
                    .map(|dt| dt.format("%H:%M UTC").to_string())
                    .unwrap_or_else(|| e.ts.to_string());
                html.push_str(&format!("\n{} <b>{:<8}</b> {}", t, e.kind, e.line));
            }
            html.push_str("</blockquote>");
        }
        if html.len() > 4000 {
            html.truncate(4000);
            html.push_str("\n…[truncated]");
        }
        html
    }

    /// Post a manual call card to the signals channel. Skips should_signal() —
    /// the operator made the call. Idempotent: if a delivery already exists,
    /// adds a timeline entry instead of posting a new message.
    pub async fn fire_call_card(&self, address: &str, note: &str, entry_mcap: f64) -> anyhow::Result<()> {
        if !self.cfg.enabled {
            return Ok(());
        }
        let channel = CALLS_CHANNEL;
        let chat_id = self.cfg.signals_chat_id.clone();
        let now = chrono::Utc::now().timestamp();
        let meta = crate::metadata::fetch(address).await.ok().flatten();
        let meta_ref = meta.as_ref();
        let price = meta_ref.and_then(|m| m.price_usd);

        let call_line = if entry_mcap > 0.0 {
            format!("manual · mc {}", compact_usd(entry_mcap))
        } else {
            "manual".to_string()
        };

        let kb = self.token_keyboard(address, meta_ref.and_then(|m| m.pair_url.as_deref()));
        let existing = self.db.get_active_delivery(address, channel)?;
        match existing {
            None => {
                let timeline = vec![TimelineEntry { ts: now, kind: "called".into(), line: call_line }];
                let html = self.render_call_card(address, meta_ref, &timeline, "active", note, "");
                let chart_png = self.build_call_chart(
                    address,
                    meta_ref.map(|m| m.symbol.as_str()).unwrap_or("?"),
                    price.unwrap_or(0.0),
                    None,
                );
                let msg_id = match chart_png {
                    Some(png) => match self.send_photo(&chat_id, png, &html, Some(&kb)).await {
                        Ok(id) => id,
                        Err(e) => {
                            tracing::warn!("manual call sendPhoto failed for {}: {} — falling back to text", address, e);
                            self.send_message_ex(&chat_id, &html, Some(&kb)).await?
                        }
                    },
                    None => self.send_message_ex(&chat_id, &html, Some(&kb)).await?,
                };
                let timeline_json = serde_json::to_string(&timeline)?;
                self.db.insert_delivery(address, channel, msg_id, 0, "MANUAL", price, None, &timeline_json)?;

                // Lounge mirror for manual calls — same dual-destination
                // shape as auto-fired calls. Operator-typed note becomes
                // the body since we don't have a TokenAnalysis snapshot
                // for manual calls.
                if !self.cfg.lounge_chat_id.is_empty()
                    && self.cfg.lounge_chat_id != self.cfg.signals_chat_id
                {
                    let lounge_html = self.render_lounge_card(
                        address, meta_ref, None, &timeline, "active", note, "",
                    );
                    if let Ok(lounge_msg_id) = self
                        .send_message_ex(&self.cfg.lounge_chat_id, &lounge_html, Some(&kb))
                        .await
                    {
                        let _ = self.db.insert_delivery(
                            address, LOUNGE_CHANNEL, lounge_msg_id, 0, "MANUAL",
                            price, None, &timeline_json,
                        );
                        self.bump_lounge_anchor().await;
                    }
                }
            }
            Some(d) if d.status == "active" => {
                let mut timeline: Vec<TimelineEntry> = serde_json::from_str(&d.timeline_json).unwrap_or_default();
                timeline.push(TimelineEntry { ts: now, kind: "called".into(), line: call_line });
                let html = self.render_call_card(address, meta_ref, &timeline, "active", note, "");
                self.edit_message_ex(&chat_id, d.message_id, &html, Some(&kb)).await?;
                let timeline_json = serde_json::to_string(&timeline)?;
                self.db.update_delivery(d.id, "active", 0, "MANUAL", price, None, &timeline_json)?;
            }
            Some(_) => {} // terminal — don't reopen
        }
        Ok(())
    }

    /// Update the call card when a position closes. Outcome: "withdrew" |
    /// "failed" | "expired" | "voided". No-op when no active delivery
    /// exists for the token. Used by the settling phase + manual
    /// /close_call. For terminal deliveries (already demoted), use
    /// `force_update_card` instead.
    pub async fn update_call_outcome(&self, address: &str, outcome: &str, exit_pct: Option<f64>, exit_note: &str) -> anyhow::Result<()> {
        self.apply_outcome_card(address, outcome, exit_pct, exit_note, false).await
    }

    /// Re-render a terminal delivery's card with a fresh outcome. Unlike
    /// `update_call_outcome`, this works on already-demoted rows — used
    /// by startup backfill to replay the outcome on cards that were
    /// edited under the old (pre-rewrite) format or were never given a
    /// proper verdict (voided cards from the orphan-cleanup migration).
    /// Idempotent: re-running on a card that already shows the right
    /// state produces a Telegram "message not modified" 400 which we
    /// treat as success.
    pub async fn force_update_card(&self, address: &str, outcome: &str, exit_pct: Option<f64>, exit_note: &str) -> anyhow::Result<()> {
        self.apply_outcome_card(address, outcome, exit_pct, exit_note, true).await
    }

    async fn apply_outcome_card(&self, address: &str, outcome: &str, exit_pct: Option<f64>, exit_note: &str, force: bool) -> anyhow::Result<()> {
        if !self.cfg.enabled {
            return Ok(());
        }
        let channel = CALLS_CHANNEL;
        let chat_id = self.cfg.signals_chat_id.clone();
        let now = chrono::Utc::now().timestamp();
        let meta = crate::metadata::fetch(address).await.ok().flatten();
        let meta_ref = meta.as_ref();

        let d = match self.db.get_active_delivery(address, channel)? {
            Some(d) if d.status == "active" || force => d,
            _ => return Ok(()),
        };

        let mut timeline: Vec<TimelineEntry> = serde_json::from_str(&d.timeline_json).unwrap_or_default();
        // Construct the line WITHOUT prepending pct — settle passes exit_note
        // already formatted with the pct ("-32.8% · scalp stop"), so adding
        // another pct prefix produces "-32.8% · -32.8% · scalp stop". Only
        // prepend when exit_note doesn't already start with a percentage.
        let already_has_pct = exit_note.trim_start().starts_with(|c: char| c == '+' || c == '-')
            && exit_note.contains('%');
        let line = if already_has_pct {
            exit_note.to_string()
        } else {
            let pct = exit_pct.map(|p| format!("{:+.1}% · ", p)).unwrap_or_default();
            format!("{}{}", pct, exit_note)
        };
        // Dedup: skip when the most-recent entry has the same outcome AND
        // a line that's a substring/superset (handles the historical
        // double-pct entries that already exist in production timelines).
        let is_dup = timeline.last().map(|e| {
            e.kind == outcome
                && (e.line == line
                    || e.line.contains(&line)
                    || line.contains(&e.line))
        }).unwrap_or(false);
        if !is_dup {
            timeline.push(TimelineEntry { ts: now, kind: outcome.to_string(), line: line.clone() });
        }

        // Recover the original entry narrative so closed cards still render
        // their thesis instead of degrading to just the verdict line.
        let entry_note = self
            .db
            .get_call_by_mint(address)
            .ok()
            .flatten()
            .map(|c| c.note)
            .unwrap_or_default();

        // Ask zeroclaw to author the close verdict line. Bucket by
        // realized pct (matches render_call_card and site logic):
        //   |pct| < 5  → flat  (skip claw, fixed breakeven line)
        //   pct >= 5   → won   (claw writes brag)
        //   pct <= -5  → loss  (claw writes loss line)
        // Bucket on realized pct alone — peak-gating was wrong since a
        // runner-retrace is a loss in the user's wallet regardless of
        // how high it printed. is_win for the prompt follows the
        // realized bucket too, not the scanner's outcome tag.
        let exit_pct_val = exit_pct.unwrap_or(0.0);
        let peak_pct_val = parse_peak_pct(exit_note).unwrap_or(exit_pct_val.max(0.0));
        let bucket = if exit_pct.is_none() { "unknown" }
            else if exit_pct_val >= 5.0 { "won" }
            else if exit_pct_val <= -5.0 { "loss" }
            else { "flat" };
        let has_ape_narrative = !entry_note.trim().is_empty()
            && !entry_note.contains("FIRST CALL")
            && !entry_note.starts_with("manual");
        let claw_body: Option<String> = if (bucket == "won" || bucket == "loss") && has_ape_narrative {
            let (horizon, _) = crate::horizon::parse_with_clean(&entry_note);
            let ticker = meta_ref.map(|m| m.symbol.as_str()).unwrap_or("?");
            let is_win = bucket == "won";
            let reason = exit_note
                .splitn(2, " · ")
                .nth(1)
                .unwrap_or(exit_note)
                .trim()
                .to_string();
            match self
                .claw_verdict_line(ticker, horizon, exit_pct_val, peak_pct_val, &reason, is_win)
                .await
            {
                Ok(line) => {
                    tracing::info!("claw verdict ({}): {} → {}", bucket, address, line);
                    Some(line)
                }
                Err(e) => {
                    tracing::warn!("claw verdict fallback for {} ({}): {}", address, bucket, e);
                    None
                }
            }
        } else {
            None
        };

        let html = self.render_call_card_with_body(
            address, meta_ref, outcome, &entry_note, exit_note, claw_body.as_deref(),
        );
        let kb = self.token_keyboard(address, meta_ref.and_then(|m| m.pair_url.as_deref()));
        // Calls cards posted by the new pipeline are sendPhoto messages,
        // which require editMessageCaption (not editMessageText) to update.
        // Try caption first; on "no text in the message" or "no caption
        // in the message" fall back to text edit (legacy text-only cards).
        let mut attempt = 0;
        let mut try_caption = true;
        loop {
            attempt += 1;
            let edit_result = if try_caption {
                self.edit_photo_caption(&chat_id, d.message_id, &html, Some(&kb)).await
            } else {
                self.edit_message_ex(&chat_id, d.message_id, &html, Some(&kb)).await
            };
            match edit_result {
                Ok(_) => break,
                Err(e) => {
                    let s = format!("{}", e);
                    // Photo→text fallback: legacy text-only deliveries
                    // raise "there is no caption in the message to edit".
                    if try_caption && s.contains("caption") {
                        try_caption = false;
                        continue;
                    }
                    // Text→photo fallback: photo deliveries raise
                    // "there is no text in the message to edit".
                    if !try_caption && s.contains("no text") {
                        try_caption = true;
                        continue;
                    }
                    if s.contains("not modified") {
                        // Already in the right state — continue and persist
                        // the timeline entry if we added one.
                        break;
                    }
                    if force && (s.contains("message to edit not found") || s.contains("MESSAGE_ID_INVALID")) {
                        tracing::info!("force_update_card: msg_id {} no longer exists for {}, skipping", d.message_id, address);
                        return Ok(());
                    }
                    // Telegram channel-edit rate limit. Parse retry_after
                    // from the JSON error body (looks like
                    // `..."retry_after":35,...`) and back off, retry once
                    // before giving up.
                    if let Some(secs) = parse_retry_after(&s) {
                        if force && attempt < 3 {
                            tracing::debug!(
                                "force_update_card: 429 on {} — sleeping {}s",
                                address, secs
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(secs + 1)).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }

        let timeline_json = serde_json::to_string(&timeline)?;
        let price = meta_ref.and_then(|m| m.price_usd);
        self.db.update_delivery(d.id, "demoted", d.snapshot_conf, &d.snapshot_class, price, d.snapshot_top_holder, &timeline_json)?;

        // Mirror the close into the lounge — heavy data card with full
        // timeline + verdict line. Soft-skip when no lounge mirror exists
        // (legacy single-channel deploys, or fires from before the split).
        if !self.cfg.lounge_chat_id.is_empty()
            && self.cfg.lounge_chat_id != self.cfg.signals_chat_id
        {
            if let Ok(Some(lounge_d)) = self.db.get_active_delivery(address, LOUNGE_CHANNEL) {
                if lounge_d.status == "active" || force {
                    let lounge_html = self.render_lounge_card(
                        address, meta_ref, None, &timeline, outcome, &entry_note, exit_note,
                    );
                    if let Err(e) = self
                        .edit_message_ex(
                            &self.cfg.lounge_chat_id,
                            lounge_d.message_id,
                            &lounge_html,
                            Some(&kb),
                        )
                        .await
                    {
                        let s = format!("{}", e);
                        if !s.contains("not modified") && !s.contains("message to edit not found") {
                            tracing::warn!("lounge close edit failed for {}: {}", address, e);
                        }
                    }
                    let _ = self.db.update_delivery(
                        lounge_d.id,
                        "demoted",
                        lounge_d.snapshot_conf,
                        &lounge_d.snapshot_class,
                        price,
                        lounge_d.snapshot_top_holder,
                        &timeline_json,
                    );
                }
            }
        }
        Ok(())
    }

    // -- Winner lifecycle ---------------------------------------------------

    /// Entry point: given a fresh analysis, decide promote/edit/demote for
    /// the winners channel. Silent no-op when nothing to do.
    pub async fn process_token(&self, a: &TokenAnalysis, effective_conf: i32) -> Result<()> {
        if !self.cfg.enabled {
            return Ok(());
        }
        // Defense against transient RPC data glitches. When
        // getMultipleAccounts returns a partial response, top_holder_pct
        // comes back 0.0 with holders > 0. Old behavior: silently skip,
        // which silenced the bot for hours during RPC degradation since
        // every analysis hit this. New behavior: try to backfill from the
        // last good snapshot of this mint within 5min; if none, skip with
        // an info-level log so the silence is observable.
        let analysis_local: TokenAnalysis;
        let a: &TokenAnalysis = if a.top_holder_pct == 0.0 && a.holder_count > 0 {
            match self.db.get_last_good_top_holder(&a.address, 300).ok().flatten() {
                Some((top1, top10)) => {
                    analysis_local = TokenAnalysis {
                        top_holder_pct: top1,
                        top10_pct: top10,
                        ..a.clone()
                    };
                    tracing::info!(
                        "process_token: data-glitch fallback for {} — using last-known top_holder={:.1}% / top10={:.1}%",
                        a.address, top1, top10
                    );
                    &analysis_local
                }
                None => {
                    tracing::info!(
                        "process_token: skipping {} — top_holder=0 with {} holders, no recent fallback (RPC degraded?)",
                        a.address, a.holder_count
                    );
                    return Ok(());
                }
            }
        } else {
            a
        };
        let channel = CALLS_CHANNEL; // legacy internal DB key — kept stable to preserve
                                 // existing telegram_deliveries rows across the rename.
        let chat_id = self.cfg.signals_chat_id.clone();

        let existing = self.db.get_active_delivery(&a.address, channel)?;
        let meta = metadata::fetch(&a.address).await.ok().flatten();
        let price = meta.as_ref().and_then(|m| m.price_usd);
        let first_seen = self
            .db
            .get_token(&a.address)
            .ok()
            .flatten()
            .map(|t| t.first_seen);

        // Layer 1 of the smart-wallet curation pipeline: when this mint
        // crosses the viability threshold for the first time, walk back
        // its first 20 buyers via Helius enhanced-txns and persist them.
        // claim_observation_trace makes the per-mint trace single-fire
        // across the process lifetime; spawn_trace is detached so the
        // hot path doesn't block on an HTTP round-trip.
        if !self.helius_api_key.is_empty() {
            let mcap = meta
                .as_ref()
                .and_then(|m| m.market_cap_usd.or(m.fdv_usd))
                .unwrap_or(0.0);
            let liq = meta.as_ref().and_then(|m| m.liquidity_usd).unwrap_or(0.0);
            let now_ts = chrono::Utc::now().timestamp();
            let age = first_seen.map(|fs| now_ts - fs).unwrap_or(0);
            if crate::wallet_observer::should_trace(
                &a.confidence.classification,
                age,
                mcap,
                liq,
                a.holder_count as i64,
            ) {
                crate::wallet_observer::spawn_trace(
                    self.db.clone(),
                    self.http.clone(),
                    self.helius_api_key.clone(),
                    a.address.clone(),
                );
            }
        }

        match existing {
            None => {
                // Two-tier gate: prefer the SHORT/LONG bucket (deeper, higher
                // win rate), fall back to SCALP for shallow tokens that just
                // printed a 1h+ move. Either passing fires a call; the chosen
                // horizon flows into settle_calls via the note tag.
                let standard_pass = self.should_signal(a, effective_conf, meta.as_ref(), first_seen);
                let scalp_pass = !standard_pass
                    && self.should_scalp_signal(a, meta.as_ref(), first_seen);
                // Moonshot gate fires only when the standard + scalp gates
                // didn't (DEVELOPING class would never pass standard, since
                // standard requires STAIRCASE/GRINDER/SPRING). Disjoint path.
                let moonshot_pass = !standard_pass
                    && !scalp_pass
                    && self.should_moonshot_signal(a, meta.as_ref(), first_seen, price.unwrap_or(0.0));
                if !standard_pass && !scalp_pass && !moonshot_pass {
                    if let Some((gate, gap)) = self.classify_near_miss(a, effective_conf, meta.as_ref(), first_seen) {
                        let mom_delta = a.delta.as_ref().map(|d| d.momentum_delta);
                        let _ = self.db.insert_near_miss(
                            &a.address,
                            &a.confidence.classification,
                            effective_conf,
                            a.top_holder_pct,
                            mom_delta,
                            gate,
                            &gap,
                        );
                    }
                    return Ok(());
                }

                let now = chrono::Utc::now().timestamp();
                let call_line = format!(
                    "{cls} {conf} · top {top:.1}% · mc {mc}",
                    cls = a.confidence.classification,
                    conf = effective_conf,
                    top = a.top_holder_pct,
                    mc = meta
                        .as_ref()
                        .and_then(|m| m.market_cap_usd.or(m.fdv_usd))
                        .map(|v| format!("${:.1}k", v / 1000.0))
                        .unwrap_or_else(|| "?".to_string()),
                );
                let timeline = vec![TimelineEntry {
                    ts: now,
                    kind: "called".into(),
                    line: call_line,
                }];

                // Resolve horizon + ape narrative BEFORE rendering — render_call_card
                // reads the entry note for the body line on the calls channel.
                let liq = meta.as_ref().and_then(|m| m.liquidity_usd).unwrap_or(0.0);
                let mcap = meta
                    .as_ref()
                    .and_then(|m| m.market_cap_usd.or(m.fdv_usd))
                    .unwrap_or(0.0);
                let sym = meta.as_ref().map(|m| m.symbol.clone()).unwrap_or_default();
                let dex = meta
                    .as_ref()
                    .and_then(|m| m.dex_id.clone())
                    .unwrap_or_default();
                let auto_horizon = if moonshot_pass {
                    crate::horizon::Horizon::Moonshot
                } else if scalp_pass {
                    crate::horizon::Horizon::Scalp
                } else if mcap >= 1_000_000.0 {
                    crate::horizon::Horizon::Long
                } else {
                    crate::horizon::Horizon::Short
                };
                let auto_horizon_tag = auto_horizon.tag().unwrap_or("");
                let narrative = compose_ape_entry(a, auto_horizon, mcap);
                let auto_note = if auto_horizon_tag.is_empty() {
                    narrative.clone()
                } else {
                    format!("{} · {}", narrative, auto_horizon_tag)
                };

                // Calls channel — ape card with rendered chart sparkline
                // (chart.rs reads token_snapshots for the price history).
                // Falls back to text-only sendMessage when chart render
                // fails (don't kill the call over a render hiccup).
                let ape_html = self.render_call_card(
                    &a.address,
                    meta.as_ref(),
                    &timeline,
                    "active",
                    &auto_note,
                    "",
                );
                let kb = self.token_keyboard(
                    &a.address,
                    meta.as_ref().and_then(|m| m.pair_url.as_deref()),
                );
                let chart_png = self.build_call_chart(
                    &a.address,
                    meta.as_ref().map(|m| m.symbol.as_str()).unwrap_or("?"),
                    price.unwrap_or(0.0),
                    None,
                );
                let msg_id = match chart_png {
                    Some(png) => match self.send_photo(&chat_id, png, &ape_html, Some(&kb)).await {
                        Ok(id) => id,
                        Err(e) => {
                            tracing::warn!("sendPhoto failed for {}: {} — falling back to text", a.address, e);
                            self.send_message_ex(&chat_id, &ape_html, Some(&kb)).await?
                        }
                    },
                    None => self.send_message_ex(&chat_id, &ape_html, Some(&kb)).await?,
                };
                let timeline_json = serde_json::to_string(&timeline)?;
                self.db.insert_delivery(
                    &a.address,
                    channel,
                    msg_id,
                    effective_conf,
                    &a.confidence.classification,
                    price,
                    Some(a.top_holder_pct),
                    &timeline_json,
                )?;

                // Lounge mirror — heavy data card with full numbers/forensics
                // + timeline blockquote. Soft-fail: if the lounge chat is the
                // same as signals_chat_id (pre-split deploys) skip the duplicate.
                if !self.cfg.lounge_chat_id.is_empty()
                    && self.cfg.lounge_chat_id != self.cfg.signals_chat_id
                {
                    let lounge_html = self.render_lounge_card(
                        &a.address,
                        meta.as_ref(),
                        Some((a, effective_conf)),
                        &timeline,
                        "active",
                        &auto_note,
                        "",
                    );
                    match self
                        .send_message_ex(&self.cfg.lounge_chat_id, &lounge_html, Some(&kb))
                        .await
                    {
                        Ok(lounge_msg_id) => {
                            let _ = self.db.insert_delivery(
                                &a.address,
                                LOUNGE_CHANNEL,
                                lounge_msg_id,
                                effective_conf,
                                &a.confidence.classification,
                                price,
                                Some(a.top_holder_pct),
                                &timeline_json,
                            );
                            self.bump_lounge_anchor().await;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "lounge mirror post failed for {}: {} — calls card still posted",
                                a.address, e
                            );
                        }
                    }
                }

                let inserted = self.db.insert_call(
                    &a.address,
                    &sym,
                    &a.confidence.classification,
                    effective_conf,
                    now,
                    mcap,
                    price.unwrap_or(0.0),
                    liq,
                    a.top_holder_pct,
                    &dex,
                    &auto_note,
                    "notifier",
                    a.tx_rate,
                );
                if let Ok(Some(call_id)) = inserted {
                    // Align expires_at with the horizon-based settling window
                    // (scanner::settle_calls). Without this, the UI badges a
                    // misleading "13d left" on every call while the settling
                    // phase actually closes SHORT at 6h.
                    // Auto-fired calls are minute-window plays — capture the
                    // signal influx, ride the trailing stop, exit green.
                    // 90min default: a position that hasn't moved 20% in 90
                    // minutes is dead inventory tying up capital. Trailing
                    // stop already extends winning positions indefinitely
                    // via peak<+20% gate, so 90min only kills flat ones.
                    // LONG horizon (auto-fired at >= $1M graduated mcap)
                    // gets 6h since deeper-cap moves develop slower.
                    let window_secs: i64 = match auto_horizon {
                        crate::horizon::Horizon::Scalp => 90 * 60,
                        crate::horizon::Horizon::Short => 90 * 60,
                        crate::horizon::Horizon::Moonshot => 90 * 60,
                        crate::horizon::Horizon::Long => 6 * 3600,
                        crate::horizon::Horizon::Unknown => 90 * 60,
                    };
                    let expires = now + window_secs;
                    let _ = self.db.set_call_expiration(&a.address, Some(expires));
                    // New auto-call landed — wake the publisher so the site
                    // shows it within ~30s instead of waiting on the 300s tick.
                    self.kick_publisher();

                    // Real-money path: if execution is wired AND enabled,
                    // spawn an async buy bound to this call. Sized adaptively
                    // from the wallet's current SOL balance × the bucket's
                    // size_pct. Single-flight via mark_buy_attempt; settle
                    // path waits on buy_signature before any sell decision.
                    if let Some(exec) = self.executor.clone() {
                        if exec.cfg.enabled {
                            let mint = a.address.clone();
                            let entry_price = price.unwrap_or(0.0);
                            let mcap_for_record = mcap;
                            let horizon_tag = auto_horizon_tag.to_string();
                            tokio::spawn(async move {
                                spawn_buy(exec, call_id, mint, horizon_tag, entry_price, mcap_for_record).await;
                            });
                        }
                    }
                }
            }
            Some(delivery) if delivery.status == "active" => {
                let mut timeline: Vec<TimelineEntry> =
                    serde_json::from_str(&delivery.timeline_json).unwrap_or_default();

                let material = self.is_material_change(
                    delivery.snapshot_conf,
                    &delivery.snapshot_class,
                    delivery.snapshot_price,
                    delivery.snapshot_top_holder,
                    effective_conf,
                    &a.confidence.classification,
                    price,
                    Some(a.top_holder_pct),
                );
                // Active calls are owned by the settling phase — its
                // horizon-aware rules decide success/failure based on price
                // and age, not on classification dips. process_token can
                // still write running timeline updates for active calls,
                // but it must NOT flip the card to FAILED preemptively.
                // ROTUS / TIME MACHINE-class bugs lived here: a transient
                // CRASHING classification or a confidence dip would demote
                // a winning long-thesis card while the price kept running.
                let has_active_call = self.db.has_active_call(&a.address).unwrap_or(false);
                let failed = !has_active_call && self.should_fail(a, effective_conf);

                if !material && !failed {
                    return Ok(());
                }

                let now = chrono::Utc::now().timestamp();
                let status = if failed { "demoted" } else { "active" };
                let kind = if failed { "failed" } else { "update" };
                let line = format!(
                    "{cls} {conf} · top {top:.1}%{price_part}",
                    cls = a.confidence.classification,
                    conf = effective_conf,
                    top = a.top_holder_pct,
                    price_part = match (delivery.snapshot_price, price) {
                        (Some(p0), Some(p1)) if p0 > 0.0 => {
                            format!(" · px {:+.1}%", (p1 - p0) / p0 * 100.0)
                        }
                        _ => String::new(),
                    },
                );
                timeline.push(TimelineEntry {
                    ts: now,
                    kind: kind.into(),
                    line,
                });

                let render_status = if failed { "failed" } else { "active" };
                let kb = self.token_keyboard(
                    &a.address,
                    meta.as_ref().and_then(|m| m.pair_url.as_deref()),
                );
                let timeline_json = serde_json::to_string(&timeline)?;

                // Lounge mirror gets every flip — that's its whole job.
                // Tokens fired before the dual-split have no lounge row;
                // soft-skip when the lookup returns None.
                if let Ok(Some(lounge_d)) =
                    self.db.get_active_delivery(&a.address, LOUNGE_CHANNEL)
                {
                    let lounge_html = self.render_lounge_card(
                        &a.address,
                        meta.as_ref(),
                        Some((a, effective_conf)),
                        &timeline,
                        render_status,
                        "",
                        "",
                    );
                    if let Err(e) = self
                        .edit_message_ex(
                            &self.cfg.lounge_chat_id,
                            lounge_d.message_id,
                            &lounge_html,
                            Some(&kb),
                        )
                        .await
                    {
                        let s = format!("{}", e);
                        if !s.contains("not modified") {
                            tracing::warn!("lounge edit failed for {}: {}", a.address, e);
                        }
                    }
                    let _ = self.db.update_delivery(
                        lounge_d.id,
                        status,
                        effective_conf,
                        &a.confidence.classification,
                        price,
                        Some(a.top_holder_pct),
                        &timeline_json,
                    );
                }

                // Calls channel stays quiet during a position's life —
                // settle's close edit is the only public update. Keep the
                // calls delivery row's snapshot fresh so settle's outcome
                // edit fires correctly without re-rendering the card.
                let _ = self.db.update_delivery(
                    delivery.id,
                    status,
                    effective_conf,
                    &a.confidence.classification,
                    price,
                    Some(a.top_holder_pct),
                    &timeline_json,
                );
            }
            Some(delivery) => {
                // Terminal-but-recoverable. Two cohorts land here:
                //   - status="demoted" cards from the legacy should_fail
                //     era that flipped on classification dips/data
                //     glitches. Token may have recovered.
                //   - status="demoted" cards from settling (close was
                //     written; we leave those alone).
                // Recovery only fires when:
                //   1. Delivery is demoted (not "settled" — those stay)
                //   2. There is no active call for this mint (settling
                //      already owns the lifecycle of any active call)
                //   3. The token now passes the full should_signal gate
                //      (would re-promote if seen fresh today)
                if delivery.status != "demoted" {
                    return Ok(());
                }
                if self.db.has_active_call(&a.address).unwrap_or(false) {
                    return Ok(());
                }
                let first_seen = self
                    .db
                    .get_token(&a.address)
                    .ok()
                    .flatten()
                    .map(|t| t.first_seen);
                if !self.should_signal(a, effective_conf, meta.as_ref(), first_seen) {
                    return Ok(());
                }
                // Recovery flip: append timeline entry, edit card to
                // active state, mark delivery active. Next tick takes
                // over with the normal update path.
                let mut timeline: Vec<TimelineEntry> =
                    serde_json::from_str(&delivery.timeline_json).unwrap_or_default();
                let now = chrono::Utc::now().timestamp();
                let line = format!(
                    "{cls} {conf} · top {top:.1}% · momentum back",
                    cls = a.confidence.classification,
                    conf = effective_conf,
                    top = a.top_holder_pct,
                );
                timeline.push(TimelineEntry {
                    ts: now,
                    kind: "rebound".into(),
                    line,
                });
                // Recover the original ape entry note so the calls card
                // re-renders the narrative (not just the header). Empty
                // when the call row is gone — falls back to header-only.
                let entry_note = self
                    .db
                    .get_call_by_mint(&a.address)
                    .ok()
                    .flatten()
                    .map(|c| c.note)
                    .unwrap_or_default();
                let html = self.render_call_card(
                    &a.address,
                    meta.as_ref(),
                    &timeline,
                    "active",
                    &entry_note,
                    "",
                );
                let kb = self.token_keyboard(
                    &a.address,
                    meta.as_ref().and_then(|m| m.pair_url.as_deref()),
                );
                if let Err(e) = self
                    .edit_message_ex(&chat_id, delivery.message_id, &html, Some(&kb))
                    .await
                {
                    // Original message gone — give up rather than try
                    // to re-post under a new id (would lose history).
                    let s = format!("{}", e);
                    if s.contains("message to edit not found") {
                        tracing::info!(
                            "rebound: msg_id {} no longer exists for {}, leaving demoted",
                            delivery.message_id, a.address
                        );
                        return Ok(());
                    }
                    return Err(e);
                }
                let timeline_json = serde_json::to_string(&timeline)?;
                self.db.update_delivery(
                    delivery.id,
                    "active",
                    effective_conf,
                    &a.confidence.classification,
                    price,
                    Some(a.top_holder_pct),
                    &timeline_json,
                )?;
                tracing::info!(
                    "rebound: {} restored to active ({} {})",
                    a.address, a.confidence.classification, effective_conf
                );
            }
        }
        Ok(())
    }

    // -- Hourly digest ------------------------------------------------------

    /// Build trap candidates from the DB for the window [since_ts, now].
    /// Each candidate has real peak+current snapshots; severity is computed,
    /// not editorialized. No LLM in the loop.
    fn build_trap_candidates(
        &self,
        since_ts: i64,
        peak_lookback_hours: i64,
    ) -> Result<Vec<TrapCandidate>> {
        let peak_since = chrono::Utc::now().timestamp() - (peak_lookback_hours * 3600);
        let tokens = self.db.get_degradation_tokens_since(since_ts)?;
        let mut candidates: Vec<TrapCandidate> = Vec::new();

        for addr in tokens {
            let peak = match self.db.get_peak_snapshot(&addr, peak_since)? {
                Some(p) => p,
                None => continue,
            };
            let current = match self.db.get_latest_snapshot(&addr)? {
                Some(c) => c,
                None => continue,
            };
            // Skip if latest is the peak (no drop to report)
            if current.timestamp == peak.timestamp {
                continue;
            }

            let top_jump = (current.top_holder_pct - peak.top_holder_pct).max(0.0);
            let momentum_loss = (peak.momentum - current.momentum).max(0) as f64;
            let conf_loss = (peak.confidence - current.confidence).max(0) as f64;
            let good = |c: &str| matches!(c, "STAIRCASE" | "GRINDER" | "SPRING" | "SURGE");
            let bad = |c: &str| {
                c.starts_with("UNSAFE") || matches!(c, "ACTIVE_TRAP" | "CRASHING" | "DEAD")
            };
            let class_penalty = if good(&peak.classification) && bad(&current.classification) {
                30.0
            } else if good(&peak.classification) && current.classification == "DEVELOPING" {
                10.0
            } else {
                0.0
            };

            let severity = top_jump * 2.0 + momentum_loss + conf_loss * 0.5 + class_penalty;
            if severity < 5.0 {
                continue;
            } // noise floor — not a real collapse

            candidates.push(TrapCandidate {
                address: addr,
                peak,
                current,
                severity,
            });
        }
        candidates.sort_by(|a, b| {
            b.severity
                .partial_cmp(&a.severity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(candidates)
    }

    /// Render one trap with maximum legibility: clickable ticker leading to the
    /// DexScreener pair (tap for live evidence), headline collapse metric in
    /// bold so the eye finds it instantly, second line for secondary context.
    async fn render_trap_line(&self, c: &TrapCandidate) -> String {
        let meta = metadata::fetch(&c.address).await.ok().flatten();

        // Primary identity — ticker is a clickable link to DexScreener evidence
        let chart_url = meta
            .as_ref()
            .and_then(|m| m.pair_url.clone())
            .unwrap_or_else(|| format!("https://dexscreener.com/solana/{}", c.address));
        let ticker_link = match &meta {
            Some(m) => format!(
                "<a href=\"{}\"><b>${}</b></a>",
                chart_url,
                html_escape(&m.symbol)
            ),
            None => format!(
                "<a href=\"{}\"><code>{}…{}</code></a>",
                chart_url,
                &c.address[..4],
                &c.address[c.address.len() - 4..]
            ),
        };

        // Headline metric: whichever dimension collapsed most — that's WHY
        let top_delta = c.current.top_holder_pct - c.peak.top_holder_pct;
        let mom_loss = c.peak.momentum - c.current.momentum;
        let conf_loss = c.peak.confidence - c.current.confidence;
        let headline = if top_delta >= 15.0 {
            format!(
                "<b>top +{:.0}%</b> (to {:.0}%)",
                top_delta, c.current.top_holder_pct
            )
        } else if mom_loss >= 30 {
            format!("<b>mom -{}</b> (to {})", mom_loss, c.current.momentum)
        } else if c.peak.classification != c.current.classification {
            format!(
                "<b>{}→{}</b>",
                c.peak.classification, c.current.classification
            )
        } else {
            format!("<b>conf -{}</b> (to {})", conf_loss, c.current.confidence)
        };

        // Context line: class transition + market data + age
        let class_part = if c.peak.classification != c.current.classification {
            format!(
                "{}({})→{}({})",
                c.peak.classification,
                c.peak.confidence,
                c.current.classification,
                c.current.confidence
            )
        } else {
            format!(
                "{}({}→{})",
                c.current.classification, c.peak.confidence, c.current.confidence
            )
        };

        let market_part = match &meta {
            Some(m) => {
                let mc = m
                    .market_cap_usd
                    .or(m.fdv_usd)
                    .map(|v| format!(" · mc {}", compact_usd(v)))
                    .unwrap_or_default();
                let px = m
                    .price_change_24h
                    .filter(|p| p.abs() > 0.5)
                    .map(|p| format!(" · 24h {:+.0}%", p))
                    .unwrap_or_default();
                format!("{}{}", mc, px)
            }
            None => String::new(),
        };

        let age = {
            let secs = (chrono::Utc::now().timestamp() - c.peak.timestamp).max(0);
            if secs < 3600 {
                format!("{}m", secs / 60)
            } else if secs < 86400 {
                format!("{:.1}h", secs as f64 / 3600.0)
            } else {
                format!("{:.1}d", secs as f64 / 86400.0)
            }
        };

        // Two lines per trap: identity+headline, then context
        format!(
            "💥 {ticker}  {headline}\n    {class}{market} · peak {age} ago",
            ticker = ticker_link,
            headline = headline,
            class = class_part,
            market = market_part,
            age = age,
        )
    }

    /// Render a trap wrap-up block for a completed hour, wrapped in an
    /// expandable blockquote so the digest stays glanceable while preserving
    /// the full evidence one tap away. Returns empty string if nothing to report.
    pub async fn render_hour_traps(&self, hour_bucket: i64, limit: usize) -> Result<String> {
        let since_ts = hour_bucket;
        let candidates = self.build_trap_candidates(since_ts, 6)?;
        let total = candidates.len();
        if total == 0 {
            return Ok(String::new());
        }

        let shown = total.min(limit);
        let mut out = format!(
            "\n\n<blockquote expandable>— hour traps ({} of {} total) ——",
            shown, total
        );
        for c in candidates.iter().take(limit) {
            out.push('\n');
            out.push_str(&self.render_trap_line(c).await);
        }
        out.push_str("</blockquote>");
        Ok(out)
    }

    /// At hour rollover, append the trap wrap-up to the previous hour's digest
    /// and mark it finalized. Safe to call every cycle — internally dedups via
    /// the `finalized` flag.
    async fn finalize_previous_hour(&self, current_hour_bucket: i64) -> Result<()> {
        let prev = match self.db.get_unfinalized_digest_before(current_hour_bucket)? {
            Some(d) => d,
            None => return Ok(()),
        };
        // Re-render the body for that hour's state (it's been drifting during the hour),
        // then append the trap wrap. We don't have the original body text stored, so we
        // just render fresh stats + append traps — simple and deterministic.
        let body = self.render_digest_body()?;
        let traps = self.render_hour_traps(prev.hour_bucket, 8).await?;
        let final_text = format!("{}{}", body, traps);
        // Target was posted to the ops chat originally
        match self
            .edit_message(&self.cfg.ops_chat_id, prev.message_id, &final_text)
            .await
        {
            Ok(_) => {
                self.db.finalize_digest(prev.id)?;
            }
            Err(e) => {
                let stale = format!("{}", e).contains("message to edit not found");
                if stale {
                    // Original digest message was deleted — nothing to
                    // append to. Mark finalized anyway so we stop trying
                    // to edit a ghost on every cycle.
                    tracing::info!(
                        "previous-hour digest {} no longer exists — finalizing without trap wrap",
                        prev.message_id
                    );
                    self.db.finalize_digest(prev.id)?;
                } else {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Render the current ops-digest body from DB state. Cheap — DB reads only.
    pub fn render_digest_body(&self) -> Result<String> {
        let now = chrono::Utc::now();
        let hour_label = now.format("%H:00 UTC").to_string();
        let queue = self.db.pending_alert_count().unwrap_or(0);
        let breakdown = self.db.pending_alerts_by_type().unwrap_or_default();
        let signals_active = self.db.active_delivery_count(CALLS_CHANNEL).unwrap_or(0);

        let emoji = |t: &str| match t {
            "concentrating" => "⚠️",
            "classification_change" => "🔄",
            "velocity_crash" => "📉",
            "developing" => "🌱",
            "active_trap" => "🪤",
            "grinder" => "⛏️",
            "staircase" => "📈",
            "spring" => "🌀",
            "surge" => "🚀",
            "crashing" => "💥",
            _ => "•",
        };

        let mut out = String::new();
        out.push_str(&format!("📟 <b>HOUR DIGEST</b> · {}\n", hour_label));
        out.push_str("<i>Why: routine hourly snapshot — queue state now, trap wrap appended at rollover</i>\n\n");
        out.push_str(&format!(
            "<b>Queue</b> {} pending · <b>Signals live</b> {}\n",
            queue, signals_active
        ));
        if breakdown.is_empty() {
            out.push_str("\n<i>no alerts in queue right now</i>");
        } else {
            out.push_str("\n<b>Alert breakdown</b>\n");
            for (t, n) in breakdown.iter().take(10) {
                out.push_str(&format!("  {} {} — {}\n", emoji(t), t, n));
            }
        }
        Ok(out)
    }

    /// Scanner calls this once per cycle. Internally buckets by hour so edits
    /// stay within the current hour's message and roll over at :00. Also
    /// finalizes any pending previous-hour digest with a trap wrap-up.
    pub async fn tick_digest_now(&self) -> Result<()> {
        if !self.cfg.enabled {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp();
        let current_hour_bucket = now - (now % 3600);

        // Close out the previous hour first (edit + append traps + mark finalized).
        // Errors are logged but must not block the current-hour post.
        if let Err(e) = self.finalize_previous_hour(current_hour_bucket).await {
            tracing::warn!("finalize_previous_hour failed: {}", e);
        }

        let body = self.render_digest_body()?;
        self.tick_digest(&body).await
    }

    /// Post a fresh digest at the top of each hour; edit throughout the hour.
    /// Stats are pulled from the DB as of the call time.
    pub async fn tick_digest(&self, body: &str) -> Result<()> {
        if !self.cfg.enabled {
            return Ok(());
        }
        let now = chrono::Utc::now().timestamp();
        let hour_bucket = now - (now % 3600);
        let chat_id = self.cfg.ops_chat_id.clone();

        match self.db.get_digest(hour_bucket)? {
            Some(d) => {
                match self.edit_message(&chat_id, d.message_id, body).await {
                    Ok(_) => {
                        self.db.touch_digest(d.id)?;
                    }
                    Err(e) => {
                        // Self-heal: when the upstream message has been
                        // deleted (Telegram returns 400 "message to edit
                        // not found"), drop the cached pointer and post
                        // anew. Otherwise the loop spins forever editing
                        // a ghost.
                        let stale = format!("{}", e).contains("message to edit not found");
                        if stale {
                            tracing::info!(
                                "digest message {} no longer exists — reposting fresh",
                                d.message_id
                            );
                            self.db.delete_digest(d.id)?;
                            let msg_id = self.send_message(&chat_id, body).await?;
                            self.db.insert_digest(hour_bucket, "ops", msg_id)?;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
            None => {
                let msg_id = self.send_message(&chat_id, body).await?;
                self.db.insert_digest(hour_bucket, "ops", msg_id)?;
            }
        }
        Ok(())
    }
}

/// Spawn a single buy attempt for a freshly-inserted call. Pulls live wallet
/// balance + open-position count + daily spend so the size is right at this
/// moment. Single-flight via `db.mark_buy_attempt` inside
/// `execute_buy_for_call`; concurrent calls for the same call_id collapse
/// into one. Logs but does not propagate errors — a buy failure shouldn't
/// kill the calling task.
async fn spawn_buy(
    exec: Arc<crate::execution::ExecutionCtx>,
    call_id: i64,
    mint: String,
    horizon_tag: String,
    price_usd: f64,
    mcap_usd: f64,
) {
    let bal = match exec.wallet_sol_balance().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("spawn_buy call {}: wallet_sol_balance failed: {}", call_id, e);
            let _ = exec.db.record_buy_failure(call_id, &format!("balance fetch failed: {}", e));
            return;
        }
    };
    let day_start = chrono::Utc::now().timestamp() - 86_400;
    let open = exec.db.count_open_positions().unwrap_or(0);
    let daily = exec.db.sum_daily_buy_sol(day_start).unwrap_or(0.0);
    let size = match crate::execution::derive_position_size_sol(
        &exec.cfg,
        &horizon_tag,
        bal,
        open,
        daily,
    ) {
        Ok(s) => s,
        Err(e) => {
            tracing::info!(
                "spawn_buy call {} ({}) skipped: {} (bal={:.4} open={} daily={:.4})",
                call_id, horizon_tag, e, bal, open, daily
            );
            let _ = exec.db.record_buy_failure(call_id, &format!("sizing skipped: {}", e));
            return;
        }
    };
    crate::execution::execute_buy_for_call(
        &exec.http,
        &exec.rpc,
        &exec.db,
        &exec.keypair,
        call_id,
        &mint,
        size,
        exec.cfg.slippage_bps,
        exec.priority_fee_lamports,
        exec.jito_tip_lamports,
        price_usd,
        mcap_usd,
    )
    .await;
}
