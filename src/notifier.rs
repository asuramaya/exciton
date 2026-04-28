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

// Gates calibrated against the 31-call closed-call backtest (2026-04-22..28).
// Confidence: conf=75 calls returned -9.2% mean / 25% win, conf 76-77 returned
// +9.7% mean / 57% win. The 75 floor is noise; 76 is signal.
pub const SIGNAL_MIN_EFFECTIVE_CONFIDENCE: i32 = 76;
// Top-holder gate: top1 < 6% AND liq >= $100k yielded 7/8 wins (+9.2% mean,
// +73% sum). Industry-corroborated (DeFade rug-pull guide: any single holder
// >5% in early stage is a red flag).
pub const SIGNAL_MAX_TOP_HOLDER_PCT: f64 = 6.0;
// Top-10 gate: catches insider networks that fragment 30-40% of supply
// across 20+ wallets to look diversified. RugCheck threshold: top10 >35%
// is a dump-risk indicator. Combined with top1<6, single addresses can't
// alone exceed this.
pub const SIGNAL_MAX_TOP10_PCT: f64 = 35.0;
pub const SIGNAL_REQUIRED_CLASSES: &[&str] = &["STAIRCASE", "GRINDER", "SPRING"];
pub const SIGNAL_DEDUP_HOURS: i64 = 6;
// Liquidity floor: in the backtest, calls below $100k liquidity won 6/22
// (27%) at -4% mean. Above $100k won 7/8 (88%) at +9.2% mean. Real exits
// need real depth.
pub const SIGNAL_MIN_LIQUIDITY_USD: f64 = 100_000.0;
// 24h volume floor — proves the token is actually trading, not a dead curve
// with a stale top_holder reading.
pub const SIGNAL_MIN_VOLUME_24H_USD: f64 = 50_000.0;
// Mcap floor — 8/8 sub-$150k mcap calls returned -5% mean / 25% win in the
// backtest. The "shallow rug" bucket needs the separate event-driven scalp
// strategy, not the SHORT ladder. Above $500k: 13/14 mean +5% / 57% win.
pub const SIGNAL_MIN_MCAP_USD: f64 = 500_000.0;
// Velocity gate (arxiv 2602.14860 Feb-2026: trading velocity is the dominant
// graduation predictor). Tx_rate is per-minute; require ≥5 to filter out
// dead post-grad books that look fine on holders but have no real flow.
pub const SIGNAL_MIN_TX_RATE_PER_MIN: f64 = 5.0;
// Holder-growth velocity gate: ≥50 new holders/hour scaled from the
// "500+ holders in first hour = green flag" industry signal. Computed from
// delta.holder_count_delta over delta.time_elapsed_seconds.
pub const SIGNAL_MIN_HOLDER_GROWTH_PER_HOUR: f64 = 50.0;
// Launch-forensics ceilings — block calls when the measured concentration
// signals a bundle / sniper-cohort / insider-network risk. Each metric is
// 0.0 when unmeasured (fresh token), so the gate only fires above the
// threshold; absence of data does NOT block (the auto-refresh will catch
// up on the next analysis cycle).
pub const SIGNAL_MAX_BUNDLE_PCT: f64 = 30.0;
pub const SIGNAL_MAX_SNIPER_PCT: f64 = 30.0;
pub const SIGNAL_MAX_INSIDER_PCT: f64 = 25.0;
// Token must be at least this old to auto-call. Fresh-deploy dumpsters that
// pump for 5 minutes off creator buying then bleed back to zero are the
// majority of -90% calls. One hour gives the holder base time to validate.
pub const SIGNAL_MIN_TOKEN_AGE_SECS: i64 = 3600;

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

// -- Notifier core -----------------------------------------------------------

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
        })
    }

    /// Wake the publisher to run an immediate snapshot. Coalesces with
    /// any in-flight or near-future tick — multiple kicks during a run
    /// collapse into one extra cycle. Cheap; safe to call from hot paths.
    pub fn kick_publisher(&self) {
        if let Some(k) = &self.publish_kick {
            k.notify_one();
        }
    }

    pub fn enabled(&self) -> bool {
        self.cfg.enabled
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
    pub fn classify_near_miss(
        &self,
        a: &TokenAnalysis,
        effective_conf: i32,
    ) -> Option<(&'static str, String)> {
        let class = a.confidence.classification.as_str();
        let class_ok = SIGNAL_REQUIRED_CLASSES.iter().any(|c| *c == class);
        let conf_ok = effective_conf >= SIGNAL_MIN_EFFECTIVE_CONFIDENCE;
        let holder_ok = a.top_holder_pct < SIGNAL_MAX_TOP_HOLDER_PCT;
        let momentum_ok = a.delta.as_ref().map_or(true, |d| d.momentum_delta >= 0);

        // Not a near-miss at all — it would have signaled
        if class_ok && conf_ok && holder_ok && momentum_ok {
            return None;
        }
        // Wrong class is a structural miss, not a near-miss
        if !class_ok {
            return None;
        }

        // Confidence near-miss: conf in [65, 74]
        if !conf_ok && effective_conf >= 65 {
            return Some((
                "conf",
                format!(
                    "conf {} < {}  (short by {})",
                    effective_conf,
                    SIGNAL_MIN_EFFECTIVE_CONFIDENCE,
                    SIGNAL_MIN_EFFECTIVE_CONFIDENCE - effective_conf
                ),
            ));
        }
        // Top-holder near-miss: top in [20.00, 25.00]
        if conf_ok && !holder_ok && a.top_holder_pct < 25.0 {
            return Some((
                "top_holder",
                format!(
                    "top {:.2}% > {:.2}%  (over by {:.2}pp)",
                    a.top_holder_pct,
                    SIGNAL_MAX_TOP_HOLDER_PCT,
                    a.top_holder_pct - SIGNAL_MAX_TOP_HOLDER_PCT
                ),
            ));
        }
        // Momentum near-miss: delta in [-5, -1]
        if conf_ok && holder_ok && !momentum_ok {
            if let Some(d) = a.delta.as_ref() {
                if d.momentum_delta >= -5 {
                    return Some((
                        "momentum_delta",
                        format!("mom_delta {} < 0 (slipping)", d.momentum_delta),
                    ));
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
            .map_or(false, |v| v >= SIGNAL_MIN_MCAP_USD);
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
        // Launch-forensics ceilings: blocked when measured > threshold. A
        // zero value means "not yet computed" — those pass through and the
        // hourly background refresh will tighten the gate retroactively.
        let bundle_ok = a.bundle_pct < SIGNAL_MAX_BUNDLE_PCT;
        let sniper_ok = a.sniper_pct < SIGNAL_MAX_SNIPER_PCT;
        let insider_ok = a.insider_pct < SIGNAL_MAX_INSIDER_PCT;
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
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.cfg.bot_token
        );
        let mut form = vec![
            ("chat_id", chat_id.to_string()),
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

    /// Compose a signal card per the style guide:
    ///   Line 1 — badge + subject (ticker/name)
    ///   Line 2 — italic "why it fired" with concrete triggering metrics
    ///   Body   — metric rows from templates::render_card_body
    ///   Timeline — wrapped in <blockquote expandable> for tap-to-reveal history
    fn render_signal_with_timeline(
        &self,
        a: &TokenAnalysis,
        meta: Option<&TokenMeta>,
        timeline: &[TimelineEntry],
        status: &str,
        effective_conf: i32,
        _prev_class_on_fail: Option<&str>,
    ) -> String {
        let ticker_name = match meta {
            Some(m) => format!("<b>${}</b>", html_escape(&m.symbol)),
            None => format!(
                "<code>{}…{}</code>",
                &a.address[..6],
                &a.address[a.address.len() - 5..]
            ),
        };

        let header = match status {
            "failed" => format!("❌ <b>FAILED</b> · {}", ticker_name),
            _ => format!("📊 <b>SIGNAL</b> · {}", ticker_name),
        };

        // Caller voice paragraph — what the bot would *say* about this
        // token. No horizon on signal cards (auto-fired = SHORT by gate).
        let paragraph = templates::caller_paragraph(a, meta, None);

        // Collapsed numbers block — all internal scoring lives here.
        let numbers = templates::numbers_block(a, meta, effective_conf);

        let mut html = format!("{}\n{}\n\n{}", header, paragraph, numbers);

        if !timeline.is_empty() {
            html.push_str("\n<blockquote expandable>▾ history");
            for e in timeline {
                let t = chrono::DateTime::from_timestamp(e.ts, 0)
                    .map(|dt| dt.format("%H:%M UTC").to_string())
                    .unwrap_or_else(|| e.ts.to_string());
                html.push_str(&format!("\n{} <b>{:<7}</b> {}", t, e.kind, e.line));
            }
            html.push_str("</blockquote>");
        }

        if html.len() > 4000 {
            html.truncate(4000);
            html.push_str("\n…[truncated]");
        }
        html
    }

    fn render_call_card(
        &self,
        address: &str,
        meta: Option<&crate::metadata::TokenMeta>,
        timeline: &[TimelineEntry],
        status: &str,  // "active" | "withdrew" | "failed" | "expired"
        note: &str,
    ) -> String {
        let ticker_name = match meta {
            Some(m) => format!("<b>${}</b>", html_escape(&m.symbol)),
            None => {
                let end = address.len().saturating_sub(5);
                format!("<code>{}…{}</code>", &address[..6.min(address.len())], &address[end..])
            }
        };
        // Parse horizon tag from note: "horizon=SHORT" or "horizon=LONG"
        let (horizon_badge, clean_note) = parse_horizon_from_note(note);
        let term_label = horizon_badge.map(|h| format!(" · <b>{}</b>", h)).unwrap_or_default();
        // Header emoji + verb. Settling phase produces explicit verdict
        // strings ("took the win" / "2x done" / "thesis broke" / "no
        // follow-through" / "30d hold complete") that are richer than the
        // bare status — those come through `note` for closed states.
        let header = match status {
            "withdrew" | "closed" => format!("🟢 <b>BANKED</b>{} · {}", term_label, ticker_name),
            "failed"   => format!("🔴 <b>FAILED</b>{} · {}", term_label, ticker_name),
            "expired"  => format!("⏰ <b>EXPIRED</b>{} · {}", term_label, ticker_name),
            "voided"   => format!("⚪ <b>VOIDED</b>{} · {}", term_label, ticker_name),
            _          => format!("📣 <b>NEW CALL</b>{} · {}", term_label, ticker_name),
        };
        // Lead line. For active calls: operator's clean note (or a default
        // caller phrase if blank). For closed calls: the verdict line from
        // the settling phase ("+52% · took the win"), already written to
        // `note` by update_call_outcome.
        let lead = if clean_note.is_empty() {
            match status {
                "active" => "swing taken on this one.".to_string(),
                _ => "—".to_string(),
            }
        } else {
            html_escape(&clean_note)
        };
        let body = match meta {
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
                parts.join(" · ")
            }
            None => "no market data".to_string(),
        };
        let mut html = format!("{}\n{}\n\n{}", header, lead, body);
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
        let channel = "winners";
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

        let existing = self.db.get_active_delivery(address, channel)?;
        match existing {
            None => {
                let timeline = vec![TimelineEntry { ts: now, kind: "called".into(), line: call_line }];
                let html = self.render_call_card(address, meta_ref, &timeline, "active", note);
                let kb = self.token_keyboard(address, meta_ref.and_then(|m| m.pair_url.as_deref()));
                let msg_id = self.send_message_ex(&chat_id, &html, Some(&kb)).await?;
                let timeline_json = serde_json::to_string(&timeline)?;
                self.db.insert_delivery(address, channel, msg_id, 0, "MANUAL", price, None, &timeline_json)?;
            }
            Some(d) if d.status == "active" => {
                let mut timeline: Vec<TimelineEntry> = serde_json::from_str(&d.timeline_json).unwrap_or_default();
                timeline.push(TimelineEntry { ts: now, kind: "called".into(), line: call_line });
                let html = self.render_call_card(address, meta_ref, &timeline, "active", note);
                let kb = self.token_keyboard(address, meta_ref.and_then(|m| m.pair_url.as_deref()));
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
        let channel = "winners";
        let chat_id = self.cfg.signals_chat_id.clone();
        let now = chrono::Utc::now().timestamp();
        let meta = crate::metadata::fetch(address).await.ok().flatten();
        let meta_ref = meta.as_ref();

        let d = match self.db.get_active_delivery(address, channel)? {
            Some(d) if d.status == "active" || force => d,
            _ => return Ok(()),
        };

        let mut timeline: Vec<TimelineEntry> = serde_json::from_str(&d.timeline_json).unwrap_or_default();
        let line = {
            let pct = exit_pct.map(|p| format!("{:+.1}% · ", p)).unwrap_or_default();
            format!("{}{}", pct, exit_note)
        };
        // Don't append a duplicate timeline entry when the last one already
        // matches — keeps idempotent backfill from growing the timeline on
        // every restart.
        let is_dup = timeline
            .last()
            .map(|e| e.kind == outcome && e.line == line)
            .unwrap_or(false);
        if !is_dup {
            timeline.push(TimelineEntry { ts: now, kind: outcome.to_string(), line: line.clone() });
        }

        let html = self.render_call_card(address, meta_ref, &timeline, outcome, exit_note);
        let kb = self.token_keyboard(address, meta_ref.and_then(|m| m.pair_url.as_deref()));
        // Soft-error on edits: when re-running a backfill we may hit
        // "message is not modified" or "message to edit not found"
        // (channel scrubbed). 429 Too Many Requests honours retry_after.
        // Log + continue rather than aborting the whole pass.
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.edit_message_ex(&chat_id, d.message_id, &html, Some(&kb)).await {
                Ok(_) => break,
                Err(e) => {
                    let s = format!("{}", e);
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
        Ok(())
    }

    // -- Winner lifecycle ---------------------------------------------------

    /// Entry point: given a fresh analysis, decide promote/edit/demote for
    /// the winners channel. Silent no-op when nothing to do.
    pub async fn process_token(&self, a: &TokenAnalysis, effective_conf: i32) -> Result<()> {
        if !self.cfg.enabled {
            return Ok(());
        }
        // Defense against transient RPC data glitches. When getMultipleAccounts
        // returns a partial response, top_holder_pct can come back 0.0 with
        // holders > 0 — the analyzer then computes a low confidence and any
        // existing card flips to FAILED (one-way demote) on what is actually
        // a winning trade. TIME MACHINE was the canonical example: +126%
        // recovery, frozen FAILED card. Skip this snapshot entirely.
        if a.top_holder_pct == 0.0 && a.holder_count > 0 {
            tracing::debug!(
                "process_token: skipping {} — top_holder=0.0 with {} holders (data glitch)",
                a.address, a.holder_count
            );
            return Ok(());
        }
        let channel = "winners"; // legacy internal DB key — kept stable to preserve
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

        match existing {
            None => {
                // No existing card — check if this token qualifies for a new signal.
                // If not, check for a near-miss and log it for truth-telling.
                if !self.should_signal(a, effective_conf, meta.as_ref(), first_seen) {
                    if let Some((gate, gap)) = self.classify_near_miss(a, effective_conf) {
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
                let html = self.render_signal_with_timeline(
                    a,
                    meta.as_ref(),
                    &timeline,
                    "active",
                    effective_conf,
                    None,
                );
                let kb = self.token_keyboard(
                    &a.address,
                    meta.as_ref().and_then(|m| m.pair_url.as_deref()),
                );
                let msg_id = self.send_message_ex(&chat_id, &html, Some(&kb)).await?;
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

                // Mirror the promotion into the public calls ledger with the
                // entry state frozen at call-time. The MadApes.ai publisher
                // serializes this to data/calls.json on its next tick, which
                // is what the site's CALLS section reads. Idempotent via a
                // unique partial index on (mint, status='active').
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
                // Auto-call horizon heuristic: pump.fun memecoins live and die
                // in hours, but anything entering at >= $1M mcap is past the
                // bonding-curve life cycle — those are sit-on-it positions,
                // not 6h swings. Without this tag, settle_calls() would
                // expire ROTUS-class entries on its 6h SHORT timeout. Operator
                // can still override via /close_call. The settling phase
                // reads this tag on every cycle; missing tag = SHORT.
                let auto_horizon = if mcap >= 1_000_000.0 {
                    crate::horizon::Horizon::Long
                } else {
                    crate::horizon::Horizon::Short
                };
                let auto_horizon_tag = auto_horizon.tag().unwrap_or("");
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
                    auto_horizon_tag,
                    "notifier",
                    a.tx_rate,
                );
                if let Ok(Some(_)) = inserted {
                    // Align expires_at with the horizon-based settling window
                    // (scanner::settle_calls). Without this, the UI badges a
                    // misleading "13d left" on every call while the settling
                    // phase actually closes SHORT at 6h.
                    let window_secs: i64 = match auto_horizon {
                        crate::horizon::Horizon::Short => 6 * 3600,
                        crate::horizon::Horizon::Long => 30 * 86_400,
                        crate::horizon::Horizon::Unknown => 14 * 86_400,
                    };
                    let expires = now + window_secs;
                    let _ = self.db.set_call_expiration(&a.address, Some(expires));
                    // New auto-call landed — wake the publisher so the site
                    // shows it within ~30s instead of waiting on the 300s tick.
                    self.kick_publisher();
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
                let prev_class = delivery.snapshot_class.clone();
                let html = self.render_signal_with_timeline(
                    a,
                    meta.as_ref(),
                    &timeline,
                    render_status,
                    effective_conf,
                    Some(&prev_class),
                );
                let kb = self.token_keyboard(
                    &a.address,
                    meta.as_ref().and_then(|m| m.pair_url.as_deref()),
                );
                self.edit_message_ex(&chat_id, delivery.message_id, &html, Some(&kb))
                    .await?;
                let timeline_json = serde_json::to_string(&timeline)?;
                self.db.update_delivery(
                    delivery.id,
                    status,
                    effective_conf,
                    &a.confidence.classification,
                    price,
                    Some(a.top_holder_pct),
                    &timeline_json,
                )?;
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
                let html = self.render_signal_with_timeline(
                    a,
                    meta.as_ref(),
                    &timeline,
                    "active",
                    effective_conf,
                    None,
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
        let signals_active = self.db.active_delivery_count("winners").unwrap_or(0);

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
