use crate::metadata::TokenMeta;
use crate::signals::{SignalLayer, TokenAnalysis};

#[derive(Debug, Clone, Copy)]
pub enum Template {
    Monster,
    Winner,
    Ops,
    Inspect,
}

impl Template {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "monster" => Some(Self::Monster),
            "winner" => Some(Self::Winner),
            "ops" => Some(Self::Ops),
            "inspect" | "inspect_full" => Some(Self::Inspect),
            _ => None,
        }
    }
}

pub fn render(analysis: &TokenAnalysis, meta: Option<&TokenMeta>, template: Template) -> String {
    match template {
        Template::Monster => render_monster(analysis, meta),
        Template::Winner => render_winner(analysis, meta),
        Template::Ops => render_ops_card(analysis, meta),
        Template::Inspect => render_inspect_full(analysis, meta),
    }
}

// -- helpers ------------------------------------------------------------------

fn short_addr(addr: &str) -> String {
    if addr.len() > 14 {
        format!("{}…{}", &addr[..6], &addr[addr.len() - 5..])
    } else {
        addr.to_string()
    }
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn find_detail<'a>(a: &'a TokenAnalysis, kind: &str) -> Option<&'a str> {
    a.scores
        .iter()
        .find(|s| s.signal_type == kind)
        .map(|s| s.details.as_str())
}

fn arrow(delta: i32) -> &'static str {
    if delta > 3 {
        "▲"
    } else if delta < -3 {
        "▼"
    } else {
        "─"
    }
}

fn pct_arrow(pct: f64) -> &'static str {
    if pct > 1.0 {
        "▲"
    } else if pct < -1.0 {
        "▼"
    } else {
        "─"
    }
}

fn emoji_for(cls: &str) -> &'static str {
    if cls.starts_with("UNSAFE") {
        return "⛔";
    }
    match cls {
        "STAIRCASE" => "📈",
        "GRINDER" => "⛏️",
        "SURGE" => "🚀",
        "SPRING" => "🌀",
        "ACTIVE_TRAP" => "🪤",
        "CRASHING" => "💥",
        "DEAD" => "☠️",
        "DEVELOPING" => "🌱",
        _ => "•",
    }
}

/// Compact USD: $1.2k, $34.5k, $1.8M
fn fmt_usd(v: f64) -> String {
    let a = v.abs();
    if a >= 1_000_000_000.0 {
        format!("${:.1}B", v / 1e9)
    } else if a >= 1_000_000.0 {
        format!("${:.1}M", v / 1e6)
    } else if a >= 1_000.0 {
        format!("${:.1}k", v / 1e3)
    } else if a >= 1.0 {
        format!("${:.0}", v)
    } else if a >= 0.01 {
        format!("${:.4}", v)
    } else {
        format!("${:.2e}", v)
    }
}

fn fmt_pct(v: f64) -> String {
    format!("{}{:.1}%", if v >= 0.0 { "+" } else { "" }, v)
}

fn momentum_dir(a: &TokenAnalysis) -> &'static str {
    a.delta
        .as_ref()
        .map(|d| arrow(d.momentum_delta))
        .unwrap_or("─")
}

/// Caller voice — a single natural-language paragraph the operator would
/// speak when fronting this call. Names the structural read in trader
/// language ("stair-stepping with steady accumulation", "compressing with
/// momentum building underneath"), then drops the few numbers that matter
/// at the entry: mcap, liquidity, top-holder posture, holder count.
///
/// The full scoring breakdown (mom/dist/spring/tpm/conf-math) belongs in
/// the collapsed `numbers_block` — readers who want internals can tap.
/// Cards that lead with this paragraph read like a caller's note, not a
/// model dump. Default voice: confident but not screaming. No emojis in
/// the paragraph itself — those live in the header.
pub fn caller_paragraph(
    a: &TokenAnalysis,
    meta: Option<&TokenMeta>,
    horizon: Option<&str>,
) -> String {
    let class = a.confidence.classification.as_str();
    // Lead phrase per classification — what the bot is *seeing*.
    let lead: &str = match class {
        "STAIRCASE" => "stair-stepping with steady accumulation",
        "GRINDER" => "grinding sideways with quiet accumulation",
        "SPRING" => "compressing with momentum building underneath",
        "SURGE" => "surging — sharp lift on heavy buys",
        "DEVELOPING" => "still building out the base",
        "CRASHING" => "rolling over hard",
        "DEAD" => "no flow, no buyers",
        "ACTIVE_TRAP" => "distribution score collapsed — looks like a trap",
        c if c.starts_with("UNSAFE") => "vetoed on-chain — do not touch",
        _ => "in motion",
    };
    let top = a.top_holder_pct;
    let holders = a.holder_count;
    let mcap = meta
        .and_then(|m| m.market_cap_usd.or(m.fdv_usd))
        .map(|v| format!(" at {} mcap", fmt_usd(v)))
        .unwrap_or_default();
    let liq = meta
        .and_then(|m| m.liquidity_usd)
        .map(|v| format!(", liquidity {}", fmt_usd(v)))
        .unwrap_or_default();
    let horizon_clause = match horizon {
        Some("SHORT TERM") | Some("SHORT") => " — taking it short.",
        Some("LONG TERM") | Some("LONG") => " — sitting on this one.",
        _ => ".",
    };
    format!(
        "{lead}, top holder at {top:.1}% with {holders} buyers in{mcap}{liq}{horizon}",
        lead = lead,
        top = top,
        holders = holders,
        mcap = mcap,
        liq = liq,
        horizon = horizon_clause,
    )
}

/// Collapsed `▾ numbers` block — every internal score, only visible if
/// the reader taps. This is the old `render_card_body` content moved
/// inside an expandable blockquote so the surface card stays glanceable.
pub fn numbers_block(
    a: &TokenAnalysis,
    meta: Option<&TokenMeta>,
    effective_conf: i32,
) -> String {
    let c = &a.confidence;
    let mut lines = Vec::new();
    if let Some(p) = price_line(meta) {
        lines.push(p);
    }
    if let Some(m) = market_line(meta) {
        lines.push(m);
    }
    lines.push(format!(
        "top {top:.2}% / {t5:.2}% / {t10:.2}% · holders {h}",
        top = a.top_holder_pct,
        t5 = a.top5_pct,
        t10 = a.top10_pct,
        h = a.holder_count,
    ));
    lines.push(format!(
        "{cls} · conf {conf}  ·  mom {m}{dir} · dist {d} · spring {s} · tpm {tx:.1}",
        cls = c.classification,
        conf = effective_conf,
        m = c.momentum,
        dir = momentum_dir(a),
        d = c.distribution,
        s = c.spring,
        tx = a.tx_rate,
    ));
    // Forensics line — surfaces bundle / sniper / insider concentration
    // and smart-money holder count alongside the basic distribution.
    // Skip when forensics never measured (forensics_computed_at=0) so we
    // don't show 0%-on-everything for tokens without coverage yet.
    if a.forensics_computed_at > 0 {
        lines.push(format!(
            "bundle {b:.0}% · sniper {s:.0}% · insider {i:.0}% · smart-money holders {sm}",
            b = a.bundle_pct,
            s = a.sniper_pct,
            i = a.insider_pct,
            sm = a.smart_money_count,
        ));
    }
    // 1h tape: buys/sells + ratio. Lets the reader see if the token is
    // organic (~1.2 ratio) vs FOMO (>3) vs already-dumping (<1).
    if a.buys_h1 + a.sells_h1 > 0 {
        let ratio = if a.sells_h1 > 0 {
            a.buys_h1 as f64 / a.sells_h1 as f64
        } else {
            f64::INFINITY
        };
        lines.push(format!(
            "1h tape: {b} buys / {s} sells · ratio {r}",
            b = a.buys_h1,
            s = a.sells_h1,
            r = if ratio.is_finite() { format!("{:.2}", ratio) } else { "∞".to_string() },
        ));
    }
    if c.top_holder_bonus_pct > 0 {
        lines.push(format!(
            "score = base {b} · +{bp}% dist bonus",
            b = c.base_total,
            bp = c.top_holder_bonus_pct,
        ));
    }
    // Velocity = h1 volume / liquidity. Compact "is the token alive"
    // signal: ≥1.0 means the pool is turning over once per hour;
    // <0.1 means dead-cat. Skipped when liquidity is zero (curve-only
    // pump.fun mints with no DexScreener pair).
    if let Some(m) = meta {
        if let (Some(vol), Some(liq)) = (m.volume_24h_usd, m.liquidity_usd) {
            // h24 → h1 estimate: divide by 24. We don't have h1 vol on
            // TokenMeta directly today; this is the cheap approximation.
            // When h1 is wired through this becomes vol_h1 / liq directly.
            if liq > 0.0 {
                let velocity = (vol / 24.0) / liq;
                lines.push(format!("velocity {:.2}x · h24 vol/liq {:.1}", velocity, vol / liq));
            }
        }
    }
    if let Some(warn) = token_2022_summary(a) {
        lines.push(warn);
    }
    let body = lines.join("\n");
    format!("<blockquote expandable>▾ numbers\n{}</blockquote>", body)
}

fn ticker_line(meta: Option<&TokenMeta>) -> Option<String> {
    let m = meta?;
    let name = esc(&m.name);
    let sym = esc(&m.symbol);
    Some(format!("<b>${}</b> {}", sym, name))
}

/// Market line: mcap · liq · 24h vol · age. Only rendered when any piece present.
fn market_line(meta: Option<&TokenMeta>) -> Option<String> {
    let m = meta?;
    let mut parts: Vec<String> = Vec::new();
    if let Some(mc) = m.market_cap_usd.or(m.fdv_usd) {
        parts.push(format!("mc {}", fmt_usd(mc)));
    }
    if let Some(l) = m.liquidity_usd {
        parts.push(format!("liq {}", fmt_usd(l)));
    }
    if let Some(v) = m.volume_24h_usd {
        parts.push(format!("24h {}", fmt_usd(v)));
    }
    if let Some(age) = m.age_human() {
        parts.push(format!("age {}", age));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" · "))
    }
}

/// Price change snapshot: 5m/1h/24h with arrows
fn price_line(meta: Option<&TokenMeta>) -> Option<String> {
    let m = meta?;
    let mut parts: Vec<String> = Vec::new();
    if let Some(p) = m.price_usd {
        parts.push(format!("px {}", fmt_usd(p)));
    }
    if let Some(c) = m.price_change_5m {
        parts.push(format!("5m {}{}", pct_arrow(c), fmt_pct(c)));
    }
    if let Some(c) = m.price_change_1h {
        parts.push(format!("1h {}{}", pct_arrow(c), fmt_pct(c)));
    }
    if let Some(c) = m.price_change_24h {
        parts.push(format!("24h {}{}", pct_arrow(c), fmt_pct(c)));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" · "))
    }
}

fn links_line(a: &TokenAnalysis, meta: Option<&TokenMeta>) -> String {
    let pair_url = meta
        .and_then(|m| m.pair_url.clone())
        .unwrap_or_else(|| format!("https://dexscreener.com/solana/{}", a.address));
    format!(
        "<a href=\"{p}\">chart</a> · <a href=\"https://solscan.io/token/{a}\">solscan</a> · <a href=\"https://photon-sol.tinyastro.io/en/lp/{a}\">photon</a>",
        p = pair_url,
        a = a.address,
    )
}

// -- templates ----------------------------------------------------------------

pub fn render_monster(a: &TokenAnalysis, meta: Option<&TokenMeta>) -> String {
    let c = &a.confidence;
    let header = match ticker_line(meta) {
        Some(t) => format!(
            "{emoji} <b>{cls}</b> {conf} · {t}",
            emoji = emoji_for(&c.classification),
            cls = c.classification,
            conf = c.total,
            t = t
        ),
        None => format!(
            "{emoji} <b>{cls}</b> {conf}",
            emoji = emoji_for(&c.classification),
            cls = c.classification,
            conf = c.total,
        ),
    };
    let mut lines = vec![header];
    // Structure line: top / tx rate / mom / dist
    lines.push(format!(
        "top {top:.1}% · {tx:.0} tpm · {mom}{dir}/{dist}",
        top = a.top_holder_pct,
        tx = a.tx_rate,
        mom = c.momentum,
        dir = momentum_dir(a),
        dist = c.distribution,
    ));
    if let Some(ml) = market_line(meta) {
        lines.push(ml);
    }
    lines.push(format!("<code>{}</code>", a.address));
    lines.push(links_line(a, meta));
    lines.join("\n")
}

pub fn render_winner(a: &TokenAnalysis, meta: Option<&TokenMeta>) -> String {
    let c = &a.confidence;
    let mut lines: Vec<String> = Vec::new();

    // Header
    let ticker = ticker_line(meta).unwrap_or_else(|| "<b>—</b>".to_string());
    lines.push(format!(
        "🏆 <b>WINNER</b> {} · {} {} {}",
        c.classification,
        c.total,
        momentum_dir(a),
        ticker
    ));

    // Market data
    if let Some(p) = price_line(meta) {
        lines.push(p);
    }
    if let Some(m) = market_line(meta) {
        lines.push(m);
    }

    // Structure
    lines.push(format!(
        "mom {m}/{d}/{s} · top {t:.1}%/{t5:.1}%/{t10:.1}% · {h} holders · {tx:.0} tpm",
        m = c.momentum,
        d = c.distribution,
        s = c.spring,
        t = a.top_holder_pct,
        t5 = a.top5_pct,
        t10 = a.top10_pct,
        h = a.holder_count,
        tx = a.tx_rate,
    ));

    // Delta + transition
    if let Some(d) = &a.delta {
        let parts = [
            format!("Δmom {:+}", d.momentum_delta),
            format!("Δtop {:+.1}%", d.top_holder_delta),
            d.concentration_direction.clone(),
        ]
        .join(" · ");
        if d.classification_changed {
            lines.push(format!(
                "{} → <b>{}</b> · {}",
                d.previous.classification, c.classification, parts
            ));
        } else {
            lines.push(parts);
        }
    }

    // Critical signal snippets
    let recency = esc(find_detail(a, "recency").unwrap_or(""));
    let success = esc(find_detail(a, "tx_success_rate").unwrap_or(""));
    let exit = find_detail(a, "velocity_exit_warning").map(esc);
    let congestion = find_detail(a, "demand_congestion").map(esc);
    let mut flags: Vec<String> = Vec::new();
    if !recency.is_empty() {
        flags.push(recency);
    }
    if !success.is_empty() {
        flags.push(success);
    }
    if let Some(c) = congestion {
        flags.push(format!("🔥 {}", c));
    }
    if let Some(e) = exit {
        flags.push(format!("⚠️ {}", e));
    }
    if !flags.is_empty() {
        lines.push(flags.join(" · "));
    }

    if let Some(warn) = token_2022_summary(a) {
        lines.push(warn);
    }

    lines.push(format!("<code>{}</code>", a.address));
    lines.push(links_line(a, meta));
    lines.join("\n")
}

/// Summarize Token-2022 extension posture from the safety signal scores.
/// Returns an explicit line for risky extensions, a reassuring line for benign
/// ones, or None for classic SPL.
fn token_2022_summary(a: &TokenAnalysis) -> Option<String> {
    if !a.is_token_2022 {
        return None;
    }
    // Hard vetoes
    for v in ["permanent_delegate", "default_frozen", "non_transferable"] {
        if let Some(s) = a.scores.iter().find(|s| s.signal_type == v && s.score == 0) {
            return Some(format!("⛔ <b>VETO</b> — {}", esc(&s.details)));
        }
    }
    // Flags
    let hook = a.scores.iter().find(|s| s.signal_type == "transfer_hook");
    let fee = a.scores.iter().find(|s| s.signal_type == "transfer_fee");
    match (hook, fee) {
        (Some(h), Some(f)) => Some(format!("⚠️ {} · {}", esc(&h.details), esc(&f.details))),
        (Some(h), None) => Some(format!("⚠️ {}", esc(&h.details))),
        (None, Some(f)) => Some(format!("⚠️ {}", esc(&f.details))),
        (None, None) => {
            // Check for benign-cleared Token-2022
            if a.scores
                .iter()
                .any(|s| s.signal_type == "token_2022_extensions")
            {
                Some("✓ Token-2022 extensions benign".to_string())
            } else {
                None
            }
        }
    }
}

pub fn render_ops_card(a: &TokenAnalysis, meta: Option<&TokenMeta>) -> String {
    let c = &a.confidence;
    let sym = meta
        .map(|m| format!(" ${}", esc(&m.symbol)))
        .unwrap_or_default();
    let mc = meta
        .and_then(|m| m.market_cap_usd.or(m.fdv_usd))
        .map(|v| format!(" · {}", fmt_usd(v)))
        .unwrap_or_default();
    format!(
        "{e} <code>{addr}</code>{sym} <b>{cls}</b> {conf} {dir}{mc}",
        e = emoji_for(&c.classification),
        addr = short_addr(&a.address),
        sym = sym,
        cls = c.classification,
        conf = c.total,
        dir = momentum_dir(a),
        mc = mc,
    )
}

pub fn render_inspect_full(a: &TokenAnalysis, meta: Option<&TokenMeta>) -> String {
    let c = &a.confidence;
    let dir = momentum_dir(a);
    let mut lines: Vec<String> = Vec::new();

    // Header
    let ticker = ticker_line(meta).unwrap_or_else(|| "<i>no metadata</i>".to_string());
    lines.push(format!(
        "🔬 <b>{}</b> {} {} · {}",
        c.classification, c.total, dir, ticker
    ));

    // Market
    if let Some(p) = price_line(meta) {
        lines.push(p);
    }
    if let Some(m) = market_line(meta) {
        lines.push(m);
    }

    // Structure
    lines.push(format!(
        "mom {m}/{d}/{s} (total {t})",
        m = c.momentum,
        d = c.distribution,
        s = c.spring,
        t = c.total
    ));
    lines.push(format!(
        "{h} holders · top {top:.1}% · top5 {t5:.1}% · top10 {t10:.1}%",
        h = a.holder_count,
        top = a.top_holder_pct,
        t5 = a.top5_pct,
        t10 = a.top10_pct
    ));
    lines.push(format!(
        "{tx:.1} tpm · velocity {v:.2}x",
        tx = a.tx_rate,
        v = a.velocity
    ));

    if let Some(warn) = token_2022_summary(a) {
        lines.push(warn);
    }

    // Signal table
    lines.push(String::new()); // blank line
    lines.push("<b>signals</b>".to_string());
    let mut signal_rows: Vec<(i32, String)> = a
        .scores
        .iter()
        .filter(|s| s.layer != SignalLayer::Safety)
        .map(|s| {
            (
                s.score,
                format!(
                    "  {:3} {} — <i>{}</i>",
                    s.score,
                    esc(&s.signal_type),
                    esc(&s.details)
                ),
            )
        })
        .collect();
    signal_rows.sort_by(|a, b| b.0.cmp(&a.0));
    for (_, row) in signal_rows {
        lines.push(row);
    }

    lines.push(String::new());
    lines.push("<b>safety</b>".to_string());
    let mut safety_rows: Vec<(i32, String)> = a
        .scores
        .iter()
        .filter(|s| s.layer == SignalLayer::Safety)
        .map(|s| {
            (
                s.score,
                format!(
                    "  {:3} {} — <i>{}</i>",
                    s.score,
                    esc(&s.signal_type),
                    esc(&s.details)
                ),
            )
        })
        .collect();
    safety_rows.sort_by(|a, b| b.0.cmp(&a.0));
    for (_, row) in safety_rows {
        lines.push(row);
    }

    // Delta
    if let Some(d) = &a.delta {
        let elapsed = if d.time_elapsed_seconds < 60 {
            format!("{}s", d.time_elapsed_seconds)
        } else if d.time_elapsed_seconds < 3600 {
            format!("{}m", d.time_elapsed_seconds / 60)
        } else {
            format!("{:.1}h", d.time_elapsed_seconds as f64 / 3600.0)
        };
        let changed = if d.classification_changed {
            format!(" · {} → {}", d.previous.classification, c.classification)
        } else {
            String::new()
        };
        lines.push(String::new());
        lines.push(format!(
            "<b>Δ</b> ({} ago{}) · mom {:+} {} · top {:+.1}% · {}",
            elapsed, changed, d.momentum_delta, dir, d.top_holder_delta, d.concentration_direction
        ));
    }

    lines.push(String::new());
    lines.push(format!("<code>{}</code>", a.address));
    lines.push(links_line(a, meta));
    lines.join("\n")
}
