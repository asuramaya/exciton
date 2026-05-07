// MadApes.ai — renders live PnL + positions + activity + thoughts.
// Data comes from /data/*.json snapshots committed to the repo (~every 5 min).
// Thoughts are dated markdown files under /thoughts/, indexed by /thoughts/index.json.
//
// DOM is built with createElement/textContent (never innerHTML) for every
// piece of data we control. Rendered markdown is piped through DOMPurify
// before being inserted, so even if a thought file is tampered with, it
// can't inject executable content.

const SOLSCAN = "https://solscan.io";
const DEXSCREENER = "https://dexscreener.com/solana";
const CHART_WINDOWS = ["1h", "6h", "24h", "all"];
const CHART_WINDOW_STORAGE_KEY = "madapes.chartWindow";
const MAX_SEEN_STREAM = 500;

function fmtUsd(n) {
  if (n === null || n === undefined || isNaN(n)) return "—";
  const abs = Math.abs(n);
  if (abs >= 1000) return "$" + n.toLocaleString("en-US", { maximumFractionDigits: 0 });
  return "$" + n.toFixed(2);
}

function fmtPct(n) {
  if (n === null || n === undefined || isNaN(n)) return "—";
  const sign = n >= 0 ? "+" : "";
  return sign + n.toFixed(1) + "%";
}

function fmtTimeAgo(ts) {
  if (!ts) return "—";
  const numericTs = Number(ts);
  if (!Number.isFinite(numericTs)) return "—";
  const diff = Math.max(0, Date.now() / 1000 - numericTs);
  if (diff < 60) return Math.floor(diff) + "s ago";
  if (diff < 3600) return Math.floor(diff / 60) + "m ago";
  if (diff < 86400) return Math.floor(diff / 3600) + "h ago";
  return Math.floor(diff / 86400) + "d ago";
}

function shortAddr(a) {
  if (!a || a.length < 10) return a || "—";
  return a.slice(0, 4) + "…" + a.slice(-4);
}

function pnlClass(n) {
  if (n > 0) return "pos";
  if (n < 0) return "neg";
  return "";
}

function readStoredChartWindow() {
  try {
    const saved = window.localStorage.getItem(CHART_WINDOW_STORAGE_KEY);
    return CHART_WINDOWS.includes(saved) ? saved : "1h";
  } catch (_) {
    return "1h";
  }
}

function persistChartWindow(nextWindow) {
  try {
    window.localStorage.setItem(CHART_WINDOW_STORAGE_KEY, nextWindow);
  } catch (_) {
    // Ignore storage failures; chart still works for this page view.
  }
}

function callPctValue(call) {
  if (!call || call.entry_price_usd <= 0) return null;
  const pct = Number(call.pct_from_call);
  return Number.isFinite(pct) ? pct : null;
}

function thoughtsSignature(index) {
  if (!Array.isArray(index) || !index.length) return "";
  return index
    .map((item) => [item.date || "", item.file || "", item.title || ""].join("|"))
    .join("||");
}

// --- DOM builders ---
function el(tag, attrs, children) {
  const n = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v === null || v === undefined) continue;
      if (k === "class") n.className = v;
      else if (k === "text") n.textContent = v;
      else if (k === "href") n.setAttribute("href", v);
      else if (k === "title") n.setAttribute("title", v);
      else if (k === "target") n.setAttribute("target", v);
      else if (k === "rel") n.setAttribute("rel", v);
      else if (k === "datetime") n.setAttribute("datetime", v);
      else n.setAttribute(k, v);
    }
  }
  if (children) {
    for (const c of children) {
      if (c == null) continue;
      n.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
    }
  }
  return n;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

async function loadJson(path) {
  try {
    const res = await fetch(path + "?t=" + Date.now());
    if (!res.ok) throw new Error(res.status);
    return await res.json();
  } catch (e) {
    console.warn("load failed:", path, e);
    return null;
  }
}

async function loadText(path) {
  try {
    const res = await fetch(path + "?t=" + Date.now());
    if (!res.ok) throw new Error(res.status);
    return await res.text();
  } catch (e) {
    console.warn("load failed:", path, e);
    return null;
  }
}

// --- render: health/header ---
let HEALTH_DATA = null;

function renderHealth(h) {
  if (!h) return;
  HEALTH_DATA = h;
  const addr = document.getElementById("wallet-addr");
  const short = shortAddr(h.wallet || "");
  addr.textContent = short;
  addr.setAttribute("title", "click to copy · " + (h.wallet || ""));
  addr.setAttribute("role", "button");
  addr.style.cursor = "pointer";
  addr.onclick = () => copyToClipboard(h.wallet || "", addr);

  const solText =
    h.sol_balance != null
      ? h.sol_balance.toFixed(3) + " SOL" +
        (h.sol_price_usd ? " @ $" + h.sol_price_usd.toFixed(0) : "")
      : "—";
  document.getElementById("sol-bal").textContent = solText;
  refreshLastUpdate();

  const banner = document.getElementById("stale-banner");
  if (banner && h.last_update) {
    const ageSec = Date.now() / 1000 - h.last_update;
    const stale = ageSec > 25 * 60;
    banner.hidden = !stale;
    if (stale) {
      banner.textContent = "publisher quiet — last pulse " + fmtTimeAgo(h.last_update);
    }
  }
}

// Re-rendered every second by the live ticker so "updated Xs ago" ticks.
function refreshLastUpdate() {
  if (!HEALTH_DATA || !HEALTH_DATA.last_update) return;
  const el = document.getElementById("last-update");
  if (el) el.textContent = "updated " + fmtTimeAgo(HEALTH_DATA.last_update);
}

function copyToClipboard(text, anchorEl) {
  if (!text || !navigator.clipboard) return;
  navigator.clipboard.writeText(text).then(() => {
    if (!anchorEl) return;
    const original = anchorEl.textContent;
    anchorEl.textContent = "copied";
    anchorEl.classList.add("copy-flash");
    setTimeout(() => {
      anchorEl.textContent = original;
      anchorEl.classList.remove("copy-flash");
    }, 900);
  });
}

// --- render: calls ---
function callTerm(c) {
  const m = (c.note || "").match(/horizon=(SHORT|LONG|SCALP|MOONSHOT)/i);
  return m ? m[1].toLowerCase() : null;
}

// Strip the canonical `horizon=…` and `thesis=…` tags + any leading/trailing
// `·` separators so the narrative reads as prose. Mirrors the server's
// horizon::parse_with_clean — keeping front and back in sync prevents the
// thesis paragraph from ending in stray "horizon=SHORT" suffixes.
function cleanNote(note) {
  if (!note) return "";
  return note
    .replace(/\s*·?\s*horizon=(SHORT|LONG|SCALP|MOONSHOT)\s*/gi, " ")
    .replace(/\s*·?\s*thesis=\S+\s*/gi, " ")
    .replace(/\s*·\s*·\s*/g, " · ")
    .replace(/^\s*·\s*/, "")
    .replace(/\s*·\s*$/, "")
    .trim();
}

// Lazy chart-mount: build the DexScreener iframe only when the host scrolls
// into view. Otherwise 6+ active calls each carry a 3rd-party iframe and
// the page CPU stalls while the user is still on the masthead. The widget
// URL skips the trades pane and info bar to keep the embed tight.
let CALL_CHART_OBSERVER = null;
function chartObserver() {
  if (CALL_CHART_OBSERVER) return CALL_CHART_OBSERVER;
  if (typeof IntersectionObserver !== "function") return null;
  CALL_CHART_OBSERVER = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        const host = entry.target;
        const mint = host.dataset.mint;
        if (!mint || host.dataset.loaded === "1") continue;
        const iframe = document.createElement("iframe");
        iframe.src =
          "https://dexscreener.com/solana/" + mint +
          "?embed=1&theme=dark&trades=0&info=0&chartType=usd&interval=15";
        iframe.loading = "lazy";
        iframe.referrerPolicy = "no-referrer-when-downgrade";
        iframe.title = "chart " + mint;
        // Drop the loading skeleton once the iframe paints — keeps the
        // shimmer behind the chart from leaking pixels at the edges.
        iframe.addEventListener("load", () => {
          const skel = host.querySelector(".card-chart-skeleton");
          if (skel) skel.remove();
        });
        host.appendChild(iframe);
        host.dataset.loaded = "1";
        CALL_CHART_OBSERVER.unobserve(host);
      }
    },
    { rootMargin: "200px 0px" }
  );
  return CALL_CHART_OBSERVER;
}

// Cache of rendered cards by mint. The 30s data refresh re-runs renderCalls;
// without this cache every iframe gets destroyed and rebuilt every cycle
// (the "reload punch" — chart flashes mid-interaction). We keep cards
// alive between renders, update only the volatile parts (pct, pills,
// market data), and remove cards whose mints are no longer active.
const CALL_CARD_CACHE = new Map();

function fmtDate(ts) {
  if (!ts) return null;
  return new Date(ts * 1000).toLocaleDateString("en-US", {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false,
  });
}

// CALLS section — active calls only. Closed calls live in HISTORY (they
// were shown here in a "recent" subsection but that duplicated work and
// gave each closed call two visual identities. Single home now.)
// Build the volatile pills list (changes every refresh — pct, time-ago,
// expiry-left). Returns DOM nodes the caller swaps into the card.
function buildCallPills(c) {
  const pills = [];
  if (c.entry_mcap_usd) pills.push("mcap " + "$" + formatMcap(c.entry_mcap_usd));
  if (c.current_mcap_usd && c.outcome_type === "active") pills.push("now $" + formatMcap(c.current_mcap_usd));
  if (c.entry_top_holder_pct != null) pills.push("top1 " + c.entry_top_holder_pct.toFixed(1) + "%");
  if (c.entry_liquidity_usd) pills.push("liq $" + formatMcap(c.entry_liquidity_usd));
  pills.push(fmtTimeAgo(c.called_at));
  if (c.expires_at) {
    const left = c.expires_at - Date.now() / 1000;
    if (left > 0) {
      pills.push(left > 86400 ? Math.floor(left / 86400) + "d left" : Math.floor(left / 3600) + "h left");
    } else {
      pills.push("expired");
    }
  }
  return pills.map((p) => el("span", { class: "card-pill", text: p }));
}

// Build the static parts of a call card. Called once per mint — subsequent
// renders mutate the existing card in place via updateCallCard so the
// iframe (which is the only expensive child) survives data refreshes.
function buildCallCard(c) {
  const sym = c.symbol ? "$" + c.symbol : shortAddr(c.mint || "");
  const term = callTerm(c);
  const narrative = cleanNote(c.note);
  const sideLabel = term ? term.toUpperCase() : "AUTO";

  const headSym = el("a", { href: "#call=" + c.mint, class: "card-sym sym-link", text: sym });
  const headBadge = el("span", { class: "card-badge card-badge-" + (term || "auto"), text: sideLabel });
  const headPct = el("div", { class: "card-head-right num", text: "" });
  const header = el("div", { class: "card-head" }, [
    el("div", { class: "card-head-left" }, [headSym, headBadge]),
    headPct,
  ]);

  const chartHost = el("div", { class: "card-chart", "data-mint": c.mint }, [
    el("div", { class: "card-chart-skeleton", text: "chart loading…" }),
  ]);
  const obs = chartObserver();
  if (obs) {
    obs.observe(chartHost);
  } else {
    const iframe = document.createElement("iframe");
    iframe.src = "https://dexscreener.com/solana/" + c.mint + "?embed=1&theme=dark&trades=0&info=0";
    chartHost.appendChild(iframe);
  }

  const narrativeEl = el("div", { class: "card-narrative", text: narrative || "" });
  if (!narrative) narrativeEl.style.display = "none";

  const pillsEl = el("div", { class: "card-pills" }, buildCallPills(c));

  const linkChildren = [
    el("a", { href: DEXSCREENER + "/" + c.mint, target: "_blank", rel: "noopener", text: "📊 chart" }),
    el("a", { href: "/api/data/scouts/" + c.mint, target: "_blank", rel: "noopener", text: "scout" }),
    el("a", { href: "/api/data/whales/" + c.mint, target: "_blank", rel: "noopener", text: "whales" }),
  ];
  if (c.thesis_url) {
    linkChildren.push(
      el("a", { href: "#note=" + encodeURIComponent(c.thesis_url.replace(/^thoughts\//, "")), text: "📖 thesis" })
    );
  }
  const linksEl = el("div", { class: "card-links" }, linkChildren);

  const card = el(
    "div",
    { class: "call-card", title: c.mint },
    [header, chartHost, narrativeEl, pillsEl, linksEl]
  );
  header.addEventListener("click", (ev) => {
    if (ev.target.closest("a")) return;
    card.classList.toggle("call-card--collapsed");
  });

  // Stash mutable refs on the card so updates can target them without
  // re-querying every refresh cycle.
  card._refs = { headPct, narrativeEl, pillsEl };
  return card;
}

// In-place update of a cached card. Touches only volatile fields (pct
// number + class, narrative text if changed, pills) and never replaces
// the chartHost / iframe — that's the whole point of the cache.
function updateCallCard(card, c) {
  if (!card._refs) return;
  const pctValue = callPctValue(c);
  const pctCls = pctValue === null ? "" : pnlClass(pctValue);
  card._refs.headPct.textContent = fmtPct(pctValue);
  card._refs.headPct.className = "card-head-right num " + pctCls;

  const narrative = cleanNote(c.note);
  if (narrative) {
    if (card._refs.narrativeEl.textContent !== narrative) {
      card._refs.narrativeEl.textContent = narrative;
    }
    card._refs.narrativeEl.style.display = "";
  } else {
    card._refs.narrativeEl.style.display = "none";
  }

  // Pills are entirely volatile (time-ago + expiry-left tick every render).
  // Cheap to fully replace; nothing inside is expensive.
  clear(card._refs.pillsEl);
  for (const p of buildCallPills(c)) card._refs.pillsEl.appendChild(p);
}

// CALLS section — active calls only. Closed calls live in HISTORY (they
// were shown here in a "recent" subsection but that duplicated work and
// gave each closed call two visual identities. Single home now.)
function renderCalls(calls /*, history (intentionally unused) */) {
  const container = document.getElementById("calls-list");
  const active = calls || [];

  if (!active.length) {
    clear(container);
    CALL_CARD_CACHE.clear();
    container.appendChild(el("div", { class: "empty", text: "no active calls" }));
    return;
  }

  // Newest call first — keeps the most relevant card at the top.
  const sorted = active.slice().sort((a, b) => (b.called_at || 0) - (a.called_at || 0));
  const seen = new Set();

  for (let i = 0; i < sorted.length; i++) {
    const c = sorted[i];
    seen.add(c.mint);
    let card = CALL_CARD_CACHE.get(c.mint);
    if (card) {
      // Existing card — patch in place. iframe survives unscathed.
      updateCallCard(card, c);
    } else {
      // New mint — build full card. The first card in the list expands
      // by default; older cards collapse to header-only.
      card = buildCallCard(c);
      updateCallCard(card, c);
      if (i > 0) card.classList.add("call-card--collapsed");
      CALL_CARD_CACHE.set(c.mint, card);
    }
    // Reorder: ensure DOM order matches sorted order. appendChild on an
    // already-attached node moves it without destroying.
    container.appendChild(card);
  }

  // Drop cards for mints that are no longer active (closed, expired, etc).
  for (const [mint, card] of CALL_CARD_CACHE) {
    if (!seen.has(mint)) {
      if (card.parentNode === container) container.removeChild(card);
      CALL_CARD_CACHE.delete(mint);
    }
  }

  // Remove any stray non-card children (the "no active calls" empty-state
  // div from a prior render where the list was empty).
  for (const child of Array.from(container.children)) {
    if (!child.classList.contains("call-card")) container.removeChild(child);
  }
}

function formatMcap(n) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000) return (n / 1_000).toFixed(0) + "k";
  return n.toFixed(0);
}

// --- render: history ---
// Closed-call archive. Showed only ~5 rows on the front page before; this
// expands to full history with filter pills, paginated 20 at a time, plus
// a stats banner so the page can answer "what's the track record?" at a
// glance. Per-call extras pulled from the new server schema: peak_pct,
// trough_pct (the journey, not just entry+exit), horizon, and verdict
// note.
const HISTORY_PAGE_SIZE = 20;
let HISTORY_STATE = {
  // Default to wins so visitors land on the success cohort first. Users can
  // pivot to all/failed/expired/horizon-specific via the filter pills.
  filter: "won",
  page: 1,
  data: [],   // full ordered history
  stats: null,
};

// "Runners" — top 5 historical catches by peak%. Reframes the page so a
// small wallet doesn't read as a small operation: the *signal quality*
// is the proof, not the balance. Reads from CALLS_DATA history; no new
// server-side schema. Filtered to peak >= 50% so we don't show micro-pops.
function renderFeatured(f) {
  const section = document.getElementById("featured-token");
  if (!section) return;
  if (!f || !f.mint) {
    section.hidden = true;
    return;
  }
  section.hidden = false;
  const sym = f.symbol ? "$" + f.symbol : "$??";
  const name = f.name || "";
  const setText = (id, t) => {
    const n = document.getElementById(id);
    if (n) n.textContent = t;
  };
  setText("featured-symbol", sym);
  setText("featured-name", name);
  setText("featured-pledge", f.pledge || "");
  // Price formatting: pump.fun coins are sub-cent territory, render to
  // 8 sig-figs so the display is readable even at $0.000034.
  const priceStr =
    f.price_usd && f.price_usd > 0
      ? "$" + f.price_usd.toFixed(f.price_usd < 0.01 ? 8 : 4).replace(/0+$/, "").replace(/\.$/, "")
      : "—";
  setText("featured-price", priceStr);
  setText(
    "featured-mcap",
    f.mcap_usd ? "$" + formatMcap(f.mcap_usd) : "—"
  );
  setText(
    "featured-liq",
    f.liquidity_usd ? "$" + formatMcap(f.liquidity_usd) : "—"
  );
  const changeNode = document.getElementById("featured-change");
  if (changeNode) {
    if (f.price_change_h1 != null) {
      changeNode.textContent = fmtPct(f.price_change_h1);
      changeNode.className = "value " + pnlClass(f.price_change_h1);
    } else {
      changeNode.textContent = "—";
      changeNode.className = "value dim";
    }
  }
  // Holding line — pct + balance + pledge prominence.
  const holdingNode = document.getElementById("featured-holding");
  if (holdingNode) {
    const pct = f.ape_holding_pct;
    const bal = f.ape_balance;
    if (pct > 0) {
      holdingNode.textContent =
        pct.toFixed(2) + "%  ·  " + (bal >= 1e6 ? (bal / 1e6).toFixed(2) + "M" : Math.round(bal).toLocaleString()) + " " + sym;
      holdingNode.className = "value pos";
    } else {
      holdingNode.textContent = "wiring up…";
      holdingNode.className = "value dim";
    }
  }
  // Buy + chart links.
  const buyBtn = document.getElementById("featured-buy");
  if (buyBtn) {
    if (f.buy_url) {
      buyBtn.href = f.buy_url;
      buyBtn.hidden = false;
    } else {
      buyBtn.hidden = true;
    }
  }
  const chartBtn = document.getElementById("featured-chart-link");
  if (chartBtn) {
    chartBtn.href = "https://dexscreener.com/solana/" + f.mint;
    chartBtn.hidden = false;
  }
}

function renderRunners(history) {
  const container = document.getElementById("runners-list");
  if (!container) return;
  clear(container);
  if (!Array.isArray(history) || !history.length) {
    container.appendChild(el("div", { class: "empty", text: "no runners yet" }));
    return;
  }
  const ranked = history
    .filter((c) => c && typeof c.peak_pct === "number" && c.peak_pct >= 50)
    .sort((a, b) => b.peak_pct - a.peak_pct)
    .slice(0, 5);
  if (!ranked.length) {
    container.appendChild(el("div", { class: "empty", text: "no runners yet" }));
    return;
  }
  for (const c of ranked) {
    const sym = c.symbol ? "$" + c.symbol : shortAddr(c.mint || "");
    const term = callTerm(c);
    const peakStr = fmtPct(c.peak_pct);
    const exitPct = callPctValue(c);
    const exitStr = exitPct == null ? "—" : fmtPct(exitPct);
    const exitCls = exitPct == null ? "dim" : pnlClass(exitPct);
    const meta = [];
    if (term) meta.push(term);
    meta.push("entry $" + (c.entry_mcap_usd ? formatMcap(c.entry_mcap_usd) : "?"));
    meta.push(fmtTimeAgo(c.closed_at || c.called_at));
    // Runner icon by outcome — 🏆 won (positive realized exit),
    // 🥈 caught the move but stopped out negative. RUNNERS section is
    // ordered by peak% regardless of outcome, so the icon is the
    // shortcut signal for "did we cash this in or just watch it run."
    const wonRunner = exitPct != null && exitPct > 0;
    const runnerIcon = wonRunner ? "🏆 " : "🥈 ";
    const symEl = el("div", { class: "sym runner-sym" }, [
      runnerIcon,
      el("a", { href: "#call=" + c.mint, class: "sym-link", text: sym }),
    ]);
    const detail = el("div", { class: "detail dim", text: meta.join(" · ") });
    const peakEl = el("div", { class: "num pos", text: "peak " + peakStr });
    const exitEl = el("div", { class: "num " + exitCls, text: "exit " + exitStr });
    container.appendChild(
      el("div", { class: "row runner-row", title: c.mint }, [symEl, detail, peakEl, exitEl])
    );
  }
}

function renderHistoryStats(stats) {
  const banner = document.getElementById("history-stats");
  if (!banner) return;
  clear(banner);
  if (!stats || !stats.overall || stats.overall.count === 0) {
    banner.appendChild(el("div", { class: "empty", text: "no history yet" }));
    return;
  }
  // Sub-banners: overall, short, long, plus per-source rows (auto vs
  // manual). Skip empties so we don't render "0 calls · 0% win rate"
  // rows that confuse the eye.
  const sourceLabels = { notifier: "auto", dm: "manual", mcp: "claw", legacy: "legacy" };
  const sourceBuckets = stats.by_source
    ? Object.entries(stats.by_source)
        .filter(([, b]) => b && b.count > 0)
        .sort((a, b) => b[1].count - a[1].count) // most calls first
        .map(([k, b]) => [sourceLabels[k] || k, b])
    : [];
  const buckets = [
    ["overall", stats.overall],
    ["moonshot", stats.moonshot],
    ["short", stats.short],
    ["long", stats.long],
    ["scalp", stats.scalp],
    ...sourceBuckets,
  ].filter(([, b]) => b && b.count > 0);
  for (const [label, b] of buckets) {
    const row = el("div", { class: "history-stat-row" });
    row.appendChild(el("span", { class: "history-stat-label", text: label }));
    row.appendChild(el("span", { class: "history-stat-cell", text: b.count + " calls" }));
    if (b.wins + b.losses > 0) {
      const wrCls = b.win_rate >= 50 ? "pos" : "neg";
      row.appendChild(el("span", { class: "history-stat-cell " + wrCls, text: b.win_rate.toFixed(0) + "% win rate" }));
    }
    if (b.avg_winner_pct > 0) {
      row.appendChild(el("span", { class: "history-stat-cell pos", text: "avg winner +" + b.avg_winner_pct.toFixed(0) + "%" }));
    }
    if (b.avg_loser_pct < 0) {
      row.appendChild(el("span", { class: "history-stat-cell neg", text: "avg loser " + b.avg_loser_pct.toFixed(0) + "%" }));
    }
    if (b.best_pct > 0) {
      row.appendChild(el("span", { class: "history-stat-cell faint", text: "best " + fmtPct(b.best_pct) }));
    }
    if (b.worst_pct < 0) {
      row.appendChild(el("span", { class: "history-stat-cell faint", text: "worst " + fmtPct(b.worst_pct) }));
    }
    banner.appendChild(row);
  }
}

// PnL-based bucketing — the user mental model is "did this trade make
// money", not "which exit branch fired in the scanner". outcome_type is
// a lifecycle tag (withdrew=ladder fired, failed=event-driven exit,
// expired=time-out) but the trail-at-breakeven trigger fires on peak
// alone and can fill at materially negative pct (settle-latency slip).
// Bin by realized pct sign so the filter tabs match what the wallet
// actually saw at exit.
function pctBucket(c) {
  const pct = callPctValue(c);
  if (pct == null) return "unknown";
  if (pct >= 5) return "won";
  if (pct <= -5) return "loss";
  return "flat";
}

function isFlatClose(c) {
  return pctBucket(c) === "flat";
}

function filterHistory(rows, filter) {
  if (!Array.isArray(rows)) return [];
  if (filter === "all") return rows;
  return rows.filter((c) => {
    const bucket = pctBucket(c);
    switch (filter) {
      case "won":     return bucket === "won";
      case "flat":    return bucket === "flat";
      // FAILED tab = realized loss OR event-driven exit (the scanner
      // tagged "failed" via dev-exit/structural-collapse rules even
      // when realized was positive — the operator's *thesis* failed
      // even if the number was green at exit time).
      case "failed":  return bucket === "loss" || c.outcome_type === "failed";
      case "expired": return c.outcome_type === "expired";
      case "short":   return !((c.note || "").includes("horizon=LONG"));
      case "long":    return (c.note || "").includes("horizon=LONG");
      default:        return true;
    }
  });
}

// Single-line HISTORY row matching the CALLS row layout: symbol left,
// meta middle, pct right. Cohesion with the active-calls section
// (same `.row` skeleton, just adds outcome icon + horizon-tinted symbol).
function renderHistoryRow(c) {
  const sym = c.symbol ? "$" + c.symbol : shortAddr(c.mint || "");
  const term = callTerm(c);
  const pct = callPctValue(c);
  const bucket = pctBucket(c);
  // Pct color follows realized PnL (won=green / loss=red / flat=dim),
  // independent of the scanner's outcome_type tag. Numbers + colors
  // tell one consistent story.
  const pctCls = pct == null ? "" : (bucket === "flat" ? "dim" : pnlClass(pct));

  // Symbol icon + color follow the same bucket. Expired keeps its
  // own ⏰ since it's a lifecycle thing (no movement at all).
  const isExpired = c.outcome_type === "expired";
  const outcomeCls = "history-outcome-" + (
    isExpired ? "expired" :
    bucket === "won" ? "withdrew" :
    bucket === "flat" ? "flat" :
    bucket === "loss" ? "failed" : "closed"
  );
  const icon = isExpired ? "⏰"
             : bucket === "won" ? "🟢"
             : bucket === "flat" ? "⚪"
             : bucket === "loss" ? "🔴"
             : "·";

  // Verdict line from the settling phase ("+52% · took the win",
  // "-64% · thesis broke") — strips the percent prefix since it's
  // already shown right-aligned, leaving just the human reason.
  const verdictRaw = c.exit_note || "";
  const verdict = verdictRaw.replace(/^[+-]?\d+(?:\.\d+)?% · /, "").trim();

  const metaParts = [];
  if (term) metaParts.push(term.toUpperCase());
  if (verdict) metaParts.push(verdict);
  if (c.peak_pct != null && c.peak_pct > 0.5) metaParts.push("peak " + fmtPct(c.peak_pct));
  if (c.trough_pct != null && c.trough_pct < -0.5) metaParts.push("trough " + fmtPct(c.trough_pct));
  if (c.entry_mcap_usd) metaParts.push("entry $" + formatMcap(c.entry_mcap_usd));
  metaParts.push(fmtTimeAgo(c.closed_at || c.called_at));
  const metaStr = metaParts.join(" · ");

  const symEl = el("div", { class: "sym " + outcomeCls }, [
    icon + " ",
    el("a", { href: "#call=" + c.mint, class: "sym-link", text: sym }),
  ]);
  const detailChildren = [
    metaStr + " · ",
    el("a", { href: DEXSCREENER + "/" + c.mint, target: "_blank", rel: "noopener", text: "chart" }),
    " · ",
    el("a", { href: SOLSCAN + "/token/" + c.mint, target: "_blank", rel: "noopener", text: "solscan" }),
  ];
  if (c.thesis_url) {
    detailChildren.push(" · ");
    detailChildren.push(el("a", { href: "#note=" + encodeURIComponent(c.thesis_url.replace(/^thoughts\//, "")), text: "📖 thesis" }));
  }
  const detail = el("div", { class: "detail dim" }, detailChildren);
  const numEl = el("div", { class: "num " + pctCls, text: pct == null ? "—" : fmtPct(pct) });
  return el("div", { class: "row", title: c.mint }, [symEl, detail, numEl]);
}

function renderHistory() {
  const list = document.getElementById("history-list");
  const more = document.getElementById("history-more");
  if (!list) return;
  clear(list);
  const filtered = filterHistory(HISTORY_STATE.data, HISTORY_STATE.filter);
  if (!filtered.length) {
    list.appendChild(el("div", { class: "empty", text: "no calls match this filter" }));
    if (more) more.hidden = true;
    return;
  }
  const visible = filtered.slice(0, HISTORY_STATE.page * HISTORY_PAGE_SIZE);
  for (const c of visible) list.appendChild(renderHistoryRow(c));
  if (more) more.hidden = visible.length >= filtered.length;
}

// --- render: per-call detail page ---
// Drives the `#call=<mint>` hash route. Hides the rest of the page when
// active, fetches `data/calls/<mint>.json`, renders facts + classification
// timeline + links. Cohabits with the existing #note= notes route — when
// the hash leaves #call= space we show the main view again.

const CALL_DETAIL_FETCH_CACHE = new Map(); // mint → last detail JSON
let CALL_DETAIL_CURRENT = null;

function detailParseHash() {
  const m = location.hash.match(/#call=([A-Za-z0-9]+)/);
  return m ? m[1] : null;
}

function setCallDetailVisible(visible) {
  const panel = document.getElementById("call-detail");
  if (panel) panel.hidden = !visible;
  // Hide/show the everything-else sections so the detail page reads as
  // a focused view, not a popover. Iterate by classlist so adding new
  // sections later doesn't need touching this.
  document
    .querySelectorAll(".positions, .calls, .history, .activity, .thoughts")
    .forEach((s) => {
      s.style.display = visible ? "none" : "";
    });
}

async function renderCallDetail(mint) {
  const panel = document.getElementById("call-detail");
  const title = document.getElementById("call-detail-title");
  const body = document.getElementById("call-detail-body");
  if (!panel || !body) return;
  CALL_DETAIL_CURRENT = mint;
  setCallDetailVisible(true);
  body.textContent = "loading…";

  // Cache so flipping in/out doesn't re-fetch each time.
  let detail = CALL_DETAIL_FETCH_CACHE.get(mint);
  if (!detail) {
    detail = await loadJson("/api/data/calls/" + mint);
    if (detail) CALL_DETAIL_FETCH_CACHE.set(mint, detail);
  }
  if (!detail || !detail.call) {
    title.textContent = "call not found";
    clear(body);
    body.appendChild(
      el("div", {
        class: "empty",
        text: "no detail for " + shortAddr(mint) + " — may not be in the public ledger",
      })
    );
    return;
  }
  const c = detail.call;
  const sym = c.symbol ? "$" + c.symbol : shortAddr(c.mint);
  title.textContent = sym;

  clear(body);
  // Header strip: outcome + horizon + final pct. Bucketed by realized
  // pct except for active (in-flight) and expired (lifecycle tag).
  const bucket = pctBucket(c);
  const icon = c.outcome_type === "active" ? "📣"
             : c.outcome_type === "expired" ? "⏰"
             : bucket === "won" ? "🟢"
             : bucket === "flat" ? "⚪"
             : bucket === "loss" ? "🔴"
             : "·";
  const term = callTerm(c);
  const pct = callPctValue(c);
  const pctCls = pct == null ? "" : (bucket === "flat" ? "dim" : pnlClass(pct));
  body.appendChild(
    el("div", { class: "detail-banner" }, [
      el("span", { class: "detail-banner-icon", text: icon }),
      el("span", { class: "detail-banner-sym", text: sym }),
      term ? el("span", { class: "detail-banner-term", text: term.toUpperCase() }) : null,
      el("span", { class: "detail-banner-pct " + pctCls, text: pct == null ? "—" : fmtPct(pct) }),
    ])
  );

  // Stats grid: entry / current-or-exit / peak / trough / journey
  const statsGrid = el("div", { class: "detail-stats" });
  const stat = (label, value) =>
    el("div", { class: "detail-stat" }, [
      el("div", { class: "detail-stat-label", text: label }),
      el("div", { class: "detail-stat-value", text: value }),
    ]);
  if (c.entry_mcap_usd) statsGrid.appendChild(stat("entry mcap", "$" + formatMcap(c.entry_mcap_usd)));
  if (c.outcome_type === "active" && c.current_mcap_usd) {
    statsGrid.appendChild(stat("now mcap", "$" + formatMcap(c.current_mcap_usd)));
  }
  if (c.entry_price_usd) statsGrid.appendChild(stat("entry price", "$" + Number(c.entry_price_usd).toPrecision(4)));
  if (c.exit_price_usd) statsGrid.appendChild(stat("exit price", "$" + Number(c.exit_price_usd).toPrecision(4)));
  if (c.peak_pct != null) statsGrid.appendChild(stat("peak", fmtPct(c.peak_pct)));
  if (c.trough_pct != null) statsGrid.appendChild(stat("trough", fmtPct(c.trough_pct)));
  if (c.entry_top_holder_pct != null) statsGrid.appendChild(stat("entry top", c.entry_top_holder_pct.toFixed(2) + "%"));
  if (c.entry_liquidity_usd) statsGrid.appendChild(stat("entry liq", "$" + formatMcap(c.entry_liquidity_usd)));
  if (c.called_at) statsGrid.appendChild(stat("called", fmtTimeAgo(c.called_at)));
  if (c.closed_at) statsGrid.appendChild(stat("closed", fmtTimeAgo(c.closed_at)));
  body.appendChild(statsGrid);

  // Verdict line for closed calls
  if (c.exit_note && c.outcome_type !== "active") {
    body.appendChild(el("div", { class: "detail-verdict", text: c.exit_note }));
  }

  // Classification timeline — compact list of class flips from snapshots
  const snaps = Array.isArray(detail.snapshots) ? detail.snapshots : [];
  if (snaps.length > 1) {
    const timelineWrap = el("div", { class: "detail-timeline" });
    timelineWrap.appendChild(el("div", { class: "detail-section-title", text: "classification timeline" }));
    let prevClass = null;
    let renderedRows = 0;
    for (const s of snaps) {
      if (s.classification === prevClass) continue;
      prevClass = s.classification;
      const row = el("div", { class: "detail-timeline-row" }, [
        el("span", { class: "detail-timeline-ts", text: fmtTimeAgo(s.timestamp) }),
        el("span", { class: "detail-timeline-class", text: s.classification }),
        el("span", { class: "detail-timeline-conf dim", text: "conf " + s.confidence }),
        el("span", { class: "detail-timeline-top dim", text: "top " + s.top_holder_pct.toFixed(1) + "%" }),
      ]);
      timelineWrap.appendChild(row);
      renderedRows += 1;
      if (renderedRows >= 30) break;
    }
    if (renderedRows > 0) body.appendChild(timelineWrap);
  }

  // Links row
  const links = el("div", { class: "detail-links" });
  links.appendChild(el("a", { href: DEXSCREENER + "/" + c.mint, target: "_blank", rel: "noopener", text: "📊 chart" }));
  links.appendChild(el("a", { href: SOLSCAN + "/token/" + c.mint, target: "_blank", rel: "noopener", text: "🔍 solscan" }));
  links.appendChild(el("a", { href: "/api/data/scouts/" + c.mint, target: "_blank", rel: "noopener", text: "scout" }));
  links.appendChild(el("a", { href: "/api/data/whales/" + c.mint, target: "_blank", rel: "noopener", text: "whales" }));
  if (c.thesis_url) {
    const thesisFile = c.thesis_url.replace(/^thoughts\//, "");
    links.appendChild(el("a", { href: "#note=" + encodeURIComponent(thesisFile), text: "📖 thesis" }));
  }
  body.appendChild(links);
}

function routeCallHash() {
  const mint = detailParseHash();
  if (mint) {
    renderCallDetail(mint);
  } else if (CALL_DETAIL_CURRENT) {
    CALL_DETAIL_CURRENT = null;
    setCallDetailVisible(false);
  }
}

function bindCallDetailRouting() {
  window.addEventListener("hashchange", routeCallHash);
  const back = document.getElementById("call-detail-back");
  if (back) {
    back.addEventListener("click", () => {
      // Clear hash and let routeCallHash handle the unmount.
      history.pushState("", document.title, location.pathname + location.search);
      routeCallHash();
    });
  }
}

function bindHistoryControls() {
  const filters = document.querySelectorAll(".history-filter");
  for (const btn of filters) {
    btn.addEventListener("click", () => {
      for (const b of filters) b.classList.remove("active");
      btn.classList.add("active");
      HISTORY_STATE.filter = btn.getAttribute("data-filter");
      HISTORY_STATE.page = 1;
      renderHistory();
    });
  }
  const more = document.getElementById("history-more");
  if (more) {
    more.addEventListener("click", () => {
      HISTORY_STATE.page += 1;
      renderHistory();
    });
  }
}

// --- render: ticker strip (arena-inspired) ---
function renderTicker(health, pnl, calls, positions) {
  const ticker = document.getElementById("ticker");
  if (!ticker) return;
  clear(ticker);
  const items = [];
  if (health && health.sol_price_usd) {
    items.push({
      sym: "SOL",
      val: "$" + health.sol_price_usd.toFixed(2),
      pct: null,
    });
  }
  if (pnl && pnl.total_value_usd != null) {
    const hasOpenPositions = Array.isArray(positions) && positions.length > 0;
    // Only attach a pct when we have a real number — empty bag should
    // not render a trailing em-dash separator. Reads "BAG $19.19" cleanly.
    const bagPct = hasOpenPositions && pnl.unrealized_pnl_usd != null
      ? fmtPct((pnl.unrealized_pnl_usd / (pnl.total_value_usd || 1)) * 100)
      : null;
    items.push({
      sym: "BAG",
      val: fmtUsd(pnl.total_value_usd),
      pct: bagPct,
    });
  }
  if (calls && calls.length) {
    for (const c of calls.slice(0, 4)) {
      const symText = c.symbol && c.symbol.trim()
        ? "$" + c.symbol.toUpperCase()
        : shortAddr(c.mint);
      items.push({
        sym: symText,
        val: c.current_mcap_usd ? "$" + formatMcap(c.current_mcap_usd) : "—",
        pct: fmtPct(callPctValue(c)),
      });
    }
  }
  if (!items.length) {
    ticker.appendChild(el_("div", { class: "ticker-empty", text: "no live values yet" }));
    return;
  }
  for (const item of items) {
    const node = el_("div", { class: "ticker-item" }, [
      el_("span", { class: "ticker-sym", text: item.sym }),
      el_("span", { class: "ticker-val", text: item.val }),
      item.pct !== null && item.pct !== undefined
        ? el_("span", {
            class: "ticker-pct " + pnlClass(parseFloat(item.pct)),
            text: item.pct,
          })
        : null,
    ]);
    ticker.appendChild(node);
  }
}

// el conflicts with existing function name; local alias for ticker.
function el_(tag, attrs, children) {
  return el(tag, attrs, children);
}

// --- render: live stream side-panel ---
// Tracks which event timestamps we've already seen so "new" ones flash
// briefly on arrival — makes the feed feel alive between refresh ticks.
const SEEN_STREAM = new Set();
let STREAM_FIRST_RENDER = true;

function rememberStreamSignature(sig) {
  if (SEEN_STREAM.has(sig)) return;
  SEEN_STREAM.add(sig);
  while (SEEN_STREAM.size > MAX_SEEN_STREAM) {
    const oldest = SEEN_STREAM.values().next().value;
    if (oldest === undefined) break;
    SEEN_STREAM.delete(oldest);
  }
}

// Split a summary string and turn any `$SYMBOL` run into a link to the
// mint's DexScreener chart when we know the mint. Keeps everything else
// as textNodes so no HTML injection surface.
function linkifySummary(host, text, mint) {
  const re = /\$[A-Za-z0-9]+/g;
  const matches = [...text.matchAll(re)];
  let last = 0;
  for (const match of matches) {
    if (match.index > last) {
      host.appendChild(document.createTextNode(text.slice(last, match.index)));
    }
    if (mint) {
      host.appendChild(
        el("a", {
          href: DEXSCREENER + "/" + mint,
          target: "_blank",
          rel: "noopener",
          class: "inline-sym",
          text: match[0],
        })
      );
    } else {
      host.appendChild(document.createTextNode(match[0]));
    }
    last = match.index + match[0].length;
  }
  if (last < text.length) host.appendChild(document.createTextNode(text.slice(last)));
}

function timeOfDay() {
  const d = new Date();
  return (
    String(d.getHours()).padStart(2, "0") +
    ":" +
    String(d.getMinutes()).padStart(2, "0") +
    ":" +
    String(d.getSeconds()).padStart(2, "0")
  );
}

// Persisted across renders: which token-group cards the user has expanded.
// Otherwise every publisher tick collapses everything they were reading.
const STREAM_EXPANDED = new Set();
const STREAM_NO_MINT_KEY = "__no_mint__"; // single bucket for events without a mint

function fmtCompactUsd(n) {
  if (n == null || isNaN(n)) return "—";
  const abs = Math.abs(n);
  if (abs >= 1e9) return "$" + (n / 1e9).toFixed(1) + "B";
  if (abs >= 1e6) return "$" + (n / 1e6).toFixed(1) + "M";
  if (abs >= 1e3) return "$" + (n / 1e3).toFixed(0) + "k";
  return "$" + Math.round(n);
}

// Group events by mint. Order preserved as newest-first (events are already
// sorted by ts desc). Events without a mint go into a shared bucket that
// renders as a flat strip at the bottom.
function groupEventsByMint(events) {
  const groups = new Map();
  for (const ev of events) {
    const key = ev.mint || STREAM_NO_MINT_KEY;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(ev);
  }
  // Sort groups: latest event first (we want the freshest token at top).
  return [...groups.entries()].sort(
    (a, b) => (b[1][0]?.ts || 0) - (a[1][0]?.ts || 0)
  );
}

// Render the token-card header — the always-visible row that summarizes the
// group. Click to fold/unfold the events list.
// Compress overly long alert tag strings so they fit the 320px live-feed
// column without overflowing. Publisher uppercases + space-replaces alert
// types ("classification_change" → "CLASSIFICATION CHANGE"); those are
// 20+ chars and clip on the right edge. Short forms preserve meaning.
const STREAM_TAG_SHORT = {
  "CLASSIFICATION CHANGE": "CLASS-FLIP",
  "DEV SELLING": "DEV-OUT",
  "CONCENTRATING": "TOP-RISING",
  "VOLUME COLLAPSE": "VOL-DEAD",
  "MOMENTUM COLLAPSE": "MOM-DEAD",
  "STRUCTURAL COLLAPSE": "STRUCT-OUT",
  "SEVERE DEV EXIT": "DEV-RUG",
};
function shortenStreamTag(tag) {
  if (!tag) return tag;
  return STREAM_TAG_SHORT[tag] || tag;
}

function renderTokenHeader(mint, info, events, isExpanded, symbolDuplicates) {
  const baseSym = info?.symbol ? "$" + info.symbol : shortAddr(mint);
  // Append a mint suffix when this header shares its symbol with another
  // group in the same stream — disambiguates distinct mints with the
  // same ticker (e.g. multiple $WINNING launches firing alerts together).
  const isDup = info?.symbol && symbolDuplicates && symbolDuplicates.has(info.symbol);
  const sym = isDup ? baseSym + " (" + shortAddr(mint) + ")" : baseSym;
  const nameEl = el("span", { class: "stream-group-sym", text: sym });

  // Right side: count + most recent event time. The kind badge of the
  // freshest event signals what's happening (alert/call/trade).
  const latest = events[0];
  const latestKind = latest?.kind || "alert";
  const latestTag = shortenStreamTag(latest?.tag || latestKind.toUpperCase());

  // Numeric meta — mcap, 1h change, count. Each cell has its own colour
  // so a glance tells you the token state without reading.
  const meta = el("div", { class: "stream-group-meta" });
  if (info?.mcap_usd) {
    meta.appendChild(el("span", { class: "stream-meta-mcap", text: "mc " + fmtCompactUsd(info.mcap_usd) }));
  }
  if (info?.price_change_1h != null) {
    const pc = info.price_change_1h;
    const cls = pc > 0 ? "pos" : pc < 0 ? "neg" : "";
    meta.appendChild(el("span", { class: "stream-meta-pct " + cls, text: "1h " + fmtPct(pc) }));
  }
  if (events.length > 1) {
    meta.appendChild(el("span", { class: "stream-meta-count", text: "×" + events.length }));
  }

  // Caret indicator that flips on expand. The whole header is the click
  // target so trying to tap "the row" works without aiming.
  const caret = el("span", {
    class: "stream-group-caret",
    text: isExpanded ? "▾" : "▸",
  });
  const tag = el("span", {
    class: "stream-kind stream-kind-" + latestKind,
    text: latestTag,
  });
  const ts = el("span", {
    class: "stream-ts",
    text: fmtTimeAgo(latest?.ts),
    "data-ts": latest?.ts,
  });

  // Two-row layout: top row is symbol + tag + caret + time; bottom row
  // is the meta strip (mc / 1h / count).
  const topRow = el("div", { class: "stream-group-row" }, [caret, nameEl, tag, ts]);
  const header = el("div", { class: "stream-group-head" }, [topRow]);
  if (meta.children.length) header.appendChild(meta);
  return header;
}

// Render a single event line inside an expanded token group. Compact:
// time-ago + tag + summary + per-event links (chart/solscan/tx).
function renderEventLine(ev) {
  const time = el("span", { class: "stream-evt-ts", text: fmtTimeAgo(ev.ts), "data-ts": ev.ts });
  const tag = el("span", {
    class: "stream-kind stream-kind-" + ev.kind,
    text: shortenStreamTag(ev.tag || ev.kind),
  });
  const summaryEl = el("div", { class: "stream-evt-summary" });
  linkifySummary(summaryEl, ev.summary || "", ev.mint);

  const links = el("div", { class: "stream-evt-links" });
  if (ev.mint) {
    links.appendChild(el("a", { href: DEXSCREENER + "/" + ev.mint, target: "_blank", rel: "noopener", text: "chart" }));
    links.appendChild(document.createTextNode(" · "));
    links.appendChild(el("a", { href: SOLSCAN + "/token/" + ev.mint, target: "_blank", rel: "noopener", text: "solscan" }));
  }
  if (ev.signature) {
    if (ev.mint) links.appendChild(document.createTextNode(" · "));
    links.appendChild(el("a", { href: SOLSCAN + "/tx/" + ev.signature, target: "_blank", rel: "noopener", text: "tx" }));
  }
  const head = el("div", { class: "stream-evt-head" }, [time, tag]);
  const evEl = el("div", { class: "stream-evt" }, [head, summaryEl]);
  if (links.children.length) evEl.appendChild(links);
  return evEl;
}

// Token-card top-level links row — chart/solscan once, not per-event.
// Sits at the top of the expanded body so the most common action (open
// chart) is one tap, not buried inside an event row.
function renderTokenLinks(mint) {
  if (!mint || mint === STREAM_NO_MINT_KEY) return null;
  const row = el("div", { class: "stream-group-links" });
  row.appendChild(el("a", { href: DEXSCREENER + "/" + mint, target: "_blank", rel: "noopener", text: "📊 chart" }));
  row.appendChild(document.createTextNode(" · "));
  row.appendChild(el("a", { href: SOLSCAN + "/token/" + mint, target: "_blank", rel: "noopener", text: "🔍 solscan" }));
  return row;
}

// One row in the "watching" subsection — passing classifier but
// blocked by some secondary gate. Format mirrors the call rows so the
// eye reads it the same way: ticker, classification, conf, gap.
//
// `symbolDuplicates` is a Set of symbol strings that appear more than
// once in the watching list — for those we append a `(…abcd)` mint
// suffix so distinct tokens with the same ticker (e.g. multiple
// $WINNING launches) are visually different. Without this, three
// different mints with the same ticker render as if they're the
// same token.
function renderWatchingRow(w, symbolDuplicates) {
  const baseSym = w.symbol ? "$" + w.symbol : shortAddr(w.mint || "");
  const isDup = w.symbol && symbolDuplicates.has(w.symbol);
  const sym = isDup ? baseSym + " (" + shortAddr(w.mint || "") + ")" : baseSym;
  const ageStr = w.age_secs < 60 ? w.age_secs + "s" : Math.floor(w.age_secs / 60) + "m";
  const meta = [
    w.classification,
    "conf " + w.confidence,
    "top1 " + (w.top_holder_pct || 0).toFixed(1) + "%",
    "blocked: " + (w.gate || "?"),
    ageStr + " ago",
  ].join(" · ");
  const symEl = el("div", { class: "sym stream-watching-sym" }, [
    "👁 ",
    el("a", {
      href: DEXSCREENER + "/" + w.mint,
      target: "_blank",
      rel: "noopener",
      class: "sym-link",
      text: sym,
    }),
  ]);
  const detail = el("div", { class: "detail dim" }, [meta]);
  return el("div", { class: "row stream-watching-row", title: w.gap || "" }, [symEl, detail]);
}

function renderStream(stream) {
  const body = document.getElementById("stream-body");
  const status = document.getElementById("stream-status");
  if (!body) return;
  const events = (stream && stream.events) || [];
  const tokens = (stream && stream.tokens) || {};
  const watching = (stream && stream.watching) || [];

  // Track signatures so newly-arrived events can flash on next render.
  const newSignatures = new Set();
  for (const ev of events) {
    const sig = ev.ts + "|" + (ev.summary || "");
    if (!STREAM_FIRST_RENDER && !SEEN_STREAM.has(sig)) {
      newSignatures.add(sig);
    }
    rememberStreamSignature(sig);
  }

  clear(body);

  // "Watching" subsection — tokens passing classifier with conf >= 60 in
  // the last 30min that didn't fire a call. Renders above events because
  // it's the live thinking stream; events are historical. Server-side
  // dedupes against `events` so same mint never appears in both lists.
  if (watching.length) {
    // Find symbols that appear in >1 watching row — those need mint-suffix
    // disambiguation in the row renderer.
    const symbolCounts = new Map();
    for (const w of watching) {
      if (!w.symbol) continue;
      symbolCounts.set(w.symbol, (symbolCounts.get(w.symbol) || 0) + 1);
    }
    const symbolDuplicates = new Set(
      [...symbolCounts.entries()].filter(([, n]) => n > 1).map(([s]) => s)
    );
    const watchHeader = el("div", { class: "stream-watching-header" }, [
      "watching · " + watching.length,
    ]);
    body.appendChild(watchHeader);
    for (const w of watching) {
      body.appendChild(renderWatchingRow(w, symbolDuplicates));
    }
  }

  if (!events.length && !watching.length) {
    body.appendChild(el("div", { class: "stream-empty", text: "no events yet" }));
    if (status) status.textContent = "—";
    STREAM_FIRST_RENDER = false;
    return;
  }
  if (!events.length) {
    if (status) status.textContent = watching.length + " watching";
    STREAM_FIRST_RENDER = false;
    return;
  }

  const groups = groupEventsByMint(events);
  // Count symbols across groups to flag duplicates — same ticker, different
  // mints get a (mint-suffix) appended so they're visually distinct.
  const groupSymbolCounts = new Map();
  for (const [mint] of groups) {
    if (mint === STREAM_NO_MINT_KEY) continue;
    const sym = tokens[mint]?.symbol;
    if (!sym) continue;
    groupSymbolCounts.set(sym, (groupSymbolCounts.get(sym) || 0) + 1);
  }
  const groupSymbolDuplicates = new Set(
    [...groupSymbolCounts.entries()].filter(([, n]) => n > 1).map(([s]) => s)
  );
  if (status) {
    const watchSeg = watching.length ? " · " + watching.length + " watching" : "";
    status.textContent =
      groups.length + " tokens · " + events.length + " events" + watchSeg + " · " + timeOfDay();
  }

  for (const [mint, evList] of groups) {
    const info = mint === STREAM_NO_MINT_KEY ? null : tokens[mint];
    const isExpanded = STREAM_EXPANDED.has(mint);
    const hasNew = evList.some(
      (ev) => newSignatures.has(ev.ts + "|" + (ev.summary || ""))
    );
    const card = el("div", {
      class: "stream-group" + (hasNew ? " stream-group-new" : "") + (isExpanded ? " is-open" : ""),
    });

    // Header (always visible, click to toggle).
    const header = renderTokenHeader(mint, info, evList, isExpanded, groupSymbolDuplicates);
    header.style.cursor = "pointer";
    header.addEventListener("click", (e) => {
      // Don't toggle when the click landed on a link — let it through.
      if (e.target.closest("a")) return;
      if (STREAM_EXPANDED.has(mint)) STREAM_EXPANDED.delete(mint);
      else STREAM_EXPANDED.add(mint);
      renderStream(stream);
    });
    card.appendChild(header);

    // Body (only rendered when expanded — keeps DOM light when 50 groups exist).
    if (isExpanded) {
      const body2 = el("div", { class: "stream-group-body" });
      const linksRow = renderTokenLinks(mint);
      if (linksRow) body2.appendChild(linksRow);
      for (const ev of evList) body2.appendChild(renderEventLine(ev));
      card.appendChild(body2);
    }
    body.appendChild(card);
  }
  STREAM_FIRST_RENDER = false;
}

// --- render: pnl ---
let PNL_DATA = null;
let CHART_WINDOW = readStoredChartWindow();

function renderPnl(pnl) {
  if (!pnl) return;
  PNL_DATA = pnl;
  const totalEl = document.getElementById("total-value");
  const realizedEl = document.getElementById("realized-pnl");
  const unrealizedEl = document.getElementById("unrealized-pnl");

  totalEl.textContent = fmtUsd(pnl.total_value_usd);
  realizedEl.textContent = fmtUsd(pnl.realized_pnl_usd);
  realizedEl.className = "big " + pnlClass(pnl.realized_pnl_usd || 0);
  unrealizedEl.textContent = fmtUsd(pnl.unrealized_pnl_usd);
  unrealizedEl.className = "big " + pnlClass(pnl.unrealized_pnl_usd || 0);

  redrawChart();
}

function redrawChart() {
  if (!PNL_DATA) return;
  const series = PNL_DATA.series || [];
  const trades = PNL_DATA.trades || [];
  const now = Date.now() / 1000;
  const windowSec = {
    "1h": 3600,
    "6h": 21600,
    "24h": 86400,
    all: Infinity,
  }[CHART_WINDOW];
  const cutoff = now - windowSec;
  // Skip zero-value samples — those come from transient RPC failures
  // where sol_balance fetched as 0. They'd drag the chart to the floor
  // and misrepresent reality.
  let filteredSeries = series.filter(
    (p) => p.ts >= cutoff && p.value_usd > 0
  );
  // Trim a leading near-zero plateau — e.g. legacy samples from a prior
  // wallet whose USD balance was a fraction of the current. Without this,
  // the ALL view shows a $0 flatline for a week then a sudden jump,
  // visually suggesting "did nothing then teleported", which is just a
  // wallet swap artifact. Heuristic: drop leading entries below 20% of
  // the median value; once we cross that threshold, keep everything.
  if (filteredSeries.length >= 3) {
    const sorted = filteredSeries.map((p) => p.value_usd).sort((a, b) => a - b);
    const median = sorted[Math.floor(sorted.length / 2)];
    const threshold = median * 0.2;
    let firstReal = 0;
    for (let i = 0; i < filteredSeries.length; i++) {
      if (filteredSeries[i].value_usd >= threshold) {
        firstReal = i;
        break;
      }
    }
    if (firstReal > 0) filteredSeries = filteredSeries.slice(firstReal);
  }
  const startTs = filteredSeries.length ? filteredSeries[0].ts : cutoff;
  const filteredTrades = trades.filter((t) => t.ts >= startTs);
  renderChart(filteredSeries, filteredTrades);
}

function wireChartTabs() {
  const tabs = document.querySelectorAll(".chart-tab");
  syncChartTabs();
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      CHART_WINDOW = tab.dataset.window;
      persistChartWindow(CHART_WINDOW);
      syncChartTabs();
      redrawChart();
    });
  });
}

function syncChartTabs() {
  const tabs = document.querySelectorAll(".chart-tab");
  tabs.forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.window === CHART_WINDOW);
  });
}

function renderChart(series, trades) {
  const container = document.getElementById("pnl-chart");
  clear(container);
  if (typeof uPlot === "undefined" || !series.length) {
    const hint = el("div", { class: "chart-hint" }, [
      "ape is still sniffing — no banana count yet",
    ]);
    container.appendChild(hint);
    return;
  }
  if (series.length === 1) {
    const only = series[0];
    container.appendChild(
      el("div", { class: "chart-hint" }, [
        "1 point so far · ",
        fmtUsd(only.value_usd),
        " · ",
        fmtTimeAgo(only.ts),
      ])
    );
    return;
  }
  const xs = series.map((p) => p.ts);
  const ys = series.map((p) => p.value_usd);

  // Build aligned y-arrays for trade markers: one column per "side".
  // Values = portfolio value at the closest series timestamp, or null where
  // no trade matches that x-coordinate.
  const buyYs = new Array(xs.length).fill(null);
  const sellYs = new Array(xs.length).fill(null);
  if (trades && trades.length) {
    for (const t of trades) {
      // Nearest series index by timestamp — markers snap to a real
      // portfolio-value datapoint so the chart geometry stays honest.
      let nearest = 0;
      let bestDiff = Infinity;
      for (let i = 0; i < xs.length; i++) {
        const d = Math.abs(xs[i] - t.ts);
        if (d < bestDiff) {
          bestDiff = d;
          nearest = i;
        }
      }
      if (t.side === "buy") buyYs[nearest] = ys[nearest];
      else sellYs[nearest] = ys[nearest];
    }
  }

  const opts = {
    width: container.clientWidth,
    height: 220,
    padding: [16, 12, 0, 0],
    series: [
      {},
      {
        label: "total value",
        stroke: "#ff7a1a",
        width: 1.5,
        fill: "rgba(255,122,26,0.07)",
        points: series.length <= 2
          ? { show: true, size: 6, stroke: "#ff7a1a", fill: "#ff7a1a" }
          : { show: false },
      },
      {
        label: "buys",
        stroke: "#5fd47c",
        width: 0,
        points: { show: true, size: 8, stroke: "#5fd47c", fill: "#5fd47c" },
      },
      {
        label: "sells",
        stroke: "#ef5454",
        width: 0,
        points: { show: true, size: 8, stroke: "#ef5454", fill: "#ef5454" },
      },
    ],
    axes: [
      {
        stroke: "#4a4a4a",
        grid: { stroke: "#1f1f1f", width: 0.5 },
        ticks: { stroke: "#4a4a4a", width: 0.5 },
      },
      {
        stroke: "#4a4a4a",
        grid: { stroke: "#1f1f1f", width: 0.5 },
        ticks: { stroke: "#4a4a4a", width: 0.5 },
        values: (_, vs) => vs.map((v) => "$" + Math.round(v).toLocaleString()),
      },
    ],
    cursor: { drag: { x: true, y: false } },
    legend: { show: false },
  };
  new uPlot(opts, [xs, ys, buyYs, sellYs], container);
}

// --- render: positions ---
function renderPositions(positions) {
  const container = document.getElementById("positions-list");
  clear(container);
  // Filter out zero-value holds — tokens where price lookup failed or the
  // position was sold but balance dust remains on-chain.
  const active = (positions || []).filter((p) => (p.position_usd || 0) > 0);
  if (!active.length) {
    container.appendChild(el("div", { class: "empty", text: "bag is empty" }));
    return;
  }
  for (const p of active) {
    const sym = el("div", { class: "sym", text: "$" + (p.symbol || shortAddr(p.mint)) });
    const detail = el("div", { class: "detail" }, [
      "pos " + fmtUsd(p.position_usd) + " · entry " + fmtUsd(p.avg_entry_usd) + " · ",
      el("a", {
        href: DEXSCREENER + "/" + p.mint,
        target: "_blank",
        rel: "noopener",
        text: "chart",
      }),
    ]);
    const num = el("div", {
      class: "num " + pnlClass(p.pnl_pct || 0),
      text: fmtPct(p.pnl_pct),
    });
    container.appendChild(el("div", { class: "row" }, [sym, detail, num]));
  }
}

// --- render: activity ---
function renderActivity(acts, positions) {
  const container = document.getElementById("activity-list");
  clear(container);
  // Suppress activity for mints whose position is now zero — these are stale
  // holds where the token died and the buy record is a false positive.
  const deadMints = new Set(
    (positions || []).filter((p) => (p.position_usd || 0) <= 0).map((p) => p.mint)
  );
  const filtered = (acts || []).filter((a) => !a.mint || !deadMints.has(a.mint));
  if (!filtered.length) {
    container.appendChild(el("div", { class: "empty", text: "quiet in the jungle" }));
    return;
  }
  for (const a of filtered) {
    const time = el("time", {
      datetime: new Date((a.ts || 0) * 1000).toISOString(),
      text: fmtTimeAgo(a.ts),
    });
    const body = el("div", { class: "body", text: a.summary || "" });
    const links = el("div");
    if (a.signature) {
      links.appendChild(
        el("a", {
          href: SOLSCAN + "/tx/" + a.signature,
          target: "_blank",
          rel: "noopener",
          text: "tx",
        })
      );
      links.appendChild(document.createTextNode(" "));
    }
    if (a.mint) {
      links.appendChild(
        el("a", {
          href: DEXSCREENER + "/" + a.mint,
          target: "_blank",
          rel: "noopener",
          text: "chart",
        })
      );
    }
    container.appendChild(el("div", { class: "act" }, [time, body, links]));
  }
}

// --- render: thoughts (inline book reader) ---
//
// Notes from the jungle render on the main page as a flip-through book:
// one page visible at a time, lands on the newest. Prev/next buttons +
// keyboard arrows move through the archive. URL hash deep-links into
// any page. Same markdown source, same assets.json manifest, same
// append-only safety — just turned into a reader instead of a stack.

let BOOK_PAGES = [];
let BOOK_CURRENT = 0;
let BOOK_WIRED = false;
let BOOK_SIGNATURE = "";

function wireThoughtControls() {
  if (BOOK_WIRED) return;
  for (const id of ["nav-prev", "nav-prev-2"]) {
    document.getElementById(id).addEventListener("click", bookPrev);
  }
  for (const id of ["nav-next", "nav-next-2"]) {
    document.getElementById(id).addEventListener("click", bookNext);
  }
  window.addEventListener("keydown", (e) => {
    if (e.target && /INPUT|TEXTAREA/.test(e.target.tagName)) return;
    if (e.key === "ArrowLeft") bookPrev();
    if (e.key === "ArrowRight") bookNext();
  });
  window.addEventListener("hashchange", () => {
    const idx = readBookHashIndex();
    if (idx !== BOOK_CURRENT) {
      BOOK_CURRENT = idx;
      renderBookPage(false);
    }
  });
  BOOK_WIRED = true;
}

async function renderThoughts(index) {
  const body = document.getElementById("page-body");
  const normalized = Array.isArray(index) ? index : [];
  const nextSignature = thoughtsSignature(normalized);
  const currentFile = BOOK_PAGES[BOOK_CURRENT] ? BOOK_PAGES[BOOK_CURRENT].file : null;

  wireThoughtControls();

  if (!normalized.length) {
    BOOK_SIGNATURE = nextSignature;
    BOOK_PAGES = [];
    BOOK_CURRENT = 0;
    clear(body);
    body.removeAttribute("data-file");
    body.appendChild(el("div", { class: "empty", text: "ape hasn't scribbled yet" }));
    updateBookCounters();
    return;
  }
  if (nextSignature === BOOK_SIGNATURE && BOOK_PAGES.length) {
    return;
  }

  const assets = (await loadJson("thoughts/assets.json")) || {};
  // Sort by date desc, ties broken by index.json position (preserves
  // operator-controlled order for same-day entries — alphabetical
  // filename was previously dropping later-published entries below
  // earlier-titled ones, e.g. "morning" landing above "day" because
  // 'm' > 'd' in descending lex order).
  const sorted = [...normalized]
    .map((t, i) => ({ ...t, _idx: i }))
    .sort((a, b) => {
      const d = (b.date || "").localeCompare(a.date || "");
      if (d !== 0) return d;
      return a._idx - b._idx;
    });
  const mds = await Promise.all(
    sorted.map((t) => loadText("thoughts/" + t.file))
  );
  BOOK_PAGES = sorted.map((t, i) => ({
    ...t,
    md: mds[i] || "",
    assets: assets[t.file] || [],
  }));
  BOOK_SIGNATURE = nextSignature;

  if (location.hash.includes("#note=")) {
    BOOK_CURRENT = readBookHashIndex();
  } else if (currentFile) {
    const preserved = BOOK_PAGES.findIndex((page) => page.file === currentFile);
    BOOK_CURRENT = preserved >= 0 ? preserved : 0;
  } else {
    BOOK_CURRENT = 0;
  }

  const nextPage = BOOK_PAGES[BOOK_CURRENT];
  if (!nextPage) {
    clear(body);
    body.appendChild(el("div", { class: "empty", text: "ape hasn't scribbled yet" }));
    updateBookCounters();
    return;
  }

  if (body.getAttribute("data-file") !== nextPage.file) {
    renderBookPage(false);
  } else {
    updateBookCounters();
  }
}

function renderBookPage(animate = true) {
  const container = document.getElementById("page-body");
  if (!BOOK_PAGES.length) {
    clear(container);
    container.removeAttribute("data-file");
    container.appendChild(el("div", { class: "empty", text: "ape hasn't scribbled yet" }));
    return;
  }
  BOOK_CURRENT = Math.max(0, Math.min(BOOK_CURRENT, BOOK_PAGES.length - 1));
  const page = BOOK_PAGES[BOOK_CURRENT];
  const paint = () => {
    clear(container);
    container.setAttribute("data-file", page.file || "");
    const article = el("article", { class: "book-page" });
    article.appendChild(el("div", { class: "page-date", text: page.date || "" }));
    article.appendChild(
      el("h3", { class: "page-title", text: page.title || page.file })
    );
    const body = el("div", { class: "thought-body" });
    if (page.md && typeof marked !== "undefined") {
      renderThoughtBody(body, page.md, page.assets || []);
    }
    article.appendChild(body);
    container.appendChild(article);
    container.classList.remove("page-turning");
    updateBookCounters();
    updateBookHash(page);
  };

  if (!animate) {
    paint();
    return;
  }

  container.classList.add("page-turning");
  setTimeout(paint, 140);
}

function updateBookCounters() {
  const total = BOOK_PAGES.length;
  const pos = total ? BOOK_CURRENT + 1 : 0;
  const page = BOOK_PAGES[BOOK_CURRENT];
  const title = page ? page.title || page.file : "";
  document.getElementById("page-counter").textContent = total
    ? `page ${pos} of ${total}${title ? "  ·  " + title : ""}`
    : "—";
  document.getElementById("page-counter-2").textContent = total
    ? `${pos} / ${total}`
    : "—";
  const atNewest = BOOK_CURRENT === 0;
  const atOldest = BOOK_CURRENT === total - 1;
  for (const id of ["nav-prev", "nav-prev-2"]) {
    const b = document.getElementById(id);
    if (b) b.disabled = atOldest || !total;
  }
  for (const id of ["nav-next", "nav-next-2"]) {
    const b = document.getElementById(id);
    if (b) b.disabled = atNewest || !total;
  }
}

function updateBookHash(page) {
  if (!page || !page.file) return;
  const h = "#note=" + encodeURIComponent(page.file);
  if (location.hash !== h) history.replaceState(null, "", h);
}

function readBookHashIndex() {
  const m = location.hash.match(/#note=([^&]+)/);
  if (!m) return 0;
  const file = decodeURIComponent(m[1]);
  const idx = BOOK_PAGES.findIndex((p) => p.file === file);
  return idx >= 0 ? idx : 0;
}

function bookPrev() {
  if (BOOK_CURRENT < BOOK_PAGES.length - 1) {
    BOOK_CURRENT++;
    renderBookPage();
  }
}
function bookNext() {
  if (BOOK_CURRENT > 0) {
    BOOK_CURRENT--;
    renderBookPage();
  }
}

// Parse markdown → sanitized DOM → walk the placeholder divs in document
// order and, if the manifest has a generated image for that index, replace
// the placeholder with a proper <figure>. Leaves untouched placeholders
// intact so we gracefully degrade before assets land.
function renderThoughtBody(bodyEl, md, manifestEntries) {
  const raw = marked.parse(md);
  const clean = typeof DOMPurify !== "undefined" ? DOMPurify.sanitize(raw) : "";
  const tpl = document.createElement("template");
  tpl["inner" + "HTML"] = clean;
  const frag = tpl.content.cloneNode(true);

  const placeholders = frag.querySelectorAll("div.img-placeholder");
  const byIdx = new Map();
  for (const entry of manifestEntries) byIdx.set(entry.idx, entry);
  placeholders.forEach((ph, i) => {
    const entry = byIdx.get(i);
    if (!entry) return;
    const fig = document.createElement("figure");
    fig.className = "thought-figure";
    const img = document.createElement("img");
    img.src = entry.asset;
    img.alt = entry.caption || "";
    img.loading = "lazy";
    img.decoding = "async";
    fig.appendChild(img);
    if (entry.caption) {
      const cap = document.createElement("figcaption");
      cap.textContent = entry.caption;
      fig.appendChild(cap);
    }
    ph.replaceWith(fig);
  });
  bodyEl.appendChild(frag);
}

// --- bootstrap ---
let BOOTSTRAPPED = false;

// Fast path — the parts of the site that change between 5-min publisher
// ticks. Called on initial load AND every 30s so the page feels live.
// Thoughts + chart-tabs wiring only happen once at bootstrap.
async function refreshLiveData() {
  const [health, pnl, positions, activity, calls, stream, thoughtsIndex, featured] = await Promise.all([
    loadJson("/api/data/health"),
    loadJson("/api/data/pnl"),
    loadJson("/api/data/positions"),
    loadJson("/api/data/activity"),
    loadJson("/api/data/calls"),
    loadJson("/api/data/stream"),
    loadJson("thoughts/index.json"),
    loadJson("/api/data/featured"),
  ]);
  const posArr = (positions && positions.positions) || [];
  renderFeatured(featured);
  renderHealth(health);
  renderTicker(health, pnl, calls && calls.active, posArr);
  renderPnl(pnl);
  renderPositions(posArr);
  renderCalls(calls && calls.active, calls && calls.history);
  // History fed by the same calls.json. Server now ships up to 200 closed
  // rows; client paginates 20 at a time. Stats banner pulled from the new
  // top-level stats block in the JSON.
  HISTORY_STATE.data = (calls && calls.history) || [];
  HISTORY_STATE.stats = (calls && calls.stats) || null;
  renderHistoryStats(HISTORY_STATE.stats);
  renderRunners(HISTORY_STATE.data);
  renderHistory();
  renderActivity(activity && activity.activity, posArr);
  renderStream(stream);
  await renderThoughts(thoughtsIndex && thoughtsIndex.thoughts);
}

async function main() {
  wireChartTabs();
  bindHistoryControls();
  bindCallDetailRouting();
  await refreshLiveData();
  // Honor `#call=<mint>` on initial load (deep links from TG cards,
  // bookmarks, shared URLs).
  routeCallHash();

  // Poll every 30s for fresh JSONs. The publisher ticks every 5 min, so
  // most polls are no-ops — but when a new pulse lands, the ticker,
  // stream, and ticker numbers update without a full reload.
  if (!BOOTSTRAPPED) {
    BOOTSTRAPPED = true;
    setInterval(() => {
      refreshLiveData().catch((e) =>
        console.warn("refresh failed:", e)
      );
    }, 30_000);
    // Live "updated Xs ago" ticker — re-rendered every second on the
    // header so visitors see the freshness number tick.
    setInterval(refreshLastUpdate, 1_000);
  }
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", main);
} else {
  main();
}
