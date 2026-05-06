// Watcher view for an exciton-driven account.
//
// Pulls three KV-backed snapshots from the same-origin Worker API and
// renders them. No build step; vanilla DOM mutation. The data shapes
// mirror what src/publisher.rs ships in the consolidated POST body —
// the contract is `{ value, captured_at }` per key.

const API = ""; // same origin — Pages and Worker share a domain

document.addEventListener("DOMContentLoaded", () => {
  loadStrategy();
  loadDiary();
  loadCalls("active");

  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
      btn.classList.add("active");
      loadCalls(btn.dataset.tab);
    });
  });
});

async function fetchKey(key) {
  const r = await fetch(`${API}/api/${key}`, { headers: { Accept: "application/json" } });
  if (!r.ok) throw new Error(`/api/${key} -> ${r.status}`);
  return r.json();
}

function setMount(id, contentEl, opts = {}) {
  const root = document.querySelector(`#${id} .content`);
  if (!root) return;
  root.removeAttribute("data-loading");
  if (opts.empty) root.setAttribute("data-empty", "true");
  else root.removeAttribute("data-empty");
  root.innerHTML = "";
  if (typeof contentEl === "string") root.textContent = contentEl;
  else if (contentEl) root.append(contentEl);
}

async function loadStrategy() {
  try {
    const { value } = await fetchKey("strategy");
    if (!value || (typeof value === "object" && Object.keys(value).length === 0)) {
      setMount("strategy", "no overrides currently in effect — engine running on defaults.", { empty: true });
      return;
    }
    const grid = document.createElement("div");
    grid.className = "strategy-grid";
    for (const [key, val] of Object.entries(value)) {
      const row = document.createElement("div");
      row.className = "strategy-row";
      row.innerHTML = `<span class="key">${escapeHtml(key)}</span><span class="val">${formatVal(val)}</span>`;
      grid.append(row);
    }
    setMount("strategy", grid);
  } catch (e) {
    setMount("strategy", `failed to load: ${e.message}`, { empty: true });
  }
}

async function loadDiary() {
  try {
    const { value } = await fetchKey("diary");
    const entries = Array.isArray(value) ? value : [];
    if (entries.length === 0) {
      setMount("diary", "no diary entries yet.", { empty: true });
      return;
    }
    const frag = document.createDocumentFragment();
    for (const entry of entries.slice(0, 20)) {
      const card = document.createElement("article");
      card.className = "diary-entry";
      const ts = entry.created_at ? new Date(entry.created_at * 1000).toUTCString() : "";
      card.innerHTML = `
        <h3>${escapeHtml(entry.title || entry.summary || "(untitled)")}</h3>
        <div class="meta">${escapeHtml(entry.kind || "note")} · ${escapeHtml(ts)}</div>
        <div class="body">${escapeHtml(entry.body_md || entry.summary || "")}</div>
      `;
      frag.append(card);
    }
    setMount("diary", frag);
  } catch (e) {
    setMount("diary", `failed to load: ${e.message}`, { empty: true });
  }
}

async function loadCalls(which) {
  try {
    const { value } = await fetchKey("calls");
    const rows = Array.isArray(value?.[which]) ? value[which] : [];
    if (rows.length === 0) {
      setMount("calls", `no ${which} calls.`, { empty: true });
      return;
    }
    const table = document.createElement("table");
    table.className = "calls-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>symbol</th>
          <th>called</th>
          <th>entry mcap</th>
          <th>peak %</th>
          <th>${which === "history" ? "exit %" : "current %"}</th>
          <th>${which === "history" ? "verdict" : "status"}</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tb = table.querySelector("tbody");
    for (const c of rows.slice(0, 50)) {
      const peak = pctClass(c.peak_pct);
      const exit = pctClass(which === "history" ? c.exit_pct : c.current_pct);
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHtml(c.symbol || c.mint?.slice(0, 6) || "?")}</td>
        <td>${escapeHtml(formatTs(c.called_at))}</td>
        <td>${formatMcap(c.entry_mcap_usd)}</td>
        <td class="${peak.cls}">${peak.text}</td>
        <td class="${exit.cls}">${exit.text}</td>
        <td>${escapeHtml(c.verdict || c.status || "")}</td>
      `;
      tb.append(tr);
    }
    setMount("calls", table);
  } catch (e) {
    setMount("calls", `failed to load: ${e.message}`, { empty: true });
  }
}

function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatVal(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") return v.toLocaleString();
  return escapeHtml(JSON.stringify(v));
}

function formatTs(ts) {
  if (!ts) return "—";
  const d = new Date(ts * 1000);
  return d.toISOString().slice(0, 16).replace("T", " ");
}

function formatMcap(n) {
  if (!n) return "—";
  if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}m`;
  if (n >= 1e3) return `$${(n / 1e3).toFixed(1)}k`;
  return `$${n.toFixed(0)}`;
}

function pctClass(p) {
  if (p === null || p === undefined || isNaN(p)) return { cls: "", text: "—" };
  const cls = p > 0 ? "good" : p < 0 ? "bad" : "";
  const sign = p > 0 ? "+" : "";
  return { cls, text: `${sign}${p.toFixed(1)}%` };
}
