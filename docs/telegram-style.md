# Telegram Message Style Guide — Photon

**Core principle: every message must answer WHAT happened and WHY it was sent, at a glance. A reader looking at any card for two seconds should know:**

1. **What** kind of event this is (signal / failure / trap wrap / digest / ops)
2. **Why** it was triggered (which criterion, which metric, which transition)
3. **About what** (token, system event)

If any of these three aren't obvious in the first 1–2 lines, the template has failed.

---

## Why-first headers

Every message starts with a two-line header:

```
<emoji> <BADGE> · <subject>
<i>Triggered by: <specific reason with numbers></i>
```

Examples:

| Event | Header |
|---|---|
| New signal | `📊 SIGNAL · $TARDI` / `Triggered by: STAIRCASE · conf 82 · top 3.8% · rising` |
| Material update | (edited in-card, timeline entry) — explains criterion crossed |
| Failed signal | `❌ FAILED · $TARDI` / `Triggered by: STAIRCASE → ACTIVE_TRAP · top jumped to 42%` |
| Hour digest | `📟 HOUR DIGEST · 05:00–06:00 UTC` / `Why: routine snapshot — queue state + traps` |
| Trap line | Each line encodes its own trigger: class flip, concentration jump, velocity crash, etc. |
| Ops announcement | `🟢 GOING LIVE` / `Why: operator flipped notifier.enabled` |

**Never** post a bare message without a "why" line. If the reason isn't obvious, add it.

---

## Telegram HTML capabilities we use

| Tag | Use for | Example |
|---|---|---|
| `<b>` | Section labels, critical values | `<b>conf</b> 82` |
| `<i>` | "Why" / trigger reason line | `<i>Triggered by: STAIRCASE</i>` |
| `<code>` | Addresses, numeric tokens, raw data | `<code>4JBeo37...</code>` |
| `<pre>` | Multi-line aligned blocks (sparingly) | — |
| `<blockquote expandable>` | Timeline history — collapsed by default, tap to expand | wrap history entries |
| `<s>` | Deprecated/demoted state (inside timeline) | `<s>STAIRCASE 78</s>` |
| `<a href>` | Text links (prefer inline keyboard) | — |

**Don't use:** `<u>` (underline conflicts with link styling on mobile), `<tg-emoji>` (premium-only, not portable), `<spoiler>` (inappropriate for trading signals).

---

## Inline keyboards

Every signal card and every trap line where tappable action helps gets an **inline keyboard** — NOT plaintext `<a>` links. Buttons are bigger, tappable, persistent, and don't clutter the text flow.

Three button types we use, all stateless (channel-safe):

| Button | Purpose | JSON |
|---|---|---|
| URL | Open external tool | `{"text": "📊 Chart", "url": "https://dexscreener.com/..."}` |
| Copy | Copy mint address to clipboard | `{"text": "📋 Address", "copy_text": {"text": "<mint>"}}` |

**Never use** `callback_data` — requires per-user state tracking; channels broadcast so stateless only.

**Standard signal card keyboard (single row, 3 buttons):**
```
[📊 Chart] [🔍 Solscan] [📋 Addr]
```

For rich cards (winners with detail depth), add a second row:
```
[📊 Chart] [🔍 Solscan]
[📋 Addr]  [⚡ Photon]
```

---

## Collapsible history with `<blockquote expandable>`

Long timelines (signal cards) and long lists (trap wrap-ups, top alerts) go inside `<blockquote expandable>…</blockquote>`. The content renders collapsed by default — one tap reveals the full history.

**Rule:** the card's top ~6 lines must be enough to answer WHAT + WHY + ABOUT. History is additional context, not primary.

```html
📊 <b>SIGNAL</b> · $TARDI
<i>Triggered by: STAIRCASE · conf 82 · top 3.8% · rising</i>

<b>px</b> $0.002  ·  <b>24h</b> +18%
<b>mc</b> $2.1M  ·  <b>liq</b> $127k
<b>top</b> 3.8% / 5.6% / 6.5%  ·  <b>holders</b> 20

✓ Token-2022 benign

<blockquote expandable>— history ——
21:04 called   STAIRCASE 78 · top 3.8% · mc $2.1M
21:17 update   STAIRCASE 82 · top 4.1% · px +12%</blockquote>
```

---

## Visual hierarchy

1. **Line 1** — emoji + badge + subject. Biggest semantic weight.
2. **Line 2** — `<i>` "why it fired" — the single most important sentence.
3. **Lines 3–6** — structured metrics, `<b>`-labeled key/value pairs.
4. **Optional safety / state banner** — single line, ✓ or ⚠️ prefix.
5. **Expandable history** — tap to see timeline.
6. **Inline keyboard** — actions only.

Keep body text tight; never indulge in filler. Every word must earn its place.

---

## Emoji taxonomy (consistent, never ambiguous)

| Emoji | Meaning |
|---|---|
| 📊 | Active SIGNAL (call is open) |
| ❌ | FAILED signal (verdict collapsed) |
| 💥 | Trap / collapse / rug |
| ⚠️ | Warning / risk flag |
| ⛔ | Hard veto (UNSAFE) |
| ✓ | Clean safety check |
| ▲ / ▼ / ─ | Direction indicators |
| 📟 | Ops digest / hour wrap |
| 🟢 / 🔴 | System on / off |
| 🪤 | Active trap classification |
| 💀 / ☠️ | DEAD classification |
| 🌱 | DEVELOPING classification |
| 📈 | STAIRCASE classification |
| ⛏️ | GRINDER classification |
| 🌀 | SPRING classification |
| 🚀 | SURGE classification |

Don't introduce new emoji without adding them here. Consistency compounds over time.

---

## Link preview control

Every `sendMessage` / `editMessageText` call should explicitly set `link_preview_options`. Default: suppress. Opt-in only for cards where a specific URL's preview becomes a "hero image" (e.g., dexscreener chart).

```json
"link_preview_options": {"is_disabled": true}
```

---

## What NOT to do

- Don't post a message whose purpose is unclear in the first 2 seconds.
- Don't use plaintext URLs when an inline keyboard button will serve.
- Don't bury the "why it fired" in paragraph 3 — it's line 2.
- Don't use emoji for decoration; each emoji should mean something specific.
- Don't stack multiple `<code>` blocks in a row — use a single `<pre>` if truly tabular.
- Don't mix parse_modes within a chat — stick to HTML everywhere.
- Don't duplicate history with new messages; edit the existing card and grow its timeline.
