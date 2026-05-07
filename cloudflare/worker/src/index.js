// Exciton edge Worker.
//
// Two read APIs over KV:
//
//   /api/{diary,calls,strategy}    — simplified wrapped feed
//                                    response: { key, value, captured_at }
//
//   /api/data/<file>               — raw JSON snapshots that drive the live
//                                    site. Names: health, pnl, positions,
//                                    activity, calls, stream, featured.
//                                    Response: the JSON value itself.
//
//   /api/data/{calls,scouts,whales}/<mint>
//                                  — per-mint detail blobs; sliced from
//                                    the corresponding KV map on read.
//                                    404 if the mint isn't present.
//
// One write API:
//
//   POST /api/admin/publish        — HMAC-SHA256 signed publish from the
//                                    engine. Body shape:
//   {
//     captured_at,
//     diary, calls, strategy,                 // optional simplified feeds
//     data: {
//       health, pnl, positions, activity,
//       calls, stream, featured,              // top-level snapshots
//       calls_details: { "<mint>": {...} },
//       scouts:        { "<mint>": {...} },
//       whales:        { "<mint>": {...} },
//     }
//   }

const HMAC_HEADER_TS = "x-exciton-timestamp";
const HMAC_HEADER_SIG = "x-exciton-signature";

// Simplified wrapped keys (backwards-compat with the original API).
const PUBLISHED_KEYS = ["diary", "calls", "strategy"];

// Raw top-level data keys served at /api/data/<name>; KV key is `data:<name>`.
const DATA_KEYS = ["health", "pnl", "positions", "activity", "calls", "stream", "featured", "thoughts_index", "thoughts_assets"];

// Per-mint detail maps. Each KV value is `{ "<mint>": {...} }`.
// URL: /api/data/{key}/<mint>; KV key: `data:<storeKey>`.
const DETAIL_MAPS = {
  calls: "calls_details",
  scouts: "scouts",
  whales: "whales",
};

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (path === "/health") {
      return jsonResponse({ ok: true, ts: Math.floor(Date.now() / 1000) });
    }

    if (request.method === "GET") {
      // Simplified wrapped feeds: /api/{diary,calls,strategy}
      if (PUBLISHED_KEYS.some((k) => path === `/api/${k}`)) {
        return readWrapped(request, env, ctx, path.slice("/api/".length));
      }

      // Per-mint detail: /api/data/{calls,scouts,whales}/<mint>
      const mintMatch = path.match(/^\/api\/data\/(calls|scouts|whales)\/([^/]+)$/);
      if (mintMatch) {
        const [, group, mint] = mintMatch;
        return readDetail(request, env, ctx, group, mint);
      }

      // Raw top-level data: /api/data/<name>
      const dataMatch = path.match(/^\/api\/data\/([a-z_]+)$/);
      if (dataMatch && DATA_KEYS.includes(dataMatch[1])) {
        return readData(request, env, ctx, dataMatch[1]);
      }
    }

    if (request.method === "POST" && path === "/api/admin/publish") {
      return writeAdmin(request, env);
    }

    return new Response("not found", { status: 404, headers: CORS_HEADERS });
  },
};

async function readWrapped(request, env, ctx, key) {
  const cache = caches.default;
  const cacheKey = new Request(new URL(request.url).toString(), { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) return cached;

  const raw = await env.STATE.get(key);
  if (raw === null) {
    return jsonResponse({ key, value: null, captured_at: null }, 200);
  }
  const ttl = ttlForKey(key, env);
  const body = JSON.stringify({ key, value: safeParse(raw), captured_at: extractCapturedAt(raw) });
  const resp = jsonCached(body, ttl);
  ctx.waitUntil(cache.put(cacheKey, resp.clone()));
  return resp;
}

async function readData(request, env, ctx, name) {
  const cache = caches.default;
  const cacheKey = new Request(new URL(request.url).toString(), { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) return cached;

  const raw = await env.STATE.get(`data:${name}`);
  if (raw === null) {
    return jsonResponse({ error: "not yet published", name }, 404);
  }
  const ttl = ttlForData(env);
  const resp = jsonCached(raw, ttl);
  ctx.waitUntil(cache.put(cacheKey, resp.clone()));
  return resp;
}

async function readDetail(request, env, ctx, group, mint) {
  const cache = caches.default;
  const cacheKey = new Request(new URL(request.url).toString(), { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) return cached;

  const storeKey = DETAIL_MAPS[group];
  const raw = await env.STATE.get(`data:${storeKey}`);
  if (raw === null) {
    return jsonResponse({ error: "not yet published", group, mint }, 404);
  }
  let map;
  try {
    map = JSON.parse(raw);
  } catch {
    return jsonResponse({ error: "corrupt detail map" }, 500);
  }
  const value = map && typeof map === "object" ? map[mint] : null;
  if (value === undefined || value === null) {
    return jsonResponse({ error: "mint not found", group, mint }, 404);
  }
  const ttl = ttlForData(env);
  const resp = jsonCached(JSON.stringify(value), ttl);
  ctx.waitUntil(cache.put(cacheKey, resp.clone()));
  return resp;
}

async function writeAdmin(request, env) {
  const secret = env.PUBLISH_SECRET;
  if (!secret) {
    return jsonResponse({ error: "publisher misconfigured" }, 500);
  }

  const ts = request.headers.get(HMAC_HEADER_TS);
  const sig = request.headers.get(HMAC_HEADER_SIG);
  if (!ts || !sig) {
    return jsonResponse({ error: "missing auth headers" }, 401);
  }

  const skew = Math.abs(Math.floor(Date.now() / 1000) - parseInt(ts, 10));
  const maxSkew = parseInt(env.HMAC_MAX_SKEW_SECONDS || "300", 10);
  if (!Number.isFinite(skew) || skew > maxSkew) {
    return jsonResponse({ error: "timestamp skew" }, 401);
  }

  const body = await request.text();
  const expected = await hmacHex(secret, `${ts}.${body}`);
  if (!timingSafeEq(expected, sig)) {
    return jsonResponse({ error: "bad signature" }, 401);
  }

  let payload;
  try {
    payload = JSON.parse(body);
  } catch {
    return jsonResponse({ error: "invalid json" }, 400);
  }

  const captured_at = payload.captured_at ?? Math.floor(Date.now() / 1000);
  const written = [];
  const bustPaths = [];

  // Simplified wrapped feeds.
  for (const key of PUBLISHED_KEYS) {
    if (payload[key] !== undefined) {
      await env.STATE.put(key, JSON.stringify({ value: payload[key], captured_at }));
      written.push(key);
      bustPaths.push(`/api/${key}`);
    }
  }

  // Rich /api/data/* feed. Stored as raw JSON; reads return it verbatim.
  const data = payload.data ?? {};
  for (const name of DATA_KEYS) {
    if (data[name] !== undefined) {
      await env.STATE.put(`data:${name}`, JSON.stringify(data[name]));
      written.push(`data:${name}`);
      bustPaths.push(`/api/data/${name}`);
    }
  }

  // Per-mint detail maps. Stored as the whole map; the read path slices.
  // Cache busting for sliced reads is not exhaustive (would need per-mint
  // enumeration), but the 30-300s TTL keeps staleness bounded.
  for (const [group, storeKey] of Object.entries(DETAIL_MAPS)) {
    const map = data[storeKey];
    if (map !== undefined) {
      await env.STATE.put(`data:${storeKey}`, JSON.stringify(map));
      written.push(`data:${storeKey}`);
      // Best-effort bust: drop nothing here. Per-mint entries already have
      // short TTL so freshness is bounded; the alternative is iterating all
      // mints each publish, which scales badly.
    }
  }

  const cache = caches.default;
  const origin = new URL(request.url).origin;
  await Promise.all(
    bustPaths.map((p) => cache.delete(new Request(`${origin}${p}`, { method: "GET" })))
  );

  return jsonResponse({ ok: true, written, captured_at });
}

function ttlForKey(key, env) {
  const map = {
    diary: env.PUBLIC_CACHE_SECONDS_DIARY,
    calls: env.PUBLIC_CACHE_SECONDS_CALLS,
    strategy: env.PUBLIC_CACHE_SECONDS_STRATEGY,
  };
  const raw = parseInt(map[key] || "30", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : 30;
}

function ttlForData(env) {
  const raw = parseInt(env.PUBLIC_CACHE_SECONDS_DATA || "30", 10);
  return Number.isFinite(raw) && raw > 0 ? raw : 30;
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

function jsonCached(body, ttl) {
  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `public, max-age=${ttl}, s-maxage=${ttl}`,
      ...CORS_HEADERS,
    },
  });
}

function safeParse(s) {
  try {
    const parsed = JSON.parse(s);
    return parsed && typeof parsed === "object" && "value" in parsed ? parsed.value : parsed;
  } catch {
    return s;
  }
}

function extractCapturedAt(s) {
  try {
    const parsed = JSON.parse(s);
    return parsed && typeof parsed === "object" ? parsed.captured_at ?? null : null;
  } catch {
    return null;
  }
}

async function hmacHex(secret, message) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(message));
  return Array.from(new Uint8Array(sig))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function timingSafeEq(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}
