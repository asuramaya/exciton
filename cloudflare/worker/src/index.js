// Exciton edge Worker.
//
// Serves the public read API for an exciton-driven account from KV,
// and accepts HMAC-signed publishes from the engine's publisher.
// All compute lives on the engine VM; this Worker only stores and
// hands back snapshots.
//
// KV keys (single namespace, binding STATE):
//   diary    — array of evolution events, newest first
//   calls    — { active: [...], history: [...] } current call book
//   strategy — current effective tunables (filter floors/ceilings)
//
// Publishers POST one consolidated body per kick:
//   { diary, calls, strategy, captured_at }
// Worker validates HMAC + timestamp skew, then writes each present key.

const HMAC_HEADER_TS = "x-exciton-timestamp";
const HMAC_HEADER_SIG = "x-exciton-signature";
const PUBLISHED_KEYS = ["diary", "calls", "strategy"];

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

    if (request.method === "GET" && PUBLISHED_KEYS.some((k) => path === `/api/${k}`)) {
      const key = path.slice("/api/".length);
      return readPublic(request, env, ctx, key);
    }

    if (request.method === "POST" && path === "/api/admin/publish") {
      return writeAdmin(request, env);
    }

    return new Response("not found", { status: 404, headers: CORS_HEADERS });
  },
};

async function readPublic(request, env, ctx, key) {
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
  const resp = new Response(body, {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `public, max-age=${ttl}, s-maxage=${ttl}`,
      ...CORS_HEADERS,
    },
  });
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

  const written = [];
  for (const key of PUBLISHED_KEYS) {
    if (payload[key] !== undefined) {
      const wrapped = JSON.stringify({
        value: payload[key],
        captured_at: payload.captured_at ?? Math.floor(Date.now() / 1000),
      });
      await env.STATE.put(key, wrapped);
      written.push(key);
    }
  }

  // Bust the edge cache for written keys so the next public read sees fresh data.
  const cache = caches.default;
  const origin = new URL(request.url).origin;
  await Promise.all(
    written.map((k) => cache.delete(new Request(`${origin}/api/${k}`, { method: "GET" })))
  );

  return jsonResponse({ ok: true, written, captured_at: payload.captured_at ?? null });
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

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
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
