//! Transport to the running exciton MCP server (Streamable HTTP).
//!
//! Implements the minimum of MCP's Streamable HTTP spec needed to drive
//! exciton from claw:
//!   1. POST `initialize` → server returns `Mcp-Session-Id` in headers.
//!   2. POST `notifications/initialized` (notification, no response).
//!   3. POST `tools/call` requests with `Mcp-Session-Id` set, accepting
//!      either `application/json` (single response) or `text/event-stream`
//!      (one or more SSE events; we read the first JSON-RPC response and
//!      drop the rest).
//!
//! Server URL is `EXCITON_MCP_URL` env var, defaulting to
//! `http://127.0.0.1:8082/mcp`. `EXCITON_MCP_TOKEN`, when set, is sent
//! as `Authorization: Bearer …`.

use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

const PROTOCOL_VERSION: &str = "2025-06-18";

pub struct McpClient {
    url: String,
    http: reqwest::Client,
    bearer: Option<String>,
    session_id: Mutex<Option<String>>,
    next_id: AtomicU64,
}

#[derive(Debug, Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcNotification<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    params: Value,
}

impl McpClient {
    pub fn from_env() -> Result<Self> {
        let url = std::env::var("EXCITON_MCP_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8082/mcp".to_string());
        let bearer = std::env::var("EXCITON_MCP_TOKEN").ok().filter(|s| !s.is_empty());
        Ok(Self {
            url,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .context("build reqwest client")?,
            bearer,
            session_id: Mutex::new(None),
            next_id: AtomicU64::new(1),
        })
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn ensure_initialized(&self) -> Result<String> {
        {
            let g = self.session_id.lock().await;
            if let Some(id) = g.as_ref() {
                return Ok(id.clone());
            }
        }
        let req = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.next_id(),
            method: "initialize",
            params: json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "claw", "version": env!("CARGO_PKG_VERSION") },
            }),
        };
        let mut builder = self
            .http
            .post(&self.url)
            .header("Accept", "application/json, text/event-stream")
            .header("Content-Type", "application/json")
            .header("MCP-Protocol-Version", PROTOCOL_VERSION);
        if let Some(t) = &self.bearer {
            builder = builder.bearer_auth(t);
        }
        let resp = builder
            .json(&req)
            .send()
            .await
            .with_context(|| format!("POST initialize {}", self.url))?;
        let status = resp.status();
        let session_id = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let body_text = resp.text().await.context("read initialize body")?;
        if !status.is_success() {
            return Err(anyhow!("mcp initialize http {status}: {body_text}"));
        }
        let init_resp = parse_first_jsonrpc(&body_text)
            .with_context(|| format!("parse initialize body: {body_text}"))?;
        if let Some(err) = init_resp.get("error") {
            return Err(anyhow!("mcp initialize error: {err}"));
        }
        let session_id = session_id
            .ok_or_else(|| anyhow!("server did not return Mcp-Session-Id header"))?;

        let notif = JsonRpcNotification {
            jsonrpc: "2.0",
            method: "notifications/initialized",
            params: json!({}),
        };
        let mut nb = self
            .http
            .post(&self.url)
            .header("Accept", "application/json, text/event-stream")
            .header("Content-Type", "application/json")
            .header("MCP-Protocol-Version", PROTOCOL_VERSION)
            .header("Mcp-Session-Id", &session_id);
        if let Some(t) = &self.bearer {
            nb = nb.bearer_auth(t);
        }
        let nresp = nb.json(&notif).send().await.context("POST initialized")?;
        if !nresp.status().is_success() && nresp.status().as_u16() != 202 {
            let s = nresp.status();
            let b = nresp.text().await.unwrap_or_default();
            return Err(anyhow!("notifications/initialized http {s}: {b}"));
        }

        *self.session_id.lock().await = Some(session_id.clone());
        Ok(session_id)
    }

    /// Call an MCP tool by name. `args` is the params object; the
    /// server returns the tool's stringified result.
    pub async fn call_tool(&self, name: &str, args: Value) -> Result<String> {
        let session_id = self.ensure_initialized().await?;
        let req = JsonRpcRequest {
            jsonrpc: "2.0",
            id: self.next_id(),
            method: "tools/call",
            params: json!({ "name": name, "arguments": args }),
        };
        let mut builder = self
            .http
            .post(&self.url)
            .header("Accept", "application/json, text/event-stream")
            .header("Content-Type", "application/json")
            .header("MCP-Protocol-Version", PROTOCOL_VERSION)
            .header("Mcp-Session-Id", &session_id);
        if let Some(t) = &self.bearer {
            builder = builder.bearer_auth(t);
        }
        let resp = builder
            .json(&req)
            .send()
            .await
            .with_context(|| format!("POST {}", self.url))?;
        let status = resp.status();
        let body_text = resp.text().await.with_context(|| format!("read body from {}", self.url))?;
        if !status.is_success() {
            return Err(anyhow!("mcp http {status} from {name}: {body_text}"));
        }
        let body = parse_first_jsonrpc(&body_text)
            .with_context(|| format!("parse JSON-RPC from {}: {body_text}", self.url))?;
        if let Some(err) = body.get("error") {
            return Err(anyhow!("mcp error from {name}: {err}"));
        }
        if let Some(text) = body
            .pointer("/result/content/0/text")
            .and_then(|v| v.as_str())
        {
            return Ok(text.to_string());
        }
        Ok(body
            .get("result")
            .map(|v| v.to_string())
            .unwrap_or_else(|| body.to_string()))
    }
}

/// Parse the first JSON-RPC response object out of a body that may be
/// either plain JSON or SSE (text/event-stream). For SSE we look for
/// the first `data: { ... }` line and parse that.
fn parse_first_jsonrpc(body: &str) -> Result<Value> {
    let trimmed = body.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return serde_json::from_str(trimmed).context("parse JSON body");
    }
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            let payload = rest.trim();
            if payload.is_empty() || payload == "[DONE]" {
                continue;
            }
            return serde_json::from_str(payload).context("parse SSE data line");
        }
    }
    Err(anyhow!("no JSON or SSE data in body"))
}
