//! Transport to the running exciton MCP server.
//!
//! v0: thin wrapper over reqwest's POST to the `/mcp` endpoint with a
//! manually-constructed JSON-RPC envelope. Lets claw `analyze_outcomes`,
//! `propose_tune`, `commit_tune`, etc. without pulling rmcp's full
//! client surface in B1. Phase B3 may swap to rmcp's client if we need
//! streaming.
//!
//! Server URL is `EXCITON_MCP_URL` env var, defaulting to
//! `http://127.0.0.1:8080/mcp`.

use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use serde_json::Value;

pub struct McpClient {
    url: String,
    http: reqwest::Client,
}

#[derive(Debug, Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: Value,
}

impl McpClient {
    pub fn from_env() -> Result<Self> {
        let url = std::env::var("EXCITON_MCP_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080/mcp".to_string());
        Ok(Self {
            url,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .context("build reqwest client")?,
        })
    }

    /// Call an MCP tool by name. `args` is the params object; the
    /// server returns the tool's stringified result.
    pub async fn call_tool(&self, name: &str, args: Value) -> Result<String> {
        let req = JsonRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "tools/call",
            params: serde_json::json!({
                "name": name,
                "arguments": args,
            }),
        };
        let resp = self
            .http
            .post(&self.url)
            .json(&req)
            .send()
            .await
            .with_context(|| format!("POST {}", self.url))?;
        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .with_context(|| format!("parse JSON-RPC body from {}", self.url))?;
        if let Some(err) = body.get("error") {
            return Err(anyhow!("mcp error from {name}: {err}"));
        }
        if !status.is_success() {
            return Err(anyhow!("mcp http {status} from {name}"));
        }
        // The exciton server wraps tool output in
        // result.content[0].text for stringified payloads. Walk that
        // if present, else fall back to the result body verbatim.
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
