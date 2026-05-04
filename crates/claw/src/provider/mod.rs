//! LLM provider abstraction.
//!
//! Two implementations:
//! - `OpenAiCodexProvider` — talks to chatgpt.com/backend-api/codex/responses
//!   using a Bearer access token + `chatgpt-account-id` header. Billed
//!   against the user's ChatGPT Plus/Pro subscription.
//! - `OpenAiApiProvider` — talks to api.openai.com/v1/responses with a
//!   raw API key. Pay-per-token fallback when the codex path 429s with
//!   `usage_limit_reached`.
//!
//! Both implementations target the OpenAI Responses API shape so the
//! caller (runtime/review) can swap providers without touching message
//! construction. The trait stays narrow: `complete` is the only verb
//! claw needs in v0.

pub mod openai_api;
pub mod openai_codex;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// One turn of conversation. `tool_calls` is populated by the model
/// when it wants to invoke MCP tools; the runtime executes them and
/// feeds the results back as a follow-up message with role="tool".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// JSON-encoded arguments object as the model emitted them.
    pub arguments: String,
}

/// Tool definition the runtime advertises to the model. Schema is the
/// JSON-schema for the tool's params object; the model is expected to
/// emit `arguments` matching it.
#[derive(Debug, Clone, Serialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// What the provider returns from one model turn. When `tool_calls` is
/// non-empty the runtime should execute those tools and follow up with
/// another `complete` call carrying the tool results; otherwise
/// `content` is the model's final answer.
#[derive(Debug, Clone)]
pub struct Completion {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    pub finish_reason: String,
}

/// Errors the provider may report. The runtime distinguishes
/// `UsageLimitReached` from other failures so it can transparently
/// fall back to the API-key provider when the subscription path is
/// rate-limited.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("provider {provider} hit subscription quota: {detail}")]
    UsageLimitReached { provider: String, detail: String },
    #[error("provider {provider} unauthorized: {detail}")]
    Unauthorized { provider: String, detail: String },
    #[error("provider {0} request failed: {1}")]
    Other(String, #[source] anyhow::Error),
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;

    /// One round of completion. `messages` is the running transcript
    /// (system + user + assistant + tool results); `tools` is the MCP
    /// surface advertised to the model. The runtime does not retry —
    /// `Provider` impls handle their own internal retry/backoff.
    async fn complete(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<Completion, ProviderError>;
}
