//! OpenAI Codex provider — subscription-billed via chatgpt.com.
//!
//! Talks to `https://chatgpt.com/backend-api/codex/responses` using the
//! Bearer access token + `chatgpt-account-id` header pattern that
//! OpenAI's official codex-cli (and zeroclaw) use.
//!
//! Two important caveats for v0:
//!
//! 1. Tool calling on the subscription endpoint is **unverified**. The
//!    request body includes a `tools` array, but if the endpoint
//!    doesn't honor it, claw will get back text-only responses and the
//!    cascade in `selection.rs` won't find a tool call to dispatch.
//!    Practical fallback: when this provider returns a completion with
//!    `tool_calls.is_empty()` AND the runtime advertised tools, the
//!    runtime can either accept the text-only answer or re-issue the
//!    request through the api-key provider. v0 takes the simple route
//!    — return whatever codex emits and let the runtime decide.
//!
//! 2. SSE parsing is minimalist. We collect `response.output_text.delta`
//!    chunks into a buffer, look for `response.output_item.done` events
//!    with `function_call` items to extract tool calls, and ignore
//!    everything else (reasoning summaries, encrypted content, etc.).

use crate::auth::AuthProfile;
use crate::provider::{Completion, Message, Provider, ProviderError, ToolCall, ToolSpec};
use anyhow::anyhow;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

const CODEX_URL: &str = "https://chatgpt.com/backend-api/codex/responses";

pub struct OpenAiCodexProvider {
    pub profile: AuthProfile,
    pub model: String,
    pub http: reqwest::Client,
}

impl OpenAiCodexProvider {
    pub fn new(profile: AuthProfile) -> Self {
        let model = std::env::var("CLAW_CODEX_MODEL").unwrap_or_else(|_| "gpt-5-codex".into());
        Self {
            profile,
            model,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(180))
                .build()
                .expect("build reqwest client"),
        }
    }
}

#[derive(Serialize)]
struct CodexRequest<'a> {
    model: &'a str,
    instructions: String,
    input: Vec<CodexInput>,
    store: bool,
    stream: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<CodexTool>,
    tool_choice: &'a str,
    parallel_tool_calls: bool,
    text: CodexText,
    reasoning: CodexReasoning,
    include: Vec<&'a str>,
}

/// Each item in the Responses API `input` array. Three shapes:
///   - role-based message (user / assistant text)
///   - function_call (assistant requested a tool)
///   - function_call_output (the tool's result we feed back)
#[derive(Serialize)]
#[serde(untagged)]
enum CodexInput {
    Message {
        role: String,
        content: Vec<CodexInputContent>,
    },
    FunctionCall {
        #[serde(rename = "type")]
        kind: &'static str,
        call_id: String,
        name: String,
        arguments: String,
    },
    FunctionCallOutput {
        #[serde(rename = "type")]
        kind: &'static str,
        call_id: String,
        output: String,
    },
}

#[derive(Serialize)]
struct CodexInputContent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

#[derive(Serialize)]
struct CodexText {
    verbosity: &'static str,
}

#[derive(Serialize)]
struct CodexReasoning {
    effort: &'static str,
    summary: &'static str,
}

#[derive(Serialize)]
struct CodexTool {
    #[serde(rename = "type")]
    kind: &'static str,
    name: String,
    description: String,
    parameters: Value,
}

#[async_trait]
impl Provider for OpenAiCodexProvider {
    fn name(&self) -> &'static str {
        "openai-codex"
    }

    async fn complete(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<Completion, ProviderError> {
        let now = chrono::Utc::now().timestamp();
        if self.profile.expires_at - now < 30 {
            // Treat near-expired tokens as Unauthorized so the cascade
            // can fall through to the api-key provider. The caller is
            // expected to refresh out-of-band (claw login, or future
            // auth::refresh_if_needed integration).
            return Err(ProviderError::Unauthorized {
                provider: "openai-codex".into(),
                detail: "access token expired or expiring < 30s".into(),
            });
        }

        let (instructions, input) = build_codex_input(messages);
        let codex_tools: Vec<CodexTool> = tools
            .iter()
            .map(|t| CodexTool {
                kind: "function",
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.parameters.clone(),
            })
            .collect();

        let request = CodexRequest {
            model: &self.model,
            instructions,
            input,
            store: false,
            stream: true,
            tools: codex_tools,
            tool_choice: "auto",
            parallel_tool_calls: false,
            text: CodexText { verbosity: "medium" },
            reasoning: CodexReasoning {
                effort: "high",
                summary: "auto",
            },
            include: vec!["reasoning.encrypted_content"],
        };

        let response = self
            .http
            .post(CODEX_URL)
            .bearer_auth(&self.profile.openai_access_token)
            .header("OpenAI-Beta", "responses=experimental")
            .header("originator", "claw")
            .header("accept", "text/event-stream")
            .header("Content-Type", "application/json")
            .header("chatgpt-account-id", &self.profile.chatgpt_account_id)
            .json(&request)
            .send()
            .await
            .map_err(|e| ProviderError::Other("openai-codex".into(), e.into()))?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            let detail = response.text().await.unwrap_or_default();
            return Err(ProviderError::Unauthorized {
                provider: "openai-codex".into(),
                detail,
            });
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let detail = response.text().await.unwrap_or_default();
            // Codex returns 429 with `usage_limit_reached` in the body
            // when the subscription quota is exhausted.
            if detail.contains("usage_limit_reached") {
                return Err(ProviderError::UsageLimitReached {
                    provider: "openai-codex".into(),
                    detail,
                });
            }
            return Err(ProviderError::Other(
                "openai-codex".into(),
                anyhow!("HTTP 429 (transient): {detail}"),
            ));
        }
        if !status.is_success() {
            let detail = response.text().await.unwrap_or_default();
            return Err(ProviderError::Other(
                "openai-codex".into(),
                anyhow!("HTTP {status}: {detail}"),
            ));
        }

        let body = response
            .text()
            .await
            .map_err(|e| ProviderError::Other("openai-codex".into(), e.into()))?;
        parse_sse_response(&body).map_err(|e| ProviderError::Other("openai-codex".into(), e))
    }
}

/// Map claw's `Message` shape onto the codex Responses API input shape.
/// System messages collapse into the request's `instructions` field;
/// user + assistant text become role-based message items; assistant
/// `tool_calls` become `function_call` items; role="tool" messages
/// become `function_call_output` items. Empty assistant content is
/// dropped so we don't send blank turns alongside the function_calls.
fn build_codex_input(messages: &[Message]) -> (String, Vec<CodexInput>) {
    let mut system_parts: Vec<String> = Vec::new();
    let mut input: Vec<CodexInput> = Vec::new();
    for msg in messages {
        match msg.role.as_str() {
            "system" => system_parts.push(msg.content.clone()),
            "user" => {
                input.push(CodexInput::Message {
                    role: "user".into(),
                    content: vec![CodexInputContent {
                        kind: "input_text".into(),
                        text: Some(msg.content.clone()),
                    }],
                });
            }
            "assistant" => {
                if !msg.content.trim().is_empty() {
                    input.push(CodexInput::Message {
                        role: "assistant".into(),
                        content: vec![CodexInputContent {
                            kind: "output_text".into(),
                            text: Some(msg.content.clone()),
                        }],
                    });
                }
                for call in &msg.tool_calls {
                    input.push(CodexInput::FunctionCall {
                        kind: "function_call",
                        call_id: call.id.clone(),
                        name: call.name.clone(),
                        arguments: call.arguments.clone(),
                    });
                }
            }
            "tool" => {
                if let Some(call_id) = &msg.tool_call_id {
                    input.push(CodexInput::FunctionCallOutput {
                        kind: "function_call_output",
                        call_id: call_id.clone(),
                        output: msg.content.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    let instructions = system_parts.join("\n\n");
    (instructions, input)
}

/// Walk the SSE event stream, collect text deltas + function-call items.
/// Tolerant: unknown event types are ignored; a missing/malformed event
/// is logged but doesn't abort the response. Returns the final
/// `Completion` once the stream's `response.completed` event arrives or
/// the body ends.
fn parse_sse_response(body: &str) -> anyhow::Result<Completion> {
    let mut text_acc = String::new();
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    let mut finish_reason = "stop".to_string();

    for raw_chunk in body.split("\n\n") {
        let chunk = raw_chunk.trim();
        if chunk.is_empty() {
            continue;
        }
        // Each chunk is one SSE event with one or more `data:` lines.
        let data: String = chunk
            .lines()
            .filter_map(|l| l.strip_prefix("data:").map(str::trim))
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let event: Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(_) => {
                tracing::debug!("codex SSE: skipped non-JSON chunk");
                continue;
            }
        };
        let kind = event.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "response.output_text.delta" => {
                if let Some(s) = event.get("delta").and_then(Value::as_str) {
                    text_acc.push_str(s);
                }
            }
            "response.output_item.done" => {
                // Function calls land here as fully-realized output items.
                let item = event.get("item");
                if let Some(item) = item {
                    if item.get("type").and_then(Value::as_str) == Some("function_call") {
                        let id = item
                            .get("call_id")
                            .or_else(|| item.get("id"))
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let name = item
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let arguments = item
                            .get("arguments")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        if !name.is_empty() {
                            tool_calls.push(ToolCall {
                                id,
                                name,
                                arguments,
                            });
                        }
                    }
                }
            }
            "response.completed" | "response.done" => {
                if let Some(reason) = event
                    .get("response")
                    .and_then(|r| r.get("status"))
                    .and_then(Value::as_str)
                {
                    finish_reason = reason.to_string();
                }
                if !tool_calls.is_empty() {
                    finish_reason = "tool_calls".into();
                }
            }
            "error" => {
                let msg = event
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("codex returned error event");
                return Err(anyhow!("codex stream error: {msg}"));
            }
            _ => {}
        }
    }

    Ok(Completion {
        content: text_acc,
        tool_calls,
        finish_reason,
    })
}
