//! OpenAI raw-API provider — pay-per-token fallback.
//!
//! Uses the `/v1/chat/completions` endpoint because its tool-calling
//! request/response shapes are stable and well-documented. The newer
//! `/v1/responses` endpoint may be a better target later but isn't
//! worth the migration cost in v0.
//!
//! Activated automatically when no auth profile is on disk OR when the
//! codex provider returns `UsageLimitReached` / `Unauthorized`.

use crate::provider::{Completion, Message, Provider, ProviderError, ToolCall, ToolSpec};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const CHAT_URL: &str = "https://api.openai.com/v1/chat/completions";

pub struct OpenAiApiProvider {
    pub api_key: String,
    pub model: String,
    pub http: reqwest::Client,
}

impl OpenAiApiProvider {
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let api_key = std::env::var("OPENAI_API_KEY")
            .map_err(|_| anyhow!("OPENAI_API_KEY env var not set"))?;
        let model = std::env::var("CLAW_OPENAI_MODEL").unwrap_or_else(|_| "gpt-5.4".to_string());
        Ok(Self {
            api_key,
            model,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(180))
                .build()?,
        })
    }
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ApiMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ApiTool>,
    /// Let the model pick "auto" — it can call tools or stop.
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<&'a str>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    tool_calls: Vec<ApiToolCall>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    tool_call_id: Option<String>,
    /// Optional name on tool messages for clarity (some models prefer it).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiToolCall {
    id: String,
    #[serde(rename = "type", default = "default_function")]
    kind: String,
    function: ApiFunctionCall,
}

fn default_function() -> String {
    "function".into()
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Serialize)]
struct ApiTool {
    #[serde(rename = "type")]
    kind: &'static str,
    function: ApiToolFunction,
}

#[derive(Serialize)]
struct ApiToolFunction {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Deserialize, Debug)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize, Debug)]
struct ChatChoice {
    message: ApiMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[async_trait]
impl Provider for OpenAiApiProvider {
    fn name(&self) -> &'static str {
        "openai-api"
    }

    async fn complete(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<Completion, ProviderError> {
        let api_messages: Vec<ApiMessage> = messages
            .iter()
            .map(|m| ApiMessage {
                role: m.role.clone(),
                content: if m.content.is_empty() && !m.tool_calls.is_empty() {
                    None
                } else {
                    Some(m.content.clone())
                },
                tool_calls: m
                    .tool_calls
                    .iter()
                    .map(|tc| ApiToolCall {
                        id: tc.id.clone(),
                        kind: "function".into(),
                        function: ApiFunctionCall {
                            name: tc.name.clone(),
                            arguments: tc.arguments.clone(),
                        },
                    })
                    .collect(),
                tool_call_id: m.tool_call_id.clone(),
                name: None,
            })
            .collect();

        let api_tools: Vec<ApiTool> = tools
            .iter()
            .map(|t| ApiTool {
                kind: "function",
                function: ApiToolFunction {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    parameters: t.parameters.clone(),
                },
            })
            .collect();

        let request = ChatRequest {
            model: &self.model,
            messages: api_messages,
            tools: api_tools,
            tool_choice: Some("auto"),
        };

        let response = self
            .http
            .post(CHAT_URL)
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|e| ProviderError::Other("openai-api".into(), e.into()))?;

        let status = response.status();
        let body_text = response
            .text()
            .await
            .map_err(|e| ProviderError::Other("openai-api".into(), e.into()))?;

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(ProviderError::Unauthorized {
                provider: "openai-api".into(),
                detail: body_text,
            });
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ProviderError::UsageLimitReached {
                provider: "openai-api".into(),
                detail: body_text,
            });
        }
        if !status.is_success() {
            return Err(ProviderError::Other(
                "openai-api".into(),
                anyhow!("HTTP {status}: {body_text}"),
            ));
        }

        let chat: ChatResponse = serde_json::from_str(&body_text)
            .map_err(|e| ProviderError::Other("openai-api".into(), e.into()))
            .context("parse chat response")
            .map_err(|e| ProviderError::Other("openai-api".into(), e))?;

        let choice = chat
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| ProviderError::Other("openai-api".into(), anyhow!("no choices")))?;

        let tool_calls: Vec<ToolCall> = choice
            .message
            .tool_calls
            .into_iter()
            .map(|c| ToolCall {
                id: c.id,
                name: c.function.name,
                arguments: c.function.arguments,
            })
            .collect();

        Ok(Completion {
            content: choice.message.content.unwrap_or_default(),
            tool_calls,
            finish_reason: choice.finish_reason.unwrap_or_else(|| "stop".into()),
        })
    }
}
