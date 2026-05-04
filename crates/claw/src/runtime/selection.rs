//! Provider selection: subscription path first, raw-API fallback.
//!
//! Strategy:
//!   1. If an auth profile exists at `~/.exciton/auth.json` and is not
//!      expired, try `OpenAiCodexProvider` first.
//!   2. On `UsageLimitReached` OR `Unauthorized`, fall back to
//!      `OpenAiApiProvider` (raw API key).
//!   3. If both paths are unavailable, surface a helpful error so the
//!      operator knows to run `claw login` or set `OPENAI_API_KEY`.

use crate::auth::AuthProfile;
use crate::provider::{
    openai_api::OpenAiApiProvider, openai_codex::OpenAiCodexProvider, Completion, Message,
    Provider, ProviderError, ToolSpec,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;

/// Composite provider that tries codex first, falls back to API key.
/// Owns both inner providers; `complete` walks the cascade per call so
/// quota limits don't sticky-pin the runtime to the fallback path.
pub struct CascadingProvider {
    pub codex: Option<OpenAiCodexProvider>,
    pub api: Option<OpenAiApiProvider>,
}

impl CascadingProvider {
    pub fn from_env() -> Result<Self> {
        let codex = match AuthProfile::load_default() {
            Ok(Some(p)) => Some(OpenAiCodexProvider::new(p)),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("auth profile load failed: {e}; falling back to API key");
                None
            }
        };
        let api = OpenAiApiProvider::from_env().ok();
        if codex.is_none() && api.is_none() {
            return Err(anyhow!(
                "no auth profile and no OPENAI_API_KEY; run `claw login` or set OPENAI_API_KEY"
            ));
        }
        Ok(Self { codex, api })
    }
}

#[async_trait]
impl Provider for CascadingProvider {
    fn name(&self) -> &'static str {
        "cascade"
    }

    async fn complete(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
    ) -> Result<Completion, ProviderError> {
        if let Some(c) = &self.codex {
            match c.complete(messages, tools).await {
                Ok(out) => return Ok(out),
                Err(ProviderError::UsageLimitReached { detail, .. }) => {
                    tracing::warn!("codex quota hit ({detail}); falling back to api key");
                }
                Err(ProviderError::Unauthorized { detail, .. }) => {
                    tracing::warn!("codex unauthorized ({detail}); falling back to api key");
                }
                Err(other) => return Err(other),
            }
        }
        if let Some(a) = &self.api {
            return a.complete(messages, tools).await;
        }
        Err(ProviderError::Other(
            "cascade".into(),
            anyhow!("no provider available"),
        ))
    }
}
