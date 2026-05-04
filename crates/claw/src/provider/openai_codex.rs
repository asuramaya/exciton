//! OpenAI Codex provider — subscription-billed.
//!
//! Stubbed in B1. The full request/response port lands in B2 alongside
//! the OAuth flow. Today's behavior: every `complete` call returns
//! `Unauthorized` so the runtime falls through to the API-key provider.

use crate::auth::AuthProfile;
use crate::provider::{Completion, Message, Provider, ProviderError, ToolSpec};
use async_trait::async_trait;

pub struct OpenAiCodexProvider {
    pub profile: AuthProfile,
    pub http: reqwest::Client,
}

impl OpenAiCodexProvider {
    pub fn new(profile: AuthProfile) -> Self {
        Self {
            profile,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Provider for OpenAiCodexProvider {
    fn name(&self) -> &'static str {
        "openai-codex"
    }

    async fn complete(
        &self,
        _messages: &[Message],
        _tools: &[ToolSpec],
    ) -> Result<Completion, ProviderError> {
        Err(ProviderError::Unauthorized {
            provider: "openai-codex".into(),
            detail: "codex transport not yet implemented (Phase B2)".into(),
        })
    }
}
