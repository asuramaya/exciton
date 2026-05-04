//! Auth profile — what we store on disk for an authenticated provider.
//!
//! Schema mirrors zeroclaw's profile shape (so the OAuth port in B2
//! drops in without rewriting consumers). Stored as JSON at
//! `~/.exciton/auth.json` with mode 0600 on Unix.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProfile {
    /// Provider key, e.g. "openai-codex". One profile per provider.
    pub provider: String,
    /// Bearer token for codex `responses` calls. Short-lived (~1h).
    pub openai_access_token: String,
    /// Long-lived refresh token. Used to mint new access tokens without
    /// re-running the browser flow.
    pub refresh_token: String,
    /// Unix epoch seconds when `openai_access_token` expires.
    pub expires_at: i64,
    /// Account ID extracted from the access-token JWT. Required as the
    /// `chatgpt-account-id` header on every codex request.
    pub chatgpt_account_id: String,
    /// Optional email associated with the OAuth account. Diagnostic only.
    #[serde(default)]
    pub email: Option<String>,
}

impl AuthProfile {
    /// Read + parse the default profile path. Returns Ok(None) when the
    /// file doesn't exist; Ok(Some(_)) when present and valid.
    pub fn load_default() -> Result<Option<Self>> {
        let path = super::auth_path()?;
        if !path.exists() {
            return Ok(None);
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        let p: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parse {}", path.display()))?;
        Ok(Some(p))
    }

    /// Write to the default profile path with mode 0600 on Unix.
    /// Creates parent dirs as needed.
    pub fn save_default(&self) -> Result<()> {
        let path = super::auth_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, &json)
            .with_context(|| format!("write {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&path, perm)
                .with_context(|| format!("chmod 600 {}", path.display()))?;
        }
        Ok(())
    }
}
