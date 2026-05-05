//! Auth profile storage + OAuth flow orchestration.
//!
//! `claw login` runs one of two flows depending on `--device-code`:
//!   - Loopback flow (default): opens a browser to the OpenAI authorize
//!     URL, listens on 127.0.0.1:1455, waits for the redirect with the
//!     authorization code, exchanges it for tokens, persists.
//!   - Device-code flow (`--device-code`): for headless environments —
//!     prints a verification URL + user-code, polls the token endpoint
//!     until the user approves in their browser.
//!
//! Both flows produce a `TokenSet` that we stamp into an `AuthProfile`
//! and write to `~/.exciton/auth.json` with mode 0600.

use anyhow::{anyhow, Context, Result};
use clap::Args;
use std::path::{Path, PathBuf};
use std::time::Duration;

mod oauth_common;
mod openai_oauth;
mod profile;
mod zeroclaw_import;

pub use profile::AuthProfile;
pub use oauth_common::{generate_pkce_state, PkceState};
pub use zeroclaw_import::{run as migrate_from_zeroclaw, MigrateArgs};

#[derive(Args, Debug)]
pub struct LoginArgs {
    /// Auth provider to log into. Default: "openai-codex".
    #[arg(long, default_value = "openai-codex")]
    pub provider: String,
    /// Use the device-code flow instead of the browser loopback flow.
    /// Required on headless servers (no browser, no display).
    #[arg(long)]
    pub device_code: bool,
}

/// Run the OAuth flow for the requested provider, then persist the
/// resulting profile under `~/.exciton/auth.json`.
pub async fn login(args: LoginArgs) -> Result<()> {
    if args.provider != "openai-codex" {
        return Err(anyhow!(
            "unknown provider '{}'. v0 supports: openai-codex",
            args.provider
        ));
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("build reqwest client")?;

    let token_set = if args.device_code {
        let device = openai_oauth::start_device_code_flow(&client).await?;
        eprintln!("\nclaw login (device-code flow)");
        eprintln!("  Open: {}", device.verification_uri);
        if let Some(complete) = &device.verification_uri_complete {
            eprintln!("  Or directly: {}", complete);
        }
        eprintln!("  Code: {}", device.user_code);
        if let Some(msg) = &device.message {
            eprintln!("  Note: {}", msg);
        }
        eprintln!("  Waiting for approval...");
        openai_oauth::poll_device_code_tokens(&client, &device).await?
    } else {
        let pkce = openai_oauth::new_pkce_state();
        let url = openai_oauth::build_authorize_url(&pkce);
        eprintln!("\nclaw login (browser loopback flow)");
        eprintln!("  Open this URL to authorize:");
        eprintln!("    {}", url);
        eprintln!("  Listening on 127.0.0.1:1455 for callback (5 min timeout).");
        let code =
            openai_oauth::receive_loopback_code(&pkce.state, Duration::from_secs(300)).await?;
        openai_oauth::exchange_code_for_tokens(&client, &code, &pkce).await?
    };

    let id_token = token_set
        .id_token
        .as_deref()
        .ok_or_else(|| anyhow!("OAuth response missing id_token; cannot derive account_id"))?;
    let chatgpt_account_id = openai_oauth::extract_account_id_from_jwt(id_token)
        .ok_or_else(|| anyhow!("could not extract chatgpt-account-id from id_token JWT"))?;
    let email = openai_oauth::extract_email_from_jwt(id_token);
    let expires_at = token_set
        .expires_at
        .map(|d| d.timestamp())
        .unwrap_or_else(|| chrono::Utc::now().timestamp() + 3600);

    let profile = AuthProfile {
        provider: "openai-codex".into(),
        openai_access_token: token_set.access_token,
        refresh_token: token_set.refresh_token.unwrap_or_default(),
        expires_at,
        chatgpt_account_id: chatgpt_account_id.clone(),
        email: email.clone(),
    };
    profile.save_default()?;
    eprintln!("\nclaw: profile written to {}", auth_path()?.display());
    eprintln!("  account: {}", chatgpt_account_id);
    if let Some(e) = email {
        eprintln!("  email:   {}", e);
    }
    Ok(())
}

/// Print the active profile state. Read-only — no network call.
pub async fn whoami() -> Result<()> {
    match AuthProfile::load_default() {
        Ok(Some(p)) => {
            println!("provider:           {}", p.provider);
            println!("chatgpt_account_id: {}", p.chatgpt_account_id);
            if let Some(email) = &p.email {
                println!("email:              {}", email);
            }
            let now = chrono::Utc::now().timestamp();
            let secs_left = p.expires_at - now;
            if secs_left > 0 {
                println!("token_expires_in:   {}s", secs_left);
            } else {
                println!("token_expires_in:   EXPIRED — claw will auto-refresh on next call");
            }
            if !p.refresh_token.is_empty() {
                println!("refresh_token:      present");
            } else {
                println!("refresh_token:      ABSENT — re-run `claw login` if access token expires");
            }
        }
        Ok(None) => {
            if std::env::var("OPENAI_API_KEY").is_ok() {
                println!("no auth profile; OPENAI_API_KEY env fallback is active");
            } else {
                println!("no auth profile and no OPENAI_API_KEY env — `claw login` first");
            }
        }
        Err(e) => return Err(e.context("failed to load auth profile")),
    }
    Ok(())
}

/// Refresh the stored access token if it expires within `min_remaining`.
/// Idempotent — callers can invoke before any codex request without
/// worrying about the token state.
pub async fn refresh_if_needed(min_remaining: Duration) -> Result<Option<AuthProfile>> {
    let mut profile = match AuthProfile::load_default()? {
        Some(p) => p,
        None => return Ok(None),
    };
    let now = chrono::Utc::now().timestamp();
    if profile.expires_at - now > min_remaining.as_secs() as i64 {
        return Ok(Some(profile));
    }
    if profile.refresh_token.is_empty() {
        anyhow::bail!("access token expired and no refresh_token on file — run `claw login`");
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let new_set = openai_oauth::refresh_access_token(&client, &profile.refresh_token).await?;
    profile.openai_access_token = new_set.access_token;
    if let Some(rt) = new_set.refresh_token {
        profile.refresh_token = rt;
    }
    profile.expires_at = new_set
        .expires_at
        .map(|d| d.timestamp())
        .unwrap_or_else(|| chrono::Utc::now().timestamp() + 3600);
    if let Some(id_token) = new_set.id_token.as_deref() {
        if let Some(acct) = openai_oauth::extract_account_id_from_jwt(id_token) {
            profile.chatgpt_account_id = acct;
        }
        if let Some(email) = openai_oauth::extract_email_from_jwt(id_token) {
            profile.email = Some(email);
        }
    }
    profile.save_default()?;
    Ok(Some(profile))
}

pub fn auth_path() -> Result<PathBuf> {
    if let Ok(home) = std::env::var("EXCITON_HOME") {
        return Ok(PathBuf::from(home).join("auth.json"));
    }
    let home = std::env::var("HOME").context("$HOME not set")?;
    Ok(Path::new(&home).join(".exciton").join("auth.json"))
}
