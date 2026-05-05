//! One-shot migration from a zeroclaw auth profile.
//!
//! zeroclaw stores its OpenAI Codex profile at
//! `<zeroclaw_data>/.zeroclaw/auth-profiles.json` with `enc2:`-prefixed
//! tokens. The encryption is ChaCha20-Poly1305 with a 32-byte key
//! stored hex-encoded in `<zeroclaw_data>/.zeroclaw/.secret_key`.
//!
//! Format details (from zeroclaw-config/src/secrets.rs):
//! - key file: 64 hex chars → 32 bytes
//! - cipher value: `enc2:<hex>` where hex decodes to `nonce(12) || ct+tag`
//! - cipher: ChaCha20Poly1305 from the rustcrypto `chacha20poly1305` crate
//!
//! After this command runs, claw has its own profile at
//! `~/.exciton/auth.json` and never reads zeroclaw's files again.

use crate::auth::{auth_path, openai_oauth, AuthProfile};
use anyhow::{anyhow, Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use clap::Args;
use serde_json::Value;
use std::path::{Path, PathBuf};

#[derive(Args, Debug)]
pub struct MigrateArgs {
    /// Path to zeroclaw's data directory. Defaults to
    /// $ZEROCLAW_DATA_DIR or `~/zeroclaw-data`.
    #[arg(long)]
    pub zeroclaw_data: Option<PathBuf>,
    /// Profile slug to import. Defaults to the active openai-codex
    /// profile (typically `openai-codex:default`).
    #[arg(long)]
    pub profile_id: Option<String>,
    /// Don't overwrite an existing claw auth profile. Default: overwrite.
    #[arg(long)]
    pub no_overwrite: bool,
}

pub async fn run(args: MigrateArgs) -> Result<()> {
    let data_dir = args.zeroclaw_data.unwrap_or_else(default_data_dir);
    let zeroclaw_dir = data_dir.join(".zeroclaw");
    let profiles_path = zeroclaw_dir.join("auth-profiles.json");
    let key_path = zeroclaw_dir.join(".secret_key");

    if !profiles_path.exists() {
        return Err(anyhow!(
            "zeroclaw profiles file not found at {}",
            profiles_path.display()
        ));
    }
    if !key_path.exists() {
        return Err(anyhow!(
            "zeroclaw secret key not found at {}",
            key_path.display()
        ));
    }

    let key_bytes = load_secret_key(&key_path)?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key_bytes));

    let raw = std::fs::read_to_string(&profiles_path)
        .with_context(|| format!("read {}", profiles_path.display()))?;
    let json: Value = serde_json::from_str(&raw).context("parse auth-profiles.json")?;

    let profile_id = match &args.profile_id {
        Some(id) => id.clone(),
        None => json
            .pointer("/active_profiles/openai-codex")
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| anyhow!("no active openai-codex profile in active_profiles"))?,
    };
    let profile = json
        .pointer(&format!("/profiles/{}", profile_id.replace('/', "~1")))
        .ok_or_else(|| anyhow!("profile {} not found in profiles map", profile_id))?;

    let access_token = decrypt_value(&cipher, profile.get("access_token"))
        .context("decrypt access_token")?
        .ok_or_else(|| anyhow!("profile missing access_token"))?;
    let refresh_token = decrypt_value(&cipher, profile.get("refresh_token"))
        .context("decrypt refresh_token")?
        .unwrap_or_default();
    let id_token = decrypt_value(&cipher, profile.get("id_token"))
        .context("decrypt id_token")?;

    // chatgpt_account_id: zeroclaw stores the plaintext account_id
    // alongside encrypted tokens. Prefer that; fall back to JWT
    // extraction if the profile is missing it.
    let chatgpt_account_id = profile
        .get("account_id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            id_token
                .as_deref()
                .and_then(openai_oauth::extract_account_id_from_jwt)
        })
        .ok_or_else(|| anyhow!("could not derive chatgpt_account_id"))?;

    let email = id_token
        .as_deref()
        .and_then(openai_oauth::extract_email_from_jwt);

    // expires_at: zeroclaw stores it as ISO-8601; claw stores as i64
    // epoch seconds. Both refresh-token rotations + access-token
    // refreshes will replace this when claw next refreshes.
    let expires_at = profile
        .get("expires_at")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.timestamp())
        .unwrap_or_else(|| chrono::Utc::now().timestamp() + 3600);

    let claw_path = auth_path()?;
    if claw_path.exists() && args.no_overwrite {
        return Err(anyhow!(
            "{} already exists; pass without --no-overwrite to replace it",
            claw_path.display()
        ));
    }

    let new_profile = AuthProfile {
        provider: "openai-codex".into(),
        openai_access_token: access_token,
        refresh_token,
        expires_at,
        chatgpt_account_id: chatgpt_account_id.clone(),
        email: email.clone(),
    };
    new_profile.save_default()?;

    eprintln!("\nclaw: profile imported from zeroclaw");
    eprintln!("  source:  {}", profiles_path.display());
    eprintln!("  target:  {}", claw_path.display());
    eprintln!("  account: {}", chatgpt_account_id);
    if let Some(e) = email {
        eprintln!("  email:   {}", e);
    }
    let secs_left = expires_at - chrono::Utc::now().timestamp();
    if secs_left > 0 {
        eprintln!("  expires_in: {}s", secs_left);
    } else {
        eprintln!("  expires_in: EXPIRED — claw will auto-refresh on next call");
    }
    Ok(())
}

fn default_data_dir() -> PathBuf {
    if let Ok(v) = std::env::var("ZEROCLAW_DATA_DIR") {
        return PathBuf::from(v);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join("zeroclaw-data")
}

fn load_secret_key(path: &Path) -> Result<[u8; 32]> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    let bytes = hex::decode(raw.trim()).context("decode .secret_key (expected hex)")?;
    if bytes.len() != 32 {
        return Err(anyhow!(
            ".secret_key is {} bytes after hex decode; expected 32",
            bytes.len()
        ));
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

/// Decrypt a single `enc2:` value. Plaintext / unprefixed values pass
/// through. None inputs return Ok(None).
fn decrypt_value(cipher: &ChaCha20Poly1305, value: Option<&Value>) -> Result<Option<String>> {
    let s = match value.and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(None),
    };
    if let Some(hex_str) = s.strip_prefix("enc2:") {
        let blob = hex::decode(hex_str).context("decode enc2 hex")?;
        if blob.len() < 13 {
            return Err(anyhow!("enc2 blob too short ({} bytes)", blob.len()));
        }
        let (nonce_bytes, ciphertext) = blob.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| anyhow!("decrypt failed — wrong key or tampered data"))?;
        let s = String::from_utf8(plaintext).context("decrypted value is not UTF-8")?;
        Ok(Some(s))
    } else if s.starts_with("enc:") {
        Err(anyhow!(
            "legacy enc: format not supported — refresh zeroclaw to upgrade tokens to enc2:"
        ))
    } else {
        // Plaintext (zeroclaw config can disable encryption) — pass through.
        Ok(Some(s.to_string()))
    }
}
