//! Image generation for diary entries.
//!
//! Architectural rule: bytes never touch the Cloudflare Worker.
//! - Engine on the docker box owns the Recraft key + R2 credentials.
//! - Engine renders via Recraft v3, PUTs the PNG to R2 via S3-sigv4,
//!   then writes only the public URL into `thoughts/assets.json`.
//! - Publisher's normal data tick ships `assets.json` to the Worker;
//!   the page reads it from `/api/data/thoughts_assets`.
//!
//! Decoupled from the publisher tick: this loop runs on its own
//! cadence so a slow image render can't stall a 60s publisher budget.

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use chrono::Utc;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::config::MadapesConfig;

type HmacSha256 = Hmac<Sha256>;

/// Spawn the image-gen background loop. Returns immediately. Does
/// nothing (logs once) when any required credential is empty so OSS
/// forks can run without R2 + OpenAI.
pub fn spawn(cfg: Arc<MadapesConfig>) {
    if cfg.r2_account_id.is_empty()
        || cfg.r2_bucket.is_empty()
        || cfg.r2_access_key_id.is_empty()
        || cfg.r2_secret_access_key.is_empty()
        || cfg.cdn_base_url.is_empty()
        || cfg.recraft_api_key.is_empty()
    {
        tracing::info!("image_gen: disabled (one or more credentials/URLs missing)");
        return;
    }
    let interval = Duration::from_secs(cfg.image_gen_interval_seconds.max(15));
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .user_agent(concat!("exciton-image-gen/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("build image_gen client");
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            match run_once(&client, cfg.as_ref()).await {
                Ok(generated) => {
                    if generated > 0 {
                        tracing::info!("image_gen: generated {} asset(s) this tick", generated);
                    }
                }
                Err(e) => tracing::warn!("image_gen: tick failed: {:#}", e),
            }
        }
    });
}

/// One scan: find entries missing assets, generate up to one image, return count.
async fn run_once(client: &reqwest::Client, cfg: &MadapesConfig) -> Result<u32> {
    let thoughts = PathBuf::from(&cfg.repo_path).join("thoughts");
    if !thoughts.is_dir() {
        return Ok(0);
    }
    let assets_path = thoughts.join("assets.json");
    let mut assets: Value = if assets_path.exists() {
        serde_json::from_str(&std::fs::read_to_string(&assets_path)?)
            .unwrap_or(Value::Object(Default::default()))
    } else {
        Value::Object(Default::default())
    };
    let assets_obj = assets
        .as_object_mut()
        .ok_or_else(|| anyhow!("assets.json is not an object"))?;

    let entries = std::fs::read_dir(&thoughts).context("read thoughts/")?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.ends_with(".md") || name == "README.md" {
            continue;
        }
        let existing = assets_obj
            .get(name)
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        if (existing as u32) >= cfg.images_per_entry {
            continue;
        }
        let body = std::fs::read_to_string(&path)?;
        let Some((slug, prompts)) = build_prompts(&body, cfg.images_per_entry as usize) else {
            continue;
        };
        let next_idx = existing as u32;
        let prompt = prompts
            .get(next_idx as usize)
            .ok_or_else(|| anyhow!("not enough prompts for idx {}", next_idx))?;
        let png = render_image(
            client,
            &cfg.recraft_api_key,
            &cfg.recraft_model,
            &cfg.recraft_style,
            prompt,
        )
        .await?;
        let key = format!("thoughts/{}_{:02}.png", slug, next_idx);
        put_r2(client, cfg, &key, &png, "image/png").await?;
        let url = format!("{}/{}", cfg.cdn_base_url.trim_end_matches('/'), key);
        let arr = assets_obj
            .entry(name.to_string())
            .or_insert_with(|| Value::Array(vec![]))
            .as_array_mut()
            .ok_or_else(|| anyhow!("assets[{}] is not an array", name))?;
        arr.push(json!({ "idx": next_idx, "caption": prompt, "asset": url }));
        let pretty = serde_json::to_string_pretty(&assets)?;
        std::fs::write(&assets_path, pretty).context("write assets.json")?;
        tracing::info!(
            "image_gen: rendered {} idx={} → {}",
            name,
            next_idx,
            url
        );
        return Ok(1);
    }
    Ok(0)
}

/// Parse YAML frontmatter (tolerant — only needs `slug` and `summary`)
/// and return (slug, prompts). Returns None for entries without
/// frontmatter (legacy entries that ship pre-rendered static assets).
fn build_prompts(md: &str, n: usize) -> Option<(String, Vec<String>)> {
    let trimmed = md.trim_start();
    if !trimmed.starts_with("---") {
        return None;
    }
    let rest = &trimmed[3..];
    let end = rest.find("\n---")?;
    let front = &rest[..end];
    let mut slug: Option<String> = None;
    let mut summary: Option<String> = None;
    let mut title: Option<String> = None;
    let mut in_summary = false;
    let mut summary_lines: Vec<String> = Vec::new();
    for line in front.lines() {
        if in_summary {
            if line.starts_with("  ") || line.starts_with('\t') {
                summary_lines.push(line.trim().to_string());
                continue;
            } else {
                in_summary = false;
                summary = Some(summary_lines.join(" "));
            }
        }
        if let Some(v) = line.strip_prefix("slug:") {
            slug = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("title:") {
            title = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("summary:") {
            let v = v.trim();
            if v == "|" || v.is_empty() {
                in_summary = true;
            } else {
                summary = Some(v.to_string());
            }
        }
    }
    if in_summary && summary.is_none() {
        summary = Some(summary_lines.join(" "));
    }
    let slug = slug?;
    let title = title.unwrap_or_else(|| slug.clone());
    let summary = summary.unwrap_or_default();
    let style = "minimalist editorial illustration, cinematic, ape protagonist, monochrome warm palette, soft grain, 4:3";
    let mut prompts = Vec::with_capacity(n);
    for i in 0..n {
        let beat = match i {
            0 => "the moment the story begins, wide cinematic shot",
            1 => "the central tension of the entry, mid-shot, ape close to the action",
            2 => "the resolution or punchline, lower-angle shot, contemplative",
            _ => "an additional thematic frame, varied composition",
        };
        prompts.push(format!(
            "{style}. Title: {title}. Summary: {summary}. Beat: {beat}. No text, no logos."
        ));
    }
    Some((slug, prompts))
}

/// Call Recraft v3. Returns PNG bytes.
async fn render_image(
    client: &reqwest::Client,
    api_key: &str,
    model: &str,
    style: &str,
    prompt: &str,
) -> Result<Vec<u8>> {
    let body = json!({
        "model": model,
        "style": style,
        "prompt": prompt,
        "n": 1,
        "size": "1024x1024",
        "response_format": "b64_json",
    });
    let resp = client
        .post("https://external.api.recraft.ai/v1/images/generations")
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await
        .context("recraft images.generations")?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("recraft image gen {}: {}", status, text);
    }
    let v: Value = serde_json::from_str(&text).context("parse recraft response")?;
    let b64 = v["data"][0]["b64_json"]
        .as_str()
        .ok_or_else(|| anyhow!("recraft response missing data[0].b64_json"))?;
    Ok(B64.decode(b64).context("decode b64 image")?)
}

/// Sign + PUT to R2 via S3-compatible sigv4. Region is `auto`.
async fn put_r2(
    client: &reqwest::Client,
    cfg: &MadapesConfig,
    key: &str,
    body: &[u8],
    content_type: &str,
) -> Result<()> {
    let host = format!("{}.r2.cloudflarestorage.com", cfg.r2_account_id);
    let url = format!("https://{}/{}/{}", host, cfg.r2_bucket, key);
    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let payload_hash = hex::encode(Sha256::digest(body));

    let canonical_uri = format!("/{}/{}", cfg.r2_bucket, encode_uri_path(key));
    let signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date";
    let canonical_headers = format!(
        "content-type:{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        content_type, host, payload_hash, amz_date
    );
    let canonical_request = format!(
        "PUT\n{}\n\n{}\n{}\n{}",
        canonical_uri, canonical_headers, signed_headers, payload_hash
    );
    let creq_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let credential_scope = format!("{}/auto/s3/aws4_request", date);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date, credential_scope, creq_hash
    );

    let k_date = hmac(format!("AWS4{}", cfg.r2_secret_access_key).as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, b"auto");
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));

    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        cfg.r2_access_key_id, credential_scope, signed_headers, signature
    );
    let resp = client
        .put(&url)
        .header("host", &host)
        .header("content-type", content_type)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash)
        .header("authorization", &auth)
        .body(body.to_vec())
        .send()
        .await
        .context("r2 put")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let txt = resp.text().await.unwrap_or_default();
        anyhow::bail!("r2 put {}: {}", status, txt);
    }
    Ok(())
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

/// Percent-encode each path segment per AWS sigv4 rules (skip '/').
fn encode_uri_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}
