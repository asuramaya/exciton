//! Recraft image-generation client — server-side only. The API key never
//! leaves this module, never lands in the public repo, never travels over
//! a channel that isn't the exciton process itself talking to Recraft.
//!
//! Used by the thought-image processor to render placeholder captions in
//! the jungle notes into cinematic 2:1 photorealistic imagery.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const RECRAFT_ENDPOINT: &str = "https://external.api.recraft.ai/v1/images/generations";
/// Near-2:1 cinematic ratio. We used 2048x1024 in the first wave but each
/// WebP was ~2.5MB — eight images on a page blew past 20MB on initial
/// load. 1820x1024 gives the same widescreen feel, Recraft-supported,
/// roughly 35% smaller on the wire. Page CSS normalizes display to 2:1
/// so the visual rhythm is preserved.
pub const DEFAULT_SIZE: &str = "1820x1024";
/// Photorealistic output — the user-facing style for MadApes notes.
pub const DEFAULT_STYLE: &str = "realistic_image";
/// Prompt suffix that consistently nudges Recraft toward the editorial
/// moody-photo look we want for the narrative device.
pub const PROMPT_SUFFIX: &str =
    ", cinematic composition, 35mm film, moody lighting, high detail, editorial photography";

#[derive(Serialize)]
struct GenerateRequest<'a> {
    prompt: &'a str,
    style: &'a str,
    size: &'a str,
    n: u32,
}

#[derive(Deserialize)]
struct GenerateResponse {
    data: Vec<GenerateImage>,
}

#[derive(Deserialize)]
struct GenerateImage {
    url: String,
}

pub struct ImageGenerator {
    api_key: String,
    http: reqwest::Client,
}

impl ImageGenerator {
    pub fn new(api_key: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(90))
            .build()
            .expect("reqwest client");
        Self { api_key, http }
    }

    /// Render a prompt → raw image bytes (WebP). Uses the default 2:1
    /// photorealistic preset; callers can wrap this with their own
    /// prompt-engineering if they need a different look.
    pub async fn generate(&self, prompt: &str) -> Result<Vec<u8>> {
        let full_prompt = format!("{}{}", prompt, PROMPT_SUFFIX);
        let req = GenerateRequest {
            prompt: &full_prompt,
            style: DEFAULT_STYLE,
            size: DEFAULT_SIZE,
            n: 1,
        };
        let resp = self
            .http
            .post(RECRAFT_ENDPOINT)
            .bearer_auth(&self.api_key)
            .json(&req)
            .send()
            .await
            .context("recraft post")?;
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("recraft api error {}: {}", code, body);
        }
        let parsed: GenerateResponse = resp.json().await.context("recraft parse")?;
        let url = parsed
            .data
            .first()
            .map(|d| d.url.clone())
            .ok_or_else(|| anyhow::anyhow!("recraft returned no images"))?;
        let img = self
            .http
            .get(&url)
            .send()
            .await
            .context("recraft download")?
            .bytes()
            .await
            .context("recraft bytes")?;
        Ok(img.to_vec())
    }
}
