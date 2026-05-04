//! Thought-image processor — scans the publisher target repo's `thoughts/`
//! folder for placeholder blocks written in-band as
//!
//!     <div class="img-placeholder">[IMAGE: caption text]</div>
//!
//! and fills them in by calling Recraft for each unprocessed placeholder.
//! Generated assets land in `assets/thoughts/<slug>_<idx>.webp` and a
//! `thoughts/assets.json` manifest records them so the static client can
//! transform the rendered placeholder into a proper `<figure>` at view
//! time — without editing the source markdown (which is append-only).
//!
//! Idempotent: re-running this task never regenerates an image that
//! already exists, so the API cost is one-and-done per placeholder.

use crate::image_gen::ImageGenerator;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
// Async git ops with hard 60s timeout per command — same fix as
// publisher::commit_and_push. A hung `git push` could otherwise
// freeze the image processor for many cycles (observed staleness
// of 45 minutes vs 15-minute interval).
use tokio::process::Command;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageEntry {
    pub idx: usize,
    pub caption: String,
    pub asset: String,
}

pub struct ThoughtImageProcessor {
    repo_path: PathBuf,
    interval: u64,
    generator: Arc<ImageGenerator>,
}

impl ThoughtImageProcessor {
    pub fn new(repo_path: PathBuf, interval_seconds: u64, api_key: String) -> Self {
        Self {
            repo_path,
            interval: interval_seconds.max(60),
            generator: Arc::new(ImageGenerator::new(api_key)),
        }
    }

    pub fn spawn(self: Arc<Self>) {
        tokio::spawn(async move {
            tracing::info!(
                "Thought-image processor active: scanning {} every {}s",
                self.repo_path.display(),
                self.interval
            );
            let mut tick = tokio::time::interval(Duration::from_secs(self.interval));
            loop {
                tick.tick().await;
                match self.run_once().await {
                    Ok(n) if n > 0 => {
                        tracing::info!("thought-images: generated {} new asset(s)", n)
                    }
                    Ok(_) => tracing::debug!("thought-images: no new placeholders"),
                    Err(e) => tracing::warn!("thought-images: run failed: {}", e),
                }
            }
        });
    }

    /// Walk thoughts/, find unprocessed placeholders, generate + save +
    /// commit. Returns the number of new assets generated this cycle.
    pub async fn run_once(&self) -> Result<usize> {
        let thoughts_dir = self.repo_path.join("thoughts");
        let assets_dir = self.repo_path.join("assets/thoughts");
        std::fs::create_dir_all(&assets_dir).context("create assets dir")?;

        let manifest_path = thoughts_dir.join("assets.json");
        let mut manifest: HashMap<String, Vec<ImageEntry>> =
            load_manifest(&manifest_path).unwrap_or_default();
        let mut generated = 0usize;

        // Enumerate every .md file in thoughts/, excluding the folder README.
        let mut note_files: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(&thoughts_dir).context("read thoughts dir")? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }
            if path.file_name().and_then(|n| n.to_str()) == Some("README.md") {
                continue;
            }
            note_files.push(path);
        }
        note_files.sort();

        for note in note_files {
            let filename = match note.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let body = std::fs::read_to_string(&note).unwrap_or_default();
            let placeholders = extract_placeholders(&body);
            if placeholders.is_empty() {
                continue;
            }
            let entries = manifest.entry(filename.clone()).or_default();
            for (idx, caption) in placeholders.iter().enumerate() {
                if entries.iter().any(|e| e.idx == idx) {
                    continue;
                }
                let slug = note
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("note")
                    .to_string();
                let asset_rel = format!("assets/thoughts/{}_{:02}.webp", slug, idx);
                let asset_abs = self.repo_path.join(&asset_rel);
                tracing::info!(
                    "thought-images: generating {} ({})",
                    asset_rel,
                    trim(caption, 80)
                );
                match self.generator.generate(caption).await {
                    Ok(bytes) => {
                        std::fs::write(&asset_abs, &bytes)
                            .with_context(|| format!("write image {}", asset_abs.display()))?;
                        entries.push(ImageEntry {
                            idx,
                            caption: caption.clone(),
                            asset: asset_rel,
                        });
                        generated += 1;
                    }
                    Err(e) => {
                        tracing::warn!("thought-images: recraft failed for idx {}: {}", idx, e);
                        // Bail on repeated failure — don't burn the rest of
                        // the cycle if Recraft is rejecting us.
                        break;
                    }
                }
            }
        }

        if generated == 0 {
            return Ok(0);
        }

        save_manifest(&manifest_path, &manifest)?;
        self.commit_and_push().await?;
        Ok(generated)
    }

    async fn commit_and_push(&self) -> Result<()> {
        let repo = self.repo_path.to_str().unwrap_or(".");
        // Only stage the generated artifacts + manifest. The publisher's
        // data/ changes are a separate concern.
        run_git(&["-C", repo, "add", "assets/", "thoughts/assets.json"])
            .await
            .context("git add")?;
        let status = run_git(&[
            "-C",
            repo,
            "status",
            "--porcelain",
            "--",
            "assets/",
            "thoughts/assets.json",
        ])
        .await
        .context("git status")?;
        if status.stdout.is_empty() {
            return Ok(());
        }
        let commit = run_git(&["-C", repo, "commit", "-m", "assets: thought-image render"])
            .await
            .context("git commit")?;
        if !commit.status.success() {
            anyhow::bail!(
                "git commit failed: {}",
                String::from_utf8_lossy(&commit.stderr)
            );
        }
        let push = run_git(&["-C", repo, "push", "--quiet"])
            .await
            .context("git push")?;
        if !push.status.success() {
            anyhow::bail!("git push failed: {}", String::from_utf8_lossy(&push.stderr));
        }
        Ok(())
    }
}

async fn run_git(args: &[&str]) -> Result<std::process::Output> {
    use std::time::Duration;
    let mut cmd = Command::new("git");
    cmd.args(args);
    let fut = cmd.output();
    match tokio::time::timeout(Duration::from_secs(60), fut).await {
        Ok(Ok(out)) => Ok(out),
        Ok(Err(e)) => Err(anyhow::anyhow!("git spawn: {}", e)),
        Err(_) => anyhow::bail!("git {} timed out after 60s", args.join(" ")),
    }
}

fn load_manifest(path: &Path) -> Result<HashMap<String, Vec<ImageEntry>>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let s = std::fs::read_to_string(path).context("read manifest")?;
    let m: HashMap<String, Vec<ImageEntry>> = serde_json::from_str(&s).unwrap_or_default();
    Ok(m)
}

fn save_manifest(path: &Path, manifest: &HashMap<String, Vec<ImageEntry>>) -> Result<()> {
    let s = serde_json::to_string_pretty(manifest).context("serialize manifest")?;
    std::fs::write(path, s).context("write manifest")?;
    Ok(())
}

/// Extract `[IMAGE: caption]` strings from `<div class="img-placeholder">`
/// blocks in the order they appear in the markdown body. Case-insensitive
/// on the prefix but preserves the caption exactly as written — the
/// caption is what gets sent to Recraft as the prompt.
fn extract_placeholders(body: &str) -> Vec<String> {
    let needle_open = "<div class=\"img-placeholder\">";
    let needle_close = "</div>";
    let mut out: Vec<String> = Vec::new();
    let mut cursor = 0usize;
    while let Some(open_rel) = body[cursor..].find(needle_open) {
        let open_abs = cursor + open_rel + needle_open.len();
        let close_rel = match body[open_abs..].find(needle_close) {
            Some(c) => c,
            None => break,
        };
        let content = &body[open_abs..open_abs + close_rel];
        let caption = strip_image_prefix(content.trim());
        if !caption.is_empty() {
            out.push(caption);
        }
        cursor = open_abs + close_rel + needle_close.len();
    }
    out
}

/// Strip the literal `[IMAGE: ... ]` framing from a placeholder's inner
/// text. Tolerant of extra whitespace and missing bracket.
fn strip_image_prefix(raw: &str) -> String {
    let trimmed = raw.trim();
    // Matches `[IMAGE: foo]` — case-insensitive on the prefix.
    if let Some(rest) = trimmed.strip_prefix('[') {
        let upper = rest.to_uppercase();
        if let Some(prefix_stripped) = upper.strip_prefix("IMAGE:") {
            let cut = rest.len() - prefix_stripped.len();
            let mut body = rest[cut..].to_string();
            body = body.trim().to_string();
            if body.ends_with(']') {
                body.pop();
            }
            return body.trim().to_string();
        }
    }
    trimmed.to_string()
}

fn trim(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let cut: String = s.chars().take(max).collect();
        format!("{}…", cut)
    }
}
