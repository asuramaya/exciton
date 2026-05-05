//! OAuth2 utilities — PKCE state, percent encoding, query parsing.
//! Ported from zeroclaw's `oauth_common.rs` (auth/), trimmed to what
//! claw uses. Randomness comes from `getrandom` directly so we avoid
//! pulling the heavier rng crates zeroclaw inherited.

use base64::Engine;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct PkceState {
    pub code_verifier: String,
    pub code_challenge: String,
    pub state: String,
}

pub fn generate_pkce_state() -> PkceState {
    let code_verifier = random_base64url(64);
    let digest = Sha256::digest(code_verifier.as_bytes());
    let code_challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
    PkceState {
        code_verifier,
        code_challenge,
        state: random_base64url(24),
    }
}

pub fn random_base64url(byte_len: usize) -> String {
    let mut bytes = vec![0_u8; byte_len];
    // getrandom pulls entropy from the OS — claw needs cryptographic
    // strength here for PKCE verifier and the OAuth state nonce.
    getrandom::getrandom(&mut bytes).expect("OS RNG unavailable");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub fn url_encode(input: &str) -> String {
    input
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

pub fn url_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = bytes[i + 1] as char;
                let lo = bytes[i + 2] as char;
                if let (Some(h), Some(l)) = (hi.to_digit(16), lo.to_digit(16)) {
                    if let Ok(value) = u8::try_from(h * 16 + l) {
                        out.push(value);
                        i += 3;
                        continue;
                    }
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

pub fn parse_query_params(input: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for pair in input.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        out.insert(url_decode(key), url_decode(value));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_challenge_is_sha256_of_verifier() {
        let pkce = generate_pkce_state();
        let expected = {
            let digest = Sha256::digest(pkce.code_verifier.as_bytes());
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
        };
        assert_eq!(pkce.code_challenge, expected);
    }

    #[test]
    fn url_roundtrip() {
        let s = "hello world! @#$%^&*()";
        assert_eq!(url_decode(&url_encode(s)), s);
    }

    #[test]
    fn parse_params_basic() {
        let p = parse_query_params("code=abc&state=xyz");
        assert_eq!(p.get("code").map(String::as_str), Some("abc"));
    }
}
