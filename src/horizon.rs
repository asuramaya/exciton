//! Shared horizon parsing for the call lifecycle. Notes carry a
//! `horizon=SHORT` / `horizon=LONG` tag set at insert_call time (auto by
//! mcap heuristic, manual by operator). The settling phase, expiry-window
//! computation, publisher stats bucket, and TG card horizon badge all
//! need to read this consistently.
//!
//! Substring-match-on-each-caller was fragile: a freeform operator note
//! that mentioned "long horizon thesis" without the canonical tag would
//! be mis-classified by some callers and not others. One parser, one
//! source of truth.

/// Canonical horizon classification. `Unknown` is the fallback when the
/// tag is missing or malformed — callers decide their own default
/// (settling treats Unknown as Short; UI treats it as no badge).
///
/// Scalp is a separate ultra-short bucket for shallow-mcap tokens that just
/// printed a 1h+ move. Different gates (lower mcap floor) and different
/// settle ladder (+30/+60/-30, 4h timeout) than Short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Horizon {
    Scalp,
    Short,
    Long,
    Unknown,
}

impl Horizon {
    /// `true` for explicit Long. Used for branch decisions where Unknown
    /// should be treated as Short (the auto-call default).
    pub fn is_long(self) -> bool {
        matches!(self, Horizon::Long)
    }

    pub fn is_scalp(self) -> bool {
        matches!(self, Horizon::Scalp)
    }

    /// Display string for TG card headers. None means no badge shown.
    pub fn display(self) -> Option<&'static str> {
        match self {
            Horizon::Scalp => Some("SCALP"),
            Horizon::Short => Some("SHORT TERM"),
            Horizon::Long => Some("LONG TERM"),
            Horizon::Unknown => None,
        }
    }

    /// Canonical serialization tag. Used at insert_call time to write
    /// the note. `Unknown` doesn't write — there's nothing to tag.
    pub fn tag(self) -> Option<&'static str> {
        match self {
            Horizon::Scalp => Some("horizon=SCALP"),
            Horizon::Short => Some("horizon=SHORT"),
            Horizon::Long => Some("horizon=LONG"),
            Horizon::Unknown => None,
        }
    }
}

/// Parse the canonical tag out of a call note. Strict match on the
/// `horizon=SHORT` / `horizon=LONG` pattern — freeform mentions of
/// "long" or "horizon" without the tag map to Unknown.
pub fn parse(note: &str) -> Horizon {
    if note.contains("horizon=SCALP") {
        Horizon::Scalp
    } else if note.contains("horizon=LONG") {
        Horizon::Long
    } else if note.contains("horizon=SHORT") {
        Horizon::Short
    } else {
        Horizon::Unknown
    }
}

/// Parse + return the note with the tag stripped (used by TG card
/// rendering — the tag is internal metadata, not display text).
/// Trims dangling separators (` · `) left behind on either side.
pub fn parse_with_clean(note: &str) -> (Horizon, String) {
    let h = parse(note);
    let tag = match h.tag() {
        Some(t) => t,
        None => return (h, note.to_string()),
    };
    let mut clean = note.to_string();
    if let Some(pos) = note.find(tag) {
        let before = note[..pos]
            .trim_end_matches(' ')
            .trim_end_matches('·')
            .trim_end_matches(' ');
        let after = &note[pos + tag.len()..];
        clean = format!("{}{}", before, after).trim().to_string();
    }
    (h, clean)
}

/// Extract a `thesis=<filename>` tag from a call note. Convention: the
/// filename is relative to the public site's `thoughts/` directory
/// (e.g. `2026-04-22_first-call-psyopanime-one-trader-vs-a-network.md`).
/// Whitespace / `·` separators delimit the tag from surrounding free-
/// form text. Returns None when the tag is absent.
///
/// Lets long-thesis calls link to their hand-curated markdown so the
/// site's CALLS row carries a 📖 link without a separate column or
/// out-of-band convention.
pub fn parse_thesis(note: &str) -> Option<String> {
    let key = "thesis=";
    let pos = note.find(key)?;
    let after = &note[pos + key.len()..];
    let end = after
        .find(|c: char| c.is_whitespace() || c == '·')
        .unwrap_or(after.len());
    let filename = after[..end].trim();
    if filename.is_empty() {
        None
    } else {
        Some(filename.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_tags_parse() {
        assert_eq!(parse("horizon=SHORT"), Horizon::Short);
        assert_eq!(parse("horizon=LONG"), Horizon::Long);
        assert_eq!(parse("note · horizon=LONG · trailing"), Horizon::Long);
    }

    #[test]
    fn missing_or_freeform_is_unknown() {
        assert_eq!(parse(""), Horizon::Unknown);
        assert_eq!(parse("long horizon thesis"), Horizon::Unknown);
        assert_eq!(parse("HORIZON=LONG"), Horizon::Unknown); // case-sensitive
    }

    #[test]
    fn clean_strips_tag_with_separators() {
        let (h, clean) = parse_with_clean("called the move · horizon=LONG");
        assert_eq!(h, Horizon::Long);
        assert_eq!(clean, "called the move");

        let (h, clean) = parse_with_clean("horizon=SHORT");
        assert_eq!(h, Horizon::Short);
        assert_eq!(clean, "");

        let (h, clean) = parse_with_clean("a · horizon=LONG · b");
        assert_eq!(h, Horizon::Long);
        assert_eq!(clean, "a · b".trim());
    }

    #[test]
    fn thesis_extracts() {
        assert_eq!(
            parse_thesis("thesis=2026-04-22_psyopanime.md"),
            Some("2026-04-22_psyopanime.md".to_string())
        );
        assert_eq!(
            parse_thesis("a thesis=note.md trailing"),
            Some("note.md".to_string())
        );
        assert_eq!(
            parse_thesis("a · thesis=t.md · horizon=LONG"),
            Some("t.md".to_string())
        );
        assert_eq!(parse_thesis(""), None);
        assert_eq!(parse_thesis("thesis="), None);
        assert_eq!(parse_thesis("no tag here"), None);
    }
}
