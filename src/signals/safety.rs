use super::{SignalLayer, SignalScore};

pub struct SafetyChecker;

impl SafetyChecker {
    pub fn new() -> Self {
        Self
    }

    pub fn check_authorities(
        &self,
        mint_authority_active: bool,
        freeze_authority_active: bool,
        has_permanent_delegate: bool,
    ) -> Vec<SignalScore> {
        let mut scores = Vec::new();

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "mint_authority",
            if mint_authority_active { 10 } else { 90 },
            if mint_authority_active {
                "DANGER: Mint authority active — team can print tokens"
            } else {
                "Mint authority renounced"
            },
        ));

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "freeze_authority",
            if freeze_authority_active { 10 } else { 90 },
            if freeze_authority_active {
                "DANGER: Freeze authority active — team can freeze your account"
            } else {
                "Freeze authority renounced"
            },
        ));

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "permanent_delegate",
            if has_permanent_delegate { 0 } else { 95 },
            if has_permanent_delegate {
                "CRITICAL: Token-2022 Permanent Delegate — tokens can be burned from your wallet"
            } else {
                "No permanent delegate extension"
            },
        ));

        scores
    }

    pub fn check_bundled_launch(&self, deployer: &str, first_buyers: &[&str]) -> SignalScore {
        let deployer_in_top3 = first_buyers.iter().take(3).any(|b| *b == deployer);
        let deployer_bought = first_buyers.iter().any(|b| *b == deployer);

        let (score, details) = if deployer_in_top3 {
            (
                5,
                "CRITICAL: Deployer is among first 3 buyers — likely bundled launch",
            )
        } else if deployer_bought {
            (30, "WARNING: Deployer bought their own token")
        } else {
            (85, "Clean launch — deployer not among early buyers")
        };

        SignalScore::new(SignalLayer::Safety, "bundled_launch", score, details)
    }
}
