use super::{SignalLayer, SignalScore};
use crate::ingester::{MintInfo, TokenHolderInfo};

pub struct SafetyChecker;

impl SafetyChecker {
    pub fn new() -> Self {
        Self
    }

    /// Analyze a live mint account for safety signals
    pub fn analyze_mint(&self, mint: &MintInfo) -> Vec<SignalScore> {
        let mut scores = Vec::new();

        // Mint authority
        let mint_details = if mint.mint_authority.is_some() {
            format!(
                "DANGER: Mint authority active ({})",
                mint.mint_authority.as_deref().unwrap_or("unknown")
            )
        } else {
            "Mint authority renounced".to_string()
        };
        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "mint_authority",
            if mint.mint_authority.is_some() {
                10
            } else {
                90
            },
            &mint_details,
        ));

        // Freeze authority
        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "freeze_authority",
            if mint.freeze_authority.is_some() {
                10
            } else {
                90
            },
            if mint.freeze_authority.is_some() {
                "DANGER: Freeze authority active — team can freeze your account"
            } else {
                "Freeze authority renounced"
            },
        ));

        // Token-2022 program itself is not a risk — the extensions are.
        // Score neutrally on the standard, then emit per-extension signals below.
        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "token_standard",
            if mint.is_token_2022 { 60 } else { 80 },
            if mint.is_token_2022 {
                "Token-2022 program — extensions evaluated below"
            } else {
                "Standard SPL Token program"
            },
        ));

        if mint.is_token_2022 {
            // Hard vetoes: PermanentDelegate, DefaultAccountState=Frozen, NonTransferable
            if let Some(pd) = &mint.permanent_delegate {
                scores.push(SignalScore::new(
                    SignalLayer::Safety,
                    "permanent_delegate",
                    0,
                    &format!(
                        "VETO: PermanentDelegate set ({}) — can move/burn any holder's tokens",
                        pd
                    ),
                ));
            }
            if mint.default_frozen {
                scores.push(SignalScore::new(
                    SignalLayer::Safety,
                    "default_frozen",
                    0,
                    "VETO: DefaultAccountState=Frozen — new holders blocked from trading",
                ));
            }
            if mint.non_transferable {
                scores.push(SignalScore::new(
                    SignalLayer::Safety,
                    "non_transferable",
                    0,
                    "VETO: NonTransferable mint — tokens cannot be transferred",
                ));
            }

            // Flags: TransferHook, TransferFeeConfig
            if let Some(hook) = &mint.transfer_hook_program {
                scores.push(SignalScore::new(
                    SignalLayer::Safety,
                    "transfer_hook",
                    25,
                    &format!(
                        "WARNING: TransferHook program {} — custom code on every transfer",
                        hook
                    ),
                ));
            }
            if let Some(bps) = mint.transfer_fee_bps {
                let score = if bps >= 1000 {
                    10
                } else if bps >= 500 {
                    30
                } else if bps >= 100 {
                    50
                } else {
                    70
                };
                scores.push(SignalScore::new(
                    SignalLayer::Safety,
                    "transfer_fee",
                    score,
                    &format!(
                        "Transfer fee {}.{}%  — {} bps skimmed on every move",
                        bps / 100,
                        bps % 100,
                        bps
                    ),
                ));
            }

            // If no risky extensions found, celebrate
            let has_risky = mint.permanent_delegate.is_some()
                || mint.default_frozen
                || mint.non_transferable
                || mint.transfer_hook_program.is_some()
                || mint.transfer_fee_bps.is_some();
            if !has_risky {
                scores.push(SignalScore::new(
                    SignalLayer::Safety,
                    "token_2022_extensions",
                    80,
                    &if mint.extensions.is_empty() {
                        "Token-2022 with no extensions — equivalent to classic SPL".to_string()
                    } else {
                        format!(
                            "Token-2022 extensions present but benign: {}",
                            mint.extensions.join(", ")
                        )
                    },
                ));
            }
        }

        scores
    }

    /// Analyze holder distribution for concentration risk
    pub fn analyze_holders(
        &self,
        holders: &[TokenHolderInfo],
        total_supply_ui: f64,
    ) -> Vec<SignalScore> {
        let mut scores = Vec::new();

        if holders.is_empty() || total_supply_ui <= 0.0 {
            scores.push(SignalScore::new(
                SignalLayer::Safety,
                "holder_concentration",
                20,
                "No holder data available",
            ));
            return scores;
        }

        // Top holder concentration
        let top1_pct = if total_supply_ui > 0.0 {
            (holders[0].ui_amount / total_supply_ui) * 100.0
        } else {
            0.0
        };

        let top5_pct: f64 = holders
            .iter()
            .take(5)
            .map(|h| {
                if total_supply_ui > 0.0 {
                    (h.ui_amount / total_supply_ui) * 100.0
                } else {
                    0.0
                }
            })
            .sum();

        let top10_pct: f64 = holders
            .iter()
            .take(10)
            .map(|h| {
                if total_supply_ui > 0.0 {
                    (h.ui_amount / total_supply_ui) * 100.0
                } else {
                    0.0
                }
            })
            .sum();

        // Top 1 holder score
        let top1_score = if top1_pct > 50.0 {
            5
        } else if top1_pct > 30.0 {
            20
        } else if top1_pct > 15.0 {
            50
        } else if top1_pct > 5.0 {
            75
        } else {
            90
        };

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "top_holder",
            top1_score,
            &format!("Top holder owns {:.1}% of supply", top1_pct),
        ));

        // Top 5 concentration score
        let top5_score = if top5_pct > 80.0 {
            10
        } else if top5_pct > 60.0 {
            30
        } else if top5_pct > 40.0 {
            60
        } else {
            85
        };

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "top5_concentration",
            top5_score,
            &format!("Top 5 holders own {:.1}% of supply", top5_pct),
        ));

        // Top 10 concentration
        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "top10_concentration",
            if top10_pct > 90.0 {
                15
            } else if top10_pct > 70.0 {
                40
            } else {
                75
            },
            &format!("Top 10 holders own {:.1}% of supply", top10_pct),
        ));

        // Number of significant holders (holding > 0.1% of supply)
        let significant_holders = holders
            .iter()
            .filter(|h| total_supply_ui > 0.0 && (h.ui_amount / total_supply_ui) > 0.001)
            .count();

        scores.push(SignalScore::new(
            SignalLayer::Safety,
            "holder_count",
            if significant_holders < 5 {
                20
            } else if significant_holders < 20 {
                50
            } else {
                80
            },
            &format!("{} significant holders (>0.1% supply)", significant_holders),
        ));

        scores
    }

    // Keep the original methods for unit tests
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
