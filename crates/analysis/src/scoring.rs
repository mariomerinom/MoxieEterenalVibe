//! Opportunity scoring and ranking.
//! Composite score based on market size, competition, infra requirements, and decay risk.
//! This is the decision engine — only opportunities that score above threshold get strategies built.

use eyre::Result;
use serde::Serialize;

use crate::concentration::ConcentrationReport;
use crate::profitability::ProfitabilityReport;

#[derive(Debug, Serialize)]
pub struct ScoredOpportunity {
    pub chain: String,
    pub mev_type: String,
    pub score: f64,               // composite 0-100
    pub monthly_profit_eth: f64,
    pub herfindahl: f64,
    pub competition_intensity: f64, // 0=empty, 1=saturated
    pub infra_complexity: f64,      // 0=simple, 1=requires co-location
    pub trend: f64,                 // positive=growing, negative=shrinking
    pub verdict: Verdict,
}

#[derive(Debug, Serialize)]
pub enum Verdict {
    Go,           // build a strategy for this
    Investigate,  // promising but needs deeper analysis
    Skip,         // not viable
}

pub fn score_opportunities(
    concentration: &[ConcentrationReport],
    profitability: &[ProfitabilityReport],
) -> Result<Vec<ScoredOpportunity>> {
    // TODO:
    // 1. Join concentration + profitability by (chain, mev_type)
    // 2. Compute composite score:
    //    - Market size weight: 40% (monthly_profit_eth normalized)
    //    - Competition weight: 30% (inverse of herfindahl, penalize monopolies)
    //    - Infra complexity weight: 20% (L2 < L1 for latency sensitivity)
    //    - Trend weight: 10% (growing > stable > shrinking)
    // 3. Rank by score, assign verdict based on thresholds
    //    - Go: score > 60 AND monthly_profit > $20K AND herfindahl < 0.3
    //    - Investigate: score > 40
    //    - Skip: everything else
    todo!("implement opportunity scoring")
}
