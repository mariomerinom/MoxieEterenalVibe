//! Compare simulator detection against ground truth.
//! Gate: >=70% detection rate, P&L within 30% of actual.

use mev_capture::types::MevExtraction;

#[derive(Debug)]
pub struct ValidationResult {
    pub total_known: usize,
    pub detected: usize,
    pub detection_rate: f64,
    pub pnl_accuracy: f64, // ratio of simulated vs actual P&L
}

pub fn validate(
    known: &[MevExtraction],
    detected_hashes: &[alloy_primitives::B256],
    simulated_pnl: f64,
) -> ValidationResult {
    let total = known.len();
    let detected = known.iter().filter(|e| detected_hashes.contains(&e.tx_hash)).count();
    let actual_pnl: f64 = known.iter().map(|e| e.profit_eth).sum();
    let pnl_accuracy = if actual_pnl > 0.0 { simulated_pnl / actual_pnl } else { 1.0 };

    ValidationResult {
        total_known: total,
        detected,
        detection_rate: if total > 0 { detected as f64 / total as f64 } else { 1.0 },
        pnl_accuracy,
    }
}
