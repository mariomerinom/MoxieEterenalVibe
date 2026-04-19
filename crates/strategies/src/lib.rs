pub mod traits;
pub mod pool_graph;
pub mod backrun;
pub mod dex_arb;
pub mod liquidation;
// Strategy priority is determined by Phase 2 analysis, not hardcoded.
// These modules are templates — activated only for data-validated opportunities.
