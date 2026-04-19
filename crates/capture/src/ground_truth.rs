//! Ground truth data from EigenPhi and ZeroMEV.
//! Used to validate simulator accuracy (Phase 3 gate: >=70% detection rate).

use eyre::Result;
use crate::types::{Chain, MevExtraction};

pub struct EigenPhiClient {
    client: reqwest::Client,
    api_key: Option<String>,
}

impl EigenPhiClient {
    pub fn new(api_key: Option<String>) -> Self {
        Self { client: reqwest::Client::new(), api_key }
    }

    pub async fn get_block_mev(&self, _chain: Chain, _block: u64) -> Result<Vec<MevExtraction>> {
        // TODO: GET eigenphi API, parse into MevExtraction
        todo!("implement EigenPhi fetch")
    }
}

pub struct ZeroMevClient {
    client: reqwest::Client,
}

impl ZeroMevClient {
    pub fn new() -> Self {
        Self { client: reqwest::Client::new() }
    }

    pub async fn get_block_mev(&self, _block: u64) -> Result<Vec<MevExtraction>> {
        // TODO: GET zeromev API, parse into MevExtraction
        // Note: ZeroMEV is Ethereum L1 only
        todo!("implement ZeroMEV fetch")
    }
}
