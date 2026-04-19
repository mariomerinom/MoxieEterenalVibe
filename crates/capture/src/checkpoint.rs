//! Checkpoint for resumable backfills.
//! Saves progress as JSON so a crashed/killed ingestion can resume.

use std::path::Path;

use chrono::Utc;
use eyre::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub chain: String,
    pub target_from: u64,
    pub target_to: u64,
    pub last_completed_block: u64,
    pub started_at: String,
    pub updated_at: String,
}

impl Checkpoint {
    pub fn new(chain: &str, from: u64, to: u64) -> Self {
        let now = Utc::now().to_rfc3339();
        Self {
            chain: chain.to_string(),
            target_from: from,
            target_to: to,
            last_completed_block: from.saturating_sub(1),
            started_at: now.clone(),
            updated_at: now,
        }
    }

    pub fn load(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(path)?;
        let cp: Self = serde_json::from_str(&content)?;
        Ok(Some(cp))
    }

    pub fn save(&mut self, path: &Path) -> Result<()> {
        self.updated_at = Utc::now().to_rfc3339();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.last_completed_block >= self.target_to
    }

    pub fn next_block(&self) -> u64 {
        self.last_completed_block + 1
    }

    pub fn remaining(&self) -> u64 {
        if self.is_complete() {
            0
        } else {
            self.target_to - self.last_completed_block
        }
    }
}
