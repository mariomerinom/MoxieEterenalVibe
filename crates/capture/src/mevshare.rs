//! MEV-Share SSE stream client. Captures transaction hints for backrun opportunities.
//! Endpoint: https://mev-share.flashbots.net (Ethereum L1 only).

use eyre::Result;
use crate::types::MevShareHint;

const DEFAULT_SSE_URL: &str = "https://mev-share.flashbots.net";

pub struct MevShareCapture {
    sse_url: String,
}

impl MevShareCapture {
    pub fn new(sse_url: Option<String>) -> Self {
        Self {
            sse_url: sse_url.unwrap_or_else(|| DEFAULT_SSE_URL.to_string()),
        }
    }

    /// Stream hints indefinitely. Sends parsed hints to the channel.
    pub async fn stream(
        &self,
        _tx: tokio::sync::mpsc::Sender<MevShareHint>,
    ) -> Result<()> {
        // TODO:
        // 1. Connect to SSE endpoint via eventsource-client
        // 2. Parse "transaction" events -> MevShareHint
        // 3. Send to channel for storage + strategy evaluation
        // 4. Reconnect on disconnect with exponential backoff
        tracing::info!(url = %self.sse_url, "connecting to MEV-Share SSE");
        todo!("implement SSE streaming")
    }
}
