//! Flashbots relay bundle submission.
//!
//! Supports eth_sendBundle and eth_callBundle across multiple relays
//! (Flashbots, Beaverbuild, Titan). Bundles are signed with a reputation
//! key per the Flashbots authentication spec.

use alloy_primitives::{keccak256, Address};
use eyre::{bail, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Raw signed transactions + targeting metadata for a Flashbots bundle.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Bundle {
    /// RLP-encoded signed transactions (hex with 0x prefix).
    pub txs: Vec<String>,
    /// Target block number (hex with 0x prefix).
    pub block_number: String,
    /// Optional: don't include before this unix timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_timestamp: Option<u64>,
    /// Optional: don't include after this unix timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_timestamp: Option<u64>,
    /// Optional: revert-protected tx hashes — bundle still lands if these revert.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reverting_tx_hashes: Option<Vec<String>>,
}

/// eth_sendBundle response.
#[derive(Debug, Clone, Deserialize)]
pub struct SendBundleResponse {
    #[serde(rename = "bundleHash")]
    pub bundle_hash: Option<String>,
}

/// eth_callBundle response — simulation result.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallBundleResponse {
    pub bundle_hash: Option<String>,
    pub coinbase_diff: Option<String>,
    pub eth_sent_to_coinbase: Option<String>,
    pub gas_fees: Option<String>,
    pub results: Option<Vec<TxSimResult>>,
    pub total_gas_used: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxSimResult {
    pub tx_hash: Option<String>,
    pub gas_used: Option<u64>,
    pub gas_price: Option<String>,
    pub coinbase_diff: Option<String>,
    pub error: Option<String>,
    pub revert: Option<String>,
    pub value: Option<String>,
}

/// JSON-RPC envelope.
#[derive(Debug, Serialize)]
struct JsonRpcRequest<T: Serialize> {
    jsonrpc: &'static str,
    id: u64,
    method: String,
    params: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    #[allow(dead_code)]
    id: Option<u64>,
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: Option<i64>,
    message: String,
}

// ---------------------------------------------------------------------------
// Relay config
// ---------------------------------------------------------------------------

/// Known builder relay endpoints.
#[derive(Debug, Clone)]
pub struct Relay {
    pub name: String,
    pub url: String,
    /// Whether this relay requires X-Flashbots-Signature header.
    pub requires_signature: bool,
}

impl Relay {
    /// Flashbots Protect relay (mainnet).
    pub fn flashbots() -> Self {
        Self {
            name: "flashbots".into(),
            url: "https://relay.flashbots.net".into(),
            requires_signature: true,
        }
    }

    /// Beaverbuild relay.
    pub fn beaverbuild() -> Self {
        Self {
            name: "beaverbuild".into(),
            url: "https://rpc.beaverbuild.org".into(),
            requires_signature: false,
        }
    }

    /// Titan builder relay.
    pub fn titan() -> Self {
        Self {
            name: "titan".into(),
            url: "https://rpc.titanbuilder.xyz".into(),
            requires_signature: false,
        }
    }

    /// rsync builder relay.
    pub fn rsync() -> Self {
        Self {
            name: "rsync".into(),
            url: "https://rsync-builder.xyz".into(),
            requires_signature: false,
        }
    }

    /// All major relays.
    pub fn all() -> Vec<Self> {
        vec![
            Self::flashbots(),
            Self::beaverbuild(),
            Self::titan(),
            Self::rsync(),
        ]
    }
}

// ---------------------------------------------------------------------------
// Signing key
// ---------------------------------------------------------------------------

/// A secp256k1 signing key for Flashbots bundle authentication.
/// This is NOT the trading key — it's a separate reputation key.
#[derive(Clone)]
pub struct SigningKey {
    secret: alloy_primitives::FixedBytes<32>,
}

impl SigningKey {
    /// Create from a 32-byte hex private key (with or without 0x prefix).
    pub fn from_hex(hex_key: &str) -> Result<Self> {
        let clean = hex_key.strip_prefix("0x").unwrap_or(hex_key);
        let bytes = hex::decode(clean)?;
        if bytes.len() != 32 {
            bail!("signing key must be 32 bytes, got {}", bytes.len());
        }
        let mut buf = [0u8; 32];
        buf.copy_from_slice(&bytes);
        Ok(Self {
            secret: alloy_primitives::FixedBytes(buf),
        })
    }

    /// Create from environment variable FLASHBOTS_SIGNING_KEY.
    pub fn from_env() -> Result<Self> {
        let key = std::env::var("FLASHBOTS_SIGNING_KEY")
            .map_err(|_| eyre::eyre!("FLASHBOTS_SIGNING_KEY env var not set"))?;
        Self::from_hex(&key)
    }

    /// Generate a random key (useful for first-time setup).
    pub fn random() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        // Simple deterministic seed from timestamp + pid for non-crypto use.
        // In production, use a proper RNG.
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let hash = keccak256(seed.to_le_bytes());
        Self { secret: hash }
    }

    /// Sign a message payload, returning (signature_hex, address).
    /// Uses EIP-191 personal_sign: keccak256("\x19Ethereum Signed Message:\n" + len + message).
    pub fn sign_message(&self, message: &[u8]) -> Result<(String, Address)> {
        use alloy::signers::{
            k256::ecdsa::SigningKey as K256Key, local::LocalSigner, SignerSync,
        };

        let secret_bytes: &[u8; 32] = self.secret.as_ref();
        let k256_key = K256Key::from_bytes(secret_bytes.into())
            .map_err(|e| eyre::eyre!("invalid signing key: {e}"))?;
        let wallet = LocalSigner::from_signing_key(k256_key);
        let address = wallet.address();

        // Flashbots wants: keccak256(body) signed, then header = address:signature
        let sig = wallet
            .sign_message_sync(message)
            .map_err(|e| eyre::eyre!("signing failed: {e}"))?;

        let sig_hex = format!("0x{}", hex::encode(sig.as_bytes()));
        Ok((sig_hex, address))
    }

    /// Produce the X-Flashbots-Signature header value for a JSON body.
    pub fn header_value(&self, body: &[u8]) -> Result<String> {
        let digest = keccak256(body);
        let (sig, addr) = self.sign_message(digest.as_ref())?;
        Ok(format!("{addr:?}:{sig}"))
    }
}

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningKey")
            .field("secret", &"[redacted]")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Multi-relay Flashbots executor. Submits bundles to all configured relays
/// in parallel and returns results from each.
pub struct FlashbotsExecutor {
    relays: Vec<Relay>,
    signing_key: SigningKey,
    client: reqwest::Client,
}

#[derive(Debug)]
pub struct RelayResult {
    pub relay: String,
    pub success: bool,
    pub bundle_hash: Option<String>,
    pub error: Option<String>,
}

impl FlashbotsExecutor {
    /// Create with default relays (Flashbots + Beaverbuild + Titan + rsync).
    pub fn new(signing_key: SigningKey) -> Self {
        Self {
            relays: Relay::all(),
            signing_key,
            client: reqwest::Client::new(),
        }
    }

    /// Create with specific relays.
    pub fn with_relays(signing_key: SigningKey, relays: Vec<Relay>) -> Self {
        Self {
            relays,
            signing_key,
            client: reqwest::Client::new(),
        }
    }

    /// Simulate a bundle via eth_callBundle on the Flashbots relay.
    /// Returns simulation results without submitting.
    pub async fn simulate_bundle(
        &self,
        bundle: &Bundle,
        state_block: &str,
    ) -> Result<CallBundleResponse> {
        let params = serde_json::json!({
            "txs": bundle.txs,
            "blockNumber": bundle.block_number,
            "stateBlockNumber": state_block,
        });

        let request = JsonRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "eth_callBundle".to_string(),
            params: vec![params],
        };

        let body = serde_json::to_vec(&request)?;
        let sig_header = self.signing_key.header_value(&body)?;

        let resp = self
            .client
            .post(&Relay::flashbots().url)
            .header("Content-Type", "application/json")
            .header("X-Flashbots-Signature", sig_header)
            .body(body)
            .send()
            .await?;

        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            bail!("Flashbots simulate HTTP {status}: {text}");
        }

        let rpc_resp: JsonRpcResponse<CallBundleResponse> = serde_json::from_str(&text)?;

        if let Some(err) = rpc_resp.error {
            bail!(
                "Flashbots simulate RPC error {}: {}",
                err.code.unwrap_or(-1),
                err.message
            );
        }

        rpc_resp
            .result
            .ok_or_else(|| eyre::eyre!("empty simulation result"))
    }

    /// Submit a bundle to all configured relays in parallel.
    /// Returns a result from each relay.
    pub async fn send_bundle(&self, bundle: &Bundle) -> Vec<RelayResult> {
        let mut handles = Vec::new();

        for relay in &self.relays {
            let relay = relay.clone();
            let bundle = bundle.clone();
            let client = self.client.clone();
            let signing_key = self.signing_key.clone();

            let handle = tokio::spawn(async move {
                match send_to_relay(&client, &relay, &bundle, &signing_key).await {
                    Ok(resp) => {
                        info!(relay = %relay.name, hash = ?resp.bundle_hash, "bundle accepted");
                        RelayResult {
                            relay: relay.name,
                            success: true,
                            bundle_hash: resp.bundle_hash,
                            error: None,
                        }
                    }
                    Err(e) => {
                        warn!(relay = %relay.name, error = %e, "bundle rejected");
                        RelayResult {
                            relay: relay.name,
                            success: false,
                            bundle_hash: None,
                            error: Some(e.to_string()),
                        }
                    }
                }
            });

            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(RelayResult {
                    relay: "unknown".into(),
                    success: false,
                    bundle_hash: None,
                    error: Some(format!("task panicked: {e}")),
                }),
            }
        }

        results
    }

    /// Convenience: simulate then send if simulation is profitable.
    /// Returns None if simulation shows the bundle would fail or be unprofitable.
    pub async fn simulate_and_send(
        &self,
        bundle: &Bundle,
        state_block: &str,
        min_profit_wei: u128,
    ) -> Result<Option<Vec<RelayResult>>> {
        // Simulate first
        let sim = self.simulate_bundle(bundle, state_block).await?;

        // Check for reverts
        if let Some(results) = &sim.results {
            for tx_result in results {
                if tx_result.revert.is_some() || tx_result.error.is_some() {
                    debug!(
                        revert = ?tx_result.revert,
                        error = ?tx_result.error,
                        "bundle simulation reverted"
                    );
                    return Ok(None);
                }
            }
        }

        // Check profitability (coinbase_diff = miner payment = our tip)
        // The actual profit to us is what's left AFTER the tip.
        // We check total gas fees + eth_sent_to_coinbase vs our min threshold.
        if let Some(coinbase_diff) = &sim.coinbase_diff {
            let diff = u128::from_str_radix(
                coinbase_diff.strip_prefix("0x").unwrap_or(coinbase_diff),
                16,
            )
            .unwrap_or(0);
            if diff < min_profit_wei {
                debug!(
                    coinbase_diff = %coinbase_diff,
                    min = %min_profit_wei,
                    "bundle not profitable enough"
                );
                return Ok(None);
            }
        }

        info!(
            sim_gas = ?sim.total_gas_used,
            coinbase_diff = ?sim.coinbase_diff,
            "simulation passed, submitting to all relays"
        );

        Ok(Some(self.send_bundle(bundle).await))
    }
}

/// Send a bundle to a single relay.
async fn send_to_relay(
    client: &reqwest::Client,
    relay: &Relay,
    bundle: &Bundle,
    signing_key: &SigningKey,
) -> Result<SendBundleResponse> {
    let params = serde_json::json!({
        "txs": bundle.txs,
        "blockNumber": bundle.block_number,
        "minTimestamp": bundle.min_timestamp,
        "maxTimestamp": bundle.max_timestamp,
        "revertingTxHashes": bundle.reverting_tx_hashes,
    });

    let request = JsonRpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "eth_sendBundle".to_string(),
        params: vec![params],
    };

    let body = serde_json::to_vec(&request)?;

    let mut req = client
        .post(&relay.url)
        .header("Content-Type", "application/json");

    if relay.requires_signature {
        let sig_header = signing_key.header_value(&body)?;
        req = req.header("X-Flashbots-Signature", sig_header);
    }

    let resp = req.body(body).send().await?;
    let status = resp.status();
    let text = resp.text().await?;

    if !status.is_success() {
        bail!("{} HTTP {status}: {text}", relay.name);
    }

    let rpc_resp: JsonRpcResponse<SendBundleResponse> = serde_json::from_str(&text)?;

    if let Some(err) = rpc_resp.error {
        bail!(
            "{} RPC error {}: {}",
            relay.name,
            err.code.unwrap_or(-1),
            err.message
        );
    }

    rpc_resp
        .result
        .ok_or_else(|| eyre::eyre!("{}: empty response", relay.name))
}

// ---------------------------------------------------------------------------
// Bundle builder helpers
// ---------------------------------------------------------------------------

impl Bundle {
    /// Create a bundle targeting the next block.
    pub fn for_next_block(txs: Vec<String>, current_block: u64) -> Self {
        Self {
            txs,
            block_number: format!("0x{:x}", current_block + 1),
            min_timestamp: None,
            max_timestamp: None,
            reverting_tx_hashes: None,
        }
    }

    /// Create a sandwich bundle: [front_run, victim_tx, back_run].
    pub fn sandwich(
        front_run: String,
        victim_tx: String,
        back_run: String,
        current_block: u64,
    ) -> Self {
        Self::for_next_block(vec![front_run, victim_tx, back_run], current_block)
    }

    /// Create an arb bundle: [arb_tx] targeting next block.
    pub fn arb(arb_tx: String, current_block: u64) -> Self {
        Self::for_next_block(vec![arb_tx], current_block)
    }

    /// Create a backrun bundle: [target_tx, our_backrun].
    pub fn backrun(target_tx: String, backrun_tx: String, current_block: u64) -> Self {
        let mut bundle = Self::for_next_block(vec![target_tx, backrun_tx], current_block);
        // The target tx might revert — we still want our backrun to land
        bundle.reverting_tx_hashes = Some(vec![]);
        bundle
    }
}
