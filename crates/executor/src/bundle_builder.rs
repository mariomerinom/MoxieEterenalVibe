//! Action-to-Bundle conversion.
//!
//! Bridges the Strategy trait's `Action` (calldata + parameters) to the
//! FlashbotsExecutor's `Bundle` (signed RLP-encoded transactions).

use alloy::eips::Encodable2718;
use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::providers::Provider;
use alloy::signers::local::LocalSigner;
use alloy_primitives::U256;
use eyre::Result;
use tracing::debug;

use crate::flashbots::Bundle;

/// Builds signed transaction bundles from strategy Actions.
pub struct BundleBuilder<P: Provider + Clone> {
    wallet: EthereumWallet,
    provider: P,
    chain_id: u64,
}

impl<P: Provider + Clone> BundleBuilder<P> {
    /// Create a new BundleBuilder.
    ///
    /// `trading_key_hex` is the private key for the EOA that owns the MevBot contract.
    /// This is NOT the Flashbots signing key.
    pub fn new(trading_key_hex: &str, provider: P, chain_id: u64) -> Result<Self> {
        let clean = trading_key_hex
            .strip_prefix("0x")
            .unwrap_or(trading_key_hex);
        let signer: LocalSigner<k256::ecdsa::SigningKey> = clean.parse()?;
        let wallet = EthereumWallet::new(signer);
        Ok(Self {
            wallet,
            provider,
            chain_id,
        })
    }

    /// Convert a strategy Action into a signed Bundle.
    ///
    /// The Action's `to` and `calldata` become the transaction target and input.
    /// Gas price is set to `base_fee * 2` with zero priority fee (builder tip
    /// goes via the contract's `payBuilder()`, not gas price).
    pub async fn action_to_bundle(
        &self,
        to: alloy_primitives::Address,
        calldata: Vec<u8>,
        value: U256,
        estimated_gas: u64,
        current_block: u64,
        base_fee: u128,
    ) -> Result<Bundle> {
        // Get nonce for trading wallet
        let signer_addr = self.wallet.default_signer().address();
        let nonce = self
            .provider
            .get_transaction_count(signer_addr)
            .await?;

        // Build EIP-1559 transaction
        let max_fee = base_fee * 2;
        let gas_limit = (estimated_gas as f64 * 1.3) as u64; // 30% safety margin

        let tx = alloy::rpc::types::TransactionRequest::default()
            .with_to(to)
            .with_input(calldata)
            .with_value(value)
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee)
            .with_max_priority_fee_per_gas(0) // tip via contract, not gas
            .with_nonce(nonce)
            .with_chain_id(self.chain_id);

        // Sign and RLP encode
        let tx_envelope = tx.build(&self.wallet).await?;
        let encoded = tx_envelope.encoded_2718();
        let hex_tx = format!("0x{}", hex::encode(&encoded));

        debug!(
            nonce,
            gas_limit,
            max_fee,
            block = current_block + 1,
            "built signed bundle tx"
        );

        Ok(Bundle::arb(hex_tx, current_block))
    }
}
