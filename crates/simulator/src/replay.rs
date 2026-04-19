//! revm-based block replay. Fork pre-state, inject hypothetical txs, compute results.
//!
//! Uses AlloyDB to lazily fetch state from an Ethereum RPC at a specific block,
//! wraps it in CacheDB for local caching, and executes transactions via revm.

use alloy_primitives::{Address, U256};
use eyre::{bail, Result};
use revm::{
    context::{BlockEnv, Context, TxEnv},
    context_interface::result::{ExecutionResult, Output},
    database::{AlloyDB, CacheDB},
    database_interface::WrapDatabaseAsync,
    handler::{ExecuteEvm, MainBuilder, MainContext, MainnetEvm},
    primitives::{hardfork::SpecId, TxKind},
};
use tracing::debug;

/// Result of simulating a transaction.
#[derive(Debug, Clone)]
pub struct SimResult {
    pub success: bool,
    pub gas_used: u64,
    pub output: Vec<u8>,
}

/// Type alias for the complex CacheDB type we use.
type ForkedDB = CacheDB<
    WrapDatabaseAsync<
        AlloyDB<
            alloy::network::Ethereum,
            alloy::providers::RootProvider<alloy::network::Ethereum>,
        >,
    >,
>;

/// Type alias for the mainnet context with our forked DB.
type ForkedContext = revm::handler::MainnetContext<ForkedDB>;

/// Replay engine: fork Ethereum state at a block and execute hypothetical transactions.
pub struct BlockReplay {
    rpc_url: String,
}

impl BlockReplay {
    pub fn new(rpc_url: String) -> Self {
        Self { rpc_url }
    }

    /// Fork state at `block_number` and return a ForkedEvm for transaction execution.
    ///
    /// The ForkedEvm lazily fetches state from the RPC as needed and caches it locally.
    /// Set block environment to block_number+1 (the target block for our bundle).
    pub async fn fork_at_block(
        &self,
        block_number: u64,
        base_fee_gwei: f64,
    ) -> Result<ForkedEvm> {
        // Create alloy HTTP provider
        let url = self
            .rpc_url
            .parse()
            .map_err(|e| eyre::eyre!("bad RPC URL: {e}"))?;
        let client = alloy::rpc::client::RpcClient::new_http(url);
        let provider =
            alloy::providers::RootProvider::<alloy::network::Ethereum>::new(client);

        // AlloyDB fetches state lazily from the RPC at the given block
        let alloy_db =
            AlloyDB::new(provider, alloy::eips::BlockId::number(block_number));

        // Wrap async DB into sync DatabaseRef, then wrap in CacheDB for local caching
        let wrapped = WrapDatabaseAsync::new(alloy_db).ok_or_else(|| {
            eyre::eyre!("failed to create WrapDatabaseAsync - no tokio runtime?")
        })?;
        let cache_db = CacheDB::new(wrapped);

        // base_fee in wei (gwei * 1e9)
        let base_fee_wei = (base_fee_gwei * 1e9) as u64;

        // Configure block environment for the NEXT block (where our bundle would land)
        let mut block_env = BlockEnv::default();
        block_env.number = U256::from(block_number + 1);
        block_env.timestamp = U256::from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 12,
        );
        block_env.basefee = base_fee_wei;
        block_env.gas_limit = 30_000_000;

        // Build EVM once, reuse for multiple transactions
        let mut ctx = Context::mainnet().with_db(cache_db);
        ctx.cfg.set_spec(SpecId::CANCUN);
        ctx.block = block_env;

        let evm = ctx.build_mainnet();

        debug!(
            block = block_number,
            target_block = block_number + 1,
            base_fee_wei,
            "forked state for simulation"
        );

        Ok(ForkedEvm { evm })
    }
}

/// A forked EVM instance ready to execute transactions.
///
/// Wraps a MainnetEvm that can execute multiple transactions against the forked state.
pub struct ForkedEvm {
    evm: MainnetEvm<ForkedContext>,
}

impl ForkedEvm {
    /// Execute a transaction against the forked state.
    ///
    /// The EVM is reusable - multiple transactions can be executed against the same fork.
    /// Each call uses `transact` (not `transact_commit`) so state changes are NOT persisted
    /// between calls (read-only simulation).
    pub fn execute_tx(
        &mut self,
        from: Address,
        to: Address,
        calldata: Vec<u8>,
        value: U256,
        gas_limit: u64,
    ) -> Result<SimResult> {
        // Build transaction environment
        let tx_env = TxEnv {
            caller: from,
            kind: TxKind::Call(to),
            data: calldata.into(),
            value,
            gas_limit,
            gas_price: self.evm.ctx.block.basefee as u128,
            ..Default::default()
        };

        let result = self.evm.transact(tx_env);

        match result {
            Ok(exec_result) => {
                let res = exec_result.result;
                match res {
                    ExecutionResult::Success {
                        gas, output, ..
                    } => {
                        let output_bytes = match output {
                            Output::Call(b) => b.to_vec(),
                            Output::Create(b, _) => b.to_vec(),
                        };
                        Ok(SimResult {
                            success: true,
                            gas_used: gas.used(),
                            output: output_bytes,
                        })
                    }
                    ExecutionResult::Revert { gas, output, .. } => {
                        let gu = gas.used();
                        debug!(gas_used = gu, "tx reverted");
                        Ok(SimResult {
                            success: false,
                            gas_used: gu,
                            output: output.to_vec(),
                        })
                    }
                    ExecutionResult::Halt { gas, reason, .. } => {
                        let gu = gas.used();
                        debug!(gas_used = gu, ?reason, "tx halted");
                        Ok(SimResult {
                            success: false,
                            gas_used: gu,
                            output: vec![],
                        })
                    }
                }
            }
            Err(e) => {
                bail!("EVM execution error: {e:?}")
            }
        }
    }

    /// Call getReserves() on a V2 pool to get current reserves.
    /// Returns (reserve0, reserve1) as raw U256 values.
    pub fn get_reserves_v2(&mut self, pool: Address) -> Result<(U256, U256)> {
        // getReserves() selector: 0x0902f1ac
        let calldata = vec![0x09, 0x02, 0xf1, 0xac];

        let result =
            self.execute_tx(Address::ZERO, pool, calldata, U256::ZERO, 100_000)?;

        if !result.success || result.output.len() < 64 {
            bail!("getReserves failed for pool {pool}");
        }

        // Decode: (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)
        // ABI encoded as 3 x 32 bytes
        let reserve0 = U256::from_be_slice(&result.output[0..32]);
        let reserve1 = U256::from_be_slice(&result.output[32..64]);

        Ok((reserve0, reserve1))
    }

    /// Call slot0() on a V3 pool to get current price state.
    /// Returns (sqrtPriceX96, tick).
    pub fn get_slot0_v3(&mut self, pool: Address) -> Result<(U256, i32)> {
        // slot0() selector: 0x3850c7bd
        let calldata = vec![0x38, 0x50, 0xc7, 0xbd];

        let result =
            self.execute_tx(Address::ZERO, pool, calldata, U256::ZERO, 100_000)?;

        if !result.success || result.output.len() < 64 {
            bail!("slot0 failed for pool {pool}");
        }

        // slot0 returns: (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex,
        //                  uint16 observationCardinality, uint16 observationCardinalityNext,
        //                  uint8 feeProtocol, bool unlocked)
        // Each ABI-encoded as a 32-byte word
        let sqrt_price_x96 = U256::from_be_slice(&result.output[0..32]);
        // tick is int24 — sign-extend from the low 3 bytes of word 1
        let tick_raw = U256::from_be_slice(&result.output[32..64]);
        let tick = {
            let low24 = tick_raw.as_limbs()[0] as i32;
            // Sign-extend from 24 bits
            if low24 & 0x800000 != 0 {
                low24 | !0xFFFFFF_i32
            } else {
                low24
            }
        };

        Ok((sqrt_price_x96, tick))
    }

    /// Call liquidity() on a V3 pool.
    /// Returns the current in-range liquidity as U256.
    pub fn get_liquidity_v3(&mut self, pool: Address) -> Result<U256> {
        // liquidity() selector: 0x1a686502
        let calldata = vec![0x1a, 0x68, 0x65, 0x02];

        let result =
            self.execute_tx(Address::ZERO, pool, calldata, U256::ZERO, 100_000)?;

        if !result.success || result.output.len() < 32 {
            bail!("liquidity() failed for pool {pool}");
        }

        let liquidity = U256::from_be_slice(&result.output[0..32]);
        Ok(liquidity)
    }
}
