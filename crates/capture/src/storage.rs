//! Parquet + DuckDB storage layer.
//!
//! Raw data lands as Parquet files partitioned by chain and date.
//! DuckDB provides SQL analytics over Parquet without a database server.
//!
//! Directory layout:
//!   data/blocks/{chain}/{date}_{batch}.parquet
//!   data/transactions/{chain}/{date}_{batch}.parquet
//!   data/events/swaps/{chain}/{date}_{batch}.parquet
//!   data/events/liquidations/{chain}/{date}_{batch}.parquet
//!   data/mev.duckdb

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use eyre::Result;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::types::{BlockData, LiquidationEvent, SwapEvent};

static BATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_batch_id() -> u64 {
    BATCH_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub struct Storage {
    data_dir: PathBuf,
    duckdb_path: PathBuf,
    block_buffer: Vec<BlockData>,
    swap_buffer: Vec<SwapEvent>,
    liquidation_buffer: Vec<LiquidationEvent>,
    flush_threshold: usize,
}

impl Storage {
    pub fn new(data_dir: PathBuf, duckdb_path: PathBuf, flush_threshold: usize) -> Self {
        Self {
            data_dir,
            duckdb_path,
            block_buffer: Vec::new(),
            swap_buffer: Vec::new(),
            liquidation_buffer: Vec::new(),
            flush_threshold,
        }
    }

    pub fn buffer_block(&mut self, block: BlockData) -> Result<()> {
        self.block_buffer.push(block);
        if self.block_buffer.len() >= self.flush_threshold {
            self.flush_blocks()?;
        }
        Ok(())
    }

    pub fn buffer_swaps(&mut self, swaps: Vec<SwapEvent>) -> Result<()> {
        self.swap_buffer.extend(swaps);
        if self.swap_buffer.len() >= self.flush_threshold * 10 {
            self.flush_swaps()?;
        }
        Ok(())
    }

    pub fn buffer_liquidations(&mut self, liquidations: Vec<LiquidationEvent>) -> Result<()> {
        self.liquidation_buffer.extend(liquidations);
        if self.liquidation_buffer.len() >= self.flush_threshold {
            self.flush_liquidations()?;
        }
        Ok(())
    }

    // ================================================================
    // Flush: blocks + transactions
    // ================================================================

    pub fn flush_blocks(&mut self) -> Result<()> {
        if self.block_buffer.is_empty() {
            return Ok(());
        }
        let blocks = std::mem::take(&mut self.block_buffer);
        tracing::info!(count = blocks.len(), "flushing blocks to parquet");

        // Group by (chain, date)
        let mut groups: HashMap<(String, String), Vec<&BlockData>> = HashMap::new();
        for block in &blocks {
            let date = block.timestamp.format("%Y-%m-%d").to_string();
            let chain = block.chain.as_str().to_string();
            groups.entry((chain, date)).or_default().push(block);
        }

        for ((chain, date), group) in &groups {
            let batch_id = next_batch_id();

            // Write blocks parquet
            self.write_blocks_parquet(chain, date, batch_id, group)?;

            // Transaction parquet disabled — events are extracted in-memory
            // and stored separately in swaps/liquidations parquet.
            // self.write_transactions_parquet(chain, date, batch_id, group)?;
        }

        Ok(())
    }

    fn write_blocks_parquet(
        &self,
        chain: &str,
        date: &str,
        batch_id: u64,
        blocks: &[&BlockData],
    ) -> Result<()> {
        let schema = blocks_schema();

        let block_numbers: Vec<u64> = blocks.iter().map(|b| b.number).collect();
        let hashes: Vec<String> = blocks.iter().map(|b| format!("{:#x}", b.hash)).collect();
        let parent_hashes: Vec<String> = blocks.iter().map(|b| format!("{:#x}", b.parent_hash)).collect();
        let timestamps: Vec<i64> = blocks.iter().map(|b| b.timestamp.timestamp()).collect();
        let base_fees: Vec<Option<f64>> = blocks
            .iter()
            .map(|b| b.base_fee.map(|f| f.to::<u128>() as f64 / 1e9))
            .collect();
        let gas_used: Vec<u64> = blocks.iter().map(|b| b.gas_used).collect();
        let gas_limit: Vec<u64> = blocks.iter().map(|b| b.gas_limit).collect();
        let tx_counts: Vec<u32> = blocks.iter().map(|b| b.tx_count as u32).collect();

        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(UInt64Array::from(block_numbers)),
                Arc::new(StringArray::from(hashes)),
                Arc::new(StringArray::from(parent_hashes)),
                Arc::new(
                    TimestampSecondArray::from(timestamps)
                        .with_timezone_opt(Some("UTC")),
                ),
                Arc::new(Float64Array::from(base_fees)),
                Arc::new(UInt64Array::from(gas_used)),
                Arc::new(UInt64Array::from(gas_limit)),
                Arc::new(UInt32Array::from(tx_counts)),
            ],
        )?;

        let path = self.parquet_path("blocks", chain, date, batch_id);
        write_parquet(&path, &batch)
    }

    fn write_transactions_parquet(
        &self,
        chain: &str,
        date: &str,
        batch_id: u64,
        blocks: &[&BlockData],
    ) -> Result<()> {
        // Flatten all transactions from blocks
        let mut block_numbers = Vec::new();
        let mut tx_hashes = Vec::new();
        let mut tx_indices = Vec::new();
        let mut froms = Vec::new();
        let mut tos: Vec<Option<String>> = Vec::new();
        let mut values = Vec::new();
        let mut gas_prices = Vec::new();
        let mut max_fees: Vec<Option<f64>> = Vec::new();
        let mut max_priorities: Vec<Option<f64>> = Vec::new();
        let mut gas_used_arr = Vec::new();
        let mut input_sizes = Vec::new();
        let mut successes = Vec::new();

        for block in blocks {
            for tx in &block.transactions {
                block_numbers.push(block.number);
                tx_hashes.push(format!("{:#x}", tx.hash));
                tx_indices.push(tx.tx_index as u32);
                froms.push(format!("{:#x}", tx.from));
                tos.push(tx.to.map(|a| format!("{:#x}", a)));
                values.push(format!("{}", tx.value));
                gas_prices.push(tx.gas_price.to::<u128>() as f64 / 1e9);
                max_fees.push(tx.max_fee_per_gas.map(|f| f.to::<u128>() as f64 / 1e9));
                max_priorities.push(
                    tx.max_priority_fee_per_gas
                        .map(|f| f.to::<u128>() as f64 / 1e9),
                );
                gas_used_arr.push(tx.gas_used);
                input_sizes.push(tx.input.len() as u32);
                successes.push(tx.success);
            }
        }

        if block_numbers.is_empty() {
            return Ok(());
        }

        let schema = transactions_schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(UInt64Array::from(block_numbers)),
                Arc::new(StringArray::from(tx_hashes)),
                Arc::new(UInt32Array::from(tx_indices)),
                Arc::new(StringArray::from(froms)),
                Arc::new(StringArray::from(tos)),
                Arc::new(StringArray::from(values)),
                Arc::new(Float64Array::from(gas_prices)),
                Arc::new(Float64Array::from(max_fees)),
                Arc::new(Float64Array::from(max_priorities)),
                Arc::new(UInt64Array::from(gas_used_arr)),
                Arc::new(UInt32Array::from(input_sizes)),
                Arc::new(BooleanArray::from(successes)),
            ],
        )?;

        let path = self.parquet_path("transactions", chain, date, batch_id);
        write_parquet(&path, &batch)
    }

    // ================================================================
    // Flush: swaps
    // ================================================================

    pub fn flush_swaps(&mut self) -> Result<()> {
        if self.swap_buffer.is_empty() {
            return Ok(());
        }
        let swaps = std::mem::take(&mut self.swap_buffer);
        tracing::info!(count = swaps.len(), "flushing swaps to parquet");

        let mut groups: HashMap<(String, String), Vec<&SwapEvent>> = HashMap::new();
        // Group by chain — we don't have timestamp on SwapEvent, so use a single date partition
        // based on the fact that we flush per-batch which corresponds to a time range.
        for swap in &swaps {
            let chain = swap.chain.as_str().to_string();
            // We'll use block_number to derive a rough date — for now just use chain grouping
            groups.entry((chain, String::new())).or_default().push(swap);
        }

        for ((chain, _), group) in &groups {
            let batch_id = next_batch_id();
            self.write_swaps_parquet(chain, batch_id, group)?;
        }

        Ok(())
    }

    fn write_swaps_parquet(
        &self,
        chain: &str,
        batch_id: u64,
        swaps: &[&SwapEvent],
    ) -> Result<()> {
        let schema = swaps_schema();

        let block_numbers: Vec<u64> = swaps.iter().map(|s| s.block_number).collect();
        let tx_hashes: Vec<String> = swaps.iter().map(|s| format!("{:#x}", s.tx_hash)).collect();
        let tx_indices: Vec<u32> = swaps.iter().map(|s| s.tx_index as u32).collect();
        let log_indices: Vec<u32> = swaps.iter().map(|s| s.log_index as u32).collect();
        let pools: Vec<String> = swaps.iter().map(|s| format!("{:#x}", s.pool)).collect();
        let protocols: Vec<String> = swaps
            .iter()
            .map(|s| format!("{:?}", s.protocol).to_lowercase())
            .collect();
        let token_ins: Vec<String> = swaps.iter().map(|s| format!("{:#x}", s.token_in)).collect();
        let token_outs: Vec<String> = swaps.iter().map(|s| format!("{:#x}", s.token_out)).collect();
        let amount_ins: Vec<String> = swaps.iter().map(|s| format!("{}", s.amount_in)).collect();
        let amount_outs: Vec<String> = swaps.iter().map(|s| format!("{}", s.amount_out)).collect();
        let senders: Vec<String> = swaps.iter().map(|s| format!("{:#x}", s.sender)).collect();
        let tx_froms: Vec<String> = swaps.iter().map(|s| format!("{:#x}", s.tx_from)).collect();

        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(UInt64Array::from(block_numbers)),
                Arc::new(StringArray::from(tx_hashes)),
                Arc::new(UInt32Array::from(tx_indices)),
                Arc::new(UInt32Array::from(log_indices)),
                Arc::new(StringArray::from(pools)),
                Arc::new(StringArray::from(protocols)),
                Arc::new(StringArray::from(token_ins)),
                Arc::new(StringArray::from(token_outs)),
                Arc::new(StringArray::from(amount_ins)),
                Arc::new(StringArray::from(amount_outs)),
                Arc::new(StringArray::from(senders)),
                Arc::new(StringArray::from(tx_froms)),
            ],
        )?;

        // Use block number range for filename since we don't have date on SwapEvent
        let min_block = swaps.iter().map(|s| s.block_number).min().unwrap_or(0);
        let max_block = swaps.iter().map(|s| s.block_number).max().unwrap_or(0);
        let dir = self
            .data_dir
            .join("events")
            .join("swaps")
            .join(chain);
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{min_block}_{max_block}_{batch_id}.parquet"));
        write_parquet(&path, &batch)
    }

    // ================================================================
    // Flush: liquidations
    // ================================================================

    pub fn flush_liquidations(&mut self) -> Result<()> {
        if self.liquidation_buffer.is_empty() {
            return Ok(());
        }
        let liquidations = std::mem::take(&mut self.liquidation_buffer);
        tracing::info!(count = liquidations.len(), "flushing liquidations to parquet");

        let mut groups: HashMap<String, Vec<&LiquidationEvent>> = HashMap::new();
        for liq in &liquidations {
            groups
                .entry(liq.chain.as_str().to_string())
                .or_default()
                .push(liq);
        }

        for (chain, group) in &groups {
            let batch_id = next_batch_id();
            self.write_liquidations_parquet(chain, batch_id, group)?;
        }

        Ok(())
    }

    fn write_liquidations_parquet(
        &self,
        chain: &str,
        batch_id: u64,
        liqs: &[&LiquidationEvent],
    ) -> Result<()> {
        let schema = liquidations_schema();

        let block_numbers: Vec<u64> = liqs.iter().map(|l| l.block_number).collect();
        let tx_hashes: Vec<String> = liqs.iter().map(|l| format!("{:#x}", l.tx_hash)).collect();
        let tx_indices: Vec<u32> = liqs.iter().map(|l| l.tx_index as u32).collect();
        let protocols: Vec<String> = liqs
            .iter()
            .map(|l| format!("{:?}", l.protocol).to_lowercase())
            .collect();
        let liquidators: Vec<String> = liqs.iter().map(|l| format!("{:#x}", l.liquidator)).collect();
        let borrowers: Vec<String> = liqs.iter().map(|l| format!("{:#x}", l.borrower)).collect();
        let col_assets: Vec<String> = liqs
            .iter()
            .map(|l| format!("{:#x}", l.collateral_asset))
            .collect();
        let debt_assets: Vec<String> = liqs
            .iter()
            .map(|l| format!("{:#x}", l.debt_asset))
            .collect();
        let debts: Vec<String> = liqs
            .iter()
            .map(|l| format!("{}", l.debt_to_cover))
            .collect();
        let collaterals: Vec<String> = liqs
            .iter()
            .map(|l| format!("{}", l.liquidated_collateral))
            .collect();
        let gas_used: Vec<u64> = liqs.iter().map(|l| l.gas_used).collect();
        let gas_prices: Vec<f64> = liqs
            .iter()
            .map(|l| l.gas_price.to::<u128>() as f64 / 1e9)
            .collect();

        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(UInt64Array::from(block_numbers)),
                Arc::new(StringArray::from(tx_hashes)),
                Arc::new(UInt32Array::from(tx_indices)),
                Arc::new(StringArray::from(protocols)),
                Arc::new(StringArray::from(liquidators)),
                Arc::new(StringArray::from(borrowers)),
                Arc::new(StringArray::from(col_assets)),
                Arc::new(StringArray::from(debt_assets)),
                Arc::new(StringArray::from(debts)),
                Arc::new(StringArray::from(collaterals)),
                Arc::new(UInt64Array::from(gas_used)),
                Arc::new(Float64Array::from(gas_prices)),
            ],
        )?;

        let min_block = liqs.iter().map(|l| l.block_number).min().unwrap_or(0);
        let max_block = liqs.iter().map(|l| l.block_number).max().unwrap_or(0);
        let dir = self
            .data_dir
            .join("events")
            .join("liquidations")
            .join(chain);
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{min_block}_{max_block}_{batch_id}.parquet"));
        write_parquet(&path, &batch)
    }

    // ================================================================
    // Flush all
    // ================================================================

    pub fn flush_all(&mut self) -> Result<()> {
        self.flush_blocks()?;
        self.flush_swaps()?;
        self.flush_liquidations()?;
        Ok(())
    }

    // ================================================================
    // DuckDB views
    // ================================================================

    pub fn init_duckdb_views(&self) -> Result<()> {
        let data_dir = self.data_dir.canonicalize().unwrap_or(self.data_dir.clone());
        let data_str = data_dir.display();

        let conn = duckdb::Connection::open(&self.duckdb_path)?;

        conn.execute_batch(&format!(
            "
            CREATE OR REPLACE VIEW blocks AS
            SELECT * FROM read_parquet('{data_str}/blocks/*/*.parquet', union_by_name=true);

            CREATE OR REPLACE VIEW transactions AS
            SELECT * FROM read_parquet('{data_str}/transactions/*/*.parquet', union_by_name=true);

            CREATE OR REPLACE VIEW swaps AS
            SELECT * FROM read_parquet('{data_str}/events/swaps/*/*.parquet', union_by_name=true);

            CREATE OR REPLACE VIEW liquidations AS
            SELECT * FROM read_parquet('{data_str}/events/liquidations/*/*.parquet', union_by_name=true);

            CREATE OR REPLACE VIEW solana_blocks AS
            SELECT * FROM read_parquet('{data_str}/blocks/solana/*.parquet', union_by_name=true);

            CREATE OR REPLACE VIEW solana_transactions AS
            SELECT * FROM read_parquet('{data_str}/transactions/solana/*.parquet', union_by_name=true);

            CREATE OR REPLACE VIEW solana_swaps AS
            SELECT * FROM read_parquet('{data_str}/events/swaps/solana/*.parquet', union_by_name=true);
            "
        ))?;

        tracing::info!("DuckDB views initialized at {}", self.duckdb_path.display());
        Ok(())
    }

    // ================================================================
    // Helpers
    // ================================================================

    fn parquet_path(
        &self,
        category: &str,
        chain: &str,
        date: &str,
        batch_id: u64,
    ) -> PathBuf {
        let dir = self.data_dir.join(category).join(chain);
        fs::create_dir_all(&dir).ok();
        dir.join(format!("{date}_{batch_id}.parquet"))
    }
}

// ================================================================
// Arrow schemas
// ================================================================

fn blocks_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("parent_hash", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        ),
        Field::new("base_fee_gwei", DataType::Float64, true),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("gas_limit", DataType::UInt64, false),
        Field::new("tx_count", DataType::UInt32, false),
    ])
}

fn transactions_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("from_address", DataType::Utf8, false),
        Field::new("to_address", DataType::Utf8, true),
        Field::new("value_wei", DataType::Utf8, false),
        Field::new("gas_price_gwei", DataType::Float64, false),
        Field::new("max_fee_gwei", DataType::Float64, true),
        Field::new("max_priority_fee_gwei", DataType::Float64, true),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("input_size", DataType::UInt32, false),
        Field::new("success", DataType::Boolean, false),
    ])
}

fn swaps_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("pool", DataType::Utf8, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("token_in", DataType::Utf8, false),
        Field::new("token_out", DataType::Utf8, false),
        Field::new("amount_in", DataType::Utf8, false),
        Field::new("amount_out", DataType::Utf8, false),
        Field::new("sender", DataType::Utf8, false),
        Field::new("tx_from", DataType::Utf8, false),
    ])
}

fn liquidations_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("liquidator", DataType::Utf8, false),
        Field::new("borrower", DataType::Utf8, false),
        Field::new("collateral_asset", DataType::Utf8, false),
        Field::new("debt_asset", DataType::Utf8, false),
        Field::new("debt_to_cover", DataType::Utf8, false),
        Field::new("liquidated_collateral", DataType::Utf8, false),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("gas_price_gwei", DataType::Float64, false),
    ])
}

// ================================================================
// Parquet writer helper
// ================================================================

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    tracing::debug!(path = %path.display(), rows = batch.num_rows(), "wrote parquet");
    Ok(())
}
