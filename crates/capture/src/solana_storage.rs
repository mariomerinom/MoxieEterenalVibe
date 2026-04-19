//! Solana-specific Parquet storage.
//!
//! Writes Solana blocks, transactions, and swap events to Parquet files
//! using the same directory layout as EVM data (partitioned by chain/date).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use eyre::Result;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::solana_types::*;

static SOLANA_BATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_batch_id() -> u64 {
    SOLANA_BATCH_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub struct SolanaStorage {
    data_dir: PathBuf,
    block_buffer: Vec<SolanaBlockData>,
    swap_buffer: Vec<SolanaSwapEvent>,
    flush_threshold: usize,
}

impl SolanaStorage {
    pub fn new(data_dir: PathBuf, flush_threshold: usize) -> Self {
        Self {
            data_dir,
            block_buffer: Vec::new(),
            swap_buffer: Vec::new(),
            flush_threshold,
        }
    }

    pub fn buffer_block(&mut self, block: SolanaBlockData) -> Result<()> {
        self.block_buffer.push(block);
        if self.block_buffer.len() >= self.flush_threshold {
            self.flush_blocks()?;
        }
        Ok(())
    }

    pub fn buffer_swaps(&mut self, swaps: Vec<SolanaSwapEvent>) -> Result<()> {
        self.swap_buffer.extend(swaps);
        if self.swap_buffer.len() >= self.flush_threshold * 10 {
            self.flush_swaps()?;
        }
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        self.flush_blocks()?;
        self.flush_swaps()?;
        Ok(())
    }

    // ── Blocks + Transactions ──

    pub fn flush_blocks(&mut self) -> Result<()> {
        if self.block_buffer.is_empty() {
            return Ok(());
        }
        let blocks = std::mem::take(&mut self.block_buffer);
        tracing::info!(count = blocks.len(), "flushing solana blocks to parquet");

        // Group by date
        let mut groups: HashMap<String, Vec<&SolanaBlockData>> = HashMap::new();
        for block in &blocks {
            let date = block.timestamp.format("%Y-%m-%d").to_string();
            groups.entry(date).or_default().push(block);
        }

        for (date, group) in &groups {
            let batch_id = next_batch_id();
            self.write_blocks_parquet(date, batch_id, group)?;
            // Transaction parquet disabled — swap events extracted in-memory.
            // self.write_transactions_parquet(date, batch_id, group)?;
        }
        Ok(())
    }

    fn write_blocks_parquet(
        &self,
        date: &str,
        batch_id: u64,
        blocks: &[&SolanaBlockData],
    ) -> Result<()> {
        let schema = solana_blocks_schema();

        let slots: Vec<u64> = blocks.iter().map(|b| b.slot).collect();
        let heights: Vec<Option<u64>> = blocks.iter().map(|b| b.block_height).collect();
        let hashes: Vec<String> = blocks.iter().map(|b| b.blockhash.clone()).collect();
        let parent_slots: Vec<u64> = blocks.iter().map(|b| b.parent_slot).collect();
        let timestamps: Vec<i64> = blocks.iter().map(|b| b.timestamp.timestamp()).collect();
        let tx_counts: Vec<u32> = blocks.iter().map(|b| b.tx_count as u32).collect();
        let success_counts: Vec<u32> = blocks.iter().map(|b| b.successful_tx_count as u32).collect();
        let compute_units: Vec<u64> = blocks.iter().map(|b| b.total_compute_units).collect();
        let fees: Vec<u64> = blocks.iter().map(|b| b.total_fees_lamports).collect();

        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(UInt64Array::from(slots)),
                Arc::new(UInt64Array::from(heights.iter().map(|h| h.unwrap_or(0)).collect::<Vec<_>>())),
                Arc::new(StringArray::from(hashes)),
                Arc::new(UInt64Array::from(parent_slots)),
                Arc::new(
                    TimestampSecondArray::from(timestamps)
                        .with_timezone_opt(Some("UTC")),
                ),
                Arc::new(UInt32Array::from(tx_counts)),
                Arc::new(UInt32Array::from(success_counts)),
                Arc::new(UInt64Array::from(compute_units)),
                Arc::new(UInt64Array::from(fees)),
            ],
        )?;

        let path = self.parquet_path("blocks", date, batch_id);
        write_parquet(&path, &batch)
    }

    fn write_transactions_parquet(
        &self,
        date: &str,
        batch_id: u64,
        blocks: &[&SolanaBlockData],
    ) -> Result<()> {
        let schema = solana_transactions_schema();

        let mut slots = Vec::new();
        let mut signatures = Vec::new();
        let mut tx_indices = Vec::new();
        let mut successes = Vec::new();
        let mut fees = Vec::new();
        let mut cus = Vec::new();
        let mut signers = Vec::new();
        let mut num_ixs = Vec::new();
        let mut program_id_strs = Vec::new();

        for block in blocks {
            for tx in &block.transactions {
                slots.push(block.slot);
                signatures.push(tx.signature.clone());
                tx_indices.push(tx.tx_index);
                successes.push(tx.success);
                fees.push(tx.fee_lamports);
                cus.push(tx.compute_units_consumed);
                signers.push(tx.signer.clone());
                num_ixs.push(tx.num_instructions);
                program_id_strs.push(tx.program_ids.join(","));
            }
        }

        if slots.is_empty() {
            return Ok(());
        }

        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(UInt64Array::from(slots)),
                Arc::new(StringArray::from(signatures)),
                Arc::new(UInt32Array::from(tx_indices)),
                Arc::new(BooleanArray::from(successes)),
                Arc::new(UInt64Array::from(fees)),
                Arc::new(UInt64Array::from(cus)),
                Arc::new(StringArray::from(signers)),
                Arc::new(UInt32Array::from(num_ixs)),
                Arc::new(StringArray::from(program_id_strs)),
            ],
        )?;

        let path = self.parquet_path("transactions", date, batch_id);
        write_parquet(&path, &batch)
    }

    // ── Swaps ──

    pub fn flush_swaps(&mut self) -> Result<()> {
        if self.swap_buffer.is_empty() {
            return Ok(());
        }
        let swaps = std::mem::take(&mut self.swap_buffer);
        tracing::info!(count = swaps.len(), "flushing solana swaps to parquet");

        let schema = solana_swaps_schema();
        let batch_id = next_batch_id();

        let slots: Vec<u64> = swaps.iter().map(|s| s.slot).collect();
        let sigs: Vec<String> = swaps.iter().map(|s| s.signature.clone()).collect();
        let tx_idxs: Vec<u32> = swaps.iter().map(|s| s.tx_index).collect();
        let ix_idxs: Vec<u32> = swaps.iter().map(|s| s.instruction_index).collect();
        let pools: Vec<String> = swaps.iter().map(|s| s.pool.clone()).collect();
        let protocols: Vec<String> = swaps.iter().map(|s| s.protocol.as_str().to_string()).collect();
        let in_mints: Vec<String> = swaps.iter().map(|s| s.token_in_mint.clone()).collect();
        let out_mints: Vec<String> = swaps.iter().map(|s| s.token_out_mint.clone()).collect();
        let amounts_in: Vec<String> = swaps.iter().map(|s| s.amount_in.to_string()).collect();
        let amounts_out: Vec<String> = swaps.iter().map(|s| s.amount_out.to_string()).collect();
        let signers: Vec<String> = swaps.iter().map(|s| s.signer.clone()).collect();

        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(UInt64Array::from(slots)),
                Arc::new(StringArray::from(sigs)),
                Arc::new(UInt32Array::from(tx_idxs)),
                Arc::new(UInt32Array::from(ix_idxs)),
                Arc::new(StringArray::from(pools)),
                Arc::new(StringArray::from(protocols)),
                Arc::new(StringArray::from(in_mints)),
                Arc::new(StringArray::from(out_mints)),
                Arc::new(StringArray::from(amounts_in)),
                Arc::new(StringArray::from(amounts_out)),
                Arc::new(StringArray::from(signers)),
            ],
        )?;

        // Use a generic date from the first swap's slot (approximate)
        let path = self.parquet_path("events/swaps", "all", batch_id);
        write_parquet(&path, &batch)
    }

    // ── Helpers ──

    fn parquet_path(&self, category: &str, date: &str, batch_id: u64) -> PathBuf {
        self.data_dir
            .join(category)
            .join("solana")
            .join(format!("{}_{}.parquet", date, batch_id))
    }
}

// ── Arrow Schemas ──

fn solana_blocks_schema() -> Schema {
    Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_height", DataType::UInt64, true),
        Field::new("blockhash", DataType::Utf8, false),
        Field::new("parent_slot", DataType::UInt64, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Second, Some("UTC".into())), false),
        Field::new("tx_count", DataType::UInt32, false),
        Field::new("successful_tx_count", DataType::UInt32, false),
        Field::new("total_compute_units", DataType::UInt64, false),
        Field::new("total_fees_lamports", DataType::UInt64, false),
    ])
}

fn solana_transactions_schema() -> Schema {
    Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("signature", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("success", DataType::Boolean, false),
        Field::new("fee_lamports", DataType::UInt64, false),
        Field::new("compute_units_consumed", DataType::UInt64, false),
        Field::new("signer", DataType::Utf8, false),
        Field::new("num_instructions", DataType::UInt32, false),
        Field::new("program_ids", DataType::Utf8, false),
    ])
}

fn solana_swaps_schema() -> Schema {
    Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("signature", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("instruction_index", DataType::UInt32, false),
        Field::new("pool", DataType::Utf8, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("token_in_mint", DataType::Utf8, false),
        Field::new("token_out_mint", DataType::Utf8, false),
        Field::new("amount_in", DataType::Utf8, false),
        Field::new("amount_out", DataType::Utf8, false),
        Field::new("signer", DataType::Utf8, false),
    ])
}

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    tracing::debug!(path = %path.display(), rows = batch.num_rows(), "wrote parquet");
    Ok(())
}
