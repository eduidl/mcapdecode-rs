use std::{path::PathBuf, str::FromStr};

use anyhow::{Result, bail};
use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use mcap2arrow::{
    McapReader,
    arrow::{
        ArrayPolicy, FlattenPolicy, ListPolicy, MapPolicy, StructPolicy, flatten_record_batch,
        project_record_batch,
    },
};

use crate::{
    format::OutputFormat,
    writer::{CsvWriter, JsonlWriter, ParquetWriter, RecordBatchWriter},
};

#[derive(Args)]
pub struct ConvertArgs {
    /// Path to the mcap file
    input: PathBuf,

    /// Output format
    #[arg(short, long, value_enum, default_value_t = OutputFormat::Jsonl)]
    format: OutputFormat,

    /// Filter by topic name
    #[arg(short, long)]
    topic: String,

    /// Output file path (stdout if not specified)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Policy for List columns: drop | keep | flatten-fixed
    #[arg(long, value_parser = parse_list_policy)]
    list_policy: Option<ListPolicy>,

    /// Number of columns generated when --list-policy flatten-fixed is used
    #[arg(long)]
    list_flatten_size: Option<usize>,

    /// Policy for FixedSizeList columns: drop | keep | flatten
    #[arg(long, value_parser = parse_array_policy)]
    array_policy: Option<ArrayPolicy>,

    /// Policy for Map columns: drop | keep
    #[arg(long, value_parser = parse_map_policy)]
    map_policy: Option<MapPolicy>,

    /// Comma-separated list of fields to include in the output (all fields if not specified).
    /// Field paths are applied before flattening (e.g. "x,y,z" or "position.x,position.y").
    #[arg(long, value_delimiter = ',')]
    fields: Option<Vec<String>>,

    /// Enable parallel chunk decompression and decoding.
    #[arg(short, long)]
    parallel: bool,
}

impl ConvertArgs {
    pub fn run(self) -> Result<()> {
        let reader = McapReader::builder()
            .with_default_decoders()
            .with_parallel(self.parallel)
            .build();
        let flatten_policy = self.flatten_policy()?;

        let count = reader.message_count(&self.input, &self.topic)?;
        let pb = ProgressBar::new(count);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}, ETA: {eta})",
            )?
            .progress_chars("=>-"),
        );

        let mut writer: Box<dyn RecordBatchWriter> = match self.format {
            OutputFormat::Jsonl => Box::new(JsonlWriter::new(self.output.as_deref())?),
            OutputFormat::Csv => Box::new(CsvWriter::new(self.output.as_deref())?),
            OutputFormat::Parquet => {
                let path = self
                    .output
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("Parquet output requires -o <file>"))?;
                Box::new(ParquetWriter::new(path)?)
            }
        };
        let mut dropped_warned = false;

        reader.for_each_record_batch(&self.input, &self.topic, |batch| {
            let projected = if let Some(fields) = &self.fields {
                project_record_batch(&batch, fields)?
            } else {
                batch
            };
            let (flat_batch, dropped_columns) =
                flatten_record_batch(&projected, None, &flatten_policy)?;
            if !dropped_warned && !dropped_columns.is_empty() {
                dropped_warned = true;
                eprintln!(
                    "Warning: output policy skipped columns: {}",
                    dropped_columns.join(", ")
                );
            }
            let n = flat_batch.num_rows() as u64;
            writer.write_batch(flat_batch)?;
            pb.inc(n);
            Ok(())
        })?;

        writer.finish()?;
        pb.finish_with_message("done");
        Ok(())
    }

    fn flatten_policy(&self) -> Result<FlattenPolicy> {
        let mut policy = self.format.default_policy();

        if let Some(v) = self.list_policy {
            policy.list = v;
        }
        if let Some(v) = self.list_flatten_size {
            if policy.list != ListPolicy::FlattenFixed {
                bail!("--list-flatten-size requires --list-policy flatten-fixed");
            }
            policy.list_flatten_fixed_size = v;
        }
        if let Some(v) = self.array_policy {
            policy.array = v;
        }
        if let Some(v) = self.map_policy {
            policy.map = v;
        }
        policy.struct_ = match self.format {
            OutputFormat::Jsonl => StructPolicy::Keep,
            OutputFormat::Csv | OutputFormat::Parquet => StructPolicy::Flatten,
        };

        Ok(policy)
    }
}

fn parse_list_policy(raw: &str) -> Result<ListPolicy, String> {
    ListPolicy::from_str(raw)
}

fn parse_array_policy(raw: &str) -> Result<ArrayPolicy, String> {
    ArrayPolicy::from_str(raw)
}

fn parse_map_policy(raw: &str) -> Result<MapPolicy, String> {
    MapPolicy::from_str(raw)
}
