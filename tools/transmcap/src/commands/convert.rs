use std::{path::PathBuf, str::FromStr};

use anyhow::Result;
use clap::Args;
use mcapdecode::arrow::{ArrayPolicy, ListPolicy, MapPolicy};

use crate::{
    commands::batch::{TopicSettings, TopicSpec, run_conversion},
    format::OutputFormat,
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

    /// Prefix for the log_time and publish_time metadata columns.
    /// Keeps them apart from payload fields of the same name. Dots are not
    /// allowed because field paths and flattened columns use them as separators.
    #[arg(long, default_value = "@")]
    metadata_prefix: String,

    /// Emit log_time and publish_time as Unix epoch nanoseconds (`int64`).
    #[arg(long)]
    time_ns: bool,

    /// Emit JSONL fields and map entries whose values are null.
    #[arg(long)]
    jsonl_explicit_null: bool,

    /// Enable parallel chunk decompression and decoding.
    #[arg(short, long)]
    parallel: bool,
}

impl ConvertArgs {
    pub fn run(self) -> Result<()> {
        let spec = TopicSpec::new(
            self.topic,
            self.output,
            TopicSettings {
                format: self.format,
                fields: self.fields,
                list_policy: self.list_policy,
                list_flatten_size: self.list_flatten_size,
                array_policy: self.array_policy,
                map_policy: self.map_policy,
                metadata_prefix: self.metadata_prefix,
                time_ns: self.time_ns,
                jsonl_explicit_null: self.jsonl_explicit_null,
            },
        )?;
        run_conversion(&self.input, self.parallel, vec![spec], None)
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
