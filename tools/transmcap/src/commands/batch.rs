use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    fs::File,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use arrow::record_batch::RecordBatch;
use clap::{Args, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use mcapdecode::{
    McapReader, McapReaderError, MessageBatchSchema, MetadataColumns, MetadataTimestampFormat,
    NonFiniteFloats, RecordBatchOptions,
    arrow::{
        ArrayPolicy, FlattenPolicy, ListPolicy, MapPolicy, flatten_record_batch,
        project_record_batch,
    },
    core::DecodedMessage,
};
use serde::Deserialize;

use crate::{
    format::OutputFormat,
    writer::{CsvWriter, JsonlWriter, ParquetWriter, RecordBatchWriter},
};

#[derive(Args)]
pub struct BatchArgs {
    /// Path to the MCAP file
    input: PathBuf,
    /// JSON configuration file
    #[arg(long)]
    config: PathBuf,
    /// Enable parallel decoding
    #[arg(short, long)]
    parallel: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default)]
    defaults: serde_json::Map<String, serde_json::Value>,
    topics: Vec<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    topic: Option<String>,
    output: Option<PathBuf>,
    format: Option<String>,
    fields: Option<Vec<String>>,
    list_policy: Option<String>,
    list_flatten_size: Option<usize>,
    array_policy: Option<String>,
    map_policy: Option<String>,
    metadata_prefix: Option<String>,
    time_ns: Option<bool>,
    jsonl_explicit_null: Option<bool>,
}

pub(super) struct TopicSettings {
    pub format: OutputFormat,
    pub fields: Option<Vec<String>>,
    pub list_policy: Option<ListPolicy>,
    pub list_flatten_size: Option<usize>,
    pub array_policy: Option<ArrayPolicy>,
    pub map_policy: Option<MapPolicy>,
    pub metadata_prefix: String,
    pub time_ns: bool,
    pub jsonl_explicit_null: bool,
}

pub(super) struct TopicSpec {
    pub name: String,
    pub output: Option<PathBuf>,
    pub format: OutputFormat,
    pub fields: Option<Vec<String>>,
    pub policy: FlattenPolicy,
    pub explicit_null: bool,
    pub metadata: MetadataColumns,
}

impl TopicSpec {
    pub(super) fn new(
        name: String,
        output: Option<PathBuf>,
        settings: TopicSettings,
    ) -> Result<Self> {
        if settings.jsonl_explicit_null
            && !matches!(
                settings.format,
                OutputFormat::Jsonl | OutputFormat::JsonlExt
            )
        {
            bail!("--jsonl-explicit-null requires --format jsonl or jsonl-ext");
        }
        let mut policy = settings.format.default_policy();
        if let Some(value) = settings.list_policy {
            policy.list = value;
        }
        if let Some(value) = settings.list_flatten_size {
            if policy.list != ListPolicy::FlattenFixed {
                bail!("--list-flatten-size requires --list-policy flatten-fixed");
            }
            policy.list_flatten_fixed_size = value;
        }
        if let Some(value) = settings.array_policy {
            policy.array = value;
        }
        if let Some(value) = settings.map_policy {
            policy.map = value;
        }
        if settings.metadata_prefix.contains('.') {
            bail!(
                "metadata prefix must not contain '.' because field paths and flattened columns use it as a separator"
            );
        }
        let mut metadata = MetadataColumns::with_prefix(&settings.metadata_prefix);
        if settings.time_ns {
            metadata = metadata.with_timestamp_format(MetadataTimestampFormat::UnixNanoseconds);
        }
        Ok(Self {
            name,
            output,
            format: settings.format,
            fields: settings.fields,
            policy,
            explicit_null: settings.jsonl_explicit_null,
            metadata,
        })
    }
}

struct Topic {
    name: String,
    output: Option<PathBuf>,
    format: OutputFormat,
    fields: Option<Vec<String>>,
    policy: FlattenPolicy,
    explicit_null: bool,
    schema: MessageBatchSchema,
    rows: Vec<DecodedMessage>,
    writer: Option<Box<dyn RecordBatchWriter>>,
    warned: Cell<bool>,
    emitted: bool,
}

impl Topic {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch = match &self.fields {
            Some(fields) => project_record_batch(&batch, fields)?,
            None => batch,
        };
        let (batch, dropped) = flatten_record_batch(&batch, None, &self.policy)?;
        if !dropped.is_empty() && !self.warned.replace(true) {
            eprintln!(
                "Warning: output policy skipped columns for {}: {}",
                self.name,
                dropped.join(", ")
            );
        }
        Ok(batch)
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let batch = self.transform(self.schema.to_record_batch(&self.rows)?)?;
        self.rows.clear();
        self.emitted = true;
        self.writer.as_mut().unwrap().write_batch(batch)
    }
}

impl BatchArgs {
    pub fn run(self) -> Result<()> {
        let config: Config = serde_json::from_reader(
            File::open(&self.config)
                .with_context(|| format!("opening {}", self.config.display()))?,
        )
        .with_context(|| format!("invalid config {}", self.config.display()))?;
        if config.topics.is_empty() {
            bail!("topics must not be empty");
        }
        if config.defaults.contains_key("topic") || config.defaults.contains_key("output") {
            bail!("topic and output must be specified for each topic");
        }
        // Validate even unused default keys.
        let _: Settings =
            serde_json::from_value(config.defaults.clone().into()).context("invalid defaults")?;
        let mut specs = Vec::new();
        for overrides in config.topics {
            let mut values = config.defaults.clone();
            values.extend(overrides);
            let settings: Settings =
                serde_json::from_value(values.into()).context("invalid topic settings")?;
            let name = settings
                .topic
                .filter(|s| !s.is_empty())
                .context("topic must be non-empty")?;
            let output = settings
                .output
                .filter(|p| !p.as_os_str().is_empty())
                .context("output is required")?;
            specs.push(TopicSpec::new(
                name,
                Some(output),
                TopicSettings {
                    format: OutputFormat::from_str(
                        settings.format.as_deref().unwrap_or("jsonl"),
                        true,
                    )
                    .map_err(anyhow::Error::msg)?,
                    fields: settings.fields,
                    list_policy: settings
                        .list_policy
                        .as_deref()
                        .map(ListPolicy::from_str)
                        .transpose()
                        .map_err(anyhow::Error::msg)?,
                    list_flatten_size: settings.list_flatten_size,
                    array_policy: settings
                        .array_policy
                        .as_deref()
                        .map(ArrayPolicy::from_str)
                        .transpose()
                        .map_err(anyhow::Error::msg)?,
                    map_policy: settings
                        .map_policy
                        .as_deref()
                        .map(MapPolicy::from_str)
                        .transpose()
                        .map_err(anyhow::Error::msg)?,
                    metadata_prefix: settings.metadata_prefix.unwrap_or_else(|| "@".into()),
                    time_ns: settings.time_ns.unwrap_or(false),
                    jsonl_explicit_null: settings.jsonl_explicit_null.unwrap_or(false),
                },
            )?);
        }
        run_conversion(&self.input, self.parallel, specs, Some(&self.config))
    }
}

pub(super) fn run_conversion(
    input: &Path,
    parallel: bool,
    specs: Vec<TopicSpec>,
    config_path: Option<&Path>,
) -> Result<()> {
    let reader = McapReader::builder()
        .with_default_decoders()
        .with_parallel(parallel)
        .build();
    let mut names = HashSet::new();
    let mut outputs = HashSet::new();
    let mut topics = Vec::new();
    let mut total = 0;
    let mut has_stats = true;
    for spec in specs {
        if !names.insert(spec.name.clone()) {
            bail!("duplicate topic: {}", spec.name);
        }
        if let Some(output) = &spec.output {
            if !outputs.insert(output.as_os_str().to_owned()) {
                bail!("duplicate output: {}", output.display());
            }
        } else if matches!(spec.format, OutputFormat::Parquet) {
            bail!("Parquet output requires -o <file>");
        }
        match reader.message_count(input, &spec.name) {
            Ok(count) => total += count,
            Err(McapReaderError::StatsNotAvailable) if config_path.is_some() => has_stats = false,
            Err(error) => return Err(error.into()),
        }
        let schema = reader.topic_batch_schema(
            input,
            &spec.name,
            &RecordBatchOptions {
                metadata: spec.metadata,
                ..Default::default()
            },
        )?;
        let topic = Topic {
            name: spec.name,
            output: spec.output,
            format: spec.format,
            fields: spec.fields,
            policy: spec.policy,
            explicit_null: spec.explicit_null,
            schema,
            rows: Vec::new(),
            writer: None,
            warned: Cell::new(false),
            emitted: false,
        };
        topic.transform(RecordBatch::new_empty(topic.schema.schema().clone()))?;
        topics.push(topic);
    }
    let pb = if has_stats {
        ProgressBar::new(total)
    } else {
        ProgressBar::new_spinner()
    };
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({per_sec}, ETA: {eta})")?.progress_chars("=>-"));
    for topic in &mut topics {
        topic.writer = Some(match topic.format {
            OutputFormat::Jsonl => Box::new(JsonlWriter::new(
                topic.output.as_deref(),
                NonFiniteFloats::Null,
                topic.explicit_null,
            )?),
            OutputFormat::JsonlExt => Box::new(JsonlWriter::new(
                topic.output.as_deref(),
                NonFiniteFloats::String,
                topic.explicit_null,
            )?),
            OutputFormat::Csv => Box::new(CsvWriter::new(topic.output.as_deref())?),
            OutputFormat::Parquet => {
                Box::new(ParquetWriter::new(topic.output.as_deref().unwrap())?)
            }
        });
    }
    let index: HashMap<_, _> = topics
        .iter()
        .enumerate()
        .map(|(i, t)| (t.name.clone(), i))
        .collect();
    let names: Vec<_> = topics.iter().map(|t| t.name.clone()).collect();
    let names: Vec<_> = names.iter().map(String::as_str).collect();
    reader.for_each_topic_decoded_message(input, &names, |name, message| {
        let topic = &mut topics[index[name]];
        topic.rows.push(message);
        if topic.rows.len() >= 1024 {
            topic.flush()?;
            pb.inc(1024);
        }
        Ok(())
    })?;
    for topic in &mut topics {
        if topic.rows.is_empty() && matches!(topic.format, OutputFormat::Parquet) && !topic.emitted
        {
            let batch = topic.transform(RecordBatch::new_empty(topic.schema.schema().clone()))?;
            topic.writer.as_mut().unwrap().write_batch(batch)?;
        }
        let remaining = topic.rows.len() as u64;
        topic.flush()?;
        pb.inc(remaining);
        topic.writer.as_mut().unwrap().finish()?;
        if let (OutputFormat::Parquet, Some(path)) = (topic.format, &topic.output) {
            eprintln!("Written to {}", path.display());
        }
    }
    pb.finish_with_message("done");
    Ok(())
}
