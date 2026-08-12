use clap::ValueEnum;
use mcapdecode::arrow::{ArrayPolicy, FlattenPolicy, ListPolicy, MapPolicy, StructPolicy};

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum OutputFormat {
    Jsonl,
    JsonlExt,
    Csv,
    Parquet,
}

impl OutputFormat {
    pub fn default_policy(&self) -> FlattenPolicy {
        match self {
            OutputFormat::Jsonl | OutputFormat::JsonlExt => FlattenPolicy {
                list: ListPolicy::Keep,
                list_flatten_fixed_size: 1,
                array: ArrayPolicy::Keep,
                map: MapPolicy::Keep,
                struct_: StructPolicy::Keep,
            },
            OutputFormat::Csv => FlattenPolicy {
                list: ListPolicy::Drop,
                list_flatten_fixed_size: 1,
                array: ArrayPolicy::Drop,
                map: MapPolicy::Drop,
                struct_: StructPolicy::Flatten,
            },
            OutputFormat::Parquet => FlattenPolicy {
                list: ListPolicy::Keep,
                list_flatten_fixed_size: 1,
                array: ArrayPolicy::Keep,
                map: MapPolicy::Keep,
                struct_: StructPolicy::Flatten,
            },
        }
    }
}
