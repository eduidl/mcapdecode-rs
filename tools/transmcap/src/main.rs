mod commands;
mod format;
mod writer;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{batch::BatchArgs, convert::ConvertArgs, schema::SchemaArgs};

#[derive(Parser)]
#[command(name = "transmcap", about = "Convert mcap files to various formats")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert mcap to jsonl/csv/parquet
    Convert(ConvertArgs),
    /// Convert multiple topics using a JSON configuration
    Batch(BatchArgs),
    /// Print Arrow schema for a topic
    Schema(SchemaArgs),
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert(args) => args.run(),
        Commands::Batch(args) => args.run(),
        Commands::Schema(args) => args.run(),
    }
}
