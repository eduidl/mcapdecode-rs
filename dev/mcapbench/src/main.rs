use std::path::PathBuf;

use clap::Parser;
use mcapbench::{CompressionKind, Encoding, FileShape, Layout, PayloadCase, generate};

#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    case: PayloadCase,
    #[arg(long, value_enum)]
    encoding: Encoding,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, value_enum, default_value_t=CompressionKind::None)]
    compression: CompressionKind,
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u8).range(0..=100))]
    select_percent: u8,
    /// Zero would make the writer flush a chunk per message, so the shape is rejected
    /// rather than silently producing hundreds of thousands of chunks.
    #[arg(long, default_value_t = 1_048_576, value_parser = clap::value_parser!(u64).range(1..))]
    chunk_bytes: u64,
    #[arg(long, value_enum, default_value_t=Layout::Interleaved)]
    layout: Layout,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    generate(
        &cli.output,
        cli.case,
        cli.encoding,
        FileShape {
            select_percent: cli.select_percent,
            compression: cli.compression,
            chunk_bytes: cli.chunk_bytes as usize,
            layout: cli.layout,
        },
    )
}
