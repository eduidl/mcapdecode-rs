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
    #[arg(long, default_value_t = 1_048_576)]
    chunk_bytes: usize,
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
            chunk_bytes: cli.chunk_bytes,
            layout: cli.layout,
        },
    )
}
