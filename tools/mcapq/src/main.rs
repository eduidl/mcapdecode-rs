use std::{path::PathBuf, process::ExitCode};

use clap::Parser;

#[derive(Parser)]
#[command(name = "mcapq", about = "MCP server for inspecting MCAP files")]
struct Cli {
    /// Directory containing MCAP files that tools may read. Specify at least once.
    #[arg(long = "allow-root", required = true, value_name = "DIR")]
    allow_roots: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match mcapq::serve_stdio(cli.allow_roots).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mcapq: {error}");
            ExitCode::FAILURE
        }
    }
}
