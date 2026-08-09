use std::process::ExitCode;

use clap::{Parser, Subcommand};
use commands::info::InfoArgs;

mod commands;

#[derive(Parser)]
#[command(name = "mcapq", about = "Machine-readable MCAP inspection")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List topics and their schema metadata as JSON.
    Info(InfoArgs),
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            emit_error("invalid_arguments", &error.to_string());
            return ExitCode::from(2);
        }
    };

    let result = match cli.command {
        Commands::Info(args) => args.run(),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            emit_error("runtime_error", &error);
            ExitCode::from(1)
        }
    }
}

fn emit_error(code: &str, message: &str) {
    eprintln!(
        "{}",
        serde_json::json!({"error": {"code": code, "message": message}})
    );
}
