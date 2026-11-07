use std::{fs, path::PathBuf};

use anyhow::Result;
use clap::Args;
use mcapdecode::{McapReader, core::format_field_defs};

#[derive(Args)]
pub struct SchemaArgs {
    /// Path to the mcap file
    input: PathBuf,

    /// Filter by topic name
    #[arg(short, long)]
    topic: String,

    /// Output file path (stdout if not specified)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

impl SchemaArgs {
    pub fn run(self) -> Result<()> {
        let reader = McapReader::open(&self.input)?;
        let field_defs = reader.topic_field_defs(&self.topic)?;
        let text = format_field_defs(&field_defs)?;

        match self.output {
            Some(path) => fs::write(path, format!("{text}\n"))?,
            None => println!("{text}"),
        }
        Ok(())
    }
}
