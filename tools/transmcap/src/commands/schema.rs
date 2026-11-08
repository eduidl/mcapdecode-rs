use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;
use mcapdecode::McapReader;

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
        let mut schemas = reader.read().topic(&self.topic).field_defs()?.into_iter();
        let (first, field_defs) = schemas.next().context("topic has no channels")?;
        for (channel, other) in schemas {
            if other != field_defs {
                bail!(
                    "channels {} and {} of topic {} have different schemas",
                    first.id,
                    channel.id,
                    self.topic
                );
            }
        }
        let text = field_defs.to_string();

        match self.output {
            Some(path) => fs::write(path, format!("{text}\n"))?,
            None => println!("{text}"),
        }
        Ok(())
    }
}
