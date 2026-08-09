use std::path::PathBuf;

use clap::Args;
use mcapdecode::{McapReader, TopicDecodeStatus};
use serde::Serialize;

#[derive(Args)]
pub struct InfoArgs {
    /// Path to the MCAP file.
    input: PathBuf,
}

#[derive(Serialize)]
struct InfoOutput {
    file: FileInfo,
    topics: Vec<TopicOutput>,
}

#[derive(Serialize)]
struct FileInfo {
    path: String,
    size_bytes: u64,
}

#[derive(Serialize)]
struct TopicOutput {
    topic: String,
    schema: Option<String>,
    count: Option<u64>,
    decodable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    decode_error: Option<String>,
}

impl InfoArgs {
    pub fn run(self) -> Result<(), String> {
        let metadata = std::fs::metadata(&self.input)
            .map_err(|error| format!("failed to read '{}': {error}", self.input.display()))?;
        let reader = McapReader::builder().with_default_decoders().build();
        let topics = reader
            .list_topics_with_decode_status(&self.input)
            .map_err(|error| error.to_string())?;

        let output = InfoOutput {
            file: FileInfo {
                path: self.input.display().to_string(),
                size_bytes: metadata.len(),
            },
            topics: topics.iter().map(topic_output).collect(),
        };
        println!(
            "{}",
            serde_json::to_string(&output).map_err(|error| error.to_string())?
        );
        Ok(())
    }
}

fn topic_output(status: &TopicDecodeStatus) -> TopicOutput {
    TopicOutput {
        topic: status.topic.topic.clone(),
        schema: status.topic.schema_name.clone(),
        count: status.topic.message_count,
        decodable: status.decodable,
        decode_error: status.decode_error.clone(),
    }
}
