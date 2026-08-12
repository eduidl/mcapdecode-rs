use std::path::Path;

use mcapdecode::TopicDecodeStatus;
use rmcp::schemars;
use serde::Serialize;
use serde_json::Value;

/// The MCAP file a response was read from.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct FileRef {
    /// Canonical path of the file that was read.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: u64,
}

impl FileRef {
    pub fn new(path: &Path, size_bytes: u64) -> Result<Self, String> {
        let path = path.to_str().ok_or_else(|| {
            "MCAP path is not valid UTF-8 and cannot be returned to the client".to_string()
        })?;
        Ok(Self {
            path: path.to_owned(),
            size_bytes,
        })
    }
}

/// `mcap_info` response.
#[derive(Serialize, schemars::JsonSchema)]
pub struct InfoResponse {
    pub file: FileRef,
    pub topics: Vec<TopicResponse>,
}

/// One topic within an `mcap_info` response.
#[derive(Serialize, schemars::JsonSchema)]
pub struct TopicResponse {
    pub topic: String,
    /// Schema name declared in the MCAP file, if any.
    pub schema: Option<String>,
    /// Message count from the summary section, if the file has one.
    pub count: Option<u64>,
    /// Whether this topic's payload can be decoded.
    pub decodable: bool,
    /// Why decoding is unavailable. Absent when `decodable` is true.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decode_error: Option<String>,
}

/// `mcap_schema` response.
#[derive(Serialize, schemars::JsonSchema)]
pub struct SchemaResponse {
    /// Always `"jtd"` (JSON Type Definition).
    pub format: &'static str,
    pub schema: Value,
}

pub fn topic_json(status: &TopicDecodeStatus) -> TopicResponse {
    TopicResponse {
        topic: status.topic.topic.clone(),
        schema: status.topic.schema_name.clone(),
        count: status.topic.message_count,
        decodable: status.decodable,
        decode_error: status.decode_error.clone(),
    }
}
