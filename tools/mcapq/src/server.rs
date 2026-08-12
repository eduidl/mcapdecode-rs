pub mod jtd;
pub mod response;

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use jtd::jtd_schema;
use mcapdecode::{McapReader, MetadataColumns, MetadataTimestampFormat};
use response::{FileRef, InfoResponse, SchemaResponse, topic_json};
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};

/// Return the payload as `structuredContent` only.
///
/// rmcp's `Json` wrapper would additionally serialize the whole payload into a
/// `content` text block, roughly doubling every response.
fn structured_only<T: Serialize>(value: T) -> Result<CallToolResult, String> {
    let value = serde_json::to_value(value)
        .map_err(|error| format!("failed to serialize response: {error}"))?;
    let mut result = CallToolResult::structured(value);
    result.content.clear();
    Ok(result)
}

#[derive(Clone)]
pub struct McapServer {
    allowed_roots: Arc<Vec<PathBuf>>,
    #[allow(dead_code)] // The rmcp tool_router macro reads this field.
    tool_router: ToolRouter<Self>,
}

impl McapServer {
    fn new(allowed_roots: Vec<PathBuf>) -> Result<Self, String> {
        let allowed_roots = allowed_roots
            .into_iter()
            .map(|root| {
                let root = root.canonicalize().map_err(|error| {
                    format!("failed to resolve allow root '{}': {error}", root.display())
                })?;
                if root.is_dir() {
                    Ok(root)
                } else {
                    Err(format!(
                        "allow root '{}' is not a directory",
                        root.display()
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            allowed_roots: Arc::new(allowed_roots),
            tool_router: Self::tool_router(),
        })
    }

    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce(Arc<Vec<PathBuf>>) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let roots = Arc::clone(&self.allowed_roots);
        tokio::task::spawn_blocking(move || work(roots))
            .await
            .map_err(|error| format!("MCAP task failed: {error}"))?
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct PathParams {
    /// Absolute path to an MCAP file beneath an allowed root.
    path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SchemaParams {
    /// Absolute path to an MCAP file beneath an allowed root.
    path: String,
    /// Topic whose payload schema to inspect.
    topic: String,
}

#[tool_router]
impl McapServer {
    #[tool(
        name = "mcap_info",
        description = "List topics, schemas, message counts, and decode availability in an MCAP file.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<InfoResponse>()
    )]
    async fn info(
        &self,
        Parameters(params): Parameters<PathParams>,
    ) -> Result<CallToolResult, String> {
        let response = self
            .blocking(move |roots| {
                let path = resolve_path(&roots, &params.path)?;
                let metadata = fs::metadata(&path)
                    .map_err(|error| format!("failed to read '{}': {error}", path.display()))?;
                let reader = McapReader::builder().with_default_decoders().build();
                let topics = reader
                    .list_topics_with_decode_status(&path)
                    .map_err(|error| error.to_string())?;
                Ok(InfoResponse {
                    file: FileRef::new(&path, metadata.len()),
                    topics: topics.iter().map(topic_json).collect(),
                })
            })
            .await?;
        structured_only(response)
    }

    #[tool(
        name = "mcap_schema",
        description = "Describe one decodable MCAP topic with JTD.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<SchemaResponse>()
    )]
    async fn schema(
        &self,
        Parameters(params): Parameters<SchemaParams>,
    ) -> Result<CallToolResult, String> {
        let response = self
            .blocking(move |roots| {
                let path = resolve_path(&roots, &params.path)?;
                let reader = McapReader::builder().with_default_decoders().build();
                let schema = reader
                    .topic_schema(&path, &params.topic)
                    .map_err(|error| error.to_string())?;
                Ok(SchemaResponse {
                    format: "jtd",
                    schema: jtd_schema(
                        &schema.field_defs,
                        &schema.info.schema_name.unwrap_or_default(),
                        &schema.info.schema_encoding,
                        &schema.info.message_encoding,
                        &metadata_columns(),
                    )?,
                })
            })
            .await?;
        structured_only(response)
    }
}

pub fn metadata_columns() -> MetadataColumns {
    MetadataColumns::default().with_timestamp_format(MetadataTimestampFormat::UnixNanoseconds)
}

pub fn resolve_path(allowed_roots: &[PathBuf], raw_path: &str) -> Result<PathBuf, String> {
    let raw_path = Path::new(raw_path);
    if !raw_path.is_absolute() {
        return Err(format!(
            "MCAP path '{}' must be absolute",
            raw_path.display()
        ));
    }
    let path: PathBuf = raw_path.canonicalize().map_err(|error| {
        format!(
            "failed to resolve MCAP path '{}': {error}",
            raw_path.display()
        )
    })?;
    if !path.is_file() {
        return Err(format!("MCAP path '{}' is not a file", path.display()));
    }
    if allowed_roots.iter().any(|root| path.starts_with(root)) {
        Ok(path)
    } else {
        Err(format!(
            "MCAP path '{}' is outside the configured allow roots",
            path.display()
        ))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for McapServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "Inspect MCAP files with mcap_info, then describe one topic's payload with mcap_schema.",
        )
    }
}

pub async fn serve_stdio(allowed_roots: Vec<PathBuf>) -> Result<(), String> {
    let server = McapServer::new(allowed_roots)?;
    server
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|error| error.to_string())?
        .waiting()
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}
