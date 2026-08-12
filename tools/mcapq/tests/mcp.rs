use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
};

use mcap::{WriteOptions, Writer, records::MessageHeader};
use serde_json::{Value, json};

static FIXTURE_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new(label: &str) -> Self {
        let id = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("mcapq-{label}-{}-{id}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct McpClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl McpClient {
    fn start(allow_root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mcapq"))
            .args(["--allow-root", allow_root.to_str().unwrap()])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut client = Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        };
        let initialized = client.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "mcapq-test", "version": "0"},
            }),
        );
        assert_eq!(initialized["result"]["capabilities"]["tools"], json!({}));
        client.notify("notifications/initialized", json!({}));
        client
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.write(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let mut line = String::new();
            let bytes = self.stdout.read_line(&mut line).unwrap();
            assert_ne!(bytes, 0, "MCP server closed before responding to {method}");
            let response: Value = serde_json::from_str(&line).unwrap();
            if response["id"] == id {
                return response;
            }
        }
    }

    fn notify(&mut self, method: &str, params: Value) {
        self.write(json!({"jsonrpc": "2.0", "method": method, "params": params}));
    }

    fn tool(&mut self, name: &str, arguments: Value) -> Value {
        self.request("tools/call", json!({"name": name, "arguments": arguments}))
    }

    fn tool_result(&mut self, name: &str, arguments: Value) -> Value {
        let response = self.tool(name, arguments);
        assert!(response.get("error").is_none(), "{response}");
        let result = &response["result"];
        assert_ne!(result["isError"], Value::Bool(true), "{result}");
        // The payload travels as structuredContent only; duplicating it into a
        // content text block would double every response.
        assert_eq!(result["content"], json!([]), "{result}");
        result["structuredContent"].clone()
    }

    fn tool_error(&mut self, name: &str, arguments: Value) -> String {
        let response = self.tool(name, arguments);
        let result = &response["result"];
        assert_eq!(result["isError"], Value::Bool(true), "{response}");
        result["content"][0]["text"].as_str().unwrap().to_string()
    }

    fn write(&mut self, request: Value) {
        writeln!(self.stdin, "{request}").unwrap();
        self.stdin.flush().unwrap();
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn demo_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mcapq-demo.mcap")
}

fn write_float_fixture(path: &Path, messages: usize) {
    write_float_topic_fixture(path, "/float", messages);
}

fn write_float_topic_fixture(path: &Path, topic: &str, messages: usize) {
    let file = File::create(path).unwrap();
    let mut writer = Writer::with_options(file, WriteOptions::new().library("mcapq-test")).unwrap();
    let schema_id = writer
        .add_schema(
            "example/msg/FloatSample",
            "ros2msg",
            b"float32 reading\nfloat64 positive\nfloat64 nan\nfloat64 negative\n",
        )
        .unwrap();
    let channel_id = writer
        .add_channel(schema_id, topic, "cdr", &BTreeMap::new())
        .unwrap();
    for sequence in 0..messages {
        let mut data = vec![0, 1, 0, 0];
        data.extend_from_slice(&0.1_f32.to_le_bytes());
        data.extend_from_slice(&[0; 4]);
        data.extend_from_slice(&f64::INFINITY.to_le_bytes());
        data.extend_from_slice(&f64::NAN.to_le_bytes());
        data.extend_from_slice(&f64::NEG_INFINITY.to_le_bytes());
        writer
            .write_to_known_channel(
                &MessageHeader {
                    channel_id,
                    sequence: sequence as u32,
                    log_time: sequence as u64,
                    publish_time: sequence as u64,
                },
                &data,
            )
            .unwrap();
    }
    writer.finish().unwrap();
}

#[test]
fn mcp_discovers_tools_and_inspects_topics_and_schemas() {
    let fixture = demo_fixture();
    let mut client = McpClient::start(fixture.parent().unwrap());

    let listed = client.request("tools/list", json!({}));
    let names = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["mcap_info", "mcap_schema"]);
    let info_tool = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "mcap_info")
        .unwrap();
    assert_eq!(
        info_tool["outputSchema"]["properties"]["topics"]["type"],
        "array"
    );

    let info = client.tool_result("mcap_info", json!({"path": fixture}));
    assert_eq!(info["topics"].as_array().unwrap().len(), 6);
    assert_eq!(info["topics"][5]["topic"], "/demo/velocity");

    let jtd = client.tool_result(
        "mcap_schema",
        json!({"path": fixture, "topic": "/demo/velocity"}),
    );
    assert_eq!(jtd["format"], "jtd");
    assert_eq!(jtd["schema"]["properties"]["log_time"]["type"], "int64");
    assert_eq!(jtd["schema"]["properties"]["speed_mps"]["type"], "float64");
}

#[test]
fn mcp_rereads_changed_files_and_rejects_unsafe_requests() {
    let directory = TemporaryDirectory::new("reread");
    let path = directory.path().join("float.mcap");
    write_float_fixture(&path, 1);
    let mut client = McpClient::start(directory.path());

    let first = client.tool_result("mcap_info", json!({"path": path}));
    assert_eq!(first["topics"][0]["count"], 1);

    write_float_fixture(&path, 2);
    let refreshed = client.tool_result("mcap_info", json!({"path": path}));
    assert_eq!(refreshed["topics"][0]["count"], 2);

    let outside = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md");
    let outside_error = client.tool_error("mcap_info", json!({"path": outside}));
    assert!(outside_error.contains("outside the configured allow roots"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let escaped_link = directory.path().join("escaped.mcap");
        symlink(demo_fixture(), &escaped_link).unwrap();
        let escaped_error = client.tool_error("mcap_info", json!({"path": escaped_link}));
        assert!(escaped_error.contains("outside the configured allow roots"));
    }

    let relative_error = client.tool_error("mcap_info", json!({"path": "float.mcap"}));
    assert!(relative_error.contains("must be absolute"));
}
