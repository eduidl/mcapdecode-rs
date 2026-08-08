//! Turning a model into an encoded message and writing it out as an MCAP file.

use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
};

use mcap::{Compression, WriteOptions, Writer, records::MessageHeader};
use prost::Message;
use prost_reflect::DescriptorPool;

use crate::{
    BenchResult, CompressionKind, Encoding, FileShape, Layout, OTHER_TOPIC, PayloadCase, TOPIC,
    model::{PACKAGE, model, sample},
    protobuf, ros2,
};

/// Rough size of each generated file. Small enough to regenerate routinely, large
/// enough that chunking and compression behave like they do on a real recording.
const TARGET_BYTES: usize = 24 * 1024 * 1024;

/// Recorded in the MCAP header, and hashed into the fixture path.
const LIBRARY: &str = "mcapbench";

/// Bump whenever [`generate`] writes a different file for inputs that already hash the
/// same: channel layout, message ordering, writer options beyond [`FileShape`]. The
/// schema, the payload, the file shape and [`TARGET_BYTES`] are hashed directly and do
/// not need a bump.
const GENERATOR_VERSION: u32 = 1;

/// Schema blob plus one encoded message for a given case/encoding pair.
pub struct Fixture {
    pub schema_name: String,
    pub schema_encoding: &'static str,
    pub message_encoding: &'static str,
    pub schema: Vec<u8>,
    pub payload: Vec<u8>,
}

pub fn fixture(case: PayloadCase, encoding: Encoding) -> BenchResult<Fixture> {
    if encoding == Encoding::Protobuf && case == PayloadCase::Strings {
        return Err("strings is intentionally ros2idl-only".into());
    }
    let model = model(case);
    let sample = sample(case);
    Ok(match encoding {
        Encoding::Ros2idl => Fixture {
            schema_name: format!("{PACKAGE}/msg/Sample"),
            schema_encoding: "ros2idl",
            message_encoding: "cdr",
            schema: ros2::render_idl(&model).into_bytes(),
            payload: ros2::encode_cdr(&sample),
        },
        Encoding::Ros2msg => Fixture {
            schema_name: format!("{PACKAGE}/msg/Sample"),
            schema_encoding: "ros2msg",
            message_encoding: "cdr",
            schema: ros2::render_msg(&model).into_bytes(),
            payload: ros2::encode_cdr(&sample),
        },
        Encoding::Protobuf => {
            let schema = protobuf::descriptor_set(&model);
            let pool = DescriptorPool::decode(schema.as_slice())?;
            let message = protobuf::encode(&pool, &model, model.root(), &sample)?;
            Fixture {
                schema_name: format!("{PACKAGE}.Sample"),
                schema_encoding: "protobuf",
                message_encoding: "protobuf",
                schema,
                payload: message.encode_to_vec(),
            }
        }
    })
}

fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Directory holding the cached fixtures.
///
/// They are large and numerous enough to be worth keeping together: a full benchmark run
/// materialises every combination, so a single directory turns cleanup into one `rm -rf`.
/// `MCAPBENCH_FIXTURE_DIR` moves them off a RAM-backed `/tmp`.
pub fn fixture_dir() -> PathBuf {
    std::env::var_os("MCAPBENCH_FIXTURE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("mcapbench"))
}

/// Content-addressed file name.
///
/// The digest covers everything that decides the bytes on disk: the schema, the
/// payload, the file shape and the writer settings. Changing the generator therefore
/// invalidates cached fixtures instead of silently reusing a stale file.
fn digest_name(
    fixture: &Fixture,
    case: PayloadCase,
    encoding: Encoding,
    shape: FileShape,
) -> String {
    let mut digest = fnv1a(0xcbf2_9ce4_8422_2325, &fixture.schema);
    digest = fnv1a(digest, &fixture.payload);
    digest = fnv1a(
        digest,
        format!(
            "v{GENERATOR_VERSION}/{LIBRARY}/{TARGET_BYTES}/{}/{:?}/{}/{:?}",
            shape.select_percent, shape.compression, shape.chunk_bytes, shape.layout
        )
        .as_bytes(),
    );
    format!("mcapbench-{case:?}-{encoding:?}-{digest:016x}.mcap").to_lowercase()
}

/// Path a fixture would be cached at, whether or not it exists yet.
pub fn generated_path(
    case: PayloadCase,
    encoding: Encoding,
    shape: FileShape,
) -> BenchResult<PathBuf> {
    let fixture = fixture(case, encoding)?;
    Ok(fixture_dir().join(digest_name(&fixture, case, encoding, shape)))
}

/// Return the fixture path, generating the file first if it is not present yet.
///
/// The file is written to a unique sibling and renamed into place, so an interrupted or
/// concurrent run cannot leave a truncated fixture at the cached path — which, since the
/// cache check is mere existence, would otherwise poison every later run.
pub fn ensure_generated(
    case: PayloadCase,
    encoding: Encoding,
    shape: FileShape,
) -> BenchResult<PathBuf> {
    let fixture = fixture(case, encoding)?;
    let name = digest_name(&fixture, case, encoding, shape);
    let dir = fixture_dir();
    let path = dir.join(&name);
    if !path.exists() {
        std::fs::create_dir_all(&dir)?;
        let staging = dir.join(format!(".tmp-{}-{name}", std::process::id()));
        match write(&staging, &fixture, shape) {
            Ok(()) => std::fs::rename(&staging, &path)?,
            Err(e) => {
                let _ = std::fs::remove_file(&staging);
                return Err(e);
            }
        }
    }
    Ok(path)
}

pub fn generate(
    path: &Path,
    case: PayloadCase,
    encoding: Encoding,
    shape: FileShape,
) -> BenchResult<()> {
    write(path, &fixture(case, encoding)?, shape)
}

fn write(path: &Path, fixture: &Fixture, shape: FileShape) -> BenchResult<()> {
    if shape.select_percent > 100 {
        return Err(format!(
            "select_percent must be 0..=100, got {}",
            shape.select_percent
        )
        .into());
    }
    let count = (TARGET_BYTES / fixture.payload.len().max(1)).max(1);
    let selected_count = (count * usize::from(shape.select_percent)).div_ceil(100);
    let compression = match shape.compression {
        CompressionKind::None => None,
        CompressionKind::Zstd => Some(Compression::Zstd),
        CompressionKind::Lz4 => Some(Compression::Lz4),
    };

    let file = File::create(path)?;
    let mut writer = Writer::with_options(
        file,
        WriteOptions::new()
            .compression(compression)
            .chunk_size(Some(shape.chunk_bytes as u64))
            .library(LIBRARY),
    )?;
    let schema_id = writer.add_schema(
        &fixture.schema_name,
        fixture.schema_encoding,
        &fixture.schema,
    )?;
    let selected =
        writer.add_channel(schema_id, TOPIC, fixture.message_encoding, &BTreeMap::new())?;
    let other = writer.add_channel(
        schema_id,
        OTHER_TOPIC,
        fixture.message_encoding,
        &BTreeMap::new(),
    )?;

    for index in 0..count {
        let is_selected = match shape.layout {
            Layout::Interleaved => (index % 100) < usize::from(shape.select_percent),
            Layout::Clustered => index < selected_count,
        };
        writer.write_to_known_channel(
            &MessageHeader {
                channel_id: if is_selected { selected } else { other },
                sequence: index as u32,
                log_time: index as u64,
                publish_time: index as u64,
            },
            &fixture.payload,
        )?;
    }
    writer.finish()?;
    Ok(())
}
