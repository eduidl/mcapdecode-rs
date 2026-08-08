import pickle
import struct

import mcapdecode
import polars as pl
import pyarrow as pa
import pytest
from mcap.writer import Writer


def _align(payload: bytearray, alignment: int) -> None:
    relative_offset = len(payload) - 4
    padding = (alignment - relative_offset % alignment) % alignment
    payload.extend(b"\x00" * padding)


def _cdr_message(x: int, name: str, value: float) -> bytes:
    payload = bytearray(b"\x00\x01\x00\x00")
    _align(payload, 4)
    payload.extend(struct.pack("<i", x))
    encoded_name = name.encode() + b"\x00"
    _align(payload, 4)
    payload.extend(struct.pack("<I", len(encoded_name)))
    payload.extend(encoded_name)
    _align(payload, 8)
    payload.extend(struct.pack("<d", value))
    return bytes(payload)


@pytest.fixture
def fixture_path(tmp_path):
    path = tmp_path / "sample.mcap"
    with path.open("wb") as output:
        writer = Writer(output)
        writer.start(profile="ros2", library="mcapdecode-python-test")
        schema_id = writer.register_schema(
            name="example_msgs/msg/Sample",
            encoding="ros2msg",
            data=b"int32 x\nstring name\nfloat64 value\n",
        )
        channel_id = writer.register_channel(
            topic="/example/sample", message_encoding="cdr", schema_id=schema_id
        )
        writer.register_channel(
            topic="/example/empty", message_encoding="cdr", schema_id=schema_id
        )
        for timestamp, x, name, value in [(1, 10, "alpha", 1.5), (2, 20, "beta", 2.5)]:
            writer.add_message(
                channel_id=channel_id,
                log_time=timestamp,
                publish_time=timestamp,
                data=_cdr_message(x, name, value),
            )
        writer.finish()
    return path


def test_list_topics_exposes_metadata(fixture_path):
    topics = mcapdecode.list_topics(fixture_path)

    assert [topic.topic for topic in topics] == ["/example/empty", "/example/sample"]
    assert topics[0].message_count == 0
    assert topics[1].schema_encoding == "ros2msg"
    assert topics[1].message_encoding == "cdr"


def test_read_returns_pyarrow_table_and_polars_reuses_it(fixture_path):
    table = mcapdecode.read(fixture_path, "/example/sample")

    assert isinstance(table, pa.Table)
    assert table.column_names == ["@log_time", "@publish_time", "x", "name", "value"]
    assert table.column("x").to_pylist() == [10, 20]
    assert table.column("name").to_pylist() == ["alpha", "beta"]
    assert pl.from_arrow(table, rechunk=False).get_column("value").to_list() == [1.5, 2.5]


def test_read_empty_topic_preserves_schema(fixture_path):
    table = mcapdecode.read(fixture_path, "/example/empty")

    assert table.num_rows == 0
    assert table.column_names == ["@log_time", "@publish_time", "x", "name", "value"]


def test_read_reports_python_errors(fixture_path, tmp_path):
    with pytest.raises(OSError):
        mcapdecode.list_topics(tmp_path / "missing.mcap")
    with pytest.raises(mcapdecode.McapDecodeError, match="not found"):
        mcapdecode.read(fixture_path, "/unknown")


def test_public_python_types_are_picklable(fixture_path):
    error = mcapdecode.McapDecodeError("invalid recording")
    topic = mcapdecode.list_topics(fixture_path)[0]

    assert type(pickle.loads(pickle.dumps(error))) is mcapdecode.McapDecodeError
    assert type(pickle.loads(pickle.dumps(topic))) is mcapdecode.TopicInfo
