//! Deliberately ignored regression contract for the currently known hot path.
//! Remove `#[ignore]` when the corresponding implementation issue is fixed.
//!
//! The allocation half of the contract lives in `alloc_budget.rs`, because it profiles
//! the whole process and must not share a binary with anything else.

mod common;

use common::{bytes_schema, one_megabyte_cdr};
use mcapdecode_core::Value;
use mcapdecode_ros2_common::decode_cdr_to_value;

/// Contract for issue #2. It currently fails because each byte becomes `Value::U8`.
#[test]
#[ignore = "remove after sequence<uint8> uses Value::Bytes"]
fn byte_sequence_is_bulk_bytes() {
    let value = decode_cdr_to_value(&bytes_schema(), &one_megabyte_cdr()).unwrap();
    let Value::Struct(fields) = value else {
        panic!("expected struct")
    };
    assert!(matches!(fields[0], Value::Bytes(_)));
}
