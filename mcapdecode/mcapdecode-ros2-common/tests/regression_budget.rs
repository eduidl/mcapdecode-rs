//! The allocation half of the contract lives in `alloc_budget.rs`, because it profiles
//! the whole process and must not share a binary with anything else.

mod common;

use common::{bytes_schema, one_megabyte_cdr};
use mcapdecode_core::Value;
use mcapdecode_ros2_common::decode_cdr_to_value;

/// Contract for bulk decoding `sequence<uint8>` as `Value::Bytes`.
#[test]
fn byte_sequence_is_bulk_bytes() {
    let value = decode_cdr_to_value(&bytes_schema(), &one_megabyte_cdr()).unwrap();
    let Value::Struct(fields) = value else {
        panic!("expected struct")
    };
    assert!(matches!(fields[0], Value::Bytes(_)));
}
