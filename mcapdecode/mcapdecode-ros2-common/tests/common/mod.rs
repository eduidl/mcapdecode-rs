//! Shared inputs for the `sequence<uint8>` regression contracts.

use std::collections::HashMap;

use mcapdecode_ros2_common::{
    PrimitiveType, ResolvedField, ResolvedSchema, ResolvedStruct, ResolvedType,
};

pub fn bytes_schema() -> ResolvedSchema {
    let root = vec!["bench".into(), "msg".into(), "Bytes".into()];
    let mut structs = HashMap::new();
    structs.insert(
        root.clone(),
        ResolvedStruct {
            fields: vec![ResolvedField {
                name: "data".into(),
                ty: ResolvedType::Sequence {
                    elem: Box::new(ResolvedType::Primitive(PrimitiveType::U8)),
                    max_len: None,
                },
                fixed_len: None,
            }],
        },
    );
    ResolvedSchema {
        root,
        structs,
        enums: HashMap::new(),
    }
}

pub fn one_megabyte_cdr() -> Vec<u8> {
    let mut b = vec![0, 1, 0, 0];
    b.extend_from_slice(&(1024 * 1024u32).to_le_bytes());
    b.extend(std::iter::repeat_n(3, 1024 * 1024));
    b
}
