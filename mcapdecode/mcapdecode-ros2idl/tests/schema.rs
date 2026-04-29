use mcapdecode_ros2_common::{PrimitiveType, ResolvedType};
use mcapdecode_ros2idl::{SchemaBundle, parse_idl_section, resolve_schema};

// ── existing tests ─────────────────────────────────────────────────────────────

#[test]
fn schema_bundle_splits_sections_and_finds_main_type() {
    let schema = r#"
================================================================================
IDL: ex/msg/A
module ex {
  module msg {
    struct A {
      uint32 x;
    };
  };
};
================================================================================
IDL: ex/msg/B
module ex {
  module msg {
    struct B {
      uint32 y;
    };
  };
};
"#;

    let bundle = SchemaBundle::parse("ex/msg/B", schema).expect("bundle parse should succeed");
    assert_eq!(bundle.sections.len(), 2);
    assert_eq!(
        bundle.main_type("ex/msg/B"),
        Some(vec!["ex".into(), "msg".into(), "B".into()])
    );
}

#[test]
fn schema_bundle_single_section() {
    let schema = r#"
================================================================================
IDL: localization_msgs/msg/Pose
module localization_msgs {
  module msg {
    struct Pose {
      float64 x;
    };
  };
};
"#;
    let bundle = SchemaBundle::parse("localization_msgs/msg/Pose", schema)
        .expect("bundle parse should succeed");
    assert_eq!(bundle.sections.len(), 1);
    assert_eq!(
        bundle.main_type("localization_msgs/msg/Pose"),
        Some(vec![
            "localization_msgs".into(),
            "msg".into(),
            "Pose".into()
        ])
    );
}

#[test]
fn resolve_schema_supports_suffix_resolution_and_builtin_interfaces() {
    let schema = r#"
================================================================================
IDL: ex/msg/Outer
module ex {
  module msg {
    struct Outer {
      Inner nested;
      builtin_interfaces::msg::Time stamp;
    };
  };
};
================================================================================
IDL: ex/msg/Inner
module ex {
  module msg {
    struct Inner {
      uint32 value;
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Outer", schema).expect("resolve should succeed");
    let outer = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Outer".into()])
        .expect("outer struct should exist");

    match &outer.fields[0].ty {
        ResolvedType::Struct(name) => {
            assert_eq!(
                name,
                &vec!["ex".to_string(), "msg".to_string(), "Inner".to_string()]
            );
        }
        _ => panic!("nested should resolve as struct"),
    }

    match &outer.fields[1].ty {
        ResolvedType::Struct(name) => {
            assert_eq!(
                name,
                &vec![
                    "builtin_interfaces".to_string(),
                    "msg".to_string(),
                    "Time".to_string()
                ]
            );
            let builtin = resolved
                .structs
                .get(name)
                .expect("builtin Time should exist");
            assert_eq!(builtin.fields.len(), 2);
            assert!(matches!(
                builtin.fields[0].ty,
                ResolvedType::Primitive(PrimitiveType::I32)
            ));
            assert!(matches!(
                builtin.fields[1].ty,
                ResolvedType::Primitive(PrimitiveType::U32)
            ));
        }
        _ => panic!("stamp should resolve as struct"),
    }
}

#[test]
fn resolve_schema_fails_on_unresolved_type() {
    let schema = r#"
================================================================================
IDL: ex/msg/A
module ex {
  module msg {
    struct A {
      MissingType x;
    };
  };
};
"#;

    let err = resolve_schema("ex/msg/A", schema).expect_err("should fail on unresolved type");
    assert!(format!("{err:#}").contains("unresolved type"));
}

// ── new tests ──────────────────────────────────────────────────────────────────

/// An IDL enum field resolves to `ResolvedType::Enum`; the variants list is preserved.
#[test]
fn resolve_schema_enum_field_resolves_to_enum_type() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    enum Status {
      OK,
      WARN,
      ERROR
    };
    struct Msg {
      Status status;
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Msg", schema).expect("resolve should succeed");

    // The enum variant list must be stored.
    let enum_key = vec!["ex".to_string(), "msg".to_string(), "Status".to_string()];
    let variants = resolved
        .enums
        .get(&enum_key)
        .expect("enum should be in resolved schema");
    assert_eq!(variants, &vec!["OK", "WARN", "ERROR"]);

    // The field on the struct must resolve as Enum, not Struct.
    let msg = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Msg".into()])
        .expect("Msg struct should exist");
    assert!(
        matches!(&msg.fields[0].ty, ResolvedType::Enum(k) if k == &enum_key),
        "status field should be ResolvedType::Enum"
    );
}

/// When `schema_name` does not match any section path, the first section is used as root.
#[test]
fn main_type_falls_back_to_first_section_when_name_has_no_match() {
    let schema = r#"
================================================================================
IDL: ex/msg/First
module ex {
  module msg {
    struct First {
      uint8 x;
    };
  };
};
================================================================================
IDL: ex/msg/Second
module ex {
  module msg {
    struct Second {
      uint8 y;
    };
  };
};
"#;

    // "ex/msg/NoMatch" does not match either section — should fall back to First.
    let resolved =
        resolve_schema("ex/msg/NoMatch", schema).expect("resolve should succeed with fallback");
    assert_eq!(
        resolved.root,
        vec!["ex".to_string(), "msg".to_string(), "First".to_string()]
    );
}

/// A sequence field in IDL resolves correctly to `ResolvedType::Sequence`.
#[test]
fn resolve_schema_sequence_field() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    struct Msg {
      sequence<uint32> data;
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Msg", schema).expect("resolve should succeed");
    let msg = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Msg".into()])
        .expect("Msg should exist");

    assert!(
        matches!(
            &msg.fields[0].ty,
            ResolvedType::Sequence { max_len: None, .. }
        ),
        "data field should be an unbounded sequence"
    );
}

/// `builtin_interfaces::msg::Duration` is injected and resolves like `Time`.
#[test]
fn resolve_schema_builtin_duration_is_injected() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    struct Msg {
      builtin_interfaces::msg::Duration elapsed;
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Msg", schema).expect("resolve should succeed");
    let msg = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Msg".into()])
        .expect("Msg should exist");

    let duration_key = vec![
        "builtin_interfaces".to_string(),
        "msg".to_string(),
        "Duration".to_string(),
    ];
    assert!(
        matches!(&msg.fields[0].ty, ResolvedType::Struct(k) if k == &duration_key),
        "elapsed should resolve to builtin Duration struct"
    );
    assert!(
        resolved.structs.contains_key(&duration_key),
        "Duration struct must be present in resolved schema"
    );
}

#[test]
fn resolve_schema_ignores_annotations_includes_and_line_comments() {
    let schema = r#"
================================================================================
IDL: ex/msg/A
#include "ex/msg/B.idl"
module ex {
  module msg {
    struct A {
      @verbatim (language="comment", text="https://example.test/path")
      uint32 x; // trailing comment
      @default (value=0)
      uint8 y;
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/A", schema).expect("resolve should succeed");
    let a = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "A".into()])
        .expect("A should exist");
    assert_eq!(a.fields.len(), 2);
    assert_eq!(a.fields[0].name, "x");
    assert_eq!(a.fields[1].name, "y");
}

#[test]
fn resolve_schema_rejects_unsupported_union() {
    let schema = r#"
================================================================================
IDL: bad/msg/U
module bad {
  module msg {
    union U switch(uint8) {
      case 0: uint8 a;
    };
  };
};
"#;
    let err = resolve_schema("bad/msg/U", schema).expect_err("union must be rejected");
    assert!(format!("{err:#}").contains("unsupported IDL declaration"));
}

#[test]
fn resolve_schema_rejects_unclosed_struct() {
    let schema = r#"
================================================================================
IDL: ex/msg/A
module ex {
  module msg {
    struct A {
      uint32 x;
"#;
    let err = resolve_schema("ex/msg/A", schema).expect_err("unclosed struct should fail");
    assert!(format!("{err:#}").contains("unclosed struct declaration"));
}

#[test]
fn resolve_schema_parses_enum_with_blank_lines_and_commas() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    enum E {
      A,

      B = 2,
      C
    };
    struct Msg {
      E value;
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Msg", schema).expect("resolve should succeed");
    let key = vec!["ex".to_string(), "msg".to_string(), "E".to_string()];
    assert_eq!(
        resolved.enums.get(&key),
        Some(&vec!["A".to_string(), "B".to_string(), "C".to_string()])
    );
}

#[test]
fn parse_idl_section_supports_multiline_struct_open_with_consts() {
    let parsed = parse_idl_section(
        r#"
module ex {
  module msg {
    struct Sample
    {
      const uint8 KIND_A = 1;
      uint8 kind;
    };
  };
};
"#,
    )
    .expect("IDL should parse");

    let sample = parsed
        .structs
        .get(&vec![
            "ex".to_string(),
            "msg".to_string(),
            "Sample".to_string(),
        ])
        .expect("Sample struct should exist");
    assert_eq!(sample.consts.len(), 1);
    assert_eq!(sample.fields.len(), 1);
    assert_eq!(sample.fields[0].name, "kind");
}

#[test]
fn resolve_schema_parses_fixed_array_field() {
    let schema = r#"
================================================================================
IDL: ex/msg/Sample
module ex {
  module msg {
    struct Sample {
      float32 element[2];
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Sample", schema).expect("resolve should succeed");
    let sample = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Sample".into()])
        .expect("Sample should exist");
    assert_eq!(sample.fields[0].name, "element");
    assert_eq!(sample.fields[0].fixed_len, Some(2));
}

#[test]
fn resolve_schema_supports_idl_basic_type_aliases() {
    let schema = r#"
================================================================================
IDL: ex/msg/Aliases
module ex {
  module msg {
    struct Aliases {
      short a;
      unsigned short b;
      long c;
      unsigned long d;
      long long e;
      unsigned long long f;
      float g;
      double h;
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Aliases", schema).expect("resolve should succeed");
    let aliases = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Aliases".into()])
        .expect("Aliases should exist");

    assert!(matches!(
        aliases.fields[0].ty,
        ResolvedType::Primitive(PrimitiveType::I16)
    ));
    assert!(matches!(
        aliases.fields[1].ty,
        ResolvedType::Primitive(PrimitiveType::U16)
    ));
    assert!(matches!(
        aliases.fields[2].ty,
        ResolvedType::Primitive(PrimitiveType::I32)
    ));
    assert!(matches!(
        aliases.fields[3].ty,
        ResolvedType::Primitive(PrimitiveType::U32)
    ));
    assert!(matches!(
        aliases.fields[4].ty,
        ResolvedType::Primitive(PrimitiveType::I64)
    ));
    assert!(matches!(
        aliases.fields[5].ty,
        ResolvedType::Primitive(PrimitiveType::U64)
    ));
    assert!(matches!(
        aliases.fields[6].ty,
        ResolvedType::Primitive(PrimitiveType::F32)
    ));
    assert!(matches!(
        aliases.fields[7].ty,
        ResolvedType::Primitive(PrimitiveType::F64)
    ));
}

#[test]
fn resolve_schema_rejects_long_double_as_unsupported() {
    let schema = r#"
================================================================================
IDL: ex/msg/Unsupported
module ex {
  module msg {
    struct Unsupported {
      long double x;
    };
  };
};
"#;

    let err =
        resolve_schema("ex/msg/Unsupported", schema).expect_err("long double must be rejected");
    assert!(format!("{err:#}").contains("unsupported IDL type `long double`"));
}

#[test]
fn resolve_schema_accepts_constants_before_multiline_struct() {
    let schema = r#"
================================================================================
IDL: ex/msg/Sample
module ex {
  module msg {
    const uint8 KIND_A = 1;
    const uint8 KIND_B = 2;
    struct Sample
    {
      string name;
      uint8 kind;
    };
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Sample", schema).expect("resolve should succeed");
    let sample = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Sample".into()])
        .expect("Sample should exist");

    assert_eq!(sample.fields.len(), 2);
    assert_eq!(sample.fields[0].name, "name");
    assert_eq!(sample.fields[1].name, "kind");
}

#[test]
fn resolve_schema_supports_chained_module_lines() {
    let schema = r#"
================================================================================
IDL: pkg/msg/Root
#include "pkg/msg/Child.idl"
#include "pkg/msg/Change.idl"
module pkg { module msg {
struct Root {
  sequence<Change, 16> changes;
  sequence<Child, 16> children;
};
};
};
================================================================================
IDL: pkg/msg/Child
module pkg { module msg {
struct Child {
  string id;
};
};
};
================================================================================
IDL: pkg/msg/Change
module pkg { module msg {
struct Change {
  string id;
};
};
};
"#;

    let resolved = resolve_schema("pkg/msg/Root", schema).expect("resolve should succeed");

    assert_eq!(
        resolved.root,
        vec!["pkg".to_string(), "msg".to_string(), "Root".to_string()]
    );
    let root = resolved
        .structs
        .get(&vec!["pkg".into(), "msg".into(), "Root".into()])
        .expect("Root should exist");
    assert_eq!(root.fields.len(), 2);
}

#[test]
fn parse_idl_section_supports_multiple_close_tokens_on_one_line() {
    let parsed = parse_idl_section(
        r#"
module ex {
  module msg {
    struct Sample {
      uint32 x;
    }; }; };
"#,
    )
    .expect("IDL should parse");

    assert!(parsed.structs.contains_key(&vec![
        "ex".to_string(),
        "msg".to_string(),
        "Sample".to_string(),
    ]));
}

#[test]
fn resolve_schema_ignores_multiline_block_comments() {
    let schema = r#"
================================================================================
IDL: ex/msg/Root
#include "ex/msg/Item.idl"
#include "ex/msg/Limits.idl"
module ex {
  module msg {
    /**
    * Block comment before the struct declaration.
    */
    struct Root {
      /* Inline block comment before a named bound. */
      sequence<Item, kItemsCapacity> items;
    };
  };
};
================================================================================
IDL: ex/msg/Item
module ex {
  module msg {
    struct Item {
      uint32 id;
    };
  };
};
================================================================================
IDL: ex/msg/Limits
module ex {
  module msg {
    const uint16 kItemsCapacity = 500;
  };
};
"#;

    let resolved = resolve_schema("ex/msg/Root", schema).expect("resolve should succeed");

    let root = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Root".into()])
        .expect("Root should exist");
    assert_eq!(root.fields.len(), 1);
    assert_eq!(root.fields[0].name, "items");
    assert!(matches!(
        root.fields[0].ty,
        ResolvedType::Sequence { max_len: None, .. }
    ));
}
