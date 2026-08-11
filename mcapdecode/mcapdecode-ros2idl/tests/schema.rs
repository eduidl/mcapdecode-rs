use mcapdecode_core::EnumVariant;
use mcapdecode_ros2_common::{PrimitiveType, ResolvedType, TypeExpr};
use mcapdecode_ros2idl::{parse_idl_section, parse_schema_bundle, resolve_schema};

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

    let bundle = parse_schema_bundle("ex/msg/B", schema).expect("bundle parse should succeed");
    assert_eq!(bundle.sections.len(), 2);
    assert_eq!(
        bundle.main_section("ex/msg/B").map(|section| &section.path),
        Some(&"ex/msg/B".to_string())
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
    let bundle = parse_schema_bundle("localization_msgs/msg/Pose", schema)
        .expect("bundle parse should succeed");
    assert_eq!(bundle.sections.len(), 1);
    assert_eq!(
        bundle
            .main_section("localization_msgs/msg/Pose")
            .map(|section| &section.path),
        Some(&"localization_msgs/msg/Pose".to_string())
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
    assert_eq!(
        variants,
        &vec![
            EnumVariant::new("OK", 0),
            EnumVariant::new("WARN", 1),
            EnumVariant::new("ERROR", 2),
        ]
    );

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

/// Consecutive `>` characters close nested templates, rather than forming the
/// constant-expression shift operator token.
#[test]
fn parse_idl_section_supports_adjacent_nested_template_closers() {
    let parsed = parse_idl_section(
        r#"
module ex { module msg {
  struct Msg {
    sequence<string<5>> names;
    sequence<sequence<uint32>> nested;
  };
}; };
"#,
    )
    .expect("nested template terminators should parse");

    let message = parsed
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Msg".into()])
        .expect("Msg should exist");
    assert_eq!(
        message.fields[0].ty,
        TypeExpr::Sequence {
            elem: Box::new(TypeExpr::BoundedString(5)),
            max_len: None,
        }
    );
    assert_eq!(
        message.fields[1].ty,
        TypeExpr::Sequence {
            elem: Box::new(TypeExpr::Sequence {
                elem: Box::new(TypeExpr::Primitive(PrimitiveType::U32)),
                max_len: None,
            }),
            max_len: None,
        }
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

/// `@value` sets an enumerator's value; the following ones continue from it.
#[test]
fn resolve_schema_preserves_explicit_and_implicit_enum_values() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    enum E {
      @value(4) A,

      B,
      @value(0x0c)
      C,
      D,
      @value(value = -1) E
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
        Some(&vec![
            EnumVariant::new("A", 4),
            EnumVariant::new("B", 5),
            EnumVariant::new("C", 12),
            EnumVariant::new("D", 13),
            EnumVariant::new("E", -1),
        ])
    );
}

/// Annotations other than `@value` are still ignored, including ones whose arguments
/// contain parentheses inside string literals.
#[test]
fn resolve_schema_ignores_other_annotations_on_enumerators() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    enum E {
      @verbatim (language="comment", text="a (parenthesized) note") A,
      @value(9) @verbatim (language="comment", text="b") B,
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
        Some(&vec![
            EnumVariant::new("A", 0),
            EnumVariant::new("B", 9),
            EnumVariant::new("C", 10),
        ])
    );
}

/// An enumerator value that does not fit in 32 bits is rejected rather than truncated
/// to a value that would alias another variant on the wire.
#[test]
fn resolve_schema_rejects_enum_value_outside_32_bits() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    enum E {
      @value(4294967296) A,
      B,
    };
    struct Msg {
      E value;
    };
  };
};
"#;

    let err = resolve_schema("ex/msg/Msg", schema).expect_err("should reject the value");
    assert!(
        format!("{err:#}").contains("32-bit"),
        "unexpected error: {err:#}"
    );
}

/// The implicit value after the largest serializable enum value must also be rejected.
#[test]
fn resolve_schema_rejects_implicit_enum_value_outside_32_bits() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    enum E {
      @value(4294967295) LAST,
      OVERFLOW
    };
    struct Msg { E value; };
  };
};
"#;

    let error = resolve_schema("ex/msg/Msg", schema).expect_err("implicit overflow must fail");
    assert!(format!("{error:#}").contains("32-bit"));
}

/// An annotation whose argument list spans several lines still binds to its enumerator.
#[test]
fn resolve_schema_handles_multiline_annotations_on_enumerators() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    enum E {
      @verbatim (language="comment",
                 text="a note") A,
      @value(
        9) B,
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
        Some(&vec![
            EnumVariant::new("A", 0),
            EnumVariant::new("B", 9),
            EnumVariant::new("C", 10),
        ])
    );
}

/// `VARIANT = 1` is not ROS 2 IDL syntax and is rejected instead of being silently
/// interpreted as an uninitialized enumerator.
#[test]
fn resolve_schema_rejects_non_idl_enum_initializers() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex {
  module msg {
    enum E {
      A = 4,
      B
    };
    struct Msg {
      E value;
    };
  };
};
"#;

    let err = resolve_schema("ex/msg/Msg", schema).expect_err("initializer must be rejected");
    assert!(format!("{err:#}").contains("unexpected trailing characters"));
}

#[test]
fn resolve_schema_accepts_single_line_idl_and_comma_separated_enumerators() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex { module msg { enum E { @value(4) A, B, @value(9) C };
struct Msg { E value; uint32 count; }; }; };
"#;

    let resolved = resolve_schema("ex/msg/Msg", schema).expect("single-line IDL should parse");
    let key = vec!["ex".to_string(), "msg".to_string(), "E".to_string()];
    assert_eq!(
        resolved.enums.get(&key),
        Some(&vec![
            EnumVariant::new("A", 4),
            EnumVariant::new("B", 5),
            EnumVariant::new("C", 9),
        ])
    );
}

#[test]
fn resolve_schema_rejects_trailing_enum_comma() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex { module msg { enum E { A, }; struct Msg { E value; }; }; };
"#;

    let err = resolve_schema("ex/msg/Msg", schema).expect_err("trailing comma must be rejected");
    assert!(format!("{err:#}").contains("trailing `,`"));
}

#[test]
fn resolve_schema_rejects_enum_variants_without_comma() {
    let schema = r#"
================================================================================
IDL: ex/msg/Msg
module ex { module msg { enum E { A B }; struct Msg { E value; }; }; };
"#;

    let err = resolve_schema("ex/msg/Msg", schema).expect_err("missing comma must be rejected");
    assert!(format!("{err:#}").contains("unexpected trailing characters"));
}

#[test]
fn parse_idl_section_accepts_comparison_and_shift_operators_in_const_values() {
    let parsed = parse_idl_section(
        r#"
module ex { module msg {
  struct Msg {
    const long FLAG = 1 << 3;
    const boolean B = 1 > 0;
    const long SHIFT_RIGHT = 8 >> 1;
    uint32 value;
  };
}; };
"#,
    )
    .expect("constant expressions with angle operators should parse");

    let message = parsed
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Msg".into()])
        .expect("Msg should exist");
    assert_eq!(message.consts[2].value, "8 >> 1");

    assert!(
        parsed
            .structs
            .get(&vec!["ex".into(), "msg".into(), "Msg".into()])
            .is_some_and(|msg| msg.fields.len() == 1)
    );
}

#[test]
fn parse_idl_section_removes_comments_from_const_values() {
    let parsed = parse_idl_section(
        "module ex { module msg { struct Msg { const long C = 1 /* two */ + 2; uint32 value; }; }; };",
    )
    .expect("comments in a constant expression should be ignored");

    let message = parsed
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Msg".into()])
        .expect("Msg should exist");
    assert_eq!(message.consts[0].value, "1 + 2");
}

#[test]
fn parse_idl_section_preserves_adjacent_const_expression_tokens() {
    let parsed = parse_idl_section(
        "module ex { module msg { struct Msg { const long NEGATIVE=-1; const float EXPONENT=1.0e-3; uint32 value; }; }; };",
    )
    .expect("adjacent constant-expression tokens should parse");

    let message = parsed
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Msg".into()])
        .expect("Msg should exist");
    assert_eq!(message.consts[0].value, "-1");
    assert_eq!(message.consts[1].value, "1.0e-3");
}

#[test]
fn parse_idl_section_ignores_comments_in_value_annotations() {
    let parsed = parse_idl_section(
        "module ex { module msg { enum E { @value(/* explicit */ 7) A, B }; }; };",
    )
    .expect("comments in @value should be ignored");

    assert_eq!(
        parsed.enums[&vec!["ex".into(), "msg".into(), "E".into()]].variants,
        vec![EnumVariant::new("A", 7), EnumVariant::new("B", 8)]
    );
}

#[test]
fn parse_idl_section_rejects_unclosed_angle_include_at_end_of_input() {
    let error = parse_idl_section("#include <")
        .expect_err("an unterminated angle include should not loop forever");

    assert!(format!("{error:#}").contains("unclosed #include path"));
}

#[test]
fn parse_idl_section_rejects_non_ascii_source_without_panicking() {
    let error =
        parse_idl_section("\u{feff}module ex { module msg { struct Msg { uint32 value; }; }; };")
            .expect_err("a BOM is not an IDL token");

    assert!(format!("{error:#}").contains("non-ASCII character"));
}

#[test]
fn parse_idl_section_reports_the_actual_line_after_blank_lines() {
    let err = parse_idl_section(
        "module ex {\nmodule msg {\nstruct Msg {\n\n\nuint32 value\n};\n};\n};\n",
    )
    .expect_err("a field without a semicolon must fail");

    assert!(format!("{err:#}").contains("line 6"));
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

#[test]
fn resolve_schema_accepts_leading_scope_separator() {
    let schema = r#"
================================================================================
IDL: ex/msg/Sample
module ex { module msg {
struct Sample {
  ::builtin_interfaces::msg::Time stamp;
};
}; };
"#;

    let resolved = resolve_schema("ex/msg/Sample", schema).expect("schema should resolve");
    let sample = resolved
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Sample".into()])
        .expect("Sample should exist");
    assert!(matches!(
        &sample.fields[0].ty,
        ResolvedType::Struct(name)
            if name == &vec![
                "builtin_interfaces".to_string(),
                "msg".to_string(),
                "Time".to_string(),
            ]
    ));
}

#[test]
fn parse_idl_section_accepts_hexadecimal_positive_bounds() {
    let parsed = parse_idl_section(
        "module ex { module msg { struct Sample { string<0x40> name; sequence<uint32, 0x20> values; uint32 ids[0x10]; }; }; };",
    )
    .expect("IDL should parse");
    let sample = parsed
        .structs
        .get(&vec!["ex".into(), "msg".into(), "Sample".into()])
        .expect("Sample should exist");
    assert!(matches!(&sample.fields[0].ty, TypeExpr::BoundedString(64)));
    assert!(matches!(
        &sample.fields[1].ty,
        TypeExpr::Sequence {
            max_len: Some(32),
            ..
        }
    ));
    assert_eq!(sample.fields[2].fixed_len, Some(16));
}

#[test]
fn parse_idl_section_rejects_zero_string_bound() {
    let err =
        parse_idl_section("module ex { module msg { struct Sample { string<0> name; }; }; };")
            .expect_err("zero string bound must be rejected");
    assert!(format!("{err:#}").contains("expected positive integer"));
}

#[test]
fn parse_idl_section_rejects_zero_fixed_array_bound() {
    let err =
        parse_idl_section("module ex { module msg { struct Sample { uint32 values[0]; }; }; };")
            .expect_err("zero fixed array length must be rejected");
    assert!(format!("{err:#}").contains("expected positive integer"));
}

#[test]
fn parse_idl_section_reports_unclosed_const_parenthesis_at_its_opening_line() {
    let err = parse_idl_section(
        "module ex { module msg {\nconst uint32 BAD = (1 + 2;\nstruct Sample { uint32 value; };\n}; };",
    )
    .expect_err("unclosed const parenthesis must be rejected");
    let message = format!("{err:#}");
    assert!(message.contains("line 2"));
    assert!(message.contains("unclosed `(` in const expression"));
}

#[test]
fn parse_idl_section_ignores_annotations_before_struct_and_module_closers() {
    parse_idl_section(
        "module ex { module msg { struct Sample { @verbatim(\"end of struct\") }; @verbatim(\"end of module\") }; };",
    )
    .expect("trailing annotations before closers should be ignored");
}

#[test]
fn parse_idl_section_reports_unsupported_multi_dimensional_arrays() {
    let err =
        parse_idl_section("module ex { module msg { struct Sample { uint32 values[3][3]; }; }; };")
            .expect_err("multi-dimensional array must be rejected");
    assert!(format!("{err:#}").contains("unsupported multi-dimensional fixed array"));
}

#[test]
fn parse_idl_section_reports_unsupported_multiple_field_declarators() {
    let err = parse_idl_section("module ex { module msg { struct Sample { uint32 a, b; }; }; };")
        .expect_err("multiple declarators must be rejected");
    assert!(format!("{err:#}").contains("unsupported multiple field declarators"));
}

#[test]
fn parse_idl_section_rejects_invalid_unsigned_type_combinations() {
    for idl in [
        "module ex { module msg { struct Sample { unsigned value; }; }; };",
        "module ex { module msg { struct Sample { unsigned char value; }; }; };",
    ] {
        let err = parse_idl_section(idl).expect_err("invalid unsigned type must be rejected");
        assert!(format!("{err:#}").contains("unsupported IDL type starting with `unsigned`"),);
    }
}
