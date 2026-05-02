use mcapdecode_ros2_common::TypeExpr;
use mcapdecode_ros2msg::{SchemaBundle, parse_msg, resolve_for_cdr};

// ── existing tests ─────────────────────────────────────────────────────────────

#[test]
fn parse_basic_primitives() {
    let msg = r#"
int32 x
float64 y
string name
"#;
    let result = parse_msg("test_msgs/msg/Basic", msg).unwrap();
    assert_eq!(result.fields.len(), 3);
    assert_eq!(result.full_name, vec!["test_msgs", "msg", "Basic"]);
}

#[test]
fn parse_fixed_arrays() {
    let msg = "float64[3] position";
    let result = parse_msg("test_msgs/msg/Array", msg).unwrap();
    assert_eq!(result.fields.len(), 1);
    assert_eq!(result.fields[0].fixed_len, Some(3));
}

#[test]
fn parse_dynamic_sequences() {
    let msg = "float64[] data";
    let result = parse_msg("test_msgs/msg/Seq", msg).unwrap();
    assert_eq!(result.fields.len(), 1);
    // TypeExpr::Sequence with max_len None
    match &result.fields[0].ty {
        TypeExpr::Sequence { max_len, .. } => {
            assert_eq!(*max_len, None);
        }
        _ => panic!("Expected Sequence type"),
    }
}

#[test]
fn parse_bounded_sequences() {
    let msg = "float64[<=10] data";
    let result = parse_msg("test_msgs/msg/BoundedSeq", msg).unwrap();
    match &result.fields[0].ty {
        TypeExpr::Sequence { max_len, .. } => {
            assert_eq!(*max_len, Some(10));
        }
        _ => panic!("Expected Sequence type"),
    }
}

#[test]
fn parse_nested_types() {
    let msg = "geometry_msgs/Point position";
    let result = parse_msg("test_msgs/msg/Nested", msg).unwrap();
    match &result.fields[0].ty {
        TypeExpr::Scoped(path) => {
            assert_eq!(path, &vec!["geometry_msgs", "msg", "Point"]);
        }
        _ => panic!("Expected Scoped type"),
    }
}

#[test]
fn parse_constants() {
    let msg = r#"
int32 STATUS_OK=0
string MODE="auto"
"#;
    let result = parse_msg("test_msgs/msg/Const", msg).unwrap();
    assert_eq!(result.consts.len(), 2);
}

#[test]
fn parse_bounded_string() {
    let msg = "string<=20 name";
    let result = parse_msg("test_msgs/msg/BoundedStr", msg).unwrap();
    match &result.fields[0].ty {
        TypeExpr::BoundedString(n) => {
            assert_eq!(*n, 20);
        }
        _ => panic!("Expected BoundedString type"),
    }
}

#[test]
fn parse_schema_name_two_parts() {
    let msg = "int32 x";
    let result = parse_msg("std_msgs/String", msg).unwrap();
    assert_eq!(result.full_name, vec!["std_msgs", "msg", "String"]);
}

#[test]
fn parse_schema_name_three_parts() {
    let msg = "int32 x";
    let result = parse_msg("geometry_msgs/msg/Point", msg).unwrap();
    assert_eq!(result.full_name, vec!["geometry_msgs", "msg", "Point"]);
}

// ── new tests ──────────────────────────────────────────────────────────────────

/// A schema name with one component (no `/`) must be rejected.
#[test]
fn parse_schema_name_single_part_is_rejected() {
    let err = parse_msg("BadName", "int32 x").unwrap_err();
    assert!(
        format!("{err:#}").contains("invalid schema name format"),
        "unexpected error: {err:#}"
    );
}

/// A schema name with four or more `/`-separated components must be rejected.
#[test]
fn parse_schema_name_four_parts_is_rejected() {
    let err = parse_msg("a/b/c/d", "int32 x").unwrap_err();
    assert!(
        format!("{err:#}").contains("invalid schema name format"),
        "unexpected error: {err:#}"
    );
}

/// A `builtin_interfaces/Time` field is parsed as a `Scoped` reference with
/// the expanded `["builtin_interfaces", "msg", "Time"]` path.
#[test]
fn parse_builtin_time_field_produces_scoped_type() {
    let msg = "builtin_interfaces/Time stamp";
    let result = parse_msg("test_msgs/msg/Stamped", msg).unwrap();
    assert_eq!(result.fields.len(), 1);
    match &result.fields[0].ty {
        TypeExpr::Scoped(path) => {
            assert_eq!(
                path,
                &vec!["builtin_interfaces", "msg", "Time"],
                "complex type should expand pkg/Type → [pkg, msg, Type]"
            );
        }
        _ => panic!("Expected Scoped type for builtin_interfaces/Time"),
    }
}

/// A `builtin_interfaces/Duration` field follows the same expansion rule.
#[test]
fn parse_builtin_duration_field_produces_scoped_type() {
    let msg = "builtin_interfaces/Duration elapsed";
    let result = parse_msg("test_msgs/msg/Timed", msg).unwrap();
    match &result.fields[0].ty {
        TypeExpr::Scoped(path) => {
            assert_eq!(path, &vec!["builtin_interfaces", "msg", "Duration"]);
        }
        _ => panic!("Expected Scoped type for builtin_interfaces/Duration"),
    }
}

/// Multiple fields of mixed primitive types are all parsed with correct names.
#[test]
fn parse_all_integer_primitives() {
    let msg = r#"
int8 a
int16 b
int32 c
int64 d
uint8 e
uint16 f
uint32 g
uint64 h
"#;
    let result = parse_msg("test_msgs/msg/Ints", msg).unwrap();
    assert_eq!(result.fields.len(), 8);
    let names: Vec<&str> = result.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b", "c", "d", "e", "f", "g", "h"]);
}

/// A `bool` field parses to a `Primitive` (not Scoped or Sequence).
#[test]
fn parse_bool_field() {
    let msg = "bool active";
    let result = parse_msg("test_msgs/msg/Flag", msg).unwrap();
    assert_eq!(result.fields.len(), 1);
    assert!(
        matches!(result.fields[0].ty, TypeExpr::Primitive(_)),
        "bool should map to a primitive type"
    );
}

#[test]
fn parse_schema_bundle_with_root_and_dependency_sections() {
    let schema = r#"
std_msgs/Header header
geometry_msgs/Vector3 magnetic_field
float64[9] magnetic_field_covariance
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
"#;
    let bundle = SchemaBundle::parse("sensor_msgs/msg/MagneticField", schema).unwrap();

    assert_eq!(bundle.sections.len(), 3);
    assert_eq!(
        bundle.sections[0].msg_path,
        vec!["sensor_msgs", "msg", "MagneticField"]
    );
    assert_eq!(
        bundle.sections[1].msg_path,
        vec!["geometry_msgs", "msg", "Vector3"]
    );
    assert_eq!(
        bundle.sections[2].msg_path,
        vec!["std_msgs", "msg", "Header"]
    );
}

#[test]
fn resolve_for_cdr_supports_ros2msg_schema_bundles() {
    let schema = r#"
std_msgs/Header header
Pose pose
================================================================================
MSG: geometry_msgs/Pose
Point position
Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x 0
float64 y 0
float64 z 0
float64 w 1
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
"#;
    let resolved = resolve_for_cdr("geometry_msgs/msg/PoseStamped", schema.as_bytes()).unwrap();

    assert_eq!(resolved.root, vec!["geometry_msgs", "msg", "PoseStamped"]);
    assert!(resolved.structs.contains_key(&vec![
        "geometry_msgs".into(),
        "msg".into(),
        "Pose".into()
    ]));
    assert!(
        resolved
            .structs
            .contains_key(&vec!["std_msgs".into(), "msg".into(), "Header".into()])
    );
}
