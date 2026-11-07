use std::sync::Arc;

use mcapdecode_core::{
    DataTypeDef, ElementDef, FieldDef, FieldPath, StructDef, Value, ValueAccessError as E,
};

fn point() -> Arc<StructDef> {
    Arc::new(StructDef::from(vec![
        FieldDef::new("x", DataTypeDef::F64, false),
        FieldDef::new("y", DataTypeDef::F64, false),
    ]))
}

fn element(data_type: DataTypeDef) -> Box<ElementDef> {
    Box::new(ElementDef::new(data_type, false))
}

fn schema(point: &Arc<StructDef>) -> Vec<FieldDef> {
    let pose = Arc::new(StructDef::from(vec![FieldDef::new(
        "position",
        DataTypeDef::Struct(Arc::clone(point)),
        true,
    )]));
    vec![
        FieldDef::new("pose", DataTypeDef::Struct(Arc::clone(&pose)), true),
        FieldDef::new(
            "poses",
            DataTypeDef::List(element(DataTypeDef::Struct(Arc::clone(&pose)))),
            false,
        ),
        FieldDef::new(
            "corners",
            DataTypeDef::Array(element(DataTypeDef::F64), 2),
            false,
        ),
        FieldDef::new("data", DataTypeDef::Uint8List, false),
        FieldDef::new(
            "labels",
            DataTypeDef::Map {
                key: element(DataTypeDef::String),
                value: element(DataTypeDef::I32),
            },
            false,
        ),
    ]
}

fn message(fields: &[FieldDef], point: &Arc<StructDef>) -> Value {
    let DataTypeDef::Struct(pose) = &fields[0].element.data_type else {
        unreachable!()
    };
    let pose_value = |x: f64| {
        let position = Value::structure(Arc::clone(point), vec![Value::F64(x), Value::F64(0.0)]);
        Value::structure(Arc::clone(pose), vec![position])
    };
    let root = Arc::new(StructDef::from(fields.to_vec()));
    Value::structure(
        root,
        vec![
            pose_value(1.0),
            Value::List(vec![
                pose_value(2.0),
                Value::structure(Arc::clone(pose), vec![Value::Null]),
            ]),
            Value::Array(vec![Value::F64(3.0), Value::F64(4.0)]),
            Value::Uint8List(Arc::from([1_u8].as_slice())),
            Value::Map(vec![(Value::string("a"), Value::I32(5))]),
        ],
    )
}

#[test]
fn resolved_path_gives_type_and_value() {
    let point = point();
    let fields = schema(&point);
    let value = message(&fields, &point);

    let path = FieldPath::parse(&fields, "pose.position.x").unwrap();
    assert_eq!(path.data_type(), &DataTypeDef::F64);
    assert!(!path.element().nullable);
    assert_eq!(path.get(&value).unwrap().try_widen_f64(), Ok(1.0));

    let from_names = FieldPath::resolve(&fields, ["pose", "position", "x"]).unwrap();
    assert_eq!(from_names.get(&value).unwrap().try_widen_f64(), Ok(1.0));

    let position = FieldPath::parse(&fields, "pose.position").unwrap();
    assert_eq!(
        position.data_type(),
        &DataTypeDef::Struct(Arc::clone(&point))
    );
    assert!(position.element().nullable);
}

#[test]
fn indices_step_into_lists_and_arrays() {
    let point = point();
    let fields = schema(&point);
    let value = message(&fields, &point);

    let first = FieldPath::parse(&fields, "poses.0.position.x").unwrap();
    assert_eq!(first.get(&value).unwrap().try_widen_f64(), Ok(2.0));
    let beyond = FieldPath::parse(&fields, "poses.5.position.x").unwrap();
    assert!(matches!(beyond.get(&value), Err(E::NotFound)));

    let corner = FieldPath::resolve(&fields, ["corners", "1"]).unwrap();
    assert_eq!(corner.get(&value).unwrap().try_widen_f64(), Ok(4.0));
    assert!(matches!(
        FieldPath::parse(&fields, "corners.2"),
        Err(E::NotFound)
    ));
}

#[test]
fn invalid_paths_fail_before_reading() {
    let fields = schema(&point());

    assert!(matches!(
        FieldPath::parse(&fields, "pose.orientation"),
        Err(E::NotFound)
    ));
    assert!(matches!(FieldPath::parse(&fields, ""), Err(E::NotFound)));
    assert!(matches!(FieldPath::parse(&fields, "0"), Err(E::NotFound)));
    assert!(matches!(
        FieldPath::resolve(&fields, [""; 0]),
        Err(E::NotFound)
    ));
    assert!(matches!(
        FieldPath::parse(&fields, "pose.position.x.y"),
        Err(E::TypeMismatch(_))
    ));
    assert!(matches!(
        FieldPath::parse(&fields, "pose.0"),
        Err(E::TypeMismatch(_))
    ));
    assert!(matches!(
        FieldPath::parse(&fields, "data.0"),
        Err(E::TypeMismatch(_))
    ));
    assert!(matches!(
        FieldPath::parse(&fields, "labels.0"),
        Err(E::TypeMismatch(_))
    ));
    assert!(matches!(
        FieldPath::parse(&fields, "labels.a"),
        Err(E::TypeMismatch(_))
    ));
}

#[test]
fn null_inside_the_path_is_reported() {
    let point = point();
    let fields = schema(&point);
    let value = message(&fields, &point);

    let missing = FieldPath::parse(&fields, "poses.1.position.x").unwrap();
    assert!(matches!(missing.get(&value), Err(E::NullValue)));
    let at_end = FieldPath::parse(&fields, "poses.1.position").unwrap();
    assert!(matches!(at_end.get(&value), Ok(Value::Null)));
}

#[test]
fn values_from_another_schema_are_rejected() {
    let point = point();
    let fields = schema(&point);
    let path = FieldPath::parse(&fields, "pose.position.y").unwrap();

    let renamed = Arc::new(StructDef::from(vec![
        FieldDef::new("x", DataTypeDef::F64, false),
        FieldDef::new("z", DataTypeDef::F64, false),
    ]));
    let other = message(&schema(&renamed), &renamed);
    assert!(matches!(path.get(&other), Err(E::SchemaMismatch)));
    assert!(matches!(path.get(&Value::I32(0)), Err(E::TypeMismatch(_))));

    // Same names and positions, but the target field has another type.
    let retyped = Arc::new(StructDef::from(vec![
        FieldDef::new("x", DataTypeDef::F64, false),
        FieldDef::new("y", DataTypeDef::I32, false),
    ]));
    let other = message(&schema(&retyped), &retyped);
    assert!(matches!(path.get(&other), Err(E::SchemaMismatch)));

    // Fields before the target only need matching names.
    let x = FieldPath::parse(&fields, "pose.position.x").unwrap();
    assert_eq!(x.get(&other).unwrap().try_widen_f64(), Ok(1.0));
}

#[test]
fn get_path_follows_values_without_a_schema() {
    let point = point();
    let fields = schema(&point);
    let value = message(&fields, &point);

    assert_eq!(
        value.get_path("pose.position.x").unwrap().try_widen_f64(),
        Ok(1.0)
    );
    assert_eq!(
        value
            .get_path("poses.0.position.x")
            .unwrap()
            .try_widen_f64(),
        Ok(2.0)
    );
    assert_eq!(value.get_path("labels.a").unwrap().try_widen_i64(), Ok(5));
    assert!(matches!(
        value.get_path("poses.1.position.x"),
        Err(E::NullValue)
    ));
    assert!(matches!(value.get_path("pose.missing"), Err(E::NotFound)));
}
