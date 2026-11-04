use mcapdecode_core::{
    DataTypeDef as T, ElementDef, FieldDef, FieldDefs, Value, ValueAccessError as E,
};

fn field(name: &str, ty: T) -> FieldDef {
    FieldDef::new(name, ty, false)
}
fn element(ty: T) -> Box<ElementDef> {
    Box::new(ElementDef::new(ty, false))
}

#[test]
fn nested_structs_sequences_and_maps_keep_their_schema() {
    let point_schema = std::sync::Arc::new(vec![field("x", T::F32)].into());
    let point = T::Struct(std::sync::Arc::clone(&point_schema));
    let fields = FieldDefs::new(vec![
        field("poses", T::List(element(point.clone()))),
        field("array", T::Array(element(point.clone()), 1)),
        field(
            "map",
            T::Map {
                key: element(T::String),
                value: element(point),
            },
        ),
    ]);
    let point = || Value::structure(std::sync::Arc::clone(&point_schema), vec![Value::F32(1.5)]);
    let value = Value::structure(
        std::sync::Arc::new(fields.into()),
        vec![
            Value::List(vec![point()]),
            Value::Array(vec![point()]),
            Value::Map(vec![(Value::string("p"), point())]),
        ],
    );
    let view = &value;
    let Value::Struct(root) = &value else {
        panic!("expected struct")
    };
    assert_eq!(
        root.fields()
            .unwrap()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        ["poses", "array", "map"]
    );
    assert_eq!(
        view.get("poses")
            .unwrap()
            .get(0)
            .unwrap()
            .get("x")
            .unwrap()
            .try_widen_f64(),
        Ok(1.5)
    );
    assert_eq!(
        view.get("array")
            .unwrap()
            .get(0)
            .unwrap()
            .get("x")
            .unwrap()
            .try_widen_f64(),
        Ok(1.5)
    );
    assert_eq!(
        view.get("map")
            .unwrap()
            .get("p")
            .unwrap()
            .get("x")
            .unwrap()
            .try_widen_f64(),
        Ok(1.5)
    );
    let map = view.get("map").unwrap().as_map().unwrap();
    assert_eq!(map.len(), 1);
    let (key, point) = map.iter().next().unwrap();
    assert_eq!(key.try_str(), Ok(Some("p")));
    assert_eq!(point.get("x").unwrap().try_widen_f64(), Ok(1.5));
    let sequence = view.get("poses").unwrap().as_sequence().unwrap();
    assert_eq!(sequence.len(), 1);
    assert_eq!(
        sequence
            .iter()
            .next()
            .unwrap()
            .get("x")
            .unwrap()
            .try_widen_f64(),
        Ok(1.5)
    );
    assert!(matches!(view.get("absent"), Err(E::NotFound)));
    assert!(matches!(
        view.get("poses").unwrap().get(1),
        Err(E::NotFound)
    ));
    assert!(matches!(
        view.get("map").unwrap().get("missing"),
        Err(E::NotFound)
    ));
    assert!(matches!(view.get(0), Err(E::TypeMismatch(_))));
}

#[test]
fn map_keys_and_schema_mismatches_are_reported() {
    let fields = FieldDefs::new(vec![
        field(
            "numbers",
            T::Map {
                key: element(T::I32),
                value: element(T::U64),
            },
        ),
        field(
            "boolean",
            T::Map {
                key: element(T::Bool),
                value: element(T::String),
            },
        ),
    ]);
    let schema = std::sync::Arc::new(fields.into());
    let value = Value::structure(
        std::sync::Arc::clone(&schema),
        vec![
            Value::Map(vec![(Value::I32(-1), Value::U64(7))]),
            Value::Map(vec![(Value::Bool(true), Value::string("yes"))]),
        ],
    );
    let view = &value;
    assert_eq!(
        view.get("numbers")
            .unwrap()
            .get_key(-1_i64)
            .unwrap()
            .try_widen_u64(),
        Ok(7)
    );
    assert!(matches!(
        view.get("numbers").unwrap().get_key(1_u8),
        Err(E::NotFound)
    ));
    assert_eq!(
        view.get("boolean")
            .unwrap()
            .get_key(true)
            .unwrap()
            .try_str(),
        Ok(Some("yes"))
    );
    let null = Value::structure(schema, vec![Value::Null, Value::Null]);
    assert!(matches!(
        null.get("numbers").unwrap().get_key(1_i32),
        Err(E::NullValue)
    ));
}

#[test]
fn positional_structs_and_length_mismatches_are_schema_mismatches() {
    let positional = Value::Struct(vec![Value::I32(1)].into());
    let Value::Struct(fields) = &positional else {
        panic!("expected struct")
    };
    assert!(matches!(fields.fields(), Err(E::SchemaMismatch)));
    assert!(matches!(positional.get("x"), Err(E::SchemaMismatch)));
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "struct values must match the schema fields")]
fn structure_rejects_a_value_count_that_differs_from_the_schema() {
    let schema = std::sync::Arc::new(vec![field("x", T::I32)].into());
    let _ = Value::structure(schema, vec![Value::I32(1), Value::I32(2)]);
}

#[test]
fn debug_output_shows_values_without_the_schema() {
    let schema = std::sync::Arc::new(vec![field("x", T::I32)].into());
    let value = Value::structure(schema, vec![Value::I32(1)]);
    assert_eq!(format!("{value:?}"), "Struct([I32(1)])");
}
