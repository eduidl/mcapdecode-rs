use arrow::datatypes::{DataType, Field, Schema};
use mcapdecode_arrow::{
    ArrowConvertError, MessageBatchSchema, MetadataColumns, MetadataTimestampFormat,
};

fn body_schema(field_names: &[&str]) -> Schema {
    Schema::new(
        field_names
            .iter()
            .map(|name| Field::new(*name, DataType::Int32, true))
            .collect::<Vec<_>>(),
    )
}

fn column_names(schema: &MessageBatchSchema) -> Vec<String> {
    schema
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

#[test]
fn default_metadata_columns_are_unprefixed() {
    let schema = MessageBatchSchema::new(body_schema(&["speed"]), MetadataColumns::default())
        .expect("no collision");

    assert_eq!(column_names(&schema), ["log_time", "publish_time", "speed"]);
}

#[test]
fn metadata_prefix_applies_to_every_metadata_column() {
    let metadata = MetadataColumns::with_prefix("@");
    assert_eq!(
        metadata.names(),
        ["@log_time".to_string(), "@publish_time".to_string()]
    );

    let schema = MessageBatchSchema::new(body_schema(&["speed"]), metadata).expect("no collision");
    assert_eq!(
        column_names(&schema),
        ["@log_time", "@publish_time", "speed"]
    );
}

#[test]
fn metadata_timestamps_can_be_emitted_as_unix_nanoseconds() {
    let metadata =
        MetadataColumns::default().with_timestamp_format(MetadataTimestampFormat::UnixNanoseconds);
    let schema = MessageBatchSchema::new(body_schema(&["speed"]), metadata).unwrap();

    assert_eq!(schema.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.schema().field(1).data_type(), &DataType::Int64);
    assert_eq!(
        schema.metadata().timestamp_format(),
        MetadataTimestampFormat::UnixNanoseconds
    );
}

#[test]
fn body_field_shadowing_a_metadata_column_is_rejected() {
    let error = MessageBatchSchema::new(
        body_schema(&["log_time", "publish_time", "speed"]),
        MetadataColumns::default(),
    )
    .expect_err("payload fields must not shadow metadata columns");

    assert!(matches!(
        error,
        ArrowConvertError::MetadataColumnCollision { .. }
    ));
    assert_eq!(
        error.to_string(),
        "metadata columns [log_time, publish_time] collide with payload fields; \
         set a metadata prefix to disambiguate"
    );
}

#[test]
fn a_prefix_resolves_a_collision() {
    let schema = MessageBatchSchema::new(
        body_schema(&["log_time", "speed"]),
        MetadataColumns::with_prefix("_"),
    )
    .expect("the prefix moves the metadata columns out of the way");

    assert_eq!(
        column_names(&schema),
        ["_log_time", "_publish_time", "log_time", "speed"]
    );
}

#[test]
fn body_keeps_only_the_payload_fields() {
    let schema = MessageBatchSchema::new(body_schema(&["speed"]), MetadataColumns::default())
        .expect("no collision");

    assert_eq!(schema.body().fields().len(), 1);
    assert_eq!(schema.body().field(0).name(), "speed");
    assert_eq!(schema.metadata(), &MetadataColumns::default());
}
