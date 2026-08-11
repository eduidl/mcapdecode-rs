use super::*;

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_table_registers_decoded_topic_for_sql_queries() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let provider = reader
        .datafusion_table(
            &fixture_path("with_summary.mcap"),
            "/decoded",
            &batch_options(1),
        )
        .unwrap();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let batches = runtime.block_on(async {
        let context = SessionContext::new();
        context.register_table("messages", provider).unwrap();
        context
            .sql("SELECT value FROM messages WHERE value > 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });

    let schema = batches
        .first()
        .expect("query should return a batch containing the decoded message")
        .schema();
    let batch = concat_batches(&schema, &batches).unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("expected Int64Array for 'value' column");
    assert_eq!(values.values(), &[2]);
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_table_prepares_topic_once() {
    let topic_decoder_builds = Arc::new(AtomicUsize::new(0));
    let reader = McapReader::builder()
        .with_decoder(Box::new(CountingJsonDecoder {
            topic_decoder_builds: Arc::clone(&topic_decoder_builds),
        }))
        .build();

    reader
        .datafusion_table(
            &fixture_path("with_summary.mcap"),
            "/decoded",
            &RecordBatchOptions::default(),
        )
        .unwrap();

    assert_eq!(topic_decoder_builds.load(Ordering::Relaxed), 1);
}
