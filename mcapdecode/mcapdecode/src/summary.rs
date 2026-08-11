//! Lookups against the MCAP summary section.

use std::{collections::BTreeMap, sync::Arc};

use crate::{TopicInfo, error::McapReaderError};

pub(crate) fn get_channel_from_summary<'a>(
    summary: &'a mcap::read::Summary,
    topic: &str,
) -> Result<&'a Arc<mcap::Channel<'a>>, McapReaderError> {
    let mut channels = summary.channels.values().filter(|ch| ch.topic == topic);
    let first = channels.next().ok_or(McapReaderError::TopicNotFound)?;
    if channels.next().is_some() {
        return Err(McapReaderError::MultipleChannels);
    }
    Ok(first)
}

pub(crate) fn topic_infos_from_summary(summary: &mcap::read::Summary) -> Vec<TopicInfo> {
    let stats = summary.stats.as_ref();
    let mut topics = BTreeMap::<String, TopicInfo>::new();

    for channel in summary.channels.values() {
        let message_count = stats.map(|summary_stats| {
            summary_stats
                .channel_message_counts
                .get(&channel.id)
                .copied()
                .unwrap_or_default()
        });
        let schema = channel.schema.as_ref();

        topics
            .entry(channel.topic.clone())
            .and_modify(|topic_info| {
                topic_info.channel_count += 1;
                if let (Some(existing), Some(current)) =
                    (topic_info.message_count.as_mut(), message_count)
                {
                    *existing += current;
                }
            })
            .or_insert_with(|| TopicInfo {
                topic: channel.topic.clone(),
                message_count,
                schema_name: schema.map(|schema| schema.name.clone()),
                schema_encoding: schema
                    .map(|schema| schema.encoding.clone())
                    .unwrap_or_default(),
                message_encoding: channel.message_encoding.clone(),
                channel_count: 1,
            });
    }

    topics.into_values().collect()
}

pub(crate) fn get_schema_from_channel<'a>(
    channel: &'a Arc<mcap::Channel>,
) -> Result<&'a Arc<mcap::Schema<'a>>, McapReaderError> {
    channel
        .schema
        .as_ref()
        .ok_or(McapReaderError::SchemaNotAvailable)
}
