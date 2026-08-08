//! The sequential and parallel read paths must agree on a clustered topic.
//!
//! This lives in the unpublished bench crate rather than in `mcapdecode` because it
//! needs generated fixtures; the allocation contract it pairs with is in
//! `reader_alloc_budget.rs`.

mod common;

use common::{clustered_fixture, read_topic};

#[test]
fn clustered_topic_is_readable_from_both_paths() {
    let path = clustered_fixture();
    let sequential = read_topic(&path, false);
    assert!(sequential > 0, "fixture should contain the topic");
    assert_eq!(sequential, read_topic(&path, true));
}
