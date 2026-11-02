//! Givens about what is registered and what is stored: a domain's
//! registered state, its streams' events and snapshots.

use cucumber::gherkin::Step;
use cucumber::given;

use angzarr_embedded::v1::ConflictResolution;

use crate::tests::world::Coordinator;

#[given(
    regex = r#"^an? "([\w-]+)" aggregate whose registered state (supports|does not support) COMMUTATIVE$"#
)]
fn registered_state(w: &mut Coordinator, domain: String, supports: String) {
    let mut resolutions = vec![
        ConflictResolution::Strict,
        ConflictResolution::AggregateHandles,
    ];
    if supports == "supports" {
        resolutions.push(ConflictResolution::Commutative);
    }
    w.domain(&domain).resolutions = resolutions;
}

/// The order stream's events, by sequence and type, from 0.
#[given("the aggregate has events:")]
fn events_table(w: &mut Coordinator, step: &Step) {
    let table = step.table.as_ref().expect("a table of events");
    let cover = w.order();
    for (n, row) in table.rows.iter().skip(1).enumerate() {
        assert_eq!(row[0].trim(), n.to_string(), "events from 0, in order");
        w.write(&cover, &[row[1].trim()]);
    }
}

#[given(regex = r#"^the "order" aggregate has a snapshot at sequence (\d+)$"#)]
fn snapshot_at(w: &mut Coordinator, sequence: u64) {
    let cover = w.order();
    let (state_type, version) = {
        let order = w.domain("order");
        (order.state_type.clone(), order.rules_version.clone())
    };
    w.snapshot(&cover, sequence, &state_type, &version);
}

#[given(regex = r#"^a new "order" aggregate with no events$"#)]
fn new_order(w: &mut Coordinator) {
    w.speak_of("new-order");
}

/// A stream whose history a snapshot covers up to `snapshot`, then
/// `first` and `second` after it: every event below the snapshot is
/// stored too, as angzarr-embedded keeps every event.
#[given(
    regex = r#"^an "order" aggregate with a snapshot at sequence (\d+) and events at sequences (\d+) and (\d+)$"#
)]
fn snapshot_and_events(w: &mut Coordinator, snapshot: u64, first: u64, second: u64) {
    w.speak_of("snapshotted-order");
    let cover = w.order();
    assert_eq!((first, second), (snapshot + 1, snapshot + 2));
    let names = vec!["ItemAdded"; second as usize + 1];
    w.write(&cover, &names);
    let (state_type, version) = {
        let order = w.domain("order");
        (order.state_type.clone(), order.rules_version.clone())
    };
    w.snapshot(&cover, snapshot, &state_type, &version);
}
