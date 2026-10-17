//! Snapshots: a framework optimisation, never business data. Used only
//! under the current rules version and state type, and never one that
//! hides part of a conflict window.

use annals::next::v1::{ConflictResolution, Snapshot};
use annals::next::{
    check, pack, Coordinator, Deduplicated, EventStore, InMemoryStore, Payload, Stream,
};

use crate::toys::{
    account, at, command, cover, opened, registry, root, set, shelf, Account, AccountCmd, Edit,
    Shelf,
};

/// A snapshot of an account at sequence 1 claiming `cents`: a poisoned one
/// shows when it is used.
fn poisoned(rules: &str, cents: i64) -> Snapshot {
    Snapshot {
        sequence: 1,
        state: Some(pack(&account::AccountState {
            open: true,
            cents,
            ..Default::default()
        })),
        rules_version: rules.to_string(),
    }
}

/// Load (steps 2 to 5) a deposit for account 1, expecting `expected` under
/// `resolution` if given.
fn load(
    store: &mut InMemoryStore,
    expected: Option<(u64, ConflictResolution)>,
) -> (Option<u64>, i64) {
    let registry = registry();
    let mut book = command(cover("account", 1), AccountCmd::Deposit(1).to_any()).id(9);
    if let Some((sequence, resolution)) = expected {
        book = book.expecting(sequence, resolution);
    }
    let mut unit = store.begin();
    let mut coordinator = Coordinator::new(&mut unit, &registry);
    let identified = check(book, &registry).unwrap().identify(&at(30));
    let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
        panic!("never applied");
    };
    let contextual = coordinator.load(novel).unwrap();
    let used = contextual.snapshot().map(|s| s.sequence);
    let prepared = contextual.decode::<Account>(&registry).unwrap();
    (used, prepared.stream().state().cents)
}

#[test]
#[ignore = "proposal: a snapshot folded under another rules version is discarded and the stream replayed"]
fn a_snapshot_under_another_rules_version_is_discarded() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);

    store
        .write_snapshot(&cover("account", 1), &poisoned("v1", 999))
        .unwrap();
    assert_eq!(
        load(&mut store, None),
        (None, 100),
        "folded under v1, the current rules are v2: replayed from 0"
    );

    store
        .write_snapshot(&cover("account", 1), &poisoned("v2", 999))
        .unwrap();
    assert_eq!(
        load(&mut store, None),
        (Some(1), 999),
        "a current one is used (and here shows its poison)"
    );
}

#[test]
#[ignore = "proposal: a snapshot of another state type is discarded"]
fn a_snapshot_of_another_state_type_is_discarded() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    let foreign = Snapshot {
        sequence: 1,
        state: Some(pack(&shelf::ShelfState::default())),
        rules_version: "v2".to_string(),
    };
    store
        .write_snapshot(&cover("account", 1), &foreign)
        .unwrap();
    assert_eq!(load(&mut store, None), (None, 100));
}

#[test]
#[ignore = "proposal: under AGGREGATE_HANDLES the snapshot used is at or below expected-1"]
fn a_snapshot_never_hides_part_of_the_window() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    store
        .write_snapshot(&cover("account", 1), &poisoned("v2", 999))
        .unwrap();

    assert_eq!(
        load(&mut store, Some((1, ConflictResolution::AggregateHandles))),
        (None, 100),
        "the snapshot (at 1) would fold the window (from 1): not used"
    );
    assert_eq!(
        load(&mut store, Some((2, ConflictResolution::AggregateHandles))),
        (Some(1), 999),
        "at or below expected-1, it is"
    );
}

#[test]
#[ignore = "proposal: a stream snapshots under the current rules version and its state type"]
fn a_stream_snapshots_under_the_current_rules_version() {
    let registry = registry();
    let mut stream = Stream::<Shelf>::new(root(3), None).unwrap();
    let log = annals::next::MemoryDeadLetters::new();
    stream
        .execute(
            command(cover("shelf", 3), set("a", 1)),
            &registry,
            &at(1),
            &log,
            &mut [],
        )
        .unwrap();
    let snapshot = stream.snapshot(&registry).unwrap();
    assert_eq!(snapshot.sequence, 0);
    assert_eq!(snapshot.rules_version, "v1");
    assert_eq!(
        snapshot.state.as_ref().unwrap().type_url,
        "/toy.shelf.v1.ShelfState"
    );
    assert!(registry.snapshot_usable("shelf", &snapshot));
    assert!(Stream::<Shelf>::new(root(4), None)
        .unwrap()
        .snapshot(&registry)
        .is_none());
}
