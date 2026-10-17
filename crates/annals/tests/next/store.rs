//! The store port: opaque pages under a cover, a deduplication lookup by
//! command id, a unit of work, and the same contract async.

use annals::next::v1::{EventMeta, EventPage};
use annals::next::{
    pack, AsyncEventStore, AsyncUnitOfWork, EventStore, InMemoryStore, Ready, StoreError,
    UnitOfWork,
};

use crate::ready;
use crate::toys::{account, command_id, cover, cover_in, instant};

/// The event at `sequence`, produced by command `n`.
fn page(sequence: u64, n: u128) -> EventPage {
    EventPage {
        payload: Some(pack(&account::Deposited {
            cents: sequence as i64,
        })),
        meta: Some(EventMeta {
            causation: Some(annals::next::v1::event_meta::Causation::CommandId(
                command_id(n),
            )),
            correlation_id: Some(command_id(n)),
            recorded_at: Some(instant(sequence as i64)),
            rules_version: "v2".to_string(),
            ..EventMeta::default()
        }),
        sequence,
    }
}

#[test]
fn append_checks_the_expected_length_and_continuity() {
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    assert_eq!(store.append(&stream, 0, &[page(0, 1), page(1, 1)]), Ok(2));
    assert_eq!(
        store.append(&stream, 1, &[page(1, 2)]),
        Err(StoreError::Conflict {
            stream: stream.clone(),
            expected: 1,
            actual: 2
        })
    );
    assert_eq!(
        store.append(&stream, 2, &[page(3, 2)]),
        Err(StoreError::Discontinuous {
            stream: stream.clone(),
            expected: 2,
            found: 3
        })
    );
    let slice = store.read_from(&stream, 1).unwrap().unwrap();
    assert_eq!(slice.pages, [page(1, 1)]);
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(slice.last_recorded_at, Some(instant(1)));
    assert_eq!(store.read(&cover("account", 2)).unwrap(), None);
}

#[test]
fn a_cover_is_domain_edition_and_root() {
    let mut store = InMemoryStore::new();
    store
        .append(&cover("account", 1), 0, &[page(0, 1)])
        .unwrap();
    store.append(&cover("ledger", 1), 0, &[page(0, 1)]).unwrap();
    store
        .append(&cover_in("account", 1, "what-if"), 0, &[page(0, 1)])
        .unwrap();
    assert_eq!(store.covers().len(), 3);
}

#[test]
fn find_command_finds_a_commands_events_on_its_stream() {
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    store
        .append(&stream, 0, &[page(0, 1), page(1, 2), page(2, 2)])
        .unwrap();
    assert_eq!(
        store.find_command(&stream, &command_id(2)),
        Ok(vec![page(1, 2), page(2, 2)])
    );
    assert_eq!(store.find_command(&stream, &command_id(3)), Ok(vec![]));
    assert_eq!(
        store.find_command(&cover("account", 2), &command_id(2)),
        Ok(vec![]),
        "per stream"
    );
}

#[test]
fn a_unit_of_work_lands_at_commit_or_not_at_all() {
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    {
        let mut unit = store.begin();
        unit.append(&stream, 0, &[page(0, 1)]).unwrap();
        assert_eq!(
            unit.find_command(&stream, &command_id(1)),
            Ok(vec![page(0, 1)])
        );
    }
    assert_eq!(store.read(&stream).unwrap(), None, "dropped: rolled back");
    let mut unit = store.begin();
    unit.append(&stream, 0, &[page(0, 1)]).unwrap();
    unit.commit().unwrap();
    assert_eq!(store.read(&stream).unwrap().unwrap().pages, [page(0, 1)]);
}

#[test]
fn the_async_port_has_the_same_contract() {
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    let mut unit = Ready(store.begin());
    assert_eq!(ready(unit.append(&stream, 0, &[page(0, 1)])), Ok(1));
    assert_eq!(
        ready(unit.find_command(&stream, &command_id(1))),
        Ok(vec![page(0, 1)])
    );
    ready(unit.commit()).unwrap();
    assert_eq!(store.read(&stream).unwrap().unwrap().next_sequence, 1);
}

#[test]
fn a_unit_of_work_checks_and_reads_its_pending_appends() {
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    store.append(&stream, 0, &[page(0, 1)]).unwrap();
    let mut unit = store.begin();
    assert_eq!(unit.read(&cover("account", 2)), Ok(None));
    assert_eq!(unit.append(&stream, 1, &[page(1, 2)]), Ok(2));
    assert_eq!(
        unit.append(&stream, 1, &[page(1, 3)]),
        Err(StoreError::Conflict {
            stream: stream.clone(),
            expected: 1,
            actual: 2
        }),
        "the pending append counts"
    );
    assert_eq!(
        unit.append(&stream, 2, &[page(2, 3), page(4, 3)]),
        Err(StoreError::Discontinuous {
            stream: stream.clone(),
            expected: 3,
            found: 4
        })
    );
    let other = cover("account", 2);
    assert_eq!(unit.append(&other, 0, &[page(0, 4)]), Ok(1));
    let slice = unit.read_from(&stream, 1).unwrap().unwrap();
    assert_eq!(slice.pages, [page(1, 2)]);
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(slice.last_recorded_at, Some(instant(1)));
    assert_eq!(unit.read(&other).unwrap().unwrap().pages, [page(0, 4)]);
    assert_eq!(
        unit.find_command(&stream, &command_id(2)),
        Ok(vec![page(1, 2)])
    );
    unit.commit().unwrap();
    assert_eq!(store.read(&stream).unwrap().unwrap().next_sequence, 2);
    assert_eq!(store.covers(), [stream, other]);
}

#[test]
fn an_empty_append_checks_and_creates_nothing() {
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    assert_eq!(store.append(&stream, 0, &[]), Ok(0));
    assert!(store.covers().is_empty());
    assert!(matches!(
        store.append(&stream, 1, &[]),
        Err(StoreError::Conflict { .. })
    ));
    let mut unit = store.begin();
    assert_eq!(unit.append(&stream, 0, &[]), Ok(0));
    assert_eq!(unit.read(&stream), Ok(None));
    unit.commit().unwrap();
    assert!(store.covers().is_empty());
}

#[test]
fn a_read_past_the_end_is_empty_but_knows_the_end() {
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    store.append(&stream, 0, &[page(0, 1), page(1, 1)]).unwrap();
    let slice = store.read_from(&stream, 7).unwrap().unwrap();
    assert!(slice.pages.is_empty());
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(slice.last_recorded_at, Some(instant(1)));
    assert_eq!(slice.stream, stream);
}

#[test]
fn a_store_keeps_the_newest_snapshot_per_stream() {
    use annals::next::v1::Snapshot;
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    let snapshot = |sequence| Snapshot {
        sequence,
        state: None,
        rules_version: "v2".to_string(),
    };
    assert_eq!(store.read_snapshot(&stream), Ok(None));
    store.write_snapshot(&stream, &snapshot(1)).unwrap();
    store.write_snapshot(&stream, &snapshot(4)).unwrap();
    assert_eq!(store.read_snapshot(&stream), Ok(Some(snapshot(4))));
    assert_eq!(store.read_snapshot(&cover("account", 2)), Ok(None));
    assert_eq!(store.begin().read_snapshot(&stream), Ok(Some(snapshot(4))));
    assert_eq!(
        ready(Ready(store.begin()).read_snapshot(&stream)),
        Ok(Some(snapshot(4)))
    );
    let mut unit = Ready(store.begin());
    assert_eq!(ready(unit.read_from(&stream, 0)), Ok(None));
}

/// A store that implements only what the port requires.
struct Minimal(InMemoryStore);

impl EventStore for Minimal {
    fn append(
        &mut self,
        stream: &annals::next::v1::Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        self.0.append(stream, expected, pages)
    }

    fn read_from(
        &mut self,
        stream: &annals::next::v1::Cover,
        from: u64,
    ) -> Result<Option<annals::next::Slice>, StoreError> {
        self.0.read_from(stream, from)
    }

    fn find_command(
        &mut self,
        stream: &annals::next::v1::Cover,
        command_id: &annals::next::v1::Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.0.find_command(stream, command_id)
    }
}

#[test]
fn a_store_keeps_no_snapshots_and_finds_no_process_events_by_default() {
    use annals::next::v1::{EventRef, Snapshot};
    let stream = cover("account", 1);
    let trigger = EventRef {
        stream: Some(stream.clone()),
        sequence: 0,
    };
    let mut minimal = Minimal(InMemoryStore::new());
    minimal.append(&stream, 0, &[page(0, 1)]).unwrap();
    assert_eq!(minimal.read(&stream).unwrap().unwrap().pages, [page(0, 1)]);
    minimal
        .write_snapshot(&stream, &Snapshot::default())
        .unwrap();
    assert_eq!(minimal.read_snapshot(&stream), Ok(None));
    assert_eq!(minimal.find_triggered(&stream, &trigger), Ok(vec![]));
    let mut store = InMemoryStore::new();
    store.append(&stream, 0, &[page(0, 1)]).unwrap();
    assert_eq!(store.find_triggered(&stream, &trigger), Ok(vec![]));

    let mut async_store = Ready(InMemoryStore::new());
    ready(async_store.write_snapshot(&stream, &Snapshot::default())).unwrap();
    assert_eq!(
        ready(async_store.read_snapshot(&stream)),
        Ok(Some(Snapshot::default()))
    );
}
