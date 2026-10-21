//! The store port: opaque pages under a cover, a deduplication lookup by
//! command id, a unit of work, and the same contract async. The contract
//! every store is held to runs here against InMemoryStore; what follows it
//! is particular to the async port and to a store that implements only what
//! the port requires.

mod contract {
    annals::store_contract!(annals::next::InMemoryStore::new());
}

use annals::next::v1::{EventMeta, EventPage, Snapshot};
use annals::next::{
    pack, AsyncEventStore, AsyncUnitOfWork, EventStore, InMemoryStore, Ready, StoreError,
};

use crate::ready;
use crate::toys::{account, command_id, cover, instant};

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
fn the_async_port_has_the_same_contract() {
    let mut store = InMemoryStore::new();
    let stream = cover("account", 1);
    let mut unit = Ready(store.begin());
    assert_eq!(ready(unit.append(&stream, 0, &[page(0, 1)])), Ok(1));
    assert_eq!(
        ready(unit.find_command(&stream, &command_id(1))),
        Ok(vec![page(0, 1)])
    );
    assert_eq!(ready(unit.covers("account")), Ok(vec![stream.clone()]));
    ready(unit.commit()).unwrap();
    assert_eq!(store.read(&stream).unwrap().unwrap().next_sequence, 1);

    let snapshot = Snapshot {
        sequence: 0,
        state: None,
        rules_version: "v2".to_string(),
    };
    store.write_snapshot(&stream, &snapshot).unwrap();
    assert_eq!(
        ready(Ready(store.begin()).read_snapshot(&stream)),
        Ok(Some(snapshot))
    );
    let mut unit = Ready(store.begin());
    assert_eq!(ready(unit.read_from(&cover("account", 2), 0)), Ok(None));
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

    fn covers(&mut self, domain: &str) -> Result<Vec<annals::next::v1::Cover>, StoreError> {
        self.0.covers(domain)
    }
}

#[test]
fn a_store_keeps_no_snapshots_and_finds_no_process_events_by_default() {
    use annals::next::v1::EventRef;
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
