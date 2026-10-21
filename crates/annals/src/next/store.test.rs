//! The store port: opaque pages under a cover, a deduplication lookup by
//! command id, a unit of work, and the same contract async. The contract
//! every store is held to runs here against InMemoryStore; what follows it
//! is particular to the async port and to a store that implements only what
//! the port requires.

mod contract {
    crate::store_contract!(crate::next::InMemoryStore::new());
    crate::store_contract!(purge, crate::next::InMemoryStore::new());
}

use crate::next::v1::{EventMeta, EventPage, Snapshot};
use crate::next::{
    pack, AsyncEventStore, AsyncUnitOfWork, EventStore, InMemoryStore, Ready, StoreError,
};

use crate::next::toys::ready;
use crate::next::toys::{account, command_id, cover, instant};

/// The event at `sequence`, produced by command `n`.
fn page(sequence: u64, n: u128) -> EventPage {
    EventPage {
        payload: Some(pack(&account::Deposited {
            cents: sequence as i64,
        })),
        meta: Some(EventMeta {
            causation: Some(crate::next::v1::event_meta::Causation::CommandId(
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
        stream: &crate::next::v1::Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        self.0.append(stream, expected, pages)
    }

    fn read_from(
        &mut self,
        stream: &crate::next::v1::Cover,
        from: u64,
    ) -> Result<Option<crate::next::Slice>, StoreError> {
        self.0.read_from(stream, from)
    }

    fn find_command(
        &mut self,
        stream: &crate::next::v1::Cover,
        command_id: &crate::next::v1::Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.0.find_command(stream, command_id)
    }

    fn covers(&mut self, domain: &str) -> Result<Vec<crate::next::v1::Cover>, StoreError> {
        self.0.covers(domain)
    }
}

#[test]
fn a_store_keeps_no_snapshots_and_finds_no_process_events_by_default() {
    use crate::next::v1::EventRef;
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

/// What a store that purges badly gets wrong.
#[derive(Clone, Copy)]
enum Fault {
    /// Its purge deletes nothing, on the store and on a unit of work.
    DeletesNothing,
    /// Its purge deletes the events but keeps the snapshot.
    KeepsSnapshots,
}

/// An [`InMemoryStore`] whose purge has `fault`: every purge check must
/// fail on it, so a check that checks nothing is caught.
struct Faulty {
    store: InMemoryStore,
    fault: Fault,
}

struct FaultyTx<'s>(crate::next::InMemoryTx<'s>);

macro_rules! delegate_event_store {
    ($ty:ty, $($inner:tt)+) => {
        impl EventStore for $ty {
            fn append(
                &mut self,
                stream: &crate::next::v1::Cover,
                expected: u64,
                pages: &[EventPage],
            ) -> Result<u64, StoreError> {
                self.$($inner)+.append(stream, expected, pages)
            }

            fn read_from(
                &mut self,
                stream: &crate::next::v1::Cover,
                from: u64,
            ) -> Result<Option<crate::next::Slice>, StoreError> {
                self.$($inner)+.read_from(stream, from)
            }

            fn find_command(
                &mut self,
                stream: &crate::next::v1::Cover,
                command_id: &crate::next::v1::Uuid,
            ) -> Result<Vec<EventPage>, StoreError> {
                self.$($inner)+.find_command(stream, command_id)
            }

            fn covers(&mut self, domain: &str) -> Result<Vec<crate::next::v1::Cover>, StoreError> {
                self.$($inner)+.covers(domain)
            }

            fn read_snapshot(
                &mut self,
                stream: &crate::next::v1::Cover,
            ) -> Result<Option<Snapshot>, StoreError> {
                self.$($inner)+.read_snapshot(stream)
            }

            fn write_snapshot(
                &mut self,
                stream: &crate::next::v1::Cover,
                snapshot: &Snapshot,
            ) -> Result<(), StoreError> {
                self.$($inner)+.write_snapshot(stream, snapshot)
            }
        }
    };
}

delegate_event_store!(Faulty, store);
delegate_event_store!(FaultyTx<'_>, 0);

impl crate::next::Purge for Faulty {
    fn purge(&mut self, stream: &crate::next::v1::Cover) -> Result<u64, StoreError> {
        match self.fault {
            Fault::DeletesNothing => Ok(0),
            Fault::KeepsSnapshots => {
                let kept = self.store.read_snapshot(stream)?;
                let deleted = self.store.purge(stream)?;
                if let Some(snapshot) = kept {
                    self.store.write_snapshot(stream, &snapshot)?;
                }
                Ok(deleted)
            }
        }
    }
}

impl crate::next::Purge for FaultyTx<'_> {
    fn purge(&mut self, _stream: &crate::next::v1::Cover) -> Result<u64, StoreError> {
        Ok(0)
    }
}

impl crate::next::UnitOfWork for FaultyTx<'_> {
    fn commit(self) -> Result<(), StoreError> {
        self.0.commit()
    }
}

impl crate::next::Store for Faulty {
    type Unit<'s> = FaultyTx<'s>;

    fn begin(&mut self) -> FaultyTx<'_> {
        FaultyTx(self.store.begin())
    }
}

/// Whether `check` fails on a store with `fault`.
fn fails<F: std::future::Future>(fault: Fault, check: impl FnOnce(Ready<Faulty>) -> F) -> bool {
    let store = Ready(Faulty {
        store: InMemoryStore::new(),
        fault,
    });
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::next::contract::block(check(store))
    }))
    .is_err()
}

#[test]
fn every_purge_check_fails_a_store_whose_purge_deletes_nothing() {
    use crate::next::contract::*;
    let fault = Fault::DeletesNothing;
    assert!(fails(
        fault,
        a_purge_deletes_a_streams_events_snapshot_and_command_ids
    ));
    assert!(fails(fault, a_purge_deletes_a_snapshot_with_no_events));
    assert!(fails(fault, a_purged_stream_starts_again_from_nothing));
    assert!(fails(
        fault,
        a_purge_in_a_unit_of_work_lands_at_commit_or_not_at_all
    ));
    assert!(fails(
        fault,
        a_unit_of_work_purges_its_own_appends_and_appends_after
    ));
}

#[test]
fn the_purge_checks_fail_a_store_whose_purge_keeps_snapshots() {
    use crate::next::contract::*;
    let fault = Fault::KeepsSnapshots;
    assert!(fails(
        fault,
        a_purge_deletes_a_streams_events_snapshot_and_command_ids
    ));
    assert!(fails(fault, a_purge_deletes_a_snapshot_with_no_events));
}
