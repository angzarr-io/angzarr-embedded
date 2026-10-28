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
    /// Its purge is sound, but its commit position is how many events it
    /// holds, so a purge lowers it.
    CountsEvents,
}

/// An [`InMemoryStore`] whose purge has `fault`: every purge check must
/// fail on it, so a check that checks nothing is caught.
struct Faulty {
    store: InMemoryStore,
    fault: Fault,
}

struct FaultyTx<'s>(crate::next::InMemoryTx<'s>, Fault);

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
            Fault::CountsEvents => self.store.purge(stream),
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
    fn purge(&mut self, stream: &crate::next::v1::Cover) -> Result<u64, StoreError> {
        match self.1 {
            Fault::CountsEvents => self.0.purge(stream),
            _ => Ok(0),
        }
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
        FaultyTx(self.store.begin(), self.fault)
    }

    fn commit_position(&mut self) -> Result<u64, StoreError> {
        match self.fault {
            Fault::CountsEvents => {
                let mut events = 0;
                for stream in self.store.streams() {
                    events += self.store.read(&stream)?.map_or(0, |s| s.next_sequence);
                }
                Ok(events)
            }
            _ => self.store.commit_position(),
        }
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

#[test]
fn the_purge_position_check_fails_a_position_that_counts_events() {
    use crate::next::contract::*;
    assert!(fails(
        Fault::CountsEvents,
        a_purge_never_lowers_the_commit_position
    ));
    // Its purge is sound: the position is all it gets wrong.
    assert!(!fails(
        Fault::CountsEvents,
        a_purge_deletes_a_streams_events_snapshot_and_command_ids
    ));
}

#[test]
fn the_two_handle_position_check_fails_a_position_that_is_the_handles() {
    use crate::next::contract::*;
    // Two stores, not two handles on one: each counts only its own commits.
    let apart = std::panic::catch_unwind(|| {
        block(the_commit_position_counts_every_handles_commits((
            Ready(InMemoryStore::new()),
            Ready(InMemoryStore::new()),
        )))
    });
    assert!(apart.is_err());
}

#[test]
fn a_commit_position_is_served_async_and_is_the_stores_own() {
    use crate::next::{AsyncStore, Store};
    let mut store = InMemoryStore::new();
    let before = store.commit_position().unwrap();
    let mut store = Ready(store);
    assert_eq!(ready(store.commit_position()), Ok(before));
    store
        .0
        .append(&cover("account", 1), 0, &[page(0, 1)])
        .unwrap();
    assert!(ready(store.commit_position()).unwrap() > before);
}

#[test]
fn a_read_unit_reads_the_store() {
    use crate::next::{AsyncReadStore, AsyncReadUnit, ReadStore, ReadUnit};
    let stream = cover("account", 1);
    let mut store = InMemoryStore::new();
    store.append(&stream, 0, &[page(0, 1), page(1, 2)]).unwrap();
    let mut reads = store.begin_read();
    assert_eq!(reads.covers("account"), Ok(vec![stream.clone()]));
    assert_eq!(reads.covers("other"), Ok(vec![]));
    let slice = reads.read(&stream).unwrap().unwrap();
    assert_eq!(slice.pages, [page(0, 1), page(1, 2)]);
    assert_eq!(slice.last_recorded_at, Some(instant(1)));
    assert_eq!(
        reads.read_from(&stream, 1).unwrap().unwrap().pages,
        [page(1, 2)]
    );
    assert_eq!(reads.read(&cover("account", 2)), Ok(None));

    let mut store = Ready(store);
    let mut reads = ready(store.begin_read());
    assert_eq!(ready(reads.covers("account")), Ok(vec![stream.clone()]));
    assert_eq!(
        ready(reads.read_from(&stream, 1)).unwrap().unwrap().pages,
        [page(1, 2)]
    );
}

/// How a [`Shared`] handle's read unit reads.
#[derive(Clone, Copy)]
enum Reading {
    /// From a copy of the store taken as the unit began: one moment.
    AsItBegan,
    /// Through to the store, each read seeing whatever is there by then:
    /// not one moment, so every moment check must fail on it.
    Live,
    /// From a copy taken as the unit began, but through the copy's own
    /// reads rather than a read unit's: one moment, and blind to whether
    /// an edition was ever created, so the refusal check must fail on it.
    Lax,
}

/// A handle on an [`InMemoryStore`] shared with other handles: a second
/// connection to the same store, so a write can land while a read unit is
/// open.
struct Shared {
    store: std::sync::Arc<std::sync::Mutex<InMemoryStore>>,
    reading: Reading,
}

/// Two handles on one empty store, a reader and a writer.
fn shared(reading: Reading) -> (Shared, Shared) {
    let store = std::sync::Arc::default();
    let handle = |reading| Shared {
        store: std::sync::Arc::clone(&store),
        reading,
    };
    (handle(reading), handle(reading))
}

impl Shared {
    fn with<T>(&self, f: impl FnOnce(&mut InMemoryStore) -> T) -> T {
        f(&mut self.store.lock().unwrap())
    }
}

impl EventStore for Shared {
    fn append(
        &mut self,
        stream: &crate::next::v1::Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        self.with(|s| s.append(stream, expected, pages))
    }

    fn read_from(
        &mut self,
        stream: &crate::next::v1::Cover,
        from: u64,
    ) -> Result<Option<crate::next::Slice>, StoreError> {
        self.with(|s| s.read_from(stream, from))
    }

    fn find_command(
        &mut self,
        stream: &crate::next::v1::Cover,
        command_id: &crate::next::v1::Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.with(|s| s.find_command(stream, command_id))
    }

    fn covers(&mut self, domain: &str) -> Result<Vec<crate::next::v1::Cover>, StoreError> {
        self.with(|s| s.covers(domain))
    }
}

impl crate::next::Purge for Shared {
    fn purge(&mut self, stream: &crate::next::v1::Cover) -> Result<u64, StoreError> {
        self.with(|s| s.purge(stream))
    }
}

/// A [`Shared`] handle's read unit: an [`InMemoryRead`](crate::next::InMemoryRead)
/// on a copy of the store as it began, or on the store itself, read by
/// read, or (lax) the copy's own reads.
enum SharedReads<'s> {
    AsItBegan(InMemoryStore),
    Live(&'s Shared),
    Lax(InMemoryStore),
}

impl crate::next::ReadUnit for SharedReads<'_> {
    fn covers(&mut self, domain: &str) -> Result<Vec<crate::next::v1::Cover>, StoreError> {
        use crate::next::ReadStore;
        match self {
            SharedReads::AsItBegan(copy) => copy.begin_read().covers(domain),
            SharedReads::Live(shared) => shared.with(|s| s.begin_read().covers(domain)),
            SharedReads::Lax(copy) => copy.covers(domain),
        }
    }

    fn read_from(
        &mut self,
        stream: &crate::next::v1::Cover,
        from: u64,
    ) -> Result<Option<crate::next::Slice>, StoreError> {
        use crate::next::ReadStore;
        match self {
            SharedReads::AsItBegan(copy) => copy.begin_read().read_from(stream, from),
            SharedReads::Live(shared) => shared.with(|s| s.begin_read().read_from(stream, from)),
            SharedReads::Lax(copy) => copy.read_from(stream, from),
        }
    }
}

impl crate::next::ReadStore for Shared {
    type Reads<'s> = SharedReads<'s>;

    fn begin_read(&mut self) -> SharedReads<'_> {
        let copy = || {
            self.with(|store| {
                let mut copy = InMemoryStore::new();
                for stream in store.streams() {
                    let pages = store.read(&stream).unwrap().unwrap().pages;
                    copy.append(&stream, 0, &pages).unwrap();
                }
                copy
            })
        };
        match self.reading {
            Reading::Live => SharedReads::Live(self),
            Reading::AsItBegan => SharedReads::AsItBegan(copy()),
            Reading::Lax => SharedReads::Lax(copy()),
        }
    }
}

mod read_contract {
    crate::store_contract!(read, super::shared(super::Reading::AsItBegan));
}

#[test]
fn every_read_check_fails_a_store_whose_read_unit_reads_live() {
    use crate::next::contract::*;
    let fails = |check: fn((Ready<Shared>, Ready<Shared>))| {
        let (reader, writer) = shared(Reading::Live);
        std::panic::catch_unwind(|| check((Ready(reader), Ready(writer)))).is_err()
    };
    assert!(fails(|pair| block(a_read_unit_sees_the_store_as_it_began(
        pair
    ))));
    assert!(fails(|pair| block(
        a_read_unit_keeps_a_stream_purged_after_it_began(pair)
    )));
}

#[test]
fn the_refusal_check_fails_a_read_unit_blind_to_editions() {
    use crate::next::contract::*;
    let check = |reading, check: fn((Ready<Shared>, Ready<Shared>))| {
        let (reader, writer) = shared(reading);
        std::panic::catch_unwind(|| check((Ready(reader), Ready(writer)))).is_err()
    };
    assert!(check(Reading::Lax, |pair| block(
        a_read_unit_refuses_an_edition_never_created(pair)
    )));
    // One moment all the same: the refusal is all it gets wrong.
    assert!(!check(Reading::Lax, |pair| block(
        a_read_unit_sees_the_store_as_it_began(pair)
    )));
}

impl crate::next::Watch for Shared {
    fn change_token(&mut self) -> Result<crate::next::ChangeToken, StoreError> {
        self.with(|s| s.change_token())
    }
}

mod watch_contract {
    crate::store_contract!(
        watch,
        crate::next::InMemoryStore::new(),
        super::shared(super::Reading::AsItBegan)
    );
}

/// A store whose token never changes, whatever is committed: every watch
/// check must fail on it.
struct Frozen<S>(S);

impl<S: EventStore> EventStore for Frozen<S> {
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

impl<S: crate::next::Purge> crate::next::Purge for Frozen<S> {
    fn purge(&mut self, stream: &crate::next::v1::Cover) -> Result<u64, StoreError> {
        self.0.purge(stream)
    }
}

impl<S: crate::next::Store> crate::next::Store for Frozen<S> {
    type Unit<'s>
        = S::Unit<'s>
    where
        Self: 's;

    fn begin(&mut self) -> S::Unit<'_> {
        self.0.begin()
    }

    fn commit_position(&mut self) -> Result<u64, StoreError> {
        self.0.commit_position()
    }
}

impl<S> crate::next::Watch for Frozen<S> {
    fn change_token(&mut self) -> Result<crate::next::ChangeToken, StoreError> {
        Ok(crate::next::ChangeToken::new([7, 7]))
    }
}

#[test]
fn every_watch_check_fails_a_store_whose_token_never_changes() {
    use crate::next::contract::*;
    let own = std::panic::catch_unwind(|| {
        block(a_change_token_changes_when_its_own_store_commits(Ready(
            Frozen(InMemoryStore::new()),
        )))
    });
    assert!(own.is_err(), "its own commits");
    let another = std::panic::catch_unwind(|| {
        let (watcher, writer) = shared(Reading::AsItBegan);
        block(a_change_token_changes_when_another_handle_commits((
            Ready(Frozen(watcher)),
            Ready(writer),
        )))
    });
    assert!(another.is_err(), "another handle's commits");
}

/// A store whose token changes every time it is asked: correct, since it
/// never misses a commit, but no use to a watcher, which would read every
/// stream again on every poll. The watch checks must fail on it.
struct Restless(u64);

impl crate::next::Watch for Restless {
    fn change_token(&mut self) -> Result<crate::next::ChangeToken, StoreError> {
        self.0 += 1;
        Ok(crate::next::ChangeToken::new([self.0, 0]))
    }
}

#[test]
fn the_watch_checks_fail_a_token_that_changes_with_nothing_committed() {
    use crate::next::contract::*;
    let another = std::panic::catch_unwind(|| {
        let (_, writer) = shared(Reading::AsItBegan);
        block(a_change_token_changes_when_another_handle_commits((
            Ready(Restless(0)),
            Ready(writer),
        )))
    });
    assert!(another.is_err());
}

#[test]
fn a_change_token_is_served_async_and_is_the_stores_own() {
    use crate::next::{AsyncWatch, Watch};
    let mut store = InMemoryStore::new();
    let before = store.change_token().unwrap();
    let mut store = Ready(store);
    assert_eq!(ready(store.change_token()), Ok(before));
    store
        .0
        .append(&cover("account", 1), 0, &[page(0, 1)])
        .unwrap();
    assert_ne!(ready(store.change_token()), Ok(before));
}

#[test]
fn an_in_memory_token_changes_on_a_snapshot_and_not_on_an_empty_append() {
    use crate::next::Watch;
    let stream = cover("account", 1);
    let mut store = InMemoryStore::new();
    let empty = store.change_token().unwrap();
    assert_eq!(store.append(&stream, 0, &[]), Ok(0));
    assert_eq!(store.change_token(), Ok(empty), "nothing appended");
    let mut unit = store.begin();
    unit.append(&stream, 0, &[page(0, 1)]).unwrap();
    drop(unit);
    assert_eq!(store.change_token(), Ok(empty), "a unit rolled back");
    let snapshot = Snapshot {
        sequence: 0,
        state: None,
        rules_version: "v2".to_string(),
    };
    store.write_snapshot(&stream, &snapshot).unwrap();
    assert_ne!(store.change_token(), Ok(empty), "a snapshot written");
}
