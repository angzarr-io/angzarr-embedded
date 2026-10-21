//! The storage ports: one synchronous, one async, the same contract.
//!
//! The event store is the application's database (Postgres, for hatpick)
//! behind this port; there is no other event-store infrastructure.
//!
//! **Opaque.** A store holds angzarr's own pages: per event, the
//! [`EventPage`] as written (its payload `Any`, its `EventMeta`, its
//! sequence), under its stream's [`Cover`]. It never decodes a payload, and
//! nothing stored is ever rewritten: upcasting happens on every read, in
//! the kernel. A database keeps the page bytes and copies the metadata into
//! typed columns:
//!
//! | column | from |
//! |---|---|
//! | `domain`, `edition`, `root` | the cover; `edition` NULL for main; `root` a `uuid` |
//! | `sequence` | `EventPage.sequence` (64-bit) |
//! | `type_url` | the payload's type URL as written, before any upcast |
//! | `page` | the `EventPage` bytes as written |
//! | `command_id` | `EventMeta.causation.command_id`, Postgres `uuid`; NULL for a process manager's process event |
//! | `trigger_domain`, `trigger_edition`, `trigger_root`, `trigger_sequence` | `EventMeta.causation.triggering_event`, NULL unless a process manager wrote it |
//! | `correlation_id` | `EventMeta`'s, Postgres `uuid` |
//! | `external_id` | `EventMeta.fact.external_id`, NULL unless a fact produced it |
//! | `recorded_at`, `rules_version` | the rest of `EventMeta` |
//!
//! with a unique key on (`domain`, `edition`, `root`, `sequence`) and a
//! **non-unique index on (`domain`, `edition`, `root`, `command_id`)**: the
//! deduplication lookup ([`EventStore::find_command`]); a process manager's
//! stream adds one on the trigger columns ([`EventStore::find_triggered`]). Not unique, because
//! one command may produce several events. There is no commands table and
//! no claim: "already applied?" is "does the stream hold events with this
//! command id?", atomic with the append because the id is written with the
//! events. The accepted costs: a decision that emitted nothing leaves
//! nothing to find, so it is decided again on redelivery; a command id
//! reused for a different command goes undetected (it is answered with the
//! first command's events); deduplication is forever.
//!
//! A command chain touches several streams and commits them all or none, so
//! the port is a **unit of work**: a database transaction. The application
//! opens one and hands it to the [`Kernel`](super::Kernel), which reads
//! through it, appends every staged stream once the whole chain has
//! succeeded, and then [`UnitOfWork::commit`]s it. Dropping it uncommitted
//! rolls everything back.
//!
//! A store answers three ways: the stream (or `None`: no such stream), a
//! refusal it is sure of ([`StoreError::Conflict`], a caller bug), or a
//! failure: [`StoreError::Unavailable`] (it could not say). An I/O failure
//! is never "no such stream". Snapshots live beside the events; a store
//! keeps the newest per stream.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EventPage, EventRef, Snapshot, Uuid};
use prost_types::Timestamp;

use super::error::StoreError;

/// Part of a stream, from a sequence on.
#[derive(Clone, PartialEq, Debug)]
pub struct Slice {
    pub stream: Cover,
    /// The stream's next sequence when read: `pages` ends there.
    pub next_sequence: u64,
    /// The `recorded_at` of the stream's last event, whatever `from` was:
    /// the floor for the next event's.
    pub last_recorded_at: Option<Timestamp>,
    pub pages: Vec<EventPage>,
}

/// Where streams are kept, synchronously.
pub trait EventStore {
    /// Append `pages` to `stream`, creating it if `expected` is 0 and it does
    /// not exist (appending no pages checks `expected` and changes nothing).
    /// Refuses (appending nothing) a stream not at `expected`
    /// ([`StoreError::Conflict`]) and pages whose sequences do not run on
    /// from `expected` ([`StoreError::Discontinuous`]). Returns the new
    /// length.
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError>;

    /// The stream's pages from sequence `from` on: none when `from` is at or
    /// past its end. `None` when there is no such stream.
    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError>;

    /// The whole stream.
    fn read(&mut self, stream: &Cover) -> Result<Option<Slice>, StoreError> {
        self.read_from(stream, 0)
    }

    /// The pages of `stream` whose `EventMeta.causation` is `command_id`,
    /// in sequence order: the deduplication lookup, over the index on
    /// (stream, command_id). Empty when the command never applied there, or
    /// applied and produced nothing.
    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError>;

    /// The pages of `stream` that a process manager wrote in reaction to
    /// `trigger` (`EventMeta.causation.triggering_event`), in sequence
    /// order: a process manager's deduplication lookup within its own
    /// stream, over an index on (stream, triggering event). annals has no
    /// process manager yet, so nothing is found by default.
    fn find_triggered(
        &mut self,
        _stream: &Cover,
        _trigger: &EventRef,
    ) -> Result<Vec<EventPage>, StoreError> {
        Ok(Vec::new())
    }

    /// Every stream of `domain` holding at least one page, in every edition:
    /// the main timeline's first, then each edition's in name order, each by
    /// root (as bytes). What a projector enumerates to catch a domain up.
    fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError>;

    /// The newest snapshot of `stream`, if any. None kept by default. The
    /// kernel checks it is usable ([`Registry::snapshot_usable`](super::Registry::snapshot_usable)).
    fn read_snapshot(&mut self, _stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        Ok(None)
    }

    /// Keep `snapshot` as `stream`'s newest, replacing the one before.
    /// Discarded by default.
    fn write_snapshot(&mut self, _stream: &Cover, _snapshot: &Snapshot) -> Result<(), StoreError> {
        Ok(())
    }
}

/// A transaction over an [`EventStore`]: what it appended lands at
/// [`UnitOfWork::commit`], all at once, or (dropped) not at all. Reads
/// through it see its own appends.
pub trait UnitOfWork: EventStore {
    fn commit(self) -> Result<(), StoreError>;
}

/// A store that opens units of work: what an application holds, and what
/// the store contract (`annals::next::contract`) runs against.
pub trait Store: EventStore {
    type Unit<'s>: UnitOfWork + Send
    where
        Self: 's;

    /// A unit of work over this store: appends land at its commit.
    fn begin(&mut self) -> Self::Unit<'_>;
}

/// [`EventStore`], async: for a database driver. Native return-position
/// `impl Future` in a trait, no runtime; an adapter writes its methods as
/// plain `async fn`.
///
/// The futures are required to be **`Send`**, so the command handler's
/// future is too, and a server on tokio's multi-threaded runtime can spawn
/// it. A single-threaded adapter whose futures are not `Send` (a browser's
/// IndexedDB) implements the synchronous [`EventStore`] instead.
pub trait AsyncEventStore {
    /// As [`EventStore::append`].
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// As [`EventStore::read_from`].
    fn read_from(
        &mut self,
        stream: &Cover,
        from: u64,
    ) -> impl Future<Output = Result<Option<Slice>, StoreError>> + Send;

    /// As [`EventStore::find_command`].
    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>> + Send;

    /// As [`EventStore::covers`].
    fn covers(
        &mut self,
        domain: &str,
    ) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + Send;

    /// As [`EventStore::read_snapshot`].
    fn read_snapshot(
        &mut self,
        _stream: &Cover,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>> + Send {
        async { Ok(None) }
    }

    /// As [`EventStore::write_snapshot`].
    fn write_snapshot(
        &mut self,
        _stream: &Cover,
        _snapshot: &Snapshot,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async { Ok(()) }
    }
}

/// [`UnitOfWork`], async. Its future is `Send`, like the async store's.
pub trait AsyncUnitOfWork: AsyncEventStore {
    fn commit(self) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// [`Store`], async: a store whose units of work are a database driver's
/// transactions. Beginning one is a future; a failure to begin is answered
/// by the unit's operations.
pub trait AsyncStore: AsyncEventStore {
    type Unit<'s>: AsyncUnitOfWork + Send
    where
        Self: 's;

    fn begin(&mut self) -> impl Future<Output = Self::Unit<'_>> + Send;
}

/// A synchronous store served through the async port, every future ready
/// at once.
pub struct Ready<S>(pub S);

impl<S: EventStore + Send> AsyncEventStore for Ready<S> {
    async fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        self.0.append(stream, expected, pages)
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        self.0.read_from(stream, from)
    }

    async fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.0.find_command(stream, command_id)
    }

    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.0.covers(domain)
    }

    async fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        self.0.read_snapshot(stream)
    }

    async fn write_snapshot(
        &mut self,
        stream: &Cover,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.0.write_snapshot(stream, snapshot)
    }
}

impl<S: UnitOfWork + Send> AsyncUnitOfWork for Ready<S> {
    async fn commit(self) -> Result<(), StoreError> {
        self.0.commit()
    }
}

impl<S: Store + Send> AsyncStore for Ready<S> {
    type Unit<'s>
        = Ready<S::Unit<'s>>
    where
        Self: 's;

    async fn begin(&mut self) -> Ready<S::Unit<'_>> {
        Ready(self.0.begin())
    }
}

/// A stream's place in an [`InMemoryStore`]: its cover's fields, ordered,
/// since prost's `Cover` has no ordering of its own.
type Key = (String, Option<String>, Vec<u8>);

fn key(stream: &Cover) -> Key {
    (
        stream.domain.clone(),
        stream.edition.clone(),
        stream
            .root
            .as_ref()
            .map(|r| r.value.to_vec())
            .unwrap_or_default(),
    )
}

/// One stream as [`InMemoryStore`] (or a unit of work's pending appends)
/// holds it.
struct Held {
    stream: Cover,
    pages: Vec<EventPage>,
}

/// Refuse an append to a stream of `length` events that does not expect
/// that length, or whose pages do not run on from it.
fn check_append(
    stream: &Cover,
    length: u64,
    expected: u64,
    pages: &[EventPage],
) -> Result<(), StoreError> {
    if expected != length {
        return Err(StoreError::Conflict {
            stream: stream.clone(),
            expected,
            actual: length,
        });
    }
    for (due, page) in (expected..).zip(pages) {
        if page.sequence != due {
            return Err(StoreError::Discontinuous {
                stream: stream.clone(),
                expected: due,
                found: page.sequence,
            });
        }
    }
    Ok(())
}

/// Whether `page` is one of `command_id`'s events.
pub(crate) fn produced_by(page: &EventPage, command_id: &Uuid) -> bool {
    matches!(
        page.meta.as_ref().and_then(|m| m.causation.as_ref()),
        Some(Causation::CommandId(id)) if id == command_id
    )
}

/// `pages`, the whole of a stream, as a slice from `from`.
fn slice(stream: &Cover, pages: &[EventPage], from: u64) -> Slice {
    let start = usize::try_from(from).map_or(pages.len(), |f| f.min(pages.len()));
    Slice {
        stream: stream.clone(),
        next_sequence: pages.len() as u64,
        last_recorded_at: pages
            .last()
            .and_then(|p| p.meta.as_ref())
            .and_then(|m| m.recorded_at),
        pages: pages[start..].to_vec(),
    }
}

/// Streams held in memory, in key order. Checks everything a real store
/// must, so tests catch what production would.
#[derive(Default)]
pub struct InMemoryStore {
    streams: BTreeMap<Key, Held>,
    snapshots: BTreeMap<Key, Snapshot>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every stream's cover, in every domain, in key order.
    pub fn streams(&self) -> Vec<Cover> {
        self.streams.values().map(|h| h.stream.clone()).collect()
    }

    /// A unit of work over this store: appends are held until
    /// [`UnitOfWork::commit`], and reads see them.
    pub fn begin(&mut self) -> InMemoryTx<'_> {
        InMemoryTx {
            store: self,
            pending: BTreeMap::new(),
        }
    }

    fn pages(&self, stream: &Cover) -> &[EventPage] {
        self.streams
            .get(&key(stream))
            .map_or(&[], |h| h.pages.as_slice())
    }
}

impl EventStore for InMemoryStore {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        check_append(stream, self.pages(stream).len() as u64, expected, pages)?;
        if pages.is_empty() {
            return Ok(expected);
        }
        let held = self.streams.entry(key(stream)).or_insert_with(|| Held {
            stream: stream.clone(),
            pages: Vec::new(),
        });
        held.pages.extend_from_slice(pages);
        Ok(held.pages.len() as u64)
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        Ok(self
            .streams
            .get(&key(stream))
            .map(|h| slice(&h.stream, &h.pages, from)))
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        Ok(self
            .pages(stream)
            .iter()
            .filter(|p| produced_by(p, command_id))
            .cloned()
            .collect())
    }

    fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        // Key order is the port's: main (no edition) first, then each
        // edition by name, each by root.
        Ok(self
            .streams
            .iter()
            .filter(|(k, _)| k.0 == domain)
            .map(|(_, held)| held.stream.clone())
            .collect())
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        Ok(self.snapshots.get(&key(stream)).cloned())
    }

    fn write_snapshot(&mut self, stream: &Cover, snapshot: &Snapshot) -> Result<(), StoreError> {
        self.snapshots.insert(key(stream), snapshot.clone());
        Ok(())
    }
}

/// A unit of work over an [`InMemoryStore`]. See [`InMemoryStore::begin`].
pub struct InMemoryTx<'s> {
    store: &'s mut InMemoryStore,
    pending: BTreeMap<Key, Held>,
}

impl InMemoryTx<'_> {
    /// The stream's pages: the store's, then the pending ones.
    fn pages(&self, stream: &Cover) -> Vec<EventPage> {
        let mut pages = self.store.pages(stream).to_vec();
        if let Some(pending) = self.pending.get(&key(stream)) {
            pages.extend_from_slice(&pending.pages);
        }
        pages
    }
}

impl EventStore for InMemoryTx<'_> {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        let length = self.pages(stream).len() as u64;
        check_append(stream, length, expected, pages)?;
        if !pages.is_empty() {
            let held = self.pending.entry(key(stream)).or_insert_with(|| Held {
                stream: stream.clone(),
                pages: Vec::new(),
            });
            held.pages.extend_from_slice(pages);
        }
        Ok(length + pages.len() as u64)
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        let k = key(stream);
        if !self.store.streams.contains_key(&k) && !self.pending.contains_key(&k) {
            return Ok(None);
        }
        Ok(Some(slice(stream, &self.pages(stream), from)))
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let mut pages = self.pages(stream);
        pages.retain(|p| produced_by(p, command_id));
        Ok(pages)
    }

    fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        // The store's streams and this unit's new ones, merged in key order.
        let held = self.store.streams.iter().chain(&self.pending);
        let covers: BTreeMap<&Key, &Cover> = held
            .filter(|(k, _)| k.0 == domain)
            .map(|(k, held)| (k, &held.stream))
            .collect();
        Ok(covers.into_values().cloned().collect())
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        self.store.read_snapshot(stream)
    }
}

impl UnitOfWork for InMemoryTx<'_> {
    fn commit(self) -> Result<(), StoreError> {
        let InMemoryTx { store, pending } = self;
        for held in pending.into_values() {
            let length = store.pages(&held.stream).len() as u64;
            store.append(&held.stream, length, &held.pages)?;
        }
        Ok(())
    }
}

impl Store for InMemoryStore {
    type Unit<'s> = InMemoryTx<'s>;

    fn begin(&mut self) -> InMemoryTx<'_> {
        InMemoryStore::begin(self)
    }
}

/// The store operations a chain makes, as futures: one chain serves the
/// sync port ([`SyncPort`], every future ready at once) and the async one
/// ([`AsyncPort`]). Not `Send`-bound, so a synchronous store need not be
/// `Send`; over the async port the futures are `Send` whenever the store's
/// are.
pub(crate) trait Port {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> impl Future<Output = Result<u64, StoreError>>;

    fn read_from(
        &mut self,
        stream: &Cover,
        from: u64,
    ) -> impl Future<Output = Result<Option<Slice>, StoreError>>;

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>>;

    fn read_snapshot(
        &mut self,
        stream: &Cover,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>>;
}

/// A synchronous store as a [`Port`].
pub(crate) struct SyncPort<'a, S>(pub(crate) &'a mut S);

impl<S: EventStore> Port for SyncPort<'_, S> {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> impl Future<Output = Result<u64, StoreError>> {
        std::future::ready(self.0.append(stream, expected, pages))
    }

    fn read_from(
        &mut self,
        stream: &Cover,
        from: u64,
    ) -> impl Future<Output = Result<Option<Slice>, StoreError>> {
        std::future::ready(self.0.read_from(stream, from))
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>> {
        std::future::ready(self.0.find_command(stream, command_id))
    }

    fn read_snapshot(
        &mut self,
        stream: &Cover,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>> {
        std::future::ready(self.0.read_snapshot(stream))
    }
}

/// An async store as a [`Port`].
pub(crate) struct AsyncPort<'a, S>(pub(crate) &'a mut S);

impl<S: AsyncEventStore> Port for AsyncPort<'_, S> {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> impl Future<Output = Result<u64, StoreError>> {
        self.0.append(stream, expected, pages)
    }

    fn read_from(
        &mut self,
        stream: &Cover,
        from: u64,
    ) -> impl Future<Output = Result<Option<Slice>, StoreError>> {
        self.0.read_from(stream, from)
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>> {
        self.0.find_command(stream, command_id)
    }

    fn read_snapshot(
        &mut self,
        stream: &Cover,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>> {
        self.0.read_snapshot(stream)
    }
}

/// Run a future over a [`SyncPort`] to its end: every future it awaits is
/// ready at once, so one poll finishes it.
pub(crate) fn block<F: Future>(future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    match pin!(future).poll(&mut cx) {
        Poll::Ready(out) => out,
        Poll::Pending => unreachable!("a synchronous port's futures are ready at once"),
    }
}
