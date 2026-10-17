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
//! | `command_id`, `correlation_id` | `EventMeta`'s, Postgres `uuid` |
//! | `cause_domain`, `cause_edition`, `cause_root`, `cause_sequence` | `EventMeta.causation_id`, NULL unless a policy's command produced it |
//! | `external_id` | `EventMeta.fact.external_id`, NULL unless a fact produced it |
//! | `recorded_at`, `rules_version` | the rest of `EventMeta` |
//!
//! with a unique key on (`domain`, `edition`, `root`, `sequence`) and a
//! **non-unique index on (`domain`, `edition`, `root`, `command_id`)**: the
//! deduplication lookup ([`EventStore::find_command`]). Not unique, because
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
    /// not exist. Refuses (appending nothing) a stream not at `expected`
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

    /// The pages of `stream` whose `EventMeta.command_id` is `command_id`,
    /// in sequence order: the deduplication lookup, over the index on
    /// (stream, command_id). Empty when the command never applied there, or
    /// applied and produced nothing.
    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError>;

    /// The pages of `stream` that a process manager wrote in reaction to
    /// `trigger` (`EventMeta.produced_by.triggering_event`), in sequence
    /// order: a process manager's deduplication lookup within its own
    /// stream, over an index on (stream, triggering event). annals has no
    /// process manager yet, and at eec6676 the page has no field to find
    /// it by, so nothing is found by default; implemented with the re-pin.
    fn find_triggered(
        &mut self,
        stream: &Cover,
        trigger: &EventRef,
    ) -> Result<Vec<EventPage>, StoreError> {
        Ok(Vec::new())
    }

    /// The newest snapshot of `stream`, if any. None kept by default. The
    /// kernel checks it is usable ([`Registry::snapshot_usable`](super::Registry::snapshot_usable)).
    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        Ok(None)
    }

    /// Keep `snapshot` as `stream`'s newest, replacing the one before.
    /// Discarded by default.
    fn write_snapshot(&mut self, stream: &Cover, snapshot: &Snapshot) -> Result<(), StoreError> {
        Ok(())
    }
}

/// A transaction over an [`EventStore`]: what it appended lands at
/// [`UnitOfWork::commit`], all at once, or (dropped) not at all. Reads
/// through it see its own appends.
pub trait UnitOfWork: EventStore {
    fn commit(self) -> Result<(), StoreError>;
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

    /// As [`EventStore::read_snapshot`].
    fn read_snapshot(
        &mut self,
        stream: &Cover,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>> + Send {
        async { Ok(None) }
    }

    /// As [`EventStore::write_snapshot`].
    fn write_snapshot(
        &mut self,
        stream: &Cover,
        snapshot: &Snapshot,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async { Ok(()) }
    }
}

/// [`UnitOfWork`], async. Its future is `Send`, like the async store's.
pub trait AsyncUnitOfWork: AsyncEventStore {
    fn commit(self) -> impl Future<Output = Result<(), StoreError>> + Send;
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

/// A stream's place in an [`InMemoryStore`]: its cover's fields, ordered,
/// since prost's `Cover` has no ordering of its own.
type Key = (String, Option<String>, Vec<u8>);

/// One stream as [`InMemoryStore`] holds it.
struct Held {
    stream: Cover,
    pages: Vec<EventPage>,
    snapshot: Option<Snapshot>,
}

/// Streams held in memory, in key order. Checks everything a real store
/// must, so tests catch what production would.
#[derive(Default)]
pub struct InMemoryStore {
    streams: BTreeMap<Key, Held>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every stream's cover, in key order.
    pub fn covers(&self) -> Vec<Cover> {
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
}

impl EventStore for InMemoryStore {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        todo!("proposal: check length and continuity, then extend")
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        todo!("proposal: clone the tail, with the last recorded_at")
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        todo!("proposal: the pages whose meta's command_id matches")
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        todo!("proposal: the held snapshot")
    }

    fn write_snapshot(&mut self, stream: &Cover, snapshot: &Snapshot) -> Result<(), StoreError> {
        todo!("proposal: replace the held snapshot")
    }
}

/// A unit of work over an [`InMemoryStore`]. See [`InMemoryStore::begin`].
pub struct InMemoryTx<'s> {
    store: &'s mut InMemoryStore,
    pending: BTreeMap<Key, Vec<EventPage>>,
}

impl EventStore for InMemoryTx<'_> {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        todo!("proposal: check against store + pending, hold in pending")
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        todo!("proposal: the store's stream with pending appends after it")
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        todo!("proposal: the store's and pending pages whose command_id matches")
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        self.store.read_snapshot(stream)
    }
}

impl UnitOfWork for InMemoryTx<'_> {
    fn commit(self) -> Result<(), StoreError> {
        todo!("proposal: move pending into the store")
    }
}
