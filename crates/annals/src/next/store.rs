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
//! first command's events); deduplication lasts as long as the stream, so
//! a command redelivered after its stream is purged is decided again.
//!
//! **Purge** is opt-in: a store that can delete a stream implements
//! [`Purge`] (or [`AsyncPurge`]), deleting its events, its snapshot and so
//! its command ids in one unit of work. A store that does not is
//! append-only by type.
//!
//! **Reading one moment** is opt-in too: a store that can hold reads to one
//! moment implements [`ReadStore`] (or [`AsyncReadStore`]), whose read-only
//! unit of work ([`ReadUnit`]) answers every read as the store stood when
//! it began. A reader takes one to list a domain's streams and read each,
//! so a write or a purge landing in between cannot mix two moments in what
//! it shows. It writes nothing and takes no write lock. Each read on the
//! store itself is one moment already; only a run of them needs the unit.
//!
//! **Watching** is opt-in as well: a store that can say cheaply whether
//! anything was committed implements [`Watch`] (or [`AsyncWatch`]), whose
//! [`ChangeToken`] a watcher compares between polls instead of reading
//! every stream again. Equal tokens from one handle mean nothing was
//! committed in between, through any handle on the store; a new token may
//! come with nothing new to read. It reads no stream and takes no write
//! lock, so a store opened only for reading can answer it.
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

use std::collections::{BTreeMap, BTreeSet};
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

    /// The store's commit position now: global (one for the whole store,
    /// every stream and every handle on its database) and monotonic. Every
    /// event committed before it is answered lies below it, and every event
    /// committed after it at or above it, so it rises with every commit
    /// that appends an event, through any handle, and never falls, a purge
    /// included. Commit order, not `recorded_at`: an event sampled before
    /// it was read but committed after lies above it. It is the
    /// divergence an edition's create records, so the edition reads
    /// exactly main's events committed before it (angzarr's EA-3; annals
    /// has no edition registry yet).
    fn commit_position(&mut self) -> Result<u64, StoreError>;
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

    /// As [`Store::commit_position`].
    fn commit_position(&mut self) -> impl Future<Output = Result<u64, StoreError>> + Send;
}

/// A store that can delete a stream. A store opts in by implementing it,
/// so one that does not stays append-only by type: nothing reaches a
/// purge through [`EventStore`] alone.
///
/// A purge is the one deletion annals has. It undoes deduplication for the
/// stream: a command redelivered after its stream is purged finds nothing,
/// and is decided again (`docs/design/audit.md`, step 3).
pub trait Purge: EventStore {
    /// Delete `stream`'s events, its snapshot, and so the command ids
    /// recorded with its events, all at once or (failing) none of them.
    /// Afterwards the stream does not exist: it is read as `None`, is in no
    /// [`covers`](EventStore::covers), and an append to it starts again
    /// from expected 0. On a unit of work it lands at commit, and reads
    /// through the unit see it at once. Returns how many events it deleted:
    /// 0 when there was no such stream.
    fn purge(&mut self, stream: &Cover) -> Result<u64, StoreError>;
}

/// [`Purge`], async. Its future is `Send`, like the async store's.
pub trait AsyncPurge: AsyncEventStore {
    /// As [`Purge::purge`].
    fn purge(&mut self, stream: &Cover) -> impl Future<Output = Result<u64, StoreError>> + Send;
}

/// A read-only unit of work: reads that all see one moment, the store as it
/// stood when the unit began. Nothing written after, through this store or
/// any other handle on its database, is seen through it, and a stream
/// purged after it began is read whole. It writes nothing and takes no
/// write lock, so it waits on no writer and no writer waits on it. It ends
/// when it is dropped.
pub trait ReadUnit {
    /// As [`EventStore::covers`], at the unit's moment.
    fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError>;

    /// As [`EventStore::read_from`], at the unit's moment.
    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError>;

    /// The whole stream, at the unit's moment.
    fn read(&mut self, stream: &Cover) -> Result<Option<Slice>, StoreError> {
        self.read_from(stream, 0)
    }
}

/// A store that opens read-only units of work: what a reader holds to list
/// a domain's streams and read each of them as one moment, rather than each
/// read seeing a moment of its own. A store opts in by implementing it; it
/// needs no other store trait, so a store opened only for reading can.
pub trait ReadStore {
    type Reads<'s>: ReadUnit + Send
    where
        Self: 's;

    /// A read-only unit of work over this store, its moment now.
    fn begin_read(&mut self) -> Self::Reads<'_>;
}

/// [`ReadUnit`], async. Its futures are `Send`, like the async store's.
pub trait AsyncReadUnit {
    /// As [`ReadUnit::covers`].
    fn covers(
        &mut self,
        domain: &str,
    ) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + Send;

    /// As [`ReadUnit::read_from`].
    fn read_from(
        &mut self,
        stream: &Cover,
        from: u64,
    ) -> impl Future<Output = Result<Option<Slice>, StoreError>> + Send;
}

/// [`ReadStore`], async: a store whose read-only units of work are a
/// database driver's read transactions. Beginning one is a future; a
/// failure to begin is answered by the unit's reads.
pub trait AsyncReadStore {
    type Reads<'s>: AsyncReadUnit + Send
    where
        Self: 's;

    /// As [`ReadStore::begin_read`].
    fn begin_read(&mut self) -> impl Future<Output = Self::Reads<'_>> + Send;
}

/// What a store's [`Watch::change_token`] answers: equal tokens from one
/// handle mean nothing was committed to the store between the two reads.
/// Opaque, and compared only for equality, and only with tokens from the
/// same handle: two handles on one database may answer different tokens
/// for the same moment.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ChangeToken([u64; 2]);

impl ChangeToken {
    /// The token a store makes from whatever it counts commits by.
    pub const fn new(parts: [u64; 2]) -> ChangeToken {
        ChangeToken(parts)
    }
}

/// A store that says, cheaply, whether anything has been committed to it
/// since it was last asked: what a watcher polls instead of reading every
/// stream again. A store opts in by implementing it; it needs no other
/// store trait, so a store opened only for reading can.
///
/// Two tokens from one handle are equal only if nothing was committed in
/// between, through this handle or any other on the store's database: no
/// append, no unit of work, no purge. A token may change when nothing a
/// reader sees did (a snapshot written, a purge of no such stream, a unit
/// of work rolled back), so a watcher that sees a new token reads again and
/// may find nothing new; it never misses a commit by seeing an old one. A
/// token reads no stream, writes nothing, and takes no write lock.
pub trait Watch {
    /// The store's token now.
    fn change_token(&mut self) -> Result<ChangeToken, StoreError>;
}

/// [`Watch`], async. Its future is `Send`, like the async store's.
pub trait AsyncWatch {
    /// As [`Watch::change_token`].
    fn change_token(&mut self) -> impl Future<Output = Result<ChangeToken, StoreError>> + Send;
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

impl<S: Purge + Send> AsyncPurge for Ready<S> {
    async fn purge(&mut self, stream: &Cover) -> Result<u64, StoreError> {
        self.0.purge(stream)
    }
}

impl<S: ReadUnit + Send> AsyncReadUnit for Ready<S> {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.0.covers(domain)
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        self.0.read_from(stream, from)
    }
}

impl<S: ReadStore + Send> AsyncReadStore for Ready<S> {
    type Reads<'s>
        = Ready<S::Reads<'s>>
    where
        Self: 's;

    async fn begin_read(&mut self) -> Ready<S::Reads<'_>> {
        Ready(self.0.begin_read())
    }
}

impl<S: Watch + Send> AsyncWatch for Ready<S> {
    async fn change_token(&mut self) -> Result<ChangeToken, StoreError> {
        self.0.change_token()
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

    async fn commit_position(&mut self) -> Result<u64, StoreError> {
        self.0.commit_position()
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
    /// How many times something was committed: its [`ChangeToken`].
    commits: u64,
    /// How many appends of at least one event have landed: its
    /// [`Store::commit_position`]. Each landed at the position it found,
    /// so every event lies below the count after it; a purge leaves it.
    position: u64,
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
            purged: BTreeSet::new(),
        }
    }

    fn pages(&self, stream: &Cover) -> &[EventPage] {
        self.streams
            .get(&key(stream))
            .map_or(&[], |h| h.pages.as_slice())
    }

    fn slice_from(&self, stream: &Cover, from: u64) -> Option<Slice> {
        self.streams
            .get(&key(stream))
            .map(|h| slice(&h.stream, &h.pages, from))
    }

    /// Count a commit, for [`Watch::change_token`].
    fn committed(&mut self) {
        self.commits = self.commits.wrapping_add(1);
    }

    fn covers_of(&self, domain: &str) -> Vec<Cover> {
        // Key order is the port's: main (no edition) first, then each
        // edition by name, each by root.
        self.streams
            .iter()
            .filter(|(k, _)| k.0 == domain)
            .map(|(_, held)| held.stream.clone())
            .collect()
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
        let length = held.pages.len() as u64;
        self.position += 1;
        self.committed();
        Ok(length)
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        Ok(self.slice_from(stream, from))
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
        Ok(self.covers_of(domain))
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        Ok(self.snapshots.get(&key(stream)).cloned())
    }

    fn write_snapshot(&mut self, stream: &Cover, snapshot: &Snapshot) -> Result<(), StoreError> {
        self.snapshots.insert(key(stream), snapshot.clone());
        self.committed();
        Ok(())
    }
}

impl Purge for InMemoryStore {
    fn purge(&mut self, stream: &Cover) -> Result<u64, StoreError> {
        let k = key(stream);
        self.snapshots.remove(&k);
        self.committed();
        Ok(self.streams.remove(&k).map_or(0, |h| h.pages.len() as u64))
    }
}

impl Watch for InMemoryStore {
    /// How many times something was committed: an append, a snapshot, a
    /// purge (of a stream or of nothing) or a unit of work.
    fn change_token(&mut self) -> Result<ChangeToken, StoreError> {
        Ok(ChangeToken::new([self.commits, 0]))
    }
}

impl ReadStore for InMemoryStore {
    type Reads<'s> = InMemoryRead<'s>;

    /// The unit borrows the store, so nothing is written while it is open.
    fn begin_read(&mut self) -> InMemoryRead<'_> {
        InMemoryRead(self)
    }
}

/// A read-only unit of work over an [`InMemoryStore`]: it borrows the store,
/// so the store cannot change under it.
pub struct InMemoryRead<'s>(&'s InMemoryStore);

impl ReadUnit for InMemoryRead<'_> {
    fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        Ok(self.0.covers_of(domain))
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        Ok(self.0.slice_from(stream, from))
    }
}

/// A unit of work over an [`InMemoryStore`]. See [`InMemoryStore::begin`].
pub struct InMemoryTx<'s> {
    store: &'s mut InMemoryStore,
    pending: BTreeMap<Key, Held>,
    /// The streams this unit purged from the store: their events and
    /// snapshots are deleted at commit, and hidden from its reads till then.
    purged: BTreeSet<Key>,
}

impl InMemoryTx<'_> {
    /// The store's stream, unless this unit purged it.
    fn stored(&self, k: &Key) -> Option<&Held> {
        if self.purged.contains(k) {
            return None;
        }
        self.store.streams.get(k)
    }

    /// The stream's pages: the store's, then the pending ones.
    fn pages(&self, stream: &Cover) -> Vec<EventPage> {
        let k = key(stream);
        let mut pages = self.stored(&k).map_or(Vec::new(), |h| h.pages.clone());
        if let Some(pending) = self.pending.get(&k) {
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
        if self.stored(&k).is_none() && !self.pending.contains_key(&k) {
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
        // The store's streams (less those this unit purged) and this unit's
        // new ones, merged in key order.
        let stored = self.store.streams.iter();
        let held = stored
            .filter(|(k, _)| !self.purged.contains(*k))
            .chain(&self.pending);
        let covers: BTreeMap<&Key, &Cover> = held
            .filter(|(k, _)| k.0 == domain)
            .map(|(k, held)| (k, &held.stream))
            .collect();
        Ok(covers.into_values().cloned().collect())
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        if self.purged.contains(&key(stream)) {
            return Ok(None);
        }
        self.store.read_snapshot(stream)
    }
}

impl Purge for InMemoryTx<'_> {
    fn purge(&mut self, stream: &Cover) -> Result<u64, StoreError> {
        let deleted = self.pages(stream).len() as u64;
        let k = key(stream);
        self.pending.remove(&k);
        self.purged.insert(k);
        Ok(deleted)
    }
}

impl UnitOfWork for InMemoryTx<'_> {
    fn commit(self) -> Result<(), StoreError> {
        let InMemoryTx {
            store,
            pending,
            purged,
        } = self;
        // Purges first: what this unit appended after purging a stream
        // starts it again.
        for k in purged {
            store.streams.remove(&k);
            store.snapshots.remove(&k);
        }
        store.committed();
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

    /// How many appends of at least one event have landed, a unit of
    /// work's each counted at its commit.
    fn commit_position(&mut self) -> Result<u64, StoreError> {
        Ok(self.position)
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

#[cfg(test)]
#[path = "store.test.rs"]
mod tests;
