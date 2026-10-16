//! The storage ports: one synchronous, one async, the same contract.
//!
//! The event store is the application's database (Postgres, for hatpick)
//! behind this port; there is no other event-store infrastructure.
//!
//! A command chain touches several streams and commits them all or none,
//! so the port is a **unit of work**: a database transaction. The
//! application opens one and hands it to the command handler
//! ([`handle`](super::handle)), which reads through it, appends every
//! staged stream through it once the whole chain has succeeded, and then
//! [`UnitOfWork::commit`]s it. Dropping it uncommitted rolls everything
//! back. The methods take `&mut self`, as a transaction does. One adapter
//! type implements [`EventStore`] once per event type it stores (a list
//! table, a draw-system table) and [`UnitOfWork`] once.
//!
//! A store holds streams and nothing else, each under its full
//! [`StreamKey`] (domain, edition, root). It answers three ways: the stream
//! (or `None`: no such stream), a refusal it is sure of
//! ([`StoreError::Conflict`], a caller bug), or a failure:
//! [`StoreError::Unavailable`] (it could not say) or
//! [`StoreError::Unreadable`] (it holds an event it cannot give back). An
//! I/O failure is never "no such stream".
//!
//! `E` is whatever the store hands the kernel. A typed store holds a
//! domain's events; an opaque one (`annals-angzarr`'s, over a database's
//! rows of type URL, page bytes and metadata columns) decodes and upcasts on
//! read and encodes on append, so the kernel only ever sees current events.

use std::collections::BTreeMap;
use std::future::Future;

use super::error::StoreError;
use super::id::{Seq, StreamKey};
use super::stream::Envelope;

/// Part of a stream, from a seq on.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Slice<E> {
    pub key: StreamKey,
    /// The stream's length when read: `events` ends there.
    pub len: Seq,
    pub events: Vec<Envelope<E>>,
}

/// Where streams are kept, synchronously. `E` is what the store hands the
/// kernel: a domain's current events (see the module docs).
pub trait EventStore<E> {
    /// Append `events` to the stream `key` names, creating it if `expected`
    /// is 0 and it does not exist. Refuses (appending nothing) a stream not
    /// at `expected` ([`StoreError::Conflict`]) and events whose seqs do not
    /// run on from `expected` ([`StoreError::Discontinuous`]). Returns the
    /// new length.
    ///
    /// **Idempotency (optional).** Every event's
    /// [`Meta::command`](super::Meta::command) names the command that
    /// recorded it. An adapter that indexes it (a unique index on the
    /// originating command id, say) may refuse an append whose originating
    /// command it already holds (the request and the command id), as
    /// [`StoreError::Duplicate`] with the range recorded then, so a wrapping
    /// runtime's retry becomes a read. annals neither requires nor builds
    /// this.
    fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> Result<Seq, StoreError>;

    /// The stream's events from seq `from` on: none when
    /// `from` is at or past its end. `None` when there is no such stream.
    fn read_from(&mut self, key: &StreamKey, from: Seq) -> Result<Option<Slice<E>>, StoreError>;

    /// The whole stream.
    fn read(&mut self, key: &StreamKey) -> Result<Option<Slice<E>>, StoreError> {
        self.read_from(key, 0)
    }
}

/// A transaction over one or more [`EventStore`]s: what it appended lands
/// at [`UnitOfWork::commit`], all at once, or (dropped) not at all.
pub trait UnitOfWork {
    fn commit(self) -> Result<(), StoreError>;
}

/// [`UnitOfWork`], async. Its future is `Send`, like the async store's.
pub trait AsyncUnitOfWork {
    fn commit(self) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// [`EventStore`], async: for a database driver. Native return-position
/// `impl Future` in a trait (Rust 1.75), no runtime and no dependency;
/// an adapter writes its methods as plain `async fn`.
///
/// The futures are required to be **`Send`**, so the command handler's
/// future is too, and a server on tokio's multi-threaded runtime (or
/// tonic) can spawn it. A single-threaded adapter whose futures are not
/// `Send` (a browser's IndexedDB) cannot implement this; it implements the
/// synchronous [`EventStore`] instead, or a `!Send` twin of this trait
/// added beside it if one is ever needed.
pub trait AsyncEventStore<E> {
    /// As [`EventStore::append`].
    fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> impl Future<Output = Result<Seq, StoreError>> + Send;

    /// As [`EventStore::read_from`].
    fn read_from(
        &mut self,
        key: &StreamKey,
        from: Seq,
    ) -> impl Future<Output = Result<Option<Slice<E>>, StoreError>> + Send;

    /// As [`EventStore::read`].
    fn read(
        &mut self,
        key: &StreamKey,
    ) -> impl Future<Output = Result<Option<Slice<E>>, StoreError>> + Send {
        self.read_from(key, 0)
    }
}

/// A synchronous store served through the async port, every future ready
/// at once.
pub struct Ready<S>(pub S);

impl<E: Sync, S: EventStore<E> + Send> AsyncEventStore<E> for Ready<S> {
    async fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> Result<Seq, StoreError> {
        self.0.append(key, expected, events)
    }

    async fn read_from(
        &mut self,
        key: &StreamKey,
        from: Seq,
    ) -> Result<Option<Slice<E>>, StoreError> {
        self.0.read_from(key, from)
    }
}

/// One stream as [`InMemoryStore`] holds it.
type Held<E> = Vec<Envelope<E>>;

impl<S: UnitOfWork + Send> AsyncUnitOfWork for Ready<S> {
    async fn commit(self) -> Result<(), StoreError> {
        self.0.commit()
    }
}

/// Streams held in memory, in key order. Checks everything a real store
/// must, so tests catch what production would.
pub struct InMemoryStore<E> {
    streams: BTreeMap<StreamKey, Held<E>>,
}

impl<E> Default for InMemoryStore<E> {
    fn default() -> Self {
        InMemoryStore {
            streams: BTreeMap::new(),
        }
    }
}

impl<E> InMemoryStore<E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every stream key, in order.
    pub fn keys(&self) -> Vec<StreamKey> {
        self.streams.keys().cloned().collect()
    }
}

impl<E: Clone> EventStore<E> for InMemoryStore<E> {
    fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> Result<Seq, StoreError> {
        todo!("proposal: check length and continuity, then extend")
    }

    fn read_from(&mut self, key: &StreamKey, from: Seq) -> Result<Option<Slice<E>>, StoreError> {
        todo!("proposal: clone the tail")
    }
}

impl<E: Clone> InMemoryStore<E> {
    /// A unit of work over this store: appends are held until
    /// [`UnitOfWork::commit`], and reads see them.
    pub fn begin(&mut self) -> InMemoryTx<'_, E> {
        InMemoryTx {
            store: self,
            pending: BTreeMap::new(),
        }
    }
}

/// A unit of work over an [`InMemoryStore`]. See [`InMemoryStore::begin`].
pub struct InMemoryTx<'s, E> {
    store: &'s mut InMemoryStore<E>,
    pending: BTreeMap<StreamKey, Held<E>>,
}

impl<E: Clone> EventStore<E> for InMemoryTx<'_, E> {
    fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> Result<Seq, StoreError> {
        todo!("proposal: check against store + pending, hold in pending")
    }

    fn read_from(&mut self, key: &StreamKey, from: Seq) -> Result<Option<Slice<E>>, StoreError> {
        todo!("proposal: the store's stream with pending appends after it")
    }
}

impl<E: Clone> UnitOfWork for InMemoryTx<'_, E> {
    fn commit(self) -> Result<(), StoreError> {
        todo!("proposal: move pending into the store")
    }
}
