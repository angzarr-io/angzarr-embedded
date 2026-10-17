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
use super::id::{Fingerprint, Seq, StreamKey};
use super::meta::Origin;
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

/// One applied command, recorded under its key: angzarr's
/// `IdempotencyClaim`, scoped to the addressed stream.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Claim {
    /// The key: a client-supplied request's id alone, or a fact's
    /// external id. Only the key fields are compared; a fact's description
    /// is not part of it.
    pub origin: Origin,
    /// The payload's fingerprint as received (a UUIDv5, 16 bytes).
    pub fingerprint: Fingerprint,
    /// The applied range, `from..to` in the stream (possibly empty: a
    /// command with nothing to do is applied too).
    pub from: Seq,
    pub to: Seq,
}

/// A transaction over one or more [`EventStore`]s: what it appended lands
/// at [`UnitOfWork::commit`], all at once, or (dropped) not at all.
///
/// **Idempotency, optional.** annals keeps no idempotency key store: a
/// chain is in process and atomic, with no at-least-once bus inside it, so
/// nothing in it is redelivered. Two edges remain, and both are answered
/// here, by the command handler calling [`UnitOfWork::record_command`] for
/// the originating book when its origin is keyed ([`Origin::is_keyed`]),
/// in the same unit of work as its events:
///
/// - a **webhook fact** redelivered by its source: unique on (stream,
///   `external_id`);
/// - an **HTTP retry** of a request with a client-supplied id: unique on
///   (stream, request id), for a server that wants retry safety. A request
///   id names one command; there is no index.
///
/// A minted request id is never recorded, and neither is a causation (a
/// chain's reactions commit with it, so nothing redelivers them). Each
/// claim records the payload's [`Fingerprint`], as angzarr's does. A key
/// already recorded is refused, the same way for every kind of key:
///
/// - with the same fingerprint: [`StoreError::Duplicate`] with the range
///   recorded then (angzarr's DUPLICATE: the retry becomes a read);
/// - with another fingerprint: [`StoreError::KeyReused`], with that range
///   too. The store does not judge it: the kernel refuses it for a request
///   (angzarr's FAILED_PRECONDITION: the key was reused for a different
///   command) and, by default, for a fact; a fact whose domain configures
///   [`FactMismatch::Duplicate`](super::FactMismatch::Duplicate) it
///   answers [`StoreError::DivergentDuplicate`] instead.
///
/// Either way the unit of work is dropped. Only applied commands are
/// recorded; a rejection writes nothing, so it may be retried. The default
/// records nothing.
pub trait UnitOfWork {
    fn commit(self) -> Result<(), StoreError>;

    /// Record `claim` against `stream`, or refuse a key already recorded
    /// there ([`StoreError::Duplicate`], [`StoreError::KeyReused`]).
    /// Records nothing by default.
    fn record_command(&mut self, stream: &StreamKey, claim: &Claim) -> Result<(), StoreError> {
        Ok(())
    }
}

/// [`UnitOfWork`], async. Its future is `Send`, like the async store's.
pub trait AsyncUnitOfWork {
    fn commit(self) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// As [`UnitOfWork::record_command`].
    fn record_command(
        &mut self,
        stream: &StreamKey,
        claim: &Claim,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        async { Ok(()) }
    }
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

    async fn record_command(
        &mut self,
        stream: &StreamKey,
        claim: &Claim,
    ) -> Result<(), StoreError> {
        self.0.record_command(stream, claim)
    }
}

/// The recorded claims, by stream and key: the origin with a fact's
/// description cleared, since it is not part of the key.
type Claims = BTreeMap<(StreamKey, Origin), Claim>;

/// Streams held in memory, in key order, and the keys its units of work
/// recorded. Checks everything a real store must, so tests catch what
/// production would.
pub struct InMemoryStore<E> {
    streams: BTreeMap<StreamKey, Held<E>>,
    claims: Claims,
}

impl<E> Default for InMemoryStore<E> {
    fn default() -> Self {
        InMemoryStore {
            streams: BTreeMap::new(),
            claims: BTreeMap::new(),
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
            claims: BTreeMap::new(),
        }
    }
}

/// A unit of work over an [`InMemoryStore`]. See [`InMemoryStore::begin`].
pub struct InMemoryTx<'s, E> {
    store: &'s mut InMemoryStore<E>,
    pending: BTreeMap<StreamKey, Held<E>>,
    claims: Claims,
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
        todo!("proposal: move pending and the claims into the store")
    }

    /// Records every claim: unique on (stream, key), the key being the
    /// origin's key fields (a request's id alone, a fact's external id
    /// alone). A repeat is [`StoreError::Duplicate`] with the same
    /// fingerprint, [`StoreError::KeyReused`] with another, whatever the
    /// kind of key.
    fn record_command(&mut self, stream: &StreamKey, claim: &Claim) -> Result<(), StoreError> {
        todo!("proposal: compare with the store's or this unit's claim for the key, else hold it")
    }
}
