//! A stream: an append-only log of stamped events and the state folded from
//! it.

use super::book::{BookOf, Sampled};
use super::domain::{Decision, Domain};
use super::error::{Failure, KernelError, ReplayError};
use super::handler::Handler;
use super::id::{EditionName, Root, Seq, StreamKey, Timestamp};
use super::meta::Meta;
use super::snapshot::Snapshot;
use super::version::Rulebook;

/// One stored event: its position, its metadata and its payload (angzarr's
/// `EventPage { sequence, meta, payload }`).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Envelope<E> {
    pub seq: Seq,
    pub meta: Meta,
    pub event: E,
}

/// A domain's event log and the state folded from it. Its state changes
/// only in [`Stream::commit`]. Its key is fixed when it is made, and its
/// domain is always `D::NAME`.
///
/// A stream resumed from a snapshot ([`Stream::resume`]) holds only the
/// events after it: [`Stream::events`] starts at [`Stream::first`], and
/// [`Stream::len`] counts from 0 all the same.
pub struct Stream<D: Domain> {
    key: StreamKey,
    /// The seq of the first held event: 0, or the snapshot's seq + 1.
    first: Seq,
    events: Vec<Envelope<D::Event>>,
    state: D::State,
    /// The last event's recorded_at: the floor for the next.
    last_recorded_at: Option<Timestamp>,
}

impl<D: Domain> Stream<D> {
    /// An empty `D` stream: `root` in `edition` (`None`: main).
    pub fn new(edition: Option<EditionName>, root: Root) -> Stream<D> {
        Stream {
            key: StreamKey::of::<D>(edition, root),
            first: 0,
            events: Vec::new(),
            state: D::State::default(),
            last_recorded_at: None,
        }
    }

    /// Handle `book` under the rulebook's current rules, without committing.
    /// Pure: the stream is untouched whatever the outcome, so a rejection
    /// leaves nothing behind. In order, before any business code:
    ///
    /// 1. the book is validated ([`Malformed`](super::Malformed)) and its
    ///    key checked against `D` ([`KernelError::WrongDomain`]);
    /// 2. its handling is resolved: an absent `expected_sequence` is the
    ///    head; a resolution absent from the book is
    ///    [`Domain::conflict_resolution`]'s; an expected sequence past the
    ///    head, or behind it under STRICT, is [`KernelError::Stale`];
    ///
    /// then the body arm picks the pipeline: a command is decided
    /// ([`Domain::decide`], in a staging [`Tx`](super::Tx), with the window
    /// and a nonce under AGGREGATE_HANDLES, checked after it returns), a
    /// fact ingested ([`Domain::ingest`], infallible). COMMUTATIVE is
    /// resolved by the kernel, by conflict units ([`Domain::COMMUTATIVE`]).
    /// Every event is stamped with one [`Meta`]: the book's actor, its
    /// resolved correlation ([`Book::correlation`](super::Book::correlation)),
    /// its resolved origin ([`Book::origin`](super::Book::origin): a
    /// request id always set, minted from `sampled.request` if absent),
    /// `recorded_at = max(sampled.now, the last event's)`, `base_sequence =
    /// the head`, and the current rules version.
    pub fn decide(
        &self,
        book: &BookOf<D>,
        sampled: &Sampled,
        rules: &Rulebook<D::Rules>,
    ) -> Result<Decision<D>, Failure<D::Rejection>> {
        todo!("proposal: validate, resolve handling, decide or ingest, check the nonce, stamp")
    }

    /// Fold a decision into the stream, then hand each of its events to
    /// every handler in order, with the state just after it. Refuses a
    /// decision made against another stream or another length
    /// ([`KernelError::Stale`]). Returns the new length.
    ///
    /// Each event is applied again here, to the live state, rather than the
    /// decide's staged copy kept: the handlers need the state after every
    /// event, and apply is pure, so the result is the same.
    pub fn commit(
        &mut self,
        decision: Decision<D>,
        rules: &Rulebook<D::Rules>,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<Seq, KernelError> {
        todo!("proposal: check stream and base, apply each event, push it, call the handlers")
    }

    /// Decide and commit with no store between: for tests and in-memory
    /// use. [`handle`](super::handle) is the full loop, with a store.
    pub fn execute(
        &mut self,
        book: &BookOf<D>,
        sampled: &Sampled,
        rules: &Rulebook<D::Rules>,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<&[Envelope<D::Event>], Failure<D::Rejection>> {
        todo!("proposal: decide, then commit")
    }

    /// Rebuild a stream from its whole log. Each event is folded with the
    /// rules its own metadata names, found in `rules`; the caller does not
    /// choose them. Refuses a key of another domain, a rules version the
    /// rulebook lacks, a seq out of order, a causation in this stream that
    /// is not earlier, and a `recorded_at` earlier than the event before.
    /// The events are already decoded at their current type: decoding and
    /// upcasting a stored form is the store adapter's (`annals-angzarr`).
    pub fn replay(
        key: StreamKey,
        events: Vec<Envelope<D::Event>>,
        rules: &Rulebook<D::Rules>,
    ) -> Result<Stream<D>, ReplayError> {
        todo!("proposal: check the domain, check each envelope, resolve its rules, fold")
    }

    /// Rebuild a stream from a snapshot and the events after it, checked as
    /// [`Stream::replay`] checks. The caller has already checked the
    /// snapshot is usable ([`Snapshot::usable`]); [`load`](super::load)
    /// does both.
    pub fn resume(
        key: StreamKey,
        snapshot: Snapshot<D::State>,
        tail: Vec<Envelope<D::Event>>,
        rules: &Rulebook<D::Rules>,
    ) -> Result<Stream<D>, ReplayError> {
        todo!("proposal: start from the snapshot's state at its seq + 1, then as replay")
    }

    /// The stream's state as a snapshot under the current rules version,
    /// for a [`SnapshotStore`](super::SnapshotStore). `None` for an empty
    /// stream.
    pub fn snapshot(&self, rules: &Rulebook<D::Rules>) -> Option<Snapshot<D::State>> {
        todo!("proposal: the last seq, the state, the current version, the last recorded_at")
    }

    /// Hand every held event from seq `from` on to the handlers, with the
    /// state just after it: rebuilding a projection, or a reaction's
    /// catch-up. One pass, however many handlers. A stream resumed from a
    /// snapshot cannot hand out events before [`Stream::first`]
    /// ([`KernelError::BeforeSnapshot`]).
    pub fn dispatch(
        &self,
        from: Seq,
        rules: &Rulebook<D::Rules>,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<(), KernelError> {
        todo!("proposal: fold from first, call the handlers from `from`")
    }

    pub fn key(&self) -> &StreamKey {
        &self.key
    }

    /// The events held, from [`Stream::first`].
    pub fn events(&self) -> &[Envelope<D::Event>] {
        &self.events
    }

    /// The seq of the first held event: 0 unless resumed from a snapshot.
    pub fn first(&self) -> Seq {
        self.first
    }

    pub fn state(&self) -> &D::State {
        &self.state
    }

    /// The number of events, which is also the next event's seq.
    pub fn len(&self) -> Seq {
        self.first + self.events.len() as Seq
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The last event's `recorded_at`: no later event is recorded before it.
    pub fn last_recorded_at(&self) -> Option<Timestamp> {
        self.last_recorded_at
    }

    pub fn into_parts(self) -> (StreamKey, Vec<Envelope<D::Event>>, D::State) {
        (self.key, self.events, self.state)
    }
}
