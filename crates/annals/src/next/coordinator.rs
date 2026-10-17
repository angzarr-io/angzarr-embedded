//! Steps 3 to 5 of the pipeline: the in-process coordinator.
//!
//! angzarr's aggregate coordinator is a service that loads a stream, checks
//! or stamps the command's expected sequence and hands a
//! `ContextualCommand` to business code. annals has no service and sends
//! nothing: the [`Coordinator`] is a struct over the application's unit of
//! work, and it does the same steps in process, domain-blind (it sees
//! `Cover`s, pages and the [`Registry`], never a domain's types):
//!
//! 3. [`Coordinator::deduplicate`]: does the addressed stream already hold
//!    events with this command id? Then the command was applied: answered
//!    [`Deduplicated::Duplicate`] with those events, and nothing below
//!    runs. Before the sequence check, so a retry of an applied STRICT
//!    command whose expected sequence is now stale is a duplicate, never
//!    stale.
//! 4. [`Coordinator::load`]: resolve the conflict resolution (the book's,
//!    the command type's, the domain's, else COMMUTATIVE where the state
//!    supports it and STRICT otherwise: [`Registry::resolve`]); load the
//!    newest usable snapshot and the events after it; stamp an absent
//!    expected sequence with the head, or check a present one: under
//!    STRICT it must be the head ([`KernelError::Stale`]); under
//!    AGGREGATE_HANDLES and COMMUTATIVE the events from it to the head are
//!    the window, delivered as events (the snapshot used is at or below
//!    expected−1, so none of the window is hidden in snapshot state), with
//!    a [`Nonce`]. An expected sequence past the head is stale under every
//!    resolution.
//!
//! Then the typed step, which needs `D`:
//!
//! 5. [`Contextual::decode`]: the snapshot's state (discarded, and the
//!    stream replayed, on a rules-version or state-type mismatch), the
//!    events and the command's payload, each through the domain's upcaster
//!    and decoded strictly; then [`Prepared::decide`] runs decide or ingest
//!    and stamps the events.
//!
//! In a [`Chain`](super::Chain), steps 3 and 4 consult the chain's staged
//! streams before the store, so a later step sees an earlier step's
//! uncommitted events (and its command ids).

use angzarr_proto::v1::{ConflictResolution, EventBook, EventPage, Snapshot};
use prost_types::Timestamp;

use super::check::{Identified, Sampled};
use super::dead_letter::DeadLetterLog;
use super::domain::{Decision, Domain};
use super::error::{Failure, KernelError, StoreError};
use super::registry::Registry;
use super::store::EventStore;
use super::stream::Stream;

/// The in-process conflict nonce (angzarr's `ContextualCommand.conflict_nonce`):
/// minted by the coordinator per delivery under AGGREGATE_HANDLES and
/// COMMUTATIVE, handed to decide with the window under AGGREGATE_HANDLES
/// (the kernel keeps it under COMMUTATIVE and acknowledges it itself when
/// the units are disjoint), and given back through
/// [`Tx::resolve_conflicts`](super::Tx::resolve_conflicts) as decide's
/// declaration that it examined the window. Not constructible, clonable or
/// copyable outside the kernel, so a decide cannot satisfy the check
/// without having received it. A counter, not a random value: the kernel
/// stays deterministic.
#[derive(PartialEq, Eq, Debug)]
pub struct Nonce(pub(crate) u64);

/// Steps 3 and 4 over the application's unit of work.
pub struct Coordinator<'u, U> {
    unit: &'u mut U,
    registry: &'u Registry,
    /// The next [`Nonce`].
    nonces: u64,
}

/// What deduplication found.
pub enum Deduplicated {
    /// The stream holds this command's events already: they are the
    /// answer (angzarr's DUPLICATE), in sequence order, in an `EventBook`
    /// for the addressed stream.
    Duplicate {
        command: Identified,
        events: EventBook,
    },
    /// Nothing found: the command goes on to [`Coordinator::load`]. A
    /// command whose earlier decision emitted nothing lands here again (an
    /// accepted cost: it is decided again, against the stream as it is
    /// now).
    Novel(Novel),
}

/// An [`Identified`] command that deduplication did not find: the only
/// thing [`Coordinator::load`] takes.
pub struct Novel(Identified);

impl Novel {
    pub fn command(&self) -> &Identified {
        &self.0
    }
}

impl<'u, U: EventStore> Coordinator<'u, U> {
    pub fn new(unit: &'u mut U, registry: &'u Registry) -> Coordinator<'u, U> {
        Coordinator {
            unit,
            registry,
            nonces: 0,
        }
    }

    /// Step 3: look the command id up on the addressed stream
    /// ([`EventStore::find_command`], over the index on (stream,
    /// command_id)).
    pub fn deduplicate(&mut self, command: Identified) -> Result<Deduplicated, StoreError> {
        todo!("proposal: find_command; pages found => Duplicate, none => Novel")
    }

    /// Step 4: resolve the conflict resolution, load the snapshot and the
    /// events, and stamp or check the expected sequence (see the module
    /// docs). STRICT's stale command is refused here, before anything is
    /// decoded.
    pub fn load(&mut self, command: Novel) -> Result<Contextual, KernelError> {
        todo!(
            "proposal: Registry::resolve; read_snapshot (usable, <= expected-1 under AH/COMM) \
             and read_from; stamp or check expected; window and nonce under AH/COMM"
        )
    }
}

/// A command with its stream's history and its handling resolved: angzarr's
/// `ContextualCommand`, in process. Domain-blind still: the history is
/// pages.
pub struct Contextual {
    pub(crate) command: Identified,
    /// Never UNSPECIFIED.
    pub(crate) resolution: ConflictResolution,
    /// The book's, or the head, stamped.
    pub(crate) expected: u64,
    /// The newest usable snapshot, at or below expected−1 under
    /// AGGREGATE_HANDLES and COMMUTATIVE.
    pub(crate) snapshot: Option<Snapshot>,
    /// The events after the snapshot (or from 0) to the head.
    pub(crate) pages: Vec<EventPage>,
    /// The stream's next sequence.
    pub(crate) head: u64,
    /// The last event's `recorded_at`: the floor for the next.
    pub(crate) last_recorded_at: Option<Timestamp>,
    /// Under AGGREGATE_HANDLES and COMMUTATIVE; `None` under STRICT.
    pub(crate) nonce: Option<Nonce>,
}

impl Contextual {
    pub fn command(&self) -> &Identified {
        &self.command
    }

    /// The resolved conflict resolution: never UNSPECIFIED.
    pub fn conflict_resolution(&self) -> ConflictResolution {
        self.resolution
    }

    /// The resolved expected sequence: the book's, or the head.
    pub fn expected_sequence(&self) -> u64 {
        self.expected
    }

    /// The stream's next sequence.
    pub fn head(&self) -> u64 {
        self.head
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// The events loaded, after the snapshot.
    pub fn pages(&self) -> &[EventPage] {
        &self.pages
    }

    /// The events from the expected sequence to the head: empty under
    /// STRICT and for a stamped command.
    pub fn window(&self) -> &[EventPage] {
        todo!("proposal: the pages with sequence >= expected")
    }

    /// Whether a nonce was minted (AGGREGATE_HANDLES or COMMUTATIVE, even
    /// for an empty window).
    pub fn has_nonce(&self) -> bool {
        self.nonce.is_some()
    }

    /// Step 5's first half, typed: the stream as `D` (from the snapshot if
    /// [`usable`](Registry::snapshot_usable), else from 0 after discarding
    /// it), every event and the command's payload through `D`'s upcaster
    /// and decoded strictly. A command or fact type `D` does not decode is
    /// [`KernelError::UnknownType`], naming its type URL; a stored event,
    /// [`StoreError::Unreadable`]; a cover of another domain,
    /// [`KernelError::WrongDomain`].
    pub fn decode<D: Domain>(self, registry: &Registry) -> Result<Prepared<D>, KernelError> {
        todo!("proposal: resume or replay the stream, upcast_command and decode the payload")
    }
}

/// The decoded input: a command for decide, or a fact for ingest.
pub(crate) enum Input<D: Domain> {
    Command(D::Command),
    Fact(D::Fact),
}

/// A command ready for business code: its stream at the head, its decoded
/// input, and its resolved handling.
pub struct Prepared<D: Domain> {
    pub(crate) command: Identified,
    pub(crate) input: Input<D>,
    pub(crate) stream: Stream<D>,
    pub(crate) resolution: ConflictResolution,
    pub(crate) expected: u64,
    pub(crate) nonce: Option<Nonce>,
}

impl<D: Domain> Prepared<D> {
    pub fn command(&self) -> &Identified {
        &self.command
    }

    /// The stream at the head.
    pub fn stream(&self) -> &Stream<D> {
        &self.stream
    }

    /// Step 5's second half: the body arm picks the pipeline.
    ///
    /// - A command is decided in a staging [`Tx`](super::Tx), against the
    ///   head. Under AGGREGATE_HANDLES decide is handed the window and the
    ///   nonce and must give the nonce back
    ///   ([`KernelError::Unresolved`] otherwise). Under COMMUTATIVE the
    ///   kernel resolves: `window = changed(state at expected, head)`,
    ///   `mine = changed(head, head with the events)`, both by the state's
    ///   generated [`State::changed`](super::State::changed); the events are
    ///   kept iff the two [do not overlap](super::Units::overlaps), else
    ///   [`KernelError::Stale`].
    /// - A fact is ingested ([`Domain::ingest`](super::Domain::ingest),
    ///   with an [`IngestCtx`](super::IngestCtx)). A panic is caught: the
    ///   chain fails with [`KernelError::IngestPanicked`] and `dead_letters`
    ///   gets a line ([`dead_letter_line`](super::dead_letter_line)).
    ///
    /// Every event is stamped with [`Identified::meta`], at `recorded_at =
    /// max(sampled.now, the stream's last)` and the current rules version.
    pub fn decide(
        &mut self,
        sampled: &Sampled,
        registry: &Registry,
        dead_letters: &dyn DeadLetterLog,
    ) -> Result<Decision<D>, Failure<D::Rejection>> {
        todo!("proposal: decide (with the window and nonce) or ingest (catching a panic), stamp")
    }

    /// The stream, to commit the decision to.
    pub fn into_stream(self) -> Stream<D> {
        self.stream
    }
}
