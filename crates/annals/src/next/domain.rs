//! Deciding, ingesting and applying, with deciding made pure.

use angzarr_proto::v1::{ConflictResolution, Cover, EventRef, FactOrigin, StateDescriptor, Uuid};
use annals_diagnostic::Diagnostic;
use prost_types::Timestamp;

use super::conflict::Units;
use super::coordinator::Nonce;
use super::payload::Payload;
use super::stream::Envelope;

/// A domain's state: a domain type, with a prost message as its snapshot
/// form ([`State::Snapshot`]), so it can be snapshotted as `Any`. angzarr's
/// generator (`angzarr_build`, backlog CR-1 and CR-4) emits the impl from the
/// state message, its `(io.angzarr.v1.state)` option and its
/// `(io.angzarr.v1.conflict)` field markers; a state that is itself a prost
/// message is its own snapshot form.
pub trait State: Default + Clone + Send + 'static {
    /// The wire form a snapshot carries. Its type URL is the state's, as
    /// [`State::descriptor`] names it; registration refuses a descriptor
    /// naming another.
    type Snapshot: prost::Message + prost::Name + Default;

    /// The state as its snapshot form. Infallible: every state has one.
    fn to_snapshot(&self) -> Self::Snapshot;

    /// The state a snapshot holds, or why it holds none: a wire value need
    /// not be a valid state (a required field absent, an enum value the
    /// domain does not define, a broken invariant). A refusal makes the
    /// snapshot unreadable ([`PayloadError::Invalid`](super::PayloadError)).
    fn from_snapshot(snapshot: Self::Snapshot) -> Result<Self, annals_diagnostic::Report>;

    /// Overwrite this instance's state with what `snapshot` holds, keeping
    /// whatever the snapshot does not (a marked-up aggregate's immutable
    /// configuration, from the registry's factory). Resuming builds a fresh
    /// instance, then restores it. The default replaces the whole instance
    /// with [`State::from_snapshot`]'s.
    fn restore(&mut self, snapshot: Self::Snapshot) -> Result<(), annals_diagnostic::Report> {
        *self = Self::from_snapshot(snapshot)?;
        Ok(())
    }

    /// The state's type URL and the conflict resolutions it supports, the
    /// full resolved list (`[COMMUTATIVE, STRICT, AGGREGATE_HANDLES]` for a
    /// state that lists COMMUTATIVE and marks its units; `[STRICT,
    /// AGGREGATE_HANDLES]`, the implicit base, for one with no option). What
    /// [`Registry::register`](super::Registry::register) records, as
    /// angzarr's `ComponentDescriptor.state`.
    fn descriptor() -> StateDescriptor;

    /// Whether the kernel takes snapshots of this state. `false` for a
    /// state with no snapshot form (a marked-up aggregate without
    /// `snapshot = ..`, whose [`State::Snapshot`] is
    /// [`NoSnapshot`](super::markup::NoSnapshot)): its streams are always
    /// replayed from the start, and [`Stream::snapshot`](super::Stream::snapshot)
    /// is `None`.
    const SNAPSHOTS: bool = true;

    /// The conflict units whose values differ between `before` and
    /// `after` (angzarr's `conflict_units` table, field kind by field kind,
    /// with the fail-safe wildcard for every unmarked field that differs).
    /// Pure. Called only under COMMUTATIVE, so only for a state that lists
    /// it; the default, for a state with no units, says "everything".
    fn changed(_before: &Self, _after: &Self) -> Units {
        Units::everything()
    }
}

/// One kind of event stream: its state, its commands and facts, its events
/// and its rules for each:
///
/// - **No `Ctx`.** Decide is a function of its explicit inputs only: the
///   state, the command and the rules, plus what the kernel stamps (the
///   recorded instant, the ids), readable through [`Tx`]. Random values
///   are sampled by the caller into the command.
/// - **Two inputs, two pipelines**, picked by the command page's body arm:
///   a command goes to the fallible [`Domain::decide`]; a fact to the
///   infallible [`Domain::ingest`], which has no error to return.
/// - **Payloads are prost messages.** Commands, facts and events are
///   [`Payload`]s, packed as `Any` by type URL; the state is a [`State`].
/// - **Business output is payloads only.** decide emits events; the kernel
///   builds the pages: their sequences and their `EventMeta`.
/// - a decide may reject at any point, emitted or not: nothing it staged is
///   kept;
/// - everything `'static` and `Send`, so a chain can stage streams of
///   several domains side by side and the async command handler's future is
///   `Send`.
pub trait Domain: Sized + 'static {
    /// Folded from the events. Never stored, except as a snapshot.
    type State: State;
    /// A request to change the stream. Decided, never stored.
    type Command: Payload + Send + 'static;
    /// Something that already happened outside the system (a webhook, an
    /// integration). Ingested, never refused, never stored as such.
    /// [`std::convert::Infallible`] for a domain that takes none.
    type Fact: Payload + Send + 'static;
    /// What a stream stores.
    type Event: Payload + Clone + Send + Sync + 'static;
    /// Why a command was refused, for the person who gave it. Kernel faults
    /// are never folded into it: see [`Failure`](super::Failure).
    type Rejection: Diagnostic + Send + 'static;
    /// Immutable reference data both halves read, one value per rules
    /// version, found through a [`Rulebook`](super::Rulebook).
    type Rules: Send + Sync + 'static;

    /// The domain's name: the `Cover.domain` of every stream of this
    /// domain, and its key in the [`Registry`](super::Registry). Stable
    /// across releases; never contains `':'`.
    const NAME: &'static str;

    /// The domain's configured conflict resolution (angzarr's component
    /// configuration). `None`: the built-in default, COMMUTATIVE where the
    /// state supports it, otherwise STRICT. Refused at registration if the
    /// state does not support it.
    const CONFLICT_RESOLUTION: Option<ConflictResolution> = None;

    /// Configured resolutions per command type, by the command message's
    /// full name (`"hatpick.list.v1.Draw"`): above the domain's own, below a
    /// book's request. Refused at registration if the state does not
    /// support one. Looked up by the command's current type, after
    /// upcasting: an old command type has its current type's setting, so
    /// only current types are named here.
    const CONFLICT_RESOLUTION_BY_COMMAND: &'static [(&'static str, ConflictResolution)] = &[];

    /// Decide a command against the current state. Pure: its only output is
    /// the events it emits through `tx`, and they are kept only if it
    /// returns `Ok`.
    fn decide(cmd: &Self::Command, tx: &mut Tx<'_, Self>) -> Result<(), Self::Rejection>;

    /// Record a fact: the events it becomes, given the state and a
    /// read-only context ([`IngestCtx`]: when it is recorded, its ids, its
    /// source, the rules). **Infallible by type**: a fact already happened,
    /// so an awkward one is recorded (a payment for an unknown order
    /// becomes an event that says so), never refused. A panic here is a
    /// bug: in process it aborts the step, which writes nothing, and the
    /// dead-letter log says so.
    fn ingest(
        state: &Self::State,
        fact: &Self::Fact,
        ctx: &IngestCtx<'_, Self>,
    ) -> Vec<Self::Event>;

    /// Fold one event into the state. Pure: the same state, event and rules
    /// always give the same result. Never sees the event's metadata.
    fn apply(state: &mut Self::State, event: &Self::Event, rules: &Self::Rules);

    /// The typed refusal object `rejection` carries, packed, if any: what a
    /// refused policy receives as `RefusalResponse.refusal`. `None` by
    /// default; a marked-up domain's is its rejection's
    /// ([`Rejection::refusal`](super::markup::Rejection::refusal)).
    fn refusal(_rejection: &Self::Rejection) -> Option<prost_types::Any> {
        None
    }
}

/// What an ingest may read besides the state and the fact, all read-only:
/// the instant the events will be recorded at, the fact's ids, where it
/// came from, and the rules it is ingested under.
pub struct IngestCtx<'a, D: Domain> {
    pub(crate) stream: &'a Cover,
    pub(crate) recorded_at: Timestamp,
    pub(crate) command_id: &'a Uuid,
    pub(crate) correlation_id: &'a Uuid,
    pub(crate) source: &'a FactOrigin,
    pub(crate) rules: &'a D::Rules,
    pub(crate) rules_version: &'a str,
    /// The stream's length: 0 for a stream with no events yet.
    pub(crate) head: u64,
}

impl<'a, D: Domain> IngestCtx<'a, D> {
    /// The stream the fact is recorded in.
    pub fn stream(&self) -> &'a Cover {
        self.stream
    }

    /// When the events will be recorded (non-decreasing within the stream).
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// The fact's command id: UUIDv5 of its external id.
    pub fn command_id(&self) -> &'a Uuid {
        self.command_id
    }

    /// The fact's correlation: as sent, else its command id.
    pub fn correlation_id(&self) -> &'a Uuid {
        self.correlation_id
    }

    /// Where the fact came from: its external id and description.
    pub fn source(&self) -> &'a FactOrigin {
        self.source
    }

    /// The current rules.
    pub fn rules(&self) -> &'a D::Rules {
        self.rules
    }

    /// The current rules' version, which the events are stamped with.
    pub fn rules_version(&self) -> &'a str {
        self.rules_version
    }
}

/// What decide is handed under AGGREGATE_HANDLES: the window it did not see
/// (the events from the book's `expected_sequence` to the head) and the
/// nonce. An empty window is delivered too, so the path is uniform.
#[derive(Debug)]
pub struct Conflicts<'a, E> {
    pub window: &'a [Envelope<E>],
    pub nonce: Nonce,
}

/// A decide in progress. Reads go through it; emitted events are staged in
/// it and folded into a copy of the state, made at the first emit, so the
/// decide sees their effects and the stream sees nothing.
pub struct Tx<'a, D: Domain> {
    pub(crate) committed: &'a D::State,
    pub(crate) staged: Option<D::State>,
    pub(crate) rules: &'a D::Rules,
    pub(crate) rules_version: &'a str,
    pub(crate) stream: &'a Cover,
    /// The head: the sequence decide sees.
    pub(crate) head: u64,
    pub(crate) recorded_at: Timestamp,
    pub(crate) command_id: &'a Uuid,
    pub(crate) correlation_id: &'a Uuid,
    pub(crate) causation_id: Option<&'a EventRef>,
    /// Taken by [`Tx::conflicts`].
    pub(crate) conflicts: Option<Conflicts<'a, D::Event>>,
    /// The nonce decide gave back, if it did.
    pub(crate) resolved: Option<Nonce>,
    pub(crate) events: Vec<D::Event>,
    /// A fault decide's glue raised (a marked-up decision past its step
    /// limit): the kernel fails the command with it, whatever decide
    /// returned.
    pub(crate) fault: Option<super::error::KernelError>,
}

impl<'a, D: Domain> Tx<'a, D> {
    /// The state with every event emitted so far folded in.
    pub fn state(&self) -> &D::State {
        self.staged.as_ref().unwrap_or(self.committed)
    }

    /// The rules of the current version: what new events are decided under.
    pub fn rules(&self) -> &'a D::Rules {
        self.rules
    }

    /// The current rules' version, which the events are stamped with.
    pub fn rules_version(&self) -> &'a str {
        self.rules_version
    }

    /// The stream being decided.
    pub fn stream(&self) -> &'a Cover {
        self.stream
    }

    /// When the events will be recorded (non-decreasing within the stream).
    /// A decide copies it into a payload where the domain's own rules need
    /// business time (a draw's instant).
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// The command's id.
    pub fn command_id(&self) -> &'a Uuid {
        self.command_id
    }

    /// The command's correlation: never absent.
    pub fn correlation_id(&self) -> &'a Uuid {
        self.correlation_id
    }

    /// The command's own causation: the triggering event, for a policy's
    /// command. Not on the events it emits, whose causation is the command.
    pub fn causation_id(&self) -> Option<&'a EventRef> {
        self.causation_id
    }

    /// Under AGGREGATE_HANDLES, the window and the nonce, once; `None`
    /// under STRICT (and under COMMUTATIVE, where the kernel resolves).
    pub fn conflicts(&mut self) -> Option<Conflicts<'a, D::Event>> {
        self.conflicts.take()
    }

    /// Declare that decide examined `window` and applied its own conflict
    /// rule: angzarr's `resolve_conflicts(window, nonce)`. Without this
    /// call (with this delivery's nonce) a decide under AGGREGATE_HANDLES
    /// that returns `Ok` fails as stale, its events discarded.
    pub fn resolve_conflicts(&mut self, _window: &[Envelope<D::Event>], nonce: Nonce) {
        self.resolved = Some(nonce);
    }

    /// Stage an event and fold it into the staged state. Returns the
    /// sequence it will have if committed. The kernel stamps its meta.
    pub fn emit(&mut self, event: D::Event) -> u64 {
        let state = self.staged.get_or_insert_with(|| self.committed.clone());
        D::apply(state, &event, self.rules);
        self.events.push(event);
        self.next_sequence() - 1
    }

    /// The sequence the next emitted event will take.
    pub fn next_sequence(&self) -> u64 {
        self.head + self.events.len() as u64
    }

    /// Events emitted so far by this decide.
    pub fn emitted(&self) -> usize {
        self.events.len()
    }
}

/// What a decide (or an ingest) staged: events stamped with their
/// sequences and one shared `EventMeta`, ready to append to the store and
/// then commit to the stream it was decided against. Dropping it discards
/// them.
#[must_use = "a decision changes nothing until it is committed"]
pub struct Decision<D: Domain> {
    pub(crate) stream: Cover,
    /// The stream's head when decided: the store's expected sequence.
    pub(crate) head: u64,
    pub(crate) command_id: Uuid,
    pub(crate) events: Vec<Envelope<D::Event>>,
}

impl<D: Domain> Decision<D> {
    pub fn stream(&self) -> &Cover {
        &self.stream
    }

    /// The stream's head when it was decided: what an append expects.
    pub fn head(&self) -> u64 {
        self.head
    }

    pub fn command_id(&self) -> &Uuid {
        &self.command_id
    }

    pub fn events(&self) -> &[Envelope<D::Event>] {
        &self.events
    }

    /// Nothing to commit. Such a decision leaves nothing to deduplicate by,
    /// so the same command is decided again if it is redelivered (an
    /// accepted cost).
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
#[path = "domain.test.rs"]
mod tests;
