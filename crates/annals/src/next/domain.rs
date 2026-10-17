//! Deciding, ingesting and applying, with deciding made pure.

use annals_diagnostic::Diagnostic;

use super::book::ConflictResolution;
use super::conflict::Changed;
use super::id::{Actor, Correlation, Seq, StreamKey, Timestamp};
use super::meta::Origin;
use super::stream::Envelope;

/// One kind of event stream: its state, its commands and facts, its events
/// and its rules for each. As the crate root's [`Domain`](crate::Domain),
/// with these changes:
///
/// - **No `Ctx`.** Decide is a function of its explicit inputs only: the
///   state, the command and the rules, plus what the kernel stamps (the
///   recorded instant, the actor, the origin), readable through [`Tx`].
///   Random values are sampled by the caller into the command; a decide
///   copies whatever it uses into the event's payload, which is all `apply`
///   sees.
/// - **Two inputs, two pipelines**, picked by the book's body arm
///   ([`Body`](super::Body)): a command goes to the fallible
///   [`Domain::decide`]; a fact to the infallible [`Domain::ingest`], which
///   has no error to return.
/// - **Business output is payloads only.** decide emits events; the kernel
///   builds the pages: their seqs and their [`Meta`](super::Meta).
/// - `State: Clone`, so a decide can fold its staged events into a copy;
/// - a decide may reject at any point, emitted or not: nothing it staged is
///   kept;
/// - everything `'static` and `Send`, so a chain can stage streams of
///   several domains side by side and the async command handler's future is
///   `Send` (tokio's multi-threaded runtime, tonic).
pub trait Domain: Sized + 'static {
    /// Folded from the events. Never stored, except as a snapshot.
    type State: Default + Clone + Send + 'static;
    /// A request to change the stream. Decided, never stored.
    type Command: Send + 'static;
    /// Something that already happened outside the system (a webhook, an
    /// integration). Ingested, never refused, never stored as such.
    /// [`std::convert::Infallible`] for a domain that takes none.
    type Fact: Send + 'static;
    /// What a stream stores.
    type Event: Clone + Send + Sync + 'static;
    /// Why a command was refused, for the person who gave it. Kernel faults
    /// are never folded into it: see [`Failure`](super::Failure).
    type Rejection: Diagnostic + Send + 'static;
    /// Immutable reference data both halves read, one value per
    /// [`RulesVersion`](super::RulesVersion), found through a
    /// [`Rulebook`](super::Rulebook).
    type Rules: Send + Sync + 'static;

    /// The domain's name: the domain of every stream key of this domain
    /// ([`StreamKey::domain`]). Stable across releases.
    const NAME: &'static str;

    /// Decide a command against the current state. Pure: its only output is
    /// the events it emits through `tx`, and they are kept only if it
    /// returns `Ok`.
    fn decide(cmd: &Self::Command, tx: &mut Tx<'_, Self>) -> Result<(), Self::Rejection>;

    /// Record a fact: the events it becomes, given the state. **Infallible
    /// by type**: a fact already happened, so an awkward one is recorded
    /// (a payment for an unknown order becomes an event that says so),
    /// never refused. A panic here is a bug; in process it aborts the whole
    /// chain, and nothing is written.
    fn ingest(state: &Self::State, fact: &Self::Fact, rules: &Self::Rules) -> Vec<Self::Event>;

    /// Fold one event into the state. Pure: the same state, event and rules
    /// always give the same result. Never sees the event's metadata.
    fn apply(state: &mut Self::State, event: &Self::Event, rules: &Self::Rules);

    /// The conflict resolution for `cmd` when its book does not choose one:
    /// the domain's configuration, per command type. STRICT by default.
    fn conflict_resolution(cmd: &Self::Command) -> ConflictResolution {
        ConflictResolution::Strict
    }

    /// The state's generated conflict-unit diff, when the state is a
    /// `commutative_state` message: `commutative::<Self::State>()`
    /// ([`commutative`](super::commutative)), naming the generated
    /// [`CommutativeState`](super::CommutativeState) impl. `None`, the
    /// default: COMMUTATIVE is refused for this domain, before decide
    /// ([`Malformed::CommutativeUnsupported`](super::Malformed::CommutativeUnsupported)),
    /// as angzarr refuses it for a state without `commutative_state`.
    ///
    /// Under COMMUTATIVE the kernel follows angzarr's resolver
    /// (`options.proto`, *The resolver*): fold to `expected_sequence` and to
    /// the head; `window = changed(expected, head)`; decide runs on the head,
    /// so its invariants see the window's effects; `mine = changed(head,
    /// head with the command's events)`; keep the events iff the two
    /// [do not overlap](super::Units::overlaps), else the command is stale.
    const COMMUTATIVE: Option<Changed<Self::State>> = None;
}

/// The in-process conflict nonce (angzarr's `ContextualCommand.conflict_nonce`):
/// minted by the kernel per delivery under AGGREGATE_HANDLES, handed to
/// decide with the window, and given back through
/// [`Tx::resolve_conflicts`] as decide's declaration that it examined the
/// window. Not constructible, clonable or copyable outside the kernel, so
/// a decide cannot satisfy the check without having received it.
#[derive(PartialEq, Eq, Debug)]
pub struct Nonce(pub(crate) u64);

/// What decide is handed under AGGREGATE_HANDLES: the window it did not see
/// (the committed events from the book's `expected_sequence` to the head)
/// and the nonce. An empty window is delivered too, so the path is uniform.
#[derive(Debug)]
pub struct Conflicts<'a, E> {
    pub window: &'a [Envelope<E>],
    pub nonce: Nonce,
}

/// A decide in progress. Reads go through it; emitted events are staged in
/// it and folded into a copy of the state, made at the first emit, so the
/// decide sees their effects and the stream sees nothing.
pub struct Tx<'a, D: Domain> {
    committed: &'a D::State,
    staged: Option<D::State>,
    rules: &'a D::Rules,
    stream: &'a StreamKey,
    /// The head: the length decide sees, and every event's base_sequence.
    base: Seq,
    recorded_at: Timestamp,
    actor: Option<&'a Actor>,
    correlation: &'a Correlation,
    origin: &'a Origin,
    /// Taken by [`Tx::conflicts`].
    conflicts: Option<Conflicts<'a, D::Event>>,
    /// The nonce decide gave back, if it did.
    resolved: Option<Nonce>,
    events: Vec<D::Event>,
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

    /// The stream being decided.
    pub fn stream(&self) -> &'a StreamKey {
        self.stream
    }

    /// When the events will be recorded (non-decreasing within the stream).
    /// A decide copies it into a payload where the domain's own rules need
    /// business time (a draw's instant).
    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// Who gave the command (the trigger's actor, for a policy's).
    pub fn actor(&self) -> Option<&'a Actor> {
        self.actor
    }

    /// The command's correlation, resolved: never empty.
    pub fn correlation(&self) -> &'a Correlation {
        self.correlation
    }

    /// Why the command exists.
    pub fn origin(&self) -> &'a Origin {
        self.origin
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
    pub fn resolve_conflicts(&mut self, window: &[Envelope<D::Event>], nonce: Nonce) {
        todo!("proposal: keep the nonce; the kernel compares it after decide returns")
    }

    /// Stage an event and fold it into the staged state. Returns the seq it
    /// will have if committed. The kernel stamps its meta.
    pub fn emit(&mut self, event: D::Event) -> Seq {
        todo!("proposal: clone the state at the first emit, apply, stage")
    }

    /// The seq the next emitted event will take.
    pub fn next_seq(&self) -> Seq {
        self.base + self.events.len() as Seq
    }

    /// Events emitted so far by this decide.
    pub fn emitted(&self) -> usize {
        self.events.len()
    }
}

/// What a decide (or an ingest) staged: events stamped with their seqs and
/// one shared meta, ready to append to the store and then commit to the
/// stream it was decided against. Dropping it discards them.
#[must_use = "a decision changes nothing until it is committed"]
pub struct Decision<D: Domain> {
    pub(crate) stream: StreamKey,
    /// The stream's length the decide read: the store's expected length,
    /// and every event's base_sequence.
    pub(crate) base: Seq,
    pub(crate) events: Vec<Envelope<D::Event>>,
}

impl<D: Domain> Decision<D> {
    pub fn stream(&self) -> &StreamKey {
        &self.stream
    }

    /// The stream's length when it was decided: what an append expects.
    pub fn base(&self) -> Seq {
        self.base
    }

    pub fn events(&self) -> &[Envelope<D::Event>] {
        &self.events
    }

    /// Nothing to commit: a redelivery, or a command with nothing to do.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}
