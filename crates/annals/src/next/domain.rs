//! Deciding and applying, with deciding made pure.

use annals_diagnostic::{Diagnostic, Report};

use super::id::{EventId, Seq, StreamKey};
use super::meta::CommandMeta;
use super::stream::Envelope;

/// One kind of event stream: its state, its commands, its events and its
/// rules for each. As the crate root's [`Domain`](crate::Domain), with
/// these changes:
///
/// - **No `Ctx`.** Decide is a function of its explicit inputs only: the
///   state, the command, the command's [`CommandMeta`] and the rules. The
///   command handler samples the clock and the actor before deciding and
///   passes them in the metadata ([`Tx::meta`]); random values are sampled
///   by the caller into the command, and a decide copies whatever it uses
///   into the event's payload, which is all `apply` sees.
/// - `State: Clone`, so a decide can fold its staged events into a copy;
/// - a decide may reject at any point, emitted or not: nothing it staged is
///   kept;
/// - everything `'static` and `Send`, so a command chain can stage streams
///   of several domains side by side and the async command handler's
///   future is `Send` (tokio's multi-threaded runtime, tonic);
/// - [`Domain::admit_fact`]: the integrity check a fact (an event a
///   reaction injects, see [`Reaction`](super::Reaction)) must pass.
pub trait Domain: Sized + 'static {
    /// Folded from the events. Never stored.
    type State: Default + Clone + Send + 'static;
    /// A request to change the stream. Decided, never stored.
    type Command: Send + 'static;
    /// A fact. The only thing a stream stores.
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

    /// Fold one event into the state. Pure: the same state, event and rules
    /// always give the same result. Never sees the event's metadata.
    fn apply(state: &mut Self::State, event: &Self::Event, rules: &Self::Rules);

    /// Whether this stream can take `fact`, an event that already happened
    /// upstream and is injected without a decide. Not a business judgement
    /// (a fact cannot be refused on business grounds) but integrity: the
    /// stream is retired, or not created and `fact` does not create it.
    /// The position check (gap, redelivery) is the kernel's and comes first.
    /// Takes every fact by default.
    fn admit_fact(state: &Self::State, fact: &Self::Event) -> Result<(), Report> {
        Ok(())
    }
}

/// A decide in progress. Reads go through it; emitted events are staged in
/// it and folded into a copy of the state, made at the first emit, so the
/// decide sees their effects and the stream sees nothing.
pub struct Tx<'a, D: Domain> {
    committed: &'a D::State,
    staged: Option<D::State>,
    rules: &'a D::Rules,
    meta: &'a CommandMeta,
    stream: StreamKey,
    base: Seq,
    events: Vec<(D::Event, Option<EventId>)>,
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

    /// The command's metadata, read-only: the instant and the actor the
    /// command handler sampled. A decide copies what the domain's own rules
    /// need into the payload (a draw's time, who accepted it); `apply` sees
    /// only what the payload holds.
    pub fn meta(&self) -> &CommandMeta {
        self.meta
    }

    /// The stream being decided.
    pub fn stream(&self) -> &StreamKey {
        &self.stream
    }

    /// Stage an event, caused by whatever caused the command, and fold it
    /// into the staged state. Returns the seq it will have if committed.
    pub fn emit(&mut self, event: D::Event) -> Seq {
        todo!("proposal: clone the state at the first emit, apply, stage with the command's cause")
    }

    /// As [`Tx::emit`], caused by `cause`: an earlier event of this stream
    /// (one already committed, or one staged by this decide) or an event of
    /// another stream.
    pub fn emit_caused(&mut self, event: D::Event, cause: EventId) -> Seq {
        todo!("proposal: as emit, with an explicit cause")
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

/// What a decide staged: events stamped with their metadata and seqs, ready
/// to append to the store and then commit to the stream it was decided
/// against. Dropping it discards them.
#[must_use = "a decision changes nothing until it is committed"]
pub struct Decision<D: Domain> {
    pub(crate) stream: StreamKey,
    /// The stream's length the decide read: the store's expected length.
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
