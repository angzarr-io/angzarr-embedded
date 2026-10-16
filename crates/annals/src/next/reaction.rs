//! Reactions to committed events, three kinds over one base.
//!
//! Every kind takes the same input, a [`Committed`] event (stream identity,
//! position, metadata, payload, the state just after it), under the same
//! [`Position`] contract (next, redelivery, gap), and runs through the same
//! dispatch: each becomes a [`Handler`] of its source. On top sit three
//! typed traits:
//!
//! - [`Policy`] (Event Storming's term; an *automation* in Event
//!   Modeling): event → **commands** to target streams. A command goes
//!   through the target's decide and may be rejected.
//! - [`Translation`] (Event Modeling; Fowler's *event-carried state
//!   transfer*): event → **facts** injected into target streams. A fact
//!   already happened upstream; the target records it without a decide and
//!   may refuse it only for integrity: a position gap, or
//!   [`Domain::admit_fact`] (retired, never created).
//! - [`Projection`]: event → a read model's fold. It produces nothing.
//!
//! Policies and translations share [`Reaction`]: which targets a source
//! event is owed to, which stream each target is, and how far each target
//! has got.
//!
//! **The contiguity invariant.** Once a target appears in
//! [`Reaction::targets`], it stays there for every source event until the
//! one that retires it (inclusive), and never appears again. A target
//! missing from one event and back for the next would never be delivered
//! the missing one, and every later delivery would be a gap: it wedges.
//! [`orders_owed`] and [`facts_owed`] check it, as
//! [`KernelError::TargetReturned`].

use std::collections::BTreeMap;
use std::marker::PhantomData;

use super::domain::Domain;
use super::error::{Failure, KernelError};
use super::handler::{Committed, Handler, Position};
use super::id::{EventId, StreamId};
use super::meta::CommandMeta;
use super::stream::{Envelope, Stream};
use super::version::Rulebook;

// ---------------------------------------------------------------- the base

/// What a policy and a translation share: a source, a target domain, and
/// the targets each source event is owed to. Stateless: live targets are a
/// fold of the source, and each target's position a fold of the target.
pub trait Reaction {
    type Source: Domain;
    type Target: Domain;
    /// Names one target.
    type TargetId: Ord + Clone;

    /// The reaction's name, stable across releases: part of every
    /// [`CommandId::Reaction`](super::CommandId::Reaction) it issues.
    const NAME: &'static str;

    /// The targets owed the event that led to `source`, the source's state
    /// just after it, including a target that event retires. Must keep the
    /// contiguity invariant (see the module docs).
    fn targets(source: &<Self::Source as Domain>::State) -> Vec<Self::TargetId>;

    /// The stream `target` is.
    fn stream(target: &Self::TargetId) -> StreamId;

    /// The last source seq `target` holds, read off its fold. The source seq
    /// travels in the delivered payload for this, since `apply` never sees
    /// metadata. [`Position::START`] means the target has not been created:
    /// there is nothing to place against, so every delivery goes to its
    /// decide (or [`Domain::admit_fact`]), which creates it or refuses.
    fn position(target: &<Self::Target as Domain>::State) -> Position;
}

/// Event → commands. Each command is decided by its target and may be
/// rejected; in the command handler a rejection fails the whole chain, as
/// [`Failure::Downstream`] with [`StepError::Rejected`](super::StepError).
pub trait Policy: Reaction {
    /// The command `target` is owed for `committed`. For the event that
    /// first makes `target` a target, whatever creates it.
    fn command(
        target: &Self::TargetId,
        committed: &Committed<'_, Self::Source>,
    ) -> <Self::Target as Domain>::Command;

    /// Compensation: the commands to issue to the source when `order` was
    /// rejected. Never called in process, where a chain is all or nothing;
    /// it is for a wrapping runtime that delivers across a process boundary
    /// and must undo instead of roll back. None by default.
    fn on_rejected(
        order: &Order<Self>,
        rejection: &annals_diagnostic::Report,
    ) -> Vec<<Self::Source as Domain>::Command> {
        Vec::new()
    }
}

/// Event → facts. Each fact is recorded in its target as it is, refused
/// only for integrity, never on business grounds.
pub trait Translation: Reaction {
    /// The fact `target` is owed for `committed`. For the event that first
    /// makes `target` a target, the fact that creates it.
    fn fact(
        target: &Self::TargetId,
        committed: &Committed<'_, Self::Source>,
    ) -> <Self::Target as Domain>::Event;
}

/// Event → read model. Produces nothing. Read models are replayed from the
/// stream on every read for now ([`Stream::dispatch`] from 0); a stored
/// read model would keep a [`Position`] per stream (see
/// [`Tracked`](super::Tracked)).
pub trait Projection<D: Domain> {
    fn project(&mut self, committed: &Committed<'_, D>);
}

// ---------------------------------------------------------------- dispatch

/// One delivery owed to one target, caused by one source event and carrying
/// its correlation: a command (`O` = the target's command) from a policy,
/// or a fact (`O` = the target's event) from a translation.
pub struct Owed<R: Reaction + ?Sized, O> {
    pub target: R::TargetId,
    /// The source event: [`EventId`] `{ source stream, seq }`.
    pub source: EventId,
    /// [`CommandMeta::reacting_to`] the source event.
    pub meta: CommandMeta,
    pub output: O,
}

/// Everything owed, by target, each target's in source order.
pub type Backlog<R, O> = BTreeMap<<R as Reaction>::TargetId, Vec<Owed<R, O>>>;

/// A policy's command for one target.
pub type Order<P> = Owed<P, <<P as Reaction>::Target as Domain>::Command>;

/// A translation's fact for one target.
pub type Fact<T> = Owed<T, <<T as Reaction>::Target as Domain>::Event>;

/// A policy as a [`Handler`] of its source: each committed event becomes
/// one [`Order`] per target, handed to `sink` in target order.
pub fn policy<P: Policy, F: FnMut(Order<P>)>(sink: F) -> Reactor<P, F> {
    Reactor {
        sink,
        reaction: PhantomData,
    }
}

/// A translation as a [`Handler`] of its source: each committed event
/// becomes one [`Fact`] per target, handed to `sink` in target order.
pub fn translation<T: Translation, F: FnMut(Fact<T>)>(sink: F) -> Translator<T, F> {
    Translator {
        sink,
        reaction: PhantomData,
    }
}

/// A projection as a [`Handler`].
pub fn projection<P>(projection: &mut P) -> Projector<'_, P> {
    Projector(projection)
}

/// See [`policy`].
pub struct Reactor<P, F> {
    sink: F,
    reaction: PhantomData<fn() -> P>,
}

/// See [`translation`].
pub struct Translator<T, F> {
    sink: F,
    reaction: PhantomData<fn() -> T>,
}

/// See [`projection`].
pub struct Projector<'p, P>(&'p mut P);

impl<P: Policy, F: FnMut(Order<P>)> Handler<P::Source> for Reactor<P, F> {
    fn handle(&mut self, committed: &Committed<'_, P::Source>) {
        todo!("proposal: owe::<P>(committed, P::command) into the sink")
    }
}

impl<T: Translation, F: FnMut(Fact<T>)> Handler<T::Source> for Translator<T, F> {
    fn handle(&mut self, committed: &Committed<'_, T::Source>) {
        todo!("proposal: owe::<T>(committed, T::fact) into the sink")
    }
}

impl<D: Domain, P: Projection<D>> Handler<D> for Projector<'_, P> {
    fn handle(&mut self, committed: &Committed<'_, D>) {
        self.0.project(committed)
    }
}

/// The one mechanism under both reactors and both backlogs: what `R` owes
/// for `committed`, one per target, `output` making each.
fn owe<R: Reaction, O>(
    committed: &Committed<'_, R::Source>,
    output: impl Fn(&R::TargetId, &Committed<'_, R::Source>) -> O,
) -> Vec<Owed<R, O>> {
    todo!("proposal: targets(state), each with meta reacting_to committed.id()")
}

// ---------------------------------------------------------------- delivery

/// What a delivery did.
#[derive(Debug, PartialEq)]
pub enum Delivered<'a, E> {
    /// The source event was the target's next: these are the events the
    /// target committed.
    Next(&'a [Envelope<E>]),
    /// The target already held the source event. Nothing ran.
    Redelivery,
}

/// What [`deliver`] returns.
pub type OrderDelivery<'a, P> = Result<
    Delivered<'a, <<P as Reaction>::Target as Domain>::Event>,
    Failure<<<P as Reaction>::Target as Domain>::Rejection>,
>;

/// What [`inject`] returns. No rejection type: a fact is not rejectable.
pub type FactDelivery<'a, T> =
    Result<Delivered<'a, <<T as Reaction>::Target as Domain>::Event>, KernelError>;

/// Deliver a policy's order to `target`, in memory. The target's
/// [`Reaction::position`] places the source event first: a redelivery runs
/// nothing and a gap is a [`KernelError::Gap`]. Only the next one is
/// decided ([`Stream::decide`], so it may be [`Failure::Rejected`]) and
/// committed.
pub fn deliver<'a, P: Policy>(
    target: &'a mut Stream<P::Target>,
    order: &Order<P>,
    rules: &Rulebook<<P::Target as Domain>::Rules>,
    handlers: &mut [&mut dyn Handler<P::Target>],
) -> OrderDelivery<'a, P> {
    todo!("proposal: place, decide, commit")
}

/// Inject a translation's fact into `target`, in memory: placed as
/// [`deliver`] places an order, then admitted ([`Stream::admit`], refused
/// only as [`KernelError::FactRefused`]) and committed.
pub fn inject<'a, T: Translation>(
    target: &'a mut Stream<T::Target>,
    fact: &Fact<T>,
    rules: &Rulebook<<T::Target as Domain>::Rules>,
    handlers: &mut [&mut dyn Handler<T::Target>],
) -> FactDelivery<'a, T> {
    todo!("proposal: place, admit, commit")
}

/// Every order a policy owes every target, from each target's position (a
/// target absent from `positions` is owed everything from where it first
/// appears), in one fold of the source, grouped by target, each group in
/// source order. Refuses a source whose targets break the contiguity
/// invariant.
pub fn orders_owed<P: Policy>(
    source: &Stream<P::Source>,
    rules: &Rulebook<<P::Source as Domain>::Rules>,
    positions: &BTreeMap<P::TargetId, Position>,
) -> Result<Backlog<P, <P::Target as Domain>::Command>, KernelError> {
    todo!("proposal: backlog::<P>(.., P::command)")
}

/// [`orders_owed`], for a translation's facts.
pub fn facts_owed<T: Translation>(
    source: &Stream<T::Source>,
    rules: &Rulebook<<T::Source as Domain>::Rules>,
    positions: &BTreeMap<T::TargetId, Position>,
) -> Result<Backlog<T, <T::Target as Domain>::Event>, KernelError> {
    todo!("proposal: backlog::<T>(.., T::fact)")
}

/// The one backlog under both: dispatch the source from 0, [`owe`] each
/// event, keep what each target's position has not yet passed, check
/// contiguity.
fn backlog<R: Reaction, O>(
    source: &Stream<R::Source>,
    rules: &Rulebook<<R::Source as Domain>::Rules>,
    positions: &BTreeMap<R::TargetId, Position>,
    output: impl Fn(&R::TargetId, &Committed<'_, R::Source>) -> O,
) -> Result<Backlog<R, O>, KernelError> {
    todo!("proposal: one dispatch, one fold")
}
