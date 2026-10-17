//! Reactions to committed events: policies and projections, over one base.
//!
//! Every kind takes the same input, a [`Committed`] event (stream identity,
//! position, metadata, payload, the state just after it), under the same
//! [`Position`] contract (next, redelivery, gap), and runs through the same
//! dispatch: each becomes a [`Handler`] of its source.
//!
//! - [`Policy`] (Event Storming's term; an *automation* in Event
//!   Modeling): event → **commands** to target streams. A command goes
//!   through the target's decide and may be rejected. **A reaction's only
//!   output is commands**: there are no reaction-injected facts (facts come
//!   from outside the system only, angzarr's decision), so the crate's
//!   earlier `Translation` is gone. Propagating one aggregate's changes into
//!   another (hatpick's list into its draw systems) is a policy whose
//!   target records the change in its own events.
//! - [`Projection`]: event → a read model's fold. It produces nothing.
//!
//! A policy names which targets a source event is owed to, which root each
//! target is, and how far each target has got. A target's stream key is
//! the target domain's, in **the source's edition**: a reaction never
//! crosses timelines.
//!
//! **The contiguity invariant.** Once a target appears in
//! [`Reaction::targets`], it stays there for every source event until the
//! one that retires it (inclusive), and never appears again. A target
//! missing from one event and back for the next would never be delivered
//! the missing one, and every later delivery would be a gap: it wedges.
//! [`orders_owed`] checks it, as [`KernelError::TargetReturned`].

use std::collections::BTreeMap;
use std::marker::PhantomData;

use super::book::{BookOf, Sampled};
use super::domain::Domain;
use super::error::{Failure, KernelError};
use super::handler::{Committed, Handler, Position};
use super::id::{Root, StreamKey};
use super::stream::{Envelope, Stream};
use super::version::Rulebook;

// ---------------------------------------------------------------- the base

/// What a policy is over: a source, a target domain, and the targets each
/// source event is owed to. Stateless: live targets are a fold of the
/// source, and each target's position a fold of the target.
pub trait Reaction {
    type Source: Domain;
    type Target: Domain;
    /// Names one target.
    type TargetId: Ord + Clone;

    /// The reaction's name, stable across releases: the `component` of
    /// every [`Causation`](super::Causation) it issues.
    const NAME: &'static str;

    /// The targets owed the event that led to `source`, the source's state
    /// just after it, including a target that event retires. Must keep the
    /// contiguity invariant (see the module docs).
    fn targets(source: &<Self::Source as Domain>::State) -> Vec<Self::TargetId>;

    /// The root of `target`'s stream. Its key is
    /// `(Target::NAME, the source's edition, root)`: see [`target_key`].
    fn root(target: &Self::TargetId) -> Root;

    /// The last source seq `target` holds, read off its fold. The source seq
    /// travels in the command's payload for this, since `apply` never sees
    /// metadata. [`Position::START`] means the target has not been created:
    /// there is nothing to place against, so every delivery goes to its
    /// decide, which creates it or refuses.
    fn position(target: &<Self::Target as Domain>::State) -> Position;
}

/// Event → commands. Each command is decided by its target and may be
/// rejected; in the command handler a rejection fails the whole chain, the
/// originating command included, as [`Failure::Downstream`] with
/// [`StepError::Rejected`](super::StepError).
///
/// Its commands carry no handling (the target is stamped at its head), and
/// their provenance is the trigger's: the kernel stamps the source event's
/// actor and correlation, and the origin [`Causation`](super::Causation)
/// `{ event, component: NAME, index }`.
pub trait Policy: Reaction {
    /// The command `target` is owed for `committed`. For the event that
    /// first makes `target` a target, whatever creates it.
    fn command(
        target: &Self::TargetId,
        committed: &Committed<'_, Self::Source>,
    ) -> <Self::Target as Domain>::Command;
}

/// Event → read model. Produces nothing. Read models are replayed from the
/// stream on every read for now ([`Stream::dispatch`] from 0); a stored
/// read model would keep a [`Position`] per stream (see
/// [`Tracked`](super::Tracked)).
pub trait Projection<D: Domain> {
    fn project(&mut self, committed: &Committed<'_, D>);
}

// ---------------------------------------------------------------- dispatch

/// One command a policy owes one target: a book with a causation origin,
/// the trigger's provenance and no handling.
pub struct Order<P: Policy> {
    pub target: P::TargetId,
    pub book: BookOf<P::Target>,
}

/// Everything owed, by target, each target's in source order.
pub type Backlog<P> = BTreeMap<<P as Reaction>::TargetId, Vec<Order<P>>>;

/// A policy as a [`Handler`] of its source: each committed event becomes
/// one [`Order`] per target, handed to `sink` in target order.
pub fn policy<P: Policy, F: FnMut(Order<P>)>(sink: F) -> Reactor<P, F> {
    Reactor {
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

/// See [`projection`].
pub struct Projector<'p, P>(&'p mut P);

impl<P: Policy, F: FnMut(Order<P>)> Handler<P::Source> for Reactor<P, F> {
    fn handle(&mut self, committed: &Committed<'_, P::Source>) {
        todo!("proposal: owe::<P>(committed) into the sink")
    }
}

impl<D: Domain, P: Projection<D>> Handler<D> for Projector<'_, P> {
    fn handle(&mut self, committed: &Committed<'_, D>) {
        self.0.project(committed)
    }
}

/// The stream `target` is, for a source event in `source`: the target
/// domain's, in the source's edition.
pub fn target_key<R: Reaction>(source: &StreamKey, target: &R::TargetId) -> StreamKey {
    todo!("proposal: StreamKey::of::<R::Target>(source.edition.clone(), R::root(target))")
}

/// The one mechanism under the reactor, the chain and the backlog: what `P`
/// owes for `committed`, one order per target, the `index`th target's
/// causation index `index`.
fn owe<P: Policy>(committed: &Committed<'_, P::Source>) -> Vec<Order<P>> {
    todo!("proposal: targets(state), each a book: target_key, command, causation, trigger's provenance")
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

/// Deliver a policy's order to `target`, in memory, outside a chain (a
/// catch-up). The target's [`Reaction::position`] places the source event
/// first: a redelivery runs nothing and a gap is a [`KernelError::Gap`].
/// Only the next one is decided ([`Stream::decide`], so it may be
/// [`Failure::Rejected`]) and committed.
pub fn deliver<'a, P: Policy>(
    target: &'a mut Stream<P::Target>,
    order: &Order<P>,
    sampled: &Sampled,
    rules: &Rulebook<<P::Target as Domain>::Rules>,
    handlers: &mut [&mut dyn Handler<P::Target>],
) -> OrderDelivery<'a, P> {
    todo!("proposal: place, decide, commit")
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
) -> Result<Backlog<P>, KernelError> {
    todo!(
        "proposal: one dispatch, one fold, owe each event, keep what each position has not passed"
    )
}
