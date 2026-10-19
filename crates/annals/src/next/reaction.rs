//! Reactions to committed events: policies and projections, over one base.
//!
//! Every kind takes the same input, a [`Committed`] event (the stream, the
//! sequence, the metadata, the payload, the state just after it), under the
//! same [`Position`] contract (next, redelivery, gap), and runs through the
//! same dispatch: each becomes a [`Handler`] of its source.
//!
//! - [`Policy`] (Event Storming's term; an *automation* in Event
//!   Modeling): event → **commands** to target streams. A command goes
//!   through the target's five steps and may be rejected. **A reaction's
//!   only output is commands**: facts come from outside the system only, so
//!   there is no reaction-injected fact (and no `Translation`).
//!   Propagating one aggregate's changes into another (hatpick's list into
//!   its draw systems) is a policy whose target records the change in its
//!   own events.
//! - [`Projection`]: event → a read model's fold. It produces nothing. Read
//!   models are replayed from the stream on every read.
//!
//! A policy's command is a `CommandBook` the kernel builds: the target's
//! cover, a `Command` page whose payload is the policy's command packed as
//! `Any`, and **no ids and no handling**. The kernel checks it like any
//! book, then identifies it from its trigger: command id
//! [`ids::reaction_command_id`](super::ids::reaction_command_id)(trigger,
//! [`Reaction::NAME`], index), the trigger's correlation, and the trigger as
//! its `causation_id`. Nothing outside the kernel can build one. The
//! command's events name the command as their causation
//! (`EventMeta.causation.command_id`), never the trigger: the trigger edge
//! is the command's, for tracing.
//!
//! A target's cover is the target domain's, in **the source's edition**: a
//! reaction never crosses timelines.
//!
//! **Targeting from the event.** [`Reaction::targets`] receives the whole
//! committed event (the event and the source's state just after it), so a
//! policy may forward **only some events** to a target: hatpick's list
//! forwards content changes to its draw systems and keeps its invites to
//! itself. A target's owed orders are the source events it was targeted
//! for, in source order.
//!
//! **Contiguity is per target.** Each [`Order`] names the source sequence
//! of the order owed to the same target just before it
//! ([`Order::previous`]), so a delivery outside a chain is placed against
//! the target's own [`Position`] (the last source sequence it holds, a fold
//! of its own stream): at or before it, a redelivery; after it while the
//! previous owed order is not yet held, a [`Gap`](super::Gap) (deliver that
//! one first); otherwise the next. Sequences a target was never owed are no
//! gap. Inside a chain every order is delivered with its trigger, so
//! neither can happen there.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use angzarr_proto::v1::{command_page, Command, CommandBook, CommandPage, Cover, EventRef, Uuid};

use super::check::{check_reaction, Checked, Malformed};
use super::dead_letter::{write_dead_letter, DeadLetterLog};
use super::domain::Domain;
use super::error::KernelError;
use super::handler::{Committed, Handler, Position};
use super::payload::Payload;
use super::registry::Registry;
use super::stream::Stream;

// ---------------------------------------------------------------- the base

/// What a policy is over: a source, a target domain, and the targets each
/// source event is owed to. Stateless: live targets are a fold of the
/// source, and each target's position a fold of the target.
pub trait Reaction {
    type Source: Domain;
    type Target: Domain;
    /// Names one target.
    type TargetId: Ord + Clone;

    /// The reaction's name, stable across releases and never empty: the
    /// `component` its commands' ids are derived from. Changing it changes
    /// every derived command id.
    ///
    /// The ids name nothing else of the reaction: not a rules version, not
    /// its code. A re-run over a trigger it already handled (catch-up after
    /// a reset, redelivery) re-derives the same ids and deduplicates, even
    /// if the policy now decides differently. Repairs are new commands with
    /// new ids, appended after what the old code wrote; history is never
    /// rewritten (angzarr decision 13.4). Inside a chain a reaction runs
    /// exactly once per trigger, atomically with it.
    const NAME: &'static str;

    /// The targets owed `committed` (the event, with the source's state just
    /// after it): every target it must reach, a target the event retires
    /// included. May depend on the event, so a policy can forward only some
    /// events (see the module docs).
    fn targets(committed: &Committed<'_, Self::Source>) -> Vec<Self::TargetId>;

    /// The root of `target`'s stream: a stored UUID, or
    /// [`ids::compute_root`](super::ids::compute_root) of a business key.
    /// Its cover is `(Target::NAME, the source's edition, root)`: see
    /// [`target_cover`].
    fn root(target: &Self::TargetId) -> Uuid;

    /// The last source sequence `target` holds, read off its fold. The
    /// source sequence travels in the command's payload for this, since
    /// `apply` never sees metadata. [`Position::START`] means the target has
    /// not been created: every delivery goes to its decide, which creates
    /// it or refuses.
    fn position(target: &<Self::Target as Domain>::State) -> Position;
}

/// Event → commands. Each command is decided by its target and may be
/// rejected; in the command handler a rejection fails the whole chain, the
/// originating command included, as [`Failure::Downstream`](super::Failure::Downstream)
/// with [`StepError::Rejected`](super::StepError::Rejected).
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

/// One command a policy owes one target, before the kernel books it.
/// Built only by the kernel, from a committed event.
pub struct Order<P: Policy> {
    pub(crate) target: P::TargetId,
    pub(crate) cover: Cover,
    pub(crate) command: <P::Target as Domain>::Command,
    pub(crate) trigger: EventRef,
    pub(crate) index: u32,
    pub(crate) correlation_id: Uuid,
    /// The source sequence of the order owed to the same target just before
    /// this one, if known: `None` for the first one, and inside a chain.
    pub(crate) previous: Option<u64>,
}

impl<P: Policy> Order<P> {
    pub fn target(&self) -> &P::TargetId {
        &self.target
    }

    /// The target's stream.
    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    pub fn command(&self) -> &<P::Target as Domain>::Command {
        &self.command
    }

    /// The source event: the command's causation (not its events').
    pub fn trigger(&self) -> &EventRef {
        &self.trigger
    }

    /// The command's index among those the trigger set off for this
    /// policy, in target order.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// The trigger's correlation, which the command keeps.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// The source sequence of the order owed to this target just before
    /// this one ([`orders_owed`] sets it; `None` for a target's first order
    /// and inside a chain): what a delivery must find the target holding.
    pub fn previous(&self) -> Option<u64> {
        self.previous
    }

    /// The order as a book, checked through the kernel's reaction entry:
    /// the target's cover, the command packed as `Any`, no ids, no
    /// handling. Malformed only by a bug (a policy or kernel one).
    pub(crate) fn check(&self, registry: &Registry) -> Result<Checked, Malformed> {
        check_reaction(
            self.book(),
            registry,
            self.trigger.clone(),
            P::NAME,
            self.index,
            self.correlation_id.clone(),
        )
    }

    /// [`Order::check`], writing a dead letter for a malformed book: only a
    /// bug makes one.
    pub(crate) fn check_logged(
        &self,
        registry: &Registry,
        dead_letters: &dyn DeadLetterLog,
    ) -> Result<Checked, Malformed> {
        self.check(registry).inspect_err(|malformed| {
            write_dead_letter(dead_letters, self.book(), P::NAME, &malformed.to_string())
        })
    }

    /// The order as the kernel books it: the target's cover, the command
    /// packed as `Any`, no ids, no handling.
    fn book(&self) -> CommandBook {
        CommandBook {
            cover: Some(self.cover.clone()),
            page: Some(CommandPage {
                body: Some(command_page::Body::Command(Command {
                    payload: Some(self.command.to_any()),
                })),
                ..CommandPage::default()
            }),
            handling: None,
        }
    }
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
        for order in owe::<P>(committed) {
            (self.sink)(order);
        }
    }
}

impl<D: Domain, P: Projection<D>> Handler<D> for Projector<'_, P> {
    fn handle(&mut self, committed: &Committed<'_, D>) {
        self.0.project(committed)
    }
}

/// The stream `target` is, for a source event in `source`: the target
/// domain's, in the source's edition.
pub fn target_cover<R: Reaction>(source: &Cover, target: &R::TargetId) -> Cover {
    Cover {
        domain: <R::Target as Domain>::NAME.to_string(),
        root: Some(R::root(target)),
        edition: source.edition.clone(),
    }
}

/// The one mechanism under the reactor, the chain and the backlog: what `P`
/// owes for `committed`, one order per target, the `index`th target's
/// order index `index`.
fn owe<P: Policy>(committed: &Committed<'_, P::Source>) -> Vec<Order<P>> {
    (0..)
        .zip(P::targets(committed))
        .map(|(index, target)| Order {
            cover: target_cover::<P>(committed.cover, &target),
            command: P::command(&target, committed),
            trigger: committed.event_ref(),
            index,
            correlation_id: committed.envelope.correlation_id().clone(),
            previous: None,
            target,
        })
        .collect()
}

/// Every order a policy owes every target, from each target's position (a
/// target absent from `positions` is owed everything it was targeted for),
/// in one fold of the source, grouped by target, each group in source
/// order, each order naming the one owed to its target before it
/// ([`Order::previous`]), whether that one is still owed or already held.
pub fn orders_owed<P: Policy>(
    source: &Stream<P::Source>,
    registry: &Registry,
    positions: &BTreeMap<P::TargetId, Position>,
) -> Result<Backlog<P>, KernelError> {
    let mut backlog = Backlog::<P>::new();
    let mut last: BTreeMap<P::TargetId, u64> = BTreeMap::new();
    let mut owing = |committed: &Committed<'_, P::Source>| {
        let sequence = committed.sequence();
        for mut order in owe::<P>(committed) {
            order.previous = last.insert(order.target.clone(), sequence);
            if positions
                .get(&order.target)
                .is_none_or(|p| sequence >= p.next())
            {
                backlog.entry(order.target.clone()).or_default().push(order);
            }
        }
    };
    source.dispatch(source.first(), registry, &mut [&mut owing])?;
    Ok(backlog)
}
