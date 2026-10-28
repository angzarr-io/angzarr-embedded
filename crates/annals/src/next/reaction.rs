//! Reactions to committed events: policies and projections, over one base.
//!
//! Every kind takes the same input, a [`Committed`] event (the stream, the
//! sequence, the metadata, the payload, the state just after it), and runs
//! through the same dispatch: each becomes a [`Handler`] of its source.
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

use std::marker::PhantomData;

use angzarr_proto::v1::{command_page, Command, CommandBook, CommandPage, Cover, EventRef, Uuid};

use super::check::{check_reaction, Checked, Malformed};
use super::dead_letter::{write_dead_letter, DeadLetterLog};
use super::domain::Domain;
use super::handler::{Committed, Handler};
use super::payload::Payload;
use super::registry::Registry;

// ---------------------------------------------------------------- the base

/// What a policy is over: a source, a target domain, and the targets each
/// source event is owed to. Stateless: live targets are a fold of the
/// source.
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
/// stream on every read ([`Stream::dispatch`](super::Stream::dispatch) from 0).
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

/// The one mechanism under the reactor and the chain: what `P`
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
            target,
        })
        .collect()
}
