//! Sagas: one domain's events, carried as commands into another's streams.
//!
//! A [`Saga`] is the write-side counterpart of a
//! [`Projection`](crate::Projection). It reacts to a **source** stream's
//! events by commanding **target** streams of another domain. It stores
//! nothing of its own:
//!
//! - **Which targets are live is a fold of the source.** [`Saga::targets`]
//!   reads them off the source's state after each event. A target owed the
//!   event that retires it is still a target for that one event, and never
//!   again after it.
//! - **How far a target has got is in its own stream.** Each source event
//!   it receives is [`Injected`] with its source seq, so its checkpoint (the
//!   last source seq it holds) replays with it. [`backlog`] turns a
//!   checkpoint into the commands that bring the target up to date.
//! - **Delivery is idempotent.** A target's decide places each injection
//!   against its checkpoint with [`admit`]: a seq it already holds is a
//!   redelivery and emits nothing, and a seq past the next one is a [`Gap`]
//!   and is refused. Delivering a backlog again is always safe.
//!
//! The caller reads the source, executes the backlog on each target and
//! appends what that emits, in one transaction: a saga has no compensation,
//! because delivery and the source's own append commit or roll back
//! together.

use std::fmt;

use annals_diagnostic::{Arg, Diagnostic};

use crate::domain::Domain;
use crate::stream::{Envelope, Seq, Stream};

/// A source event carried verbatim into a target's stream, with its seq in
/// the source. A target typically folds `event` into its copy of the source
/// with the source domain's own [`Domain::apply`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Injected<E> {
    pub source_seq: Seq,
    pub event: E,
}

/// A process manager from one source domain to one target domain.
pub trait Saga {
    type Source: Domain;
    type Target: Domain;
    /// Names one target stream.
    type TargetId: Eq;

    /// The targets owed the event that led to `source`, the source's state
    /// just after it. Includes a target that event retires.
    fn targets(source: &<Self::Source as Domain>::State) -> Vec<Self::TargetId>;

    /// The command that delivers `envelope` to `target`, given `source`, the
    /// source's state just after it: "inject this event", or for the event
    /// that first makes `target` a target, whatever creates it ("instantiate
    /// from this state", say).
    fn command(
        target: &Self::TargetId,
        source: &<Self::Source as Domain>::State,
        envelope: &Envelope<<Self::Source as Domain>::Event>,
    ) -> <Self::Target as Domain>::Command;
}

/// The commands that bring `target` up to date with `source`, in order:
/// one for each event after `checkpoint` (every event, for `None`) whose
/// state names `target` among [`Saga::targets`]. Empty when the target is up
/// to date, which it is when `checkpoint + 1` is the source's length.
///
/// One replay of the source, however long the backlog.
#[must_use]
pub fn backlog<S: Saga>(
    source: &Stream<S::Source>,
    rules: &<S::Source as Domain>::Rules,
    target: &S::TargetId,
    checkpoint: Option<Seq>,
) -> Vec<<S::Target as Domain>::Command> {
    let mut commands = Vec::new();
    source.visit_states(rules, |envelope, state| {
        if checkpoint.is_some_and(|c| envelope.seq <= c) {
            return;
        }
        if S::targets(state).contains(target) {
            commands.push(S::command(target, state, envelope));
        }
    });
    commands
}

/// Where an injected source seq falls against a target's checkpoint.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Admit {
    /// The next one: deliver it.
    Next,
    /// At or before the checkpoint: already held, so emit nothing.
    Redelivery,
}

/// Place `source_seq` against `checkpoint`, the last source seq a target
/// holds. A target with no checkpoint yet decides its first delivery
/// itself: that is where it is created.
pub fn admit(checkpoint: Seq, source_seq: Seq) -> Result<Admit, Gap> {
    let expected = checkpoint + 1;
    if source_seq < expected {
        Ok(Admit::Redelivery)
    } else if source_seq == expected {
        Ok(Admit::Next)
    } else {
        Err(Gap { expected, found: source_seq })
    }
}

/// A delivery skipped ahead: the target expected source event `expected`
/// and was given `found`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Gap {
    pub expected: Seq,
    pub found: Seq,
}

impl fmt::Display for Gap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "source event {} skips ahead of event {}, the next expected", self.found, self.expected)
    }
}

impl std::error::Error for Gap {}

impl Diagnostic for Gap {
    fn code(&self) -> &'static str {
        "annals-saga-gap"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![("expected", self.expected.into()), ("found", self.found.into())]
    }
}
