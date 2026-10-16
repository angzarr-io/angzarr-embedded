//! Event metadata: who, when, on which request and command, why, and under
//! which rules. Field for field what angzarr's `EventMeta` carries, so
//! `annals-angzarr` maps it without inventing anything.

use super::id::{Actor, CommandId, Correlation, EventId, RequestId, RulesVersion, Timestamp};

/// What the application says about one command. The kernel stamps it on
/// every event the command emits, as a [`Meta`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CommandMeta {
    pub recorded_at: Timestamp,
    /// `None` for the system itself: a scheduled job, a migration.
    pub actor: Option<Actor>,
    /// The request the command came in on; a reaction's command keeps its
    /// origin's.
    pub request: RequestId,
    pub command: CommandId,
    /// `None` when the command arrived without one: the kernel stamps
    /// [`Correlation::generated`] from the request.
    pub correlation: Option<Correlation>,
    /// The event this command reacts to, in any stream. `None` for a command
    /// a person or a job gave.
    pub cause: Option<EventId>,
}

impl CommandMeta {
    /// The first (often the only) command of `request`: no correlation
    /// given, no cause.
    pub fn new(recorded_at: Timestamp, actor: Option<Actor>, request: RequestId) -> CommandMeta {
        CommandMeta::nth(recorded_at, actor, request, 0)
    }

    /// The `index`th command of `request`.
    pub fn nth(
        recorded_at: Timestamp,
        actor: Option<Actor>,
        request: RequestId,
        index: u32,
    ) -> CommandMeta {
        CommandMeta {
            recorded_at,
            actor,
            request,
            command: CommandId::Origin { index },
            correlation: None,
            cause: None,
        }
    }

    /// This command, continuing `correlation`.
    pub fn correlated(self, correlation: Correlation) -> CommandMeta {
        CommandMeta {
            correlation: Some(correlation),
            ..self
        }
    }

    /// The `index`th output of the reaction `component` to `source`, an
    /// event recorded with `meta`: the same actor, instant, request and
    /// correlation, caused by `source`, and named [`CommandId::Reaction`].
    /// A catch-up delivered later, outside the chain, replaces
    /// `recorded_at` with its own.
    pub fn reacting_to(meta: &Meta, source: EventId, component: &str, index: u32) -> CommandMeta {
        CommandMeta {
            recorded_at: meta.recorded_at,
            actor: meta.actor.clone(),
            request: meta.request.clone(),
            command: CommandId::Reaction {
                source: source.clone(),
                component: component.to_string(),
                index,
            },
            correlation: Some(meta.correlation.clone()),
            cause: Some(source),
        }
    }

    /// The correlation the command's events are stamped with: the one
    /// given, or [`Correlation::generated`] from the request.
    pub fn correlation(&self) -> Correlation {
        todo!("proposal: the given correlation, else generated from the request")
    }

    /// This command's metadata, as stamped on one event decided under
    /// `rules`.
    pub(crate) fn stamp(&self, rules: &RulesVersion) -> Meta {
        Meta {
            recorded_at: self.recorded_at,
            actor: self.actor.clone(),
            request: self.request.clone(),
            command: self.command.clone(),
            correlation: self.correlation(),
            cause: self.cause.clone(),
            rules: rules.clone(),
        }
    }
}

/// One stored event's metadata. The kernel stamps it; `apply` never sees it.
///
/// The event's own id is not stored: it is [`EventId`] `{ stream, seq }`,
/// which the stream's key and the envelope's seq already give. There is no
/// schema version: an event's payload type names its version (a protobuf
/// type URL, for `annals-angzarr`), and upcasting is the store adapter's.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Meta {
    pub recorded_at: Timestamp,
    pub actor: Option<Actor>,
    pub request: RequestId,
    pub command: CommandId,
    /// Always set: see [`CommandMeta::correlation`].
    pub correlation: Correlation,
    /// The event this one follows from: in this stream (an earlier seq) or
    /// another (a reaction's source event).
    pub cause: Option<EventId>,
    /// The rules version the event was decided under, and that `apply`
    /// folds it with.
    pub rules: RulesVersion,
}
