//! Event metadata: who, when, on which command, and why.

use super::id::{Actor, CommandId, CorrelationId, EventId, RequestId, RulesVersion, Timestamp};

/// What the application says about one command. The kernel stamps it on
/// every event the command emits, as a [`Meta`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CommandMeta {
    pub recorded_at: Timestamp,
    /// `None` for the system itself: a scheduled job, a migration.
    pub actor: Option<Actor>,
    pub command: CommandId,
    pub correlation: CorrelationId,
    /// The event this command reacts to, in any stream. `None` for a command
    /// a person or a job gave.
    pub cause: Option<EventId>,
}

impl CommandMeta {
    /// The first (often the only) command of `request`: its own
    /// correlation, no cause.
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
            command: CommandId::Origin { request, index },
            correlation: request.into(),
            cause: None,
        }
    }

    /// The `index`th output of the reaction `component` to `source`, an
    /// event recorded with `meta`: the same actor, instant and correlation,
    /// caused by `source`, and named [`CommandId::Reaction`]. A catch-up
    /// delivered later, outside the chain, replaces `recorded_at` with its
    /// own.
    pub fn reacting_to(meta: &Meta, source: EventId, component: &str, index: u32) -> CommandMeta {
        CommandMeta {
            recorded_at: meta.recorded_at,
            actor: meta.actor.clone(),
            command: CommandId::Reaction {
                source,
                component: component.to_string(),
                index,
            },
            correlation: meta.correlation,
            cause: Some(source),
        }
    }

    /// This command's metadata, as stamped on one event decided under
    /// `rules` by code writing schema `schema`.
    pub(crate) fn stamp(&self, schema: u32, rules: &RulesVersion) -> Meta {
        Meta {
            recorded_at: self.recorded_at,
            actor: self.actor.clone(),
            command: self.command.clone(),
            correlation: self.correlation,
            cause: self.cause,
            schema,
            rules: rules.clone(),
        }
    }
}

/// One stored event's metadata. The kernel stamps it; `apply` never sees it.
///
/// The event's own id is not stored: it is [`EventId`] `{ stream, seq }`,
/// which the stream's header and the envelope's seq already give.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Meta {
    pub recorded_at: Timestamp,
    pub actor: Option<Actor>,
    pub command: CommandId,
    pub correlation: CorrelationId,
    /// The event this one follows from: in this stream (an earlier seq) or
    /// another (a policy's source event).
    pub cause: Option<EventId>,
    /// The event schema version the payload was written at.
    pub schema: u32,
    /// The rules version the event was decided under, and that `apply`
    /// folds it with.
    pub rules: RulesVersion,
}
