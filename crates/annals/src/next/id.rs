//! The identities the kernel stamps and compares. All opaque to the kernel,
//! which never mints one: the application supplies them (a clock, a UUID
//! generator and an account service are I/O, and the kernel does none).

/// An event's position in its stream, from 0.
pub type Seq = u64;

/// Names a stream. 128 bits, so a UUID fits exactly (big-endian, as
/// [`StreamId::from_bytes`] reads it) and so does any `u64`.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamId(pub u128);

impl StreamId {
    /// A UUID's 16 bytes, in the order a UUID is written.
    pub const fn from_bytes(bytes: [u8; 16]) -> StreamId {
        StreamId(u128::from_be_bytes(bytes))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }
}

impl From<u64> for StreamId {
    fn from(id: u64) -> StreamId {
        StreamId(u128::from(id))
    }
}

/// One event, anywhere: its stream and its seq in it. Unique for as long
/// as stream ids are, never minted, and the same on every replay. It is
/// the event id, and what a cause points at.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EventId {
    pub stream: StreamId,
    pub seq: Seq,
}

/// The request a command came in on: one API call, one job run. Supplied
/// by the application; a UUID fits. One request may issue several
/// commands.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RequestId(pub u128);

/// Names one command, deterministically, so it can serve as an
/// **idempotency key**: a retried command keeps its id, and a store that
/// records it can recognise the retry
/// ([`StoreError::Duplicate`](super::StoreError::Duplicate)).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum CommandId {
    /// A command the application issued: the `index`th of its request.
    Origin { request: RequestId, index: u32 },
    /// A command (or fact) a reaction issued: the `index`th output of the
    /// reaction named `component` (its [`Reaction::NAME`]) for the source
    /// event `source`. Recomputable from the log, since reactions are pure.
    ///
    /// [`Reaction::NAME`]: super::Reaction::NAME
    Reaction {
        source: EventId,
        component: String,
        index: u32,
    },
}

/// Groups every event that follows from one originating request, across
/// streams, across reaction hops and across commands (a process in a
/// wrapping runtime keeps one correlation over many). A fresh request
/// starts its own correlation ([`From<RequestId>`]); a reaction's outputs
/// keep their source's.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct CorrelationId(pub u128);

impl From<RequestId> for CorrelationId {
    fn from(request: RequestId) -> CorrelationId {
        CorrelationId(request.0)
    }
}

/// Who asked: opaque to the kernel. Make it a **pseudonymous** id, never a
/// name or address, so erasing a person deletes the mapping outside the log
/// and never rewrites history (which a hash chain would forbid anyway).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Actor(pub String);

/// When an event was recorded: microseconds since the Unix epoch, UTC.
/// Audit time, supplied by the application per command. Business time (a
/// deadline, a cooldown's start) belongs in the event's payload, where
/// `apply` can see it.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Timestamp(pub i64);

/// Names one version of a domain's rules: the reference data `apply` and
/// `decide` read. Recorded on every event, so replay can find the rules each
/// event was decided under.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RulesVersion(pub String);

impl From<&str> for RulesVersion {
    fn from(version: &str) -> RulesVersion {
        RulesVersion(version.to_string())
    }
}
