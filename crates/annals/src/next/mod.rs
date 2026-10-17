//! **Proposal, partly implemented**: the audited kernel that replaces the
//! crate root's [`Stream`](crate::Stream), [`Tx`](crate::Tx),
//! [`EventStore`](crate::EventStore), [`Projection`](crate::Projection) and
//! [`policy`](crate::policy) once the design in `docs/design/audit.md` is
//! reviewed. The parse step ([`check`], [`Checked`]), identification
//! ([`Identified`]), the [`Registry`], payload packing and the dead-letter
//! line are implemented; every other non-trivial body is
//! `todo!("proposal: ...")`, and the tests that pin it, in `tests/next/`,
//! are ignored until it is.
//!
//! annals is the in-process runtime, and it **operates on angzarr's wire
//! types directly** (`io.angzarr.v1`, re-exported as [`v1`]): a stream is a
//! [`Cover`](v1::Cover), a command a [`CommandBook`](v1::CommandBook), an
//! event an [`EventPage`](v1::EventPage) with its
//! [`EventMeta`](v1::EventMeta), a causation an [`EventRef`](v1::EventRef),
//! every id a 16-byte [`Uuid`](v1::Uuid), and derived ids are
//! [`angzarr_proto::ids`]'. There is no parallel type for any of them.
//! annals is **not** angzarr: where their execution models differ (the
//! atomic in-process chain, above all) the difference is deliberate and
//! listed in the design's *Divergences*.
//!
//! One command, end to end, in angzarr's normative order:
//!
//! 1. **Validate** ([`check`]): the one place a [`CommandBook`](v1::CommandBook)
//!    is checked, domain-blind, with the [`Registry`] for capabilities.
//!    What comes out is a [`Checked`], whose accessors are never optional
//!    where the book was required to say something. Nothing downstream
//!    unwraps a prost `Option`.
//! 2. **Identify** ([`Checked::identify`]): the command id (the client's,
//!    else the application's minted UUIDv7 from [`Sampled`]; a fact's
//!    UUIDv5 of its external id; a reaction's UUIDv5 of its trigger), the
//!    correlation (given, else the command id; a reaction's trigger's) and,
//!    for a reaction, the causation. An [`Identified`].
//! 3. **Deduplicate** ([`Coordinator::deduplicate`]): the addressed stream
//!    already holds events with this command id? Answered
//!    [`Handled::Duplicate`] with them, and nothing below runs.
//! 4. **Load and resolve** ([`Coordinator::load`]): the snapshot and newer
//!    events; the expected sequence stamped or checked under the resolved
//!    [`ConflictResolution`](v1::ConflictResolution) (STRICT refuses a stale
//!    one here); the window and a [`Nonce`] otherwise. A [`Contextual`].
//! 5. **Decode, then decide or ingest** ([`Contextual::decode`],
//!    [`Prepared::decide`]): the snapshot (discarded on a rules-version or
//!    state-type mismatch), the events and the command, each upcast by type
//!    URL; then the body arm picks the pipeline: a command is decided
//!    ([`Domain::decide`], fallible, staging in a [`Tx`]), a fact ingested
//!    ([`Domain::ingest`], infallible, with a read-only [`IngestCtx`]).
//!    Business output is payloads only: the kernel stamps every event's
//!    [`EventMeta`](v1::EventMeta).
//!
//! Each step's output type is the next step's only input, so the order
//! cannot be skipped or reordered.
//!
//! Then, in the same [`Chain`]: every [`Policy`] routed for the source's
//! domain runs on the staged events, and its commands take the same five
//! steps in their targets, depth first. Once the whole chain has
//! succeeded, every staged stream is appended through the application's
//! unit of work, each checked against the length it was loaded at, and the
//! unit committed; any failure anywhere fails it all ([`Failure`], with the
//! path to the failing [`Step`]). [`Projection`]s run on committed events
//! only.

// Proposal bodies are `todo!()`, so arguments and fields go unread until the
// implementation lands. Remove this with the last `todo!`.
#![allow(unused_variables, dead_code)]
// A failure names the path to the step that failed and carries reports, so
// it is large; failures are the cold path, and unboxed variants keep them
// matchable by pattern. Revisit (box `Downstream`) if profiling says so.
#![allow(clippy::result_large_err)]

mod check;
mod conflict;
mod coordinator;
mod dead_letter;
mod domain;
mod error;
mod fixture;
mod handle;
mod handler;
mod payload;
mod reaction;
mod registry;
mod store;
mod stream;
mod upcast;
mod version;

pub use check::{check, Body, Checked, Identified, Malformed, Sampled};
pub use conflict::{Key, UnitPath, Units};
pub use coordinator::{Contextual, Coordinator, Deduplicated, Nonce, Novel, Prepared};
pub use dead_letter::{dead_letter_line, DeadLetterLog, MemoryDeadLetters, DEAD_LETTER_PREFIX};
pub use domain::{Conflicts, Decision, Domain, IngestCtx, State, Tx};
pub use error::{
    ChainProblem, Failure, KernelError, PayloadError, Recovery, ReplayError, Step, StepError,
    StoreError, UpcastChainError, UpcastError,
};
pub use fixture::{assert_replay_fixtures, check_replay_fixtures, ReplayFixture};
pub use handle::{
    Chain, Handled, Kernel, Project, ProjectionRoute, ProjectorFailure, Recorded, Routes,
    DEFAULT_DEPTH_LIMIT,
};
pub use handler::{Admit, Committed, Gap, Handler, Position, Tracked};
pub use payload::{pack, type_name, Decoder, Payload};
pub use reaction::{
    orders_owed, policy, projection, target_cover, Backlog, Order, Policy, Projection, Projector,
    Reaction, Reactor,
};
pub use registry::{Misconfigured, Registry};
pub use store::{
    AsyncEventStore, AsyncUnitOfWork, EventStore, InMemoryStore, InMemoryTx, Ready, Slice,
    UnitOfWork,
};
pub use stream::{Envelope, Executed, ProducedBy, Stream};
pub use upcast::{UpcastFn, Upcaster};
pub use version::Rulebook;

/// angzarr's id helpers: `Display`/`FromStr` for [`v1::Uuid`] (text only at
/// display and input), and the UUIDv5 derivations ([`ids::compute_root`],
/// [`ids::reaction_command_id`], [`ids::fact_command_id`],
/// [`ids::fact_correlation`]).
pub use angzarr_proto::ids;
/// angzarr's wire contract, as the kernel uses it: the `io.angzarr.v1`
/// messages.
pub use angzarr_proto::v1;
/// The well-known types the kernel's pages carry: payloads are
/// [`Any`](prost_types::Any), instants [`Timestamp`](prost_types::Timestamp).
pub use prost_types::{Any, Timestamp};

/// The later of two instants: what `recorded_at` is raised to, so it never
/// goes backwards within a stream. prost's `Timestamp` has no ordering of
/// its own; this compares `(seconds, nanos)`.
pub fn not_before(floor: Option<&Timestamp>, now: Timestamp) -> Timestamp {
    match floor {
        Some(f) if (f.seconds, f.nanos) > (now.seconds, now.nanos) => *f,
        _ => now,
    }
}
