//! The audited kernel, designed in `docs/design/audit.md`, that is to
//! replace the crate root's [`Stream`](crate::Stream), [`Tx`](crate::Tx),
//! [`EventStore`](crate::EventStore), [`Projection`](crate::Projection) and
//! [`policy`](crate::policy). Each module's tests are in its `*.test.rs`, over
//! the toy domains of `toys.test.rs`.
//!
//! annals is the in-process runtime, and it **operates on angzarr's wire
//! types directly** (`io.angzarr.v1`, re-exported as [`v1`]): a stream is a
//! [`Cover`](v1::Cover), a command a [`CommandBook`](v1::CommandBook), an
//! event an [`EventPage`](v1::EventPage) with its
//! [`EventMeta`](v1::EventMeta) (whose `causation` is the event's direct
//! cause), a trigger an [`EventRef`](v1::EventRef),
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
//!    for a reaction, the command's own causation (its trigger: transport
//!    and trace metadata, never stamped on its events). An [`Identified`].
//! 3. **Load, then deduplicate** ([`Coordinator::deduplicate`]): the
//!    snapshot and newer events are loaded first; then, does the addressed
//!    stream already hold events with this command id? Answered
//!    [`Handled::Duplicate`] with them, and nothing below runs. (Loading
//!    first closes the race in which a retry's lookup misses an original
//!    that commits before the retry loads.)
//! 4. **Resolve** ([`Coordinator::load`]): the expected sequence stamped or
//!    checked, against the head step 3 loaded, under the resolved
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
//! only. Each policy command's trigger edge, which no event records, goes
//! to the application's [`TriggerLog`] as the chain dispatches it.

// A failure names the path to the step that failed and carries reports, so
// it is large; failures are the cold path, and unboxed variants keep them
// matchable by pattern. Revisit (box `Downstream`) if profiling says so.
#![allow(clippy::result_large_err)]

mod check;
mod conflict;
#[cfg(feature = "contract")]
pub mod contract;
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
mod wire;

// The toy domains the kernel's tests run on.
#[cfg(test)]
#[path = "toys.test.rs"]
pub(crate) mod toys;

pub use check::{check, Body, Checked, Identified, Malformed, Sampled};
pub use conflict::{Key, UnitPath, Units};
pub use coordinator::{Contextual, Coordinator, Deduplicated, Nonce, Novel, Prepared};
pub use dead_letter::{
    dead_letter_line, DeadLetterLog, MemoryDeadLetters, NoTriggerLog, TriggerLog,
    DEAD_LETTER_PREFIX,
};
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
pub use payload::{decode_as, pack, type_name, Decoder, Payload};
pub use reaction::{
    orders_owed, policy, projection, target_cover, Backlog, Order, Policy, Projection, Projector,
    Reaction, Reactor,
};
pub use registry::{Misconfigured, Registry};
pub use store::{
    AsyncEventStore, AsyncPurge, AsyncReadStore, AsyncReadUnit, AsyncStore, AsyncUnitOfWork,
    EventStore, InMemoryRead, InMemoryStore, InMemoryTx, Purge, ReadStore, ReadUnit, Ready, Slice,
    Store, UnitOfWork,
};
pub use stream::{Envelope, Executed, Stream};
pub use upcast::{UpcastFn, Upcaster};
pub use version::Rulebook;
pub use wire::WireError;

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

/// One value of every error `next` defines, each code once, for the
/// catalog parity test ([`crate::diagnostic_samples`]).
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn annals_diagnostic::Diagnostic>> {
    use annals_diagnostic::Report;
    use v1::ConflictResolution;

    let stream = || v1::Cover {
        domain: "account".to_string(),
        root: Some(v1::Uuid::from_bytes([7; 16])),
        edition: Some("what-if".to_string()),
    };
    let gap = Gap {
        expected: 3,
        found: 5,
    };
    let report = || Report::of(&gap);
    let domain = || "account".to_string();
    let malformed = [
        Malformed::NoCover,
        Malformed::UnknownDomain { domain: domain() },
        Malformed::NoRoot,
        Malformed::BadRoot { len: 15 },
        Malformed::EmptyEdition,
        Malformed::NoPage,
        Malformed::NoBody,
        Malformed::NoPayload,
        Malformed::BadCommandId { len: 1 },
        Malformed::BadCorrelationId { len: 17 },
        Malformed::CausationFromOutside,
        Malformed::IdOnReaction,
        Malformed::FactFromReaction,
        Malformed::HandlingOnReaction,
        Malformed::BadTrigger(ids::UuidError::MissingTrigger),
        Malformed::FactWithoutOrigin,
        Malformed::EmptyExternalId,
        Malformed::CommandWithFactOrigin,
        Malformed::FactCommandIdNotDerived,
        Malformed::HandlingOnFact,
        Malformed::UnknownConflictResolution { value: 9 },
        Malformed::UnsupportedConflictResolution {
            domain: domain(),
            requested: ConflictResolution::Commutative,
        },
    ];
    let kernel = [
        KernelError::Stale {
            stream: stream(),
            expected: 2,
            actual: 4,
        },
        KernelError::Unresolved {
            stream: stream(),
            expected: 2,
            actual: 4,
        },
        KernelError::UnknownType {
            stream: stream(),
            type_url: "/toy.v9.Bonus".to_string(),
        },
        KernelError::InvalidPayload {
            stream: stream(),
            type_url: "/toy.v1.Deposit".to_string(),
            reason: report(),
        },
        KernelError::Upcast {
            stream: stream(),
            error: UpcastError::Rule {
                source: "toy.v0.Credited".to_string(),
                error: report(),
            },
        },
        KernelError::IngestPanicked {
            stream: stream(),
            command_id: v1::Uuid::from_bytes([1; 16]),
        },
        KernelError::Gap {
            stream: stream(),
            gap: gap.clone(),
        },
        KernelError::WrongDomain {
            stream: stream(),
            expected: "ledger",
        },
        KernelError::ChainTooDeep { limit: 8 },
        KernelError::BeforeSnapshot {
            stream: stream(),
            first: 10,
        },
        KernelError::UnregisteredDomain { domain: domain() },
    ];
    let replay = [
        ReplayError::OutOfSequence { index: 1, found: 2 },
        ReplayError::CauseNotEarlier {
            sequence: 1,
            cause: 1,
        },
        ReplayError::RecordedBackwards { sequence: 4 },
        ReplayError::IncompleteMeta {
            sequence: 4,
            field: "correlation_id",
        },
        ReplayError::WrongDomain {
            found: "ledger".to_string(),
            expected: "account",
        },
        ReplayError::UnknownRules {
            sequence: 4,
            version: "v0".to_string(),
        },
    ];
    let store = [
        StoreError::Conflict {
            stream: stream(),
            expected: 1,
            actual: 2,
        },
        StoreError::Discontinuous {
            stream: stream(),
            expected: 2,
            found: 3,
        },
        StoreError::Unavailable(report()),
        StoreError::Unreadable {
            stream: stream(),
            sequence: 0,
            reason: Report::of(&PayloadError::UnknownType {
                type_url: "/toy.v9.Bonus".to_string(),
            }),
        },
    ];
    let missing = WireError::Missing {
        message: "toy.v1.Deposited",
        field: "at",
    };
    let payload = [
        PayloadError::Invalid {
            type_url: "/toy.v1.Deposited".to_string(),
            reason: Report::of(&missing),
        },
        PayloadError::UnknownType {
            type_url: "/toy.v9.Bonus".to_string(),
        },
        PayloadError::Decode {
            type_url: "/toy.v1.Deposited".to_string(),
            error: <Timestamp as prost::Message>::decode(&[0x08][..])
                .expect_err("a tag without its value is not a message"),
        },
    ];
    let chain = [
        ChainProblem::Duplicate {
            source: "toy.v0.Credited".to_string(),
        },
        ChainProblem::OutOfOrder {
            source: "toy.v1.Credited".to_string(),
            produced_by: "toy.v0.Credited".to_string(),
        },
        ChainProblem::DeadEnd {
            source: "toy.v0.Credited".to_string(),
            ends_at: "toy.v9.Bonus".to_string(),
        },
    ]
    .map(|problem| UpcastChainError {
        domain: domain(),
        problem,
    });
    let misconfigured = [
        Misconfigured::AlreadyRegistered { domain: domain() },
        Misconfigured::UpcasterForAnotherDomain {
            domain: domain(),
            upcaster: "ledger".to_string(),
        },
        Misconfigured::EmptyRulesVersion { domain: domain() },
        Misconfigured::BadStateDescriptor { domain: domain() },
        Misconfigured::UnsupportedResolution {
            domain: domain(),
            command_type: None,
            resolution: ConflictResolution::Commutative,
        },
        Misconfigured::UnsupportedResolution {
            domain: domain(),
            command_type: Some("toy.cmd.v1.Note".to_string()),
            resolution: ConflictResolution::Commutative,
        },
    ];

    let mut samples: Vec<Box<dyn annals_diagnostic::Diagnostic>> = Vec::new();
    samples.extend(malformed.map(|e| Box::new(e) as Box<_>));
    samples.extend(kernel.map(|e| Box::new(e) as Box<_>));
    samples.extend(replay.map(|e| Box::new(e) as Box<_>));
    samples.extend(store.map(|e| Box::new(e) as Box<_>));
    samples.extend(payload.map(|e| Box::new(e) as Box<_>));
    samples.extend(chain.map(|e| Box::new(e) as Box<_>));
    samples.extend(misconfigured.map(|e| Box::new(e) as Box<_>));
    samples.push(Box::new(UpcastError::Rule {
        source: "toy.v0.Credited".to_string(),
        error: report(),
    }));
    samples.push(Box::new(Failure::<Gap>::Downstream {
        path: Vec::new(),
        error: StepError::Kernel(KernelError::ChainTooDeep { limit: 8 }),
    }));
    samples.push(Box::new(gap));
    samples.push(Box::new(missing));
    samples.push(Box::new(WireError::UnknownEnum {
        message: "toy.v1.Closed",
        field: "reason",
        value: 0,
    }));
    samples
}
