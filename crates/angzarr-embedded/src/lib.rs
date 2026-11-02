//! A small, deterministic event-sourcing engine on angzarr's wire types:
//! angzarr's local runtime, designed in `docs/design/audit.md`: a domain
//! decides commands and ingests facts into events, applied to its folded
//! state, policies carry one domain's events into another's commands, and
//! every step is committed on its own through angzarr's shared store ports.
//!
//! The crate knows nothing about any particular application.
//!
//! Each module's tests are in its `*.test.rs`, over the toy domains of
//! `toys.test.rs`.
//!
//! angzarr-embedded is the in-process runtime, and it **operates on angzarr's wire
//! types directly** (`io.angzarr.v1`, re-exported as [`v1`]): a stream is a
//! [`Cover`](v1::Cover), a command a [`CommandBook`](v1::CommandBook), an
//! event an [`EventPage`](v1::EventPage) with its
//! [`EventMeta`](v1::EventMeta) (whose `causation` is the event's direct
//! cause), a trigger an [`EventRef`](v1::EventRef),
//! every id a 16-byte [`Uuid`](v1::Uuid), and derived ids are
//! [`angzarr_proto::ids`]'. There is no parallel type for any of them.
//! angzarr-embedded is **not** angzarr: where their execution models differ (the
//! atomic in-process chain, above all) the difference is deliberate, and
//! tagged on the coordinator-contract scenario that shows it
//! (`crates/angzarr-embedded-conformance/src/tags.test.rs`).
//!
//! One command, end to end, in angzarr's normative order:
//!
//! 1. **Validate** ([`check`]): the one place a [`CommandBook`](v1::CommandBook)
//!    is checked, domain-blind, with the [`Catalog`] for capabilities.
//!    What comes out is a [`Checked`], whose accessors are never optional
//!    where the book was required to say something. Nothing downstream
//!    unwraps a prost `Option`.
//! 2. **Identify** ([`Checked::identify`]): the command id (the client's,
//!    else the application's minted UUIDv7 from [`Sampled`]; a fact's
//!    UUIDv5 of its external id; a reaction's UUIDv5 of its trigger), the
//!    correlation (given, else the command id; a reaction's trigger's) and,
//!    for a reaction, the command's own causation (its trigger: transport
//!    and trace metadata, never stamped on its events). An [`Identified`].
//! 3. **Load, then admit**: the snapshot and newer events are loaded
//!    first; then the command id is looked up on the addressed stream, and
//!    the kernel admits the input (a refusal fact's address, deduplication
//!    by the events found, a process manager's start rule by its type after
//!    upcasting).
//!    Events found are answered [`Handled::Duplicate`], and nothing below
//!    runs. (Loading first closes the race in which a retry's lookup misses
//!    an original that commits before the retry loads.)
//! 4. **Resolve**: the expected sequence stamped or
//!    checked, against the head step 3 loaded, under the resolved
//!    [`ConflictResolution`](v1::ConflictResolution) (STRICT refuses a stale
//!    one here); the window and a nonce otherwise.
//! 5. **Dispatch**: angzarr-router's `CommandHandler` for the domain gets
//!    the `ContextualCommand` (the snapshot, the events, upcast, and the
//!    command, its handling resolved) and runs its handler: a command's,
//!    which may refuse ([`Rejected`]), or a fact's, which may not. The
//!    kernel awaits it; business code stays synchronous. Business
//!    output is payloads only: the kernel stamps every event's
//!    [`EventMeta`](v1::EventMeta).
//!
//! A domain whose streams another coordinator owns ([`Registry::remote`],
//! [`Target::Remote`]) takes steps 1 and 2 only: its command, identified,
//! is handed to that coordinator's [`CoordinatorPort`], and its answer is
//! the step's.
//!
//! Each step's output type is the next step's only input, so the order
//! cannot be skipped or reordered.
//!
//! Each step is ONE atomic write to the store (no unit of work): one
//! stream's events at the length it was loaded at, with the step's
//! positions. Then every [`Policy`] for the source's domain runs, through
//! angzarr-router's `PolicyComponent` (a `PolicyDispatch`, or a policy in
//! another process), on the written events, each of its commands a step of its own in its target,
//! depth first, with the policy's position written with it; a step that
//! fails keeps every write before it ([`Failure`], with the path to the
//! failing [`Step`]), and what is owed is resumed by position
//! ([`Kernel::resume`]). Projectors run on written events only, inline
//! ([`Registry::projector_handler`]) or caught up from the store
//! ([`CatchUp`]). A policy command's trigger edge, which no event records, is the
//! command's own causation, for tracing.

// A failure names the path to the step that failed and carries reports, so
// it is large; failures are the cold path, and unboxed variants keep them
// matchable by pattern. Revisit (box `Downstream`) if profiling says so.
#![allow(clippy::result_large_err)]

mod book;
mod catalog;
mod check;
mod conflict;
mod coordinator;
mod dead_letter;
mod domain;
mod error;
mod fixture;
mod handle;
pub mod markup;
mod misconfigured;
mod payload;
mod policy;
mod projector;
mod registry;
mod route;
mod store;
mod stream;
mod target;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
mod upcast;
mod wire;

// The toy domains the kernel's tests run on.
#[cfg(test)]
#[path = "toys.test.rs"]
pub(crate) mod toys;

// Aggregates, a process manager, policies and a projector written in
// angzarr-embedded-macros' markup (`markup` is its runtime), on angzarr-embedded as a dependent
// sees it (`::angzarr-embedded`), beside a hand-written domain.
#[cfg(test)]
#[path = "decorated.test.rs"]
mod decorated;

// An aggregate and a policy written by hand, with no macro, through
// markup's public functions alone, on angzarr-embedded as a dependent sees it.
#[cfg(test)]
#[path = "hand_written.test.rs"]
mod hand_written;

// The kernel over the shared SQLite adapter.
#[cfg(test)]
#[path = "sqlite.test.rs"]
mod sqlite;

// The crates re-exported at the root, as a dependent names them.
#[cfg(test)]
#[path = "reexports.test.rs"]
mod reexports;

/// angzarr-diagnostic, the crate every error here implements
/// (`Diagnostic`, `Arg`, `Report`, `display`, and its `catalog` parity
/// check), re-exported so a dependent names the one this crate is built on.
pub use angzarr_diagnostic;
/// angzarr-ports, angzarr's shared storage contract, which the kernel runs
/// on, re-exported. Its en-US catalog, the messages for the store errors
/// this crate forwards under their own codes ([`KernelError::Store`]), is
/// `angzarr_ports::EN_US`.
pub use angzarr_ports;
/// angzarr-proto, angzarr's wire types (`io.angzarr.v1`, whose messages
/// are also [`v1`], and [`ids`]), re-exported so an application names the
/// copy the kernel is built on without pinning it itself.
pub use angzarr_proto;
/// angzarr's shared client router, which the kernel dispatches through.
pub use angzarr_router;
/// The projector port, angzarr-router's: what [`Registry::projector_handler`]
/// registers and a [`CatchUp`] catches up.
pub use angzarr_router::router::ProjectorHandler;
/// Where a folded event sits, angzarr-router's: its stream's cover, its
/// sequence and its metadata, what [`Domain::apply`] is given.
pub use angzarr_router::PageContext;
/// angzarr-store-sqlite, the shared SQLite store, re-exported so an
/// application pins it through angzarr-embedded. Behind the `sqlite`
/// feature, off by default: it brings sqlx and SQLite.
#[cfg(feature = "sqlite")]
pub use angzarr_store_sqlite;
pub use book::{CommandBookExt, CoverExt};
pub use catalog::{Catalog, Description};
pub use check::{check, Body, Checked, Identified, Malformed, Sampled};
pub use conflict::{Key, UnitPath, Units};
pub use dead_letter::{dead_letter_line, DeadLetterLog, MemoryDeadLetters, DEAD_LETTER_PREFIX};
pub use domain::{Domain, State};
pub use error::{
    class, ChainProblem, Class, Failure, KernelError, PayloadError, Recovery, ReplayError, Step,
    StepError, UpcastChainError, UpcastError,
};
pub use fixture::{assert_replay_fixtures, check_replay_fixtures, ReplayFixture};
pub use handle::{
    Handled, Kernel, OnPolicyBug, OnRetryable, Recorded, Wait, DEFAULT_DEPTH_LIMIT,
    DEFAULT_RACE_RUNS,
};
pub use misconfigured::{validate_descriptor, Misconfigured};
pub use payload::{decode_as, pack, type_name, Decoder, Payload};
pub use policy::{target_cover, translated, CommandTarget, EventSource, Order, Policy};
pub use projector::{CatchUp, Caught};
pub use registry::Registry;
pub use route::{rejection, spec_coded, Rejected, SpecKey};
pub use store::{
    Append, ChangeToken, DeadLetter, DeadLetterAt, DeadLetterKey, DeadLetterStore, EventStore,
    Head, ListPositions, ListStreams, Outbox, OutboxEntry, PositionKey, PositionStore, Purge,
    ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice, SnapshotStore, Store, StoreError,
    StreamRef, Watch, Write, Written,
};
#[cfg(any(test, feature = "test-support"))]
pub use stream::execute::Executed;
pub use stream::{Envelope, Stream};
pub use target::{Answered, Coordinated, CoordinatorPort, Target};
pub use upcast::{UpcastFn, UpcastPort, Upcaster};
pub use wire::WireError;

/// angzarr's id helpers: `Display`/`FromStr` for [`v1::Uuid`] (text only at
/// display and input), and the UUIDv5 derivations ([`ids::compute_root`],
/// [`ids::policy_command_id`], [`ids::fact_command_id`],
/// [`ids::fact_correlation`]).
pub use angzarr_proto::ids;
/// angzarr's wire contract, as the kernel uses it: the `io.angzarr.v1`
/// messages.
pub use angzarr_proto::v1;
/// The well-known types the kernel's pages carry: payloads are
/// [`Any`], instants [`Timestamp`].
pub use prost_types::{Any, Timestamp};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `crates/angzarr-embedded/locales/en-US/angzarr-embedded.ftl`.
pub const EN_US: (&str, &str) = (
    "angzarr-embedded",
    include_str!("../locales/en-US/angzarr-embedded.ftl"),
);

/// The later of two instants: what `recorded_at` is raised to, so it never
/// goes backwards within a stream. prost's `Timestamp` has no ordering of
/// its own; this compares `(seconds, nanos)`.
pub fn not_before(floor: Option<&Timestamp>, now: Timestamp) -> Timestamp {
    let instant = |t: &Timestamp| (t.seconds, t.nanos);
    let (seconds, nanos) = floor.map_or(instant(&now), |f| instant(f).max(instant(&now)));
    Timestamp { seconds, nanos }
}

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test: each one's code must have a message in every locale,
/// using exactly its args. Its messages are in
/// `crates/angzarr-embedded/locales/en-US/angzarr-embedded.ftl`.
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    fn boxed<E: angzarr_diagnostic::Diagnostic + 'static>(
        errors: impl IntoIterator<Item = E>,
    ) -> impl Iterator<Item = Box<dyn angzarr_diagnostic::Diagnostic>> {
        errors.into_iter().map(|e| Box::new(e) as _)
    }
    let mut samples = Vec::new();
    samples.extend(boxed(malformed_samples()));
    samples.extend(boxed(kernel_samples()));
    samples.extend(boxed(replay_samples()));
    samples.extend(boxed(store_samples().into_iter().map(KernelError::Store)));
    samples.extend(boxed(payload_samples()));
    samples.extend(boxed(chain_samples()));
    samples.extend(boxed(misconfigured_samples()));
    samples.extend(boxed(upcast_samples()));
    samples.extend(boxed(failure_samples()));
    samples.extend(boxed(router_samples()));
    samples.push(Box::new(markup::NotItsStream {
        domain: sample_domain(),
        key: "swift-amber-falcon".to_string(),
    }));
    samples.extend(boxed(wire_samples()));
    samples
}

/// The stream the samples are about.
fn sample_stream() -> v1::Cover {
    v1::Cover {
        domain: "account".to_string(),
        root: Some(v1::Uuid::from_bytes([7; 16])),
        edition: Some("what-if".to_string()),
    }
}

/// The domain the samples name.
fn sample_domain() -> String {
    "account".to_string()
}

/// The cause the samples carry: a wire error.
const SAMPLE_MISSING: WireError = WireError::Missing {
    message: "toy.v1.Deposited",
    field: "at",
};

/// [`SAMPLE_MISSING`], reported.
fn sample_report() -> angzarr_diagnostic::Report {
    angzarr_diagnostic::Report::of(&SAMPLE_MISSING)
}

/// Each [`Malformed`] variant.
fn malformed_samples() -> Vec<Malformed> {
    use v1::ConflictResolution;
    vec![
        Malformed::NoCover,
        Malformed::UnknownDomain {
            domain: sample_domain(),
        },
        Malformed::NoRoot,
        Malformed::BadRoot { len: 15 },
        Malformed::EmptyEdition,
        Malformed::NoPage,
        Malformed::NoBody,
        Malformed::NoPayload,
        Malformed::BadCommandId { len: 1 },
        Malformed::BadCorrelationId { len: 17 },
        Malformed::CausationFromOutside,
        Malformed::IdOnPolicyCommand {
            field: "command_id",
        },
        Malformed::FactFromPolicy,
        Malformed::HandlingOnPolicyCommand,
        Malformed::BadTrigger(ids::UuidError::MissingTrigger),
        Malformed::FactWithoutOrigin,
        Malformed::EmptyExternalId,
        Malformed::CommandWithFactOrigin,
        Malformed::FactCommandIdNotDerived,
        Malformed::HandlingOnFact,
        Malformed::UnknownConflictResolution { value: 9 },
        Malformed::RefusalMisaddressed,
        Malformed::UnsupportedConflictResolution {
            domain: sample_domain(),
            requested: ConflictResolution::Commutative,
        },
    ]
}

/// Each [`KernelError`] variant but `Store` ([`store_samples`]) and
/// `Router` ([`router_samples`]).
fn kernel_samples() -> Vec<KernelError> {
    use angzarr_diagnostic::Report;
    vec![
        KernelError::Stale {
            stream: sample_stream(),
            expected: 2,
            actual: 4,
        },
        KernelError::ExpectedSequenceAhead {
            stream: sample_stream(),
            expected: 7,
            head: 3,
        },
        KernelError::RetryCapExhausted {
            stream: sample_stream(),
            attempts: 3,
            cap: 3,
        },
        KernelError::Unresolved {
            stream: sample_stream(),
            expected: 2,
            actual: 4,
        },
        KernelError::RulesVersionMismatch {
            stream: sample_stream(),
            registered: "1.4.2+build.7".to_string(),
            reported: "1.5.0+build.9".to_string(),
        },
        KernelError::EventsNotItsStream {
            stream: sample_stream(),
            event_stream: v1::Cover {
                edition: Some("beta".to_string()),
                ..sample_stream()
            },
        },
        KernelError::Overlap {
            stream: sample_stream(),
            expected: 2,
            actual: 4,
            window_units: "items[a],items[b]".to_string(),
            command_units: "items[b].count".to_string(),
        },
        KernelError::UnknownType {
            stream: sample_stream(),
            type_url: "/toy.v9.Bonus".to_string(),
        },
        KernelError::InvalidPayload {
            stream: sample_stream(),
            type_url: "/toy.v1.Deposit".to_string(),
            reason: sample_report(),
        },
        KernelError::Upcast {
            stream: sample_stream(),
            error: UpcastError::Rule {
                source: "toy.v0.Credited".to_string(),
                error: sample_report(),
            },
        },
        KernelError::WrongDomain {
            stream: sample_stream(),
            expected: "ledger",
        },
        KernelError::ChainTooDeep { limit: 8 },
        KernelError::TooManySteps {
            stream: sample_stream(),
            limit: 16,
        },
        KernelError::BeforeSnapshot {
            stream: sample_stream(),
            first: 10,
        },
        KernelError::UnregisteredDomain {
            domain: sample_domain(),
        },
        KernelError::Unreadable {
            stream: sample_stream(),
            sequence: 0,
            reason: Report::of(&PayloadError::UnknownType {
                type_url: "/toy.v9.Bonus".to_string(),
            }),
        },
        KernelError::HandlerUnavailable {
            stream: sample_stream(),
            cause: "not-running".to_string(),
        },
    ]
}

/// Each [`ReplayError`] variant.
fn replay_samples() -> Vec<ReplayError> {
    vec![
        ReplayError::OutOfSequence { index: 1, found: 2 },
        ReplayError::RecordedBackwards { sequence: 4 },
        ReplayError::IncompleteMeta {
            sequence: 4,
            field: "correlation_id",
        },
        ReplayError::WrongDomain {
            found: "ledger".to_string(),
            expected: "account",
        },
    ]
}

/// The store's errors, angzarr-ports': forwarded under their own codes,
/// every one a step's reads and write, or an open migrating the store, can
/// meet (an edition's creation, deletion and removal are the
/// application's).
fn store_samples() -> Vec<StoreError> {
    use angzarr_ports::Cause;
    vec![
        StoreError::Conflict {
            stream: Box::new(sample_stream()),
            expected: 1,
            actual: 2,
        },
        StoreError::Discontinuous {
            stream: Box::new(sample_stream()),
            expected: 2,
            found: 3,
        },
        StoreError::MalformedStream {
            stream: Box::new(sample_stream()),
        },
        StoreError::Unavailable(Cause::new(
            "angzarr-store-sqlite-unavailable",
            [
                ("errno", angzarr_ports::Arg::Uint(5)),
                (
                    "ext_err_txt",
                    angzarr_ports::Arg::Str("database is locked".to_string()),
                ),
            ],
        )),
        StoreError::Unreadable {
            stream: Box::new(sample_stream()),
            sequence: 0,
            reason: Cause::new(
                "angzarr-store-sqlite-undecodable",
                [(
                    "ext_err_txt",
                    angzarr_ports::Arg::Str("buffer underflow".to_string()),
                )],
            ),
        },
        StoreError::EditionNotForked {
            stream: Box::new(sample_stream()),
        },
        StoreError::EditionNotFound {
            name: "what-if".to_string(),
        },
        StoreError::NewerThanBuild {
            applied: 6,
            build: 5,
        },
        StoreError::MigrationFailed { version: 3 },
        StoreError::MigrationChanged { version: 2 },
        StoreError::MigrationMissing { version: 2 },
        StoreError::MigrationUnknown { version: 7 },
        StoreError::MigrationIrreversible { version: 4 },
        StoreError::BackupExists {
            target: "/p/state.db.pre-4-20261031T120000.000Z".to_string(),
        },
    ]
}

/// Each [`PayloadError`] variant.
fn payload_samples() -> Vec<PayloadError> {
    vec![
        PayloadError::Invalid {
            type_url: "/toy.v1.Deposited".to_string(),
            reason: sample_report(),
        },
        PayloadError::UnknownType {
            type_url: "/toy.v9.Bonus".to_string(),
        },
        PayloadError::Decode {
            type_url: "/toy.v1.Deposited".to_string(),
            error: <Timestamp as prost::Message>::decode(&[0x08][..])
                .expect_err("a tag without its value is not a message"),
        },
    ]
}

/// An [`UpcastChainError`] of each [`ChainProblem`].
fn chain_samples() -> Vec<UpcastChainError> {
    [
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
        domain: sample_domain(),
        problem,
    })
    .into()
}

/// Each [`Misconfigured`] variant, `UnsupportedResolution` for a domain and
/// for one command type.
pub(crate) fn misconfigured_samples() -> Vec<Misconfigured> {
    use v1::ConflictResolution;
    let component = || sample_domain();
    let kind = |k: v1::ComponentKind| k.as_str_name().to_string();
    vec![
        Misconfigured::AlreadyRegistered {
            domain: component(),
        },
        Misconfigured::ComponentRegistered {
            component: "bookkeeping".to_string(),
        },
        Misconfigured::UpcasterUnknownDomain {
            domain: component(),
        },
        Misconfigured::ProjectorNoInputDomain {
            component: "balances".to_string(),
        },
        Misconfigured::UpcasterForAnotherDomain {
            domain: component(),
            upcaster: "ledger".to_string(),
        },
        Misconfigured::MissingRulesVersion {
            component: component(),
        },
        Misconfigured::StartingTypesOnNonPm {
            component: component(),
            kind: kind(v1::ComponentKind::Aggregate),
        },
        Misconfigured::MissingStateType {
            component: component(),
            kind: kind(v1::ComponentKind::Aggregate),
        },
        Misconfigured::StateOnStatelessKind {
            component: "balances".to_string(),
            kind: kind(v1::ComponentKind::Projector),
        },
        Misconfigured::PolicyInputCount {
            component: "bookkeeping".to_string(),
            input_count: 2,
        },
        Misconfigured::PolicyNoOutputDomain {
            component: "bookkeeping".to_string(),
        },
        Misconfigured::OutputDomainOnNonPolicy {
            component: component(),
            kind: kind(v1::ComponentKind::ProcessManager),
        },
        Misconfigured::StateResolutionsInvalid {
            component: component(),
        },
        Misconfigured::StateTypeMismatch {
            component: component(),
            type_url: "/toy.account.v1.Account".to_string(),
            state_type_url: "/toy.account.v1.Snapshot".to_string(),
        },
        Misconfigured::UnsupportedResolution {
            domain: component(),
            command_type: None,
            resolution: ConflictResolution::Commutative,
        },
        Misconfigured::UnsupportedResolution {
            domain: component(),
            command_type: Some("toy.cmd.v1.Note".to_string()),
            resolution: ConflictResolution::Commutative,
        },
        Misconfigured::RemoteSource {
            domain: component(),
            component: "bookkeeping".to_string(),
        },
    ]
}

/// Each [`UpcastError`] variant.
fn upcast_samples() -> Vec<UpcastError> {
    vec![
        UpcastError::Unavailable {
            cause: angzarr_diagnostic::Report::of(&KernelError::HandlerUnavailable {
                stream: sample_stream(),
                cause: "not-running".to_string(),
            }),
        },
        UpcastError::Rule {
            source: "toy.v0.Credited".to_string(),
            error: sample_report(),
        },
    ]
}

/// Each [`Failure`] variant with a code of its own.
fn failure_samples() -> Vec<Failure<WireError>> {
    vec![
        Failure::Downstream {
            path: Vec::new(),
            error: StepError::Kernel(KernelError::ChainTooDeep { limit: 8 }),
            dead_letters: Vec::new(),
        },
        Failure::Projection {
            committed: Vec::new(),
            failures: vec![("balances".to_string(), sample_report())],
            dead_letters: Vec::new(),
        },
    ]
}

/// The router's answer under each of its keys, every declared arg given:
/// forwarded under the key, verbatim (decision 14.60).
fn router_samples() -> impl Iterator<Item = KernelError> {
    let router = (1..).map_while(|n| angzarr_proto::v1::RouterReason::try_from(n).ok());
    router.map(|reason| {
        let args = reason
            .args()
            .iter()
            .map(|a| (a.to_string(), "1".to_string()));
        KernelError::Router(route::internal(reason.as_str_name(), args))
    })
}

/// Each [`WireError`] variant.
fn wire_samples() -> Vec<WireError> {
    vec![
        SAMPLE_MISSING,
        WireError::UnknownEnum {
            message: "toy.v1.Closed",
            field: "reason",
            value: 0,
        },
    ]
}
