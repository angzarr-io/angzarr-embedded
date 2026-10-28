//! Errors, kept apart by whose they are: a domain's rejection is for the
//! person who gave the command; a [`Malformed`] book is the caller's; a
//! [`KernelError`] is a fault in delivery, storage or a log, for the
//! operator.
//!
//! Each is a [`Diagnostic`], its codes under `annals-next-*` and its
//! messages in `locales/en-US/annals.ftl`; a wrapper of another error
//! ([`Failure::Rejected`], [`Failure::Kernel`], [`StepError`],
//! [`KernelError::Malformed`], [`KernelError::Replay`],
//! [`KernelError::Store`], [`UpcastError::Payload`]) is that error's
//! diagnostic. The store's errors are angzarr-ports' `StoreError`, shared
//! with angzarr: [`KernelError::Store`] forwards its `angzarr-ports-*` code
//! and args unchanged, and annals' catalog holds their messages. Every error about a payload names its type URL; a stream is
//! named as `domain/root`, with `@edition` outside the main timeline.

use std::fmt;

use angzarr_ports::StoreError;
use angzarr_proto::v1::{Cover, EventBook, EventRef, Uuid};
use annals_diagnostic::{Arg, Diagnostic, Report};

use super::check::Malformed;

/// Why a command did not commit.
#[derive(Clone, PartialEq, Debug)]
pub enum Failure<R> {
    /// The domain refused it. Nothing was staged, appended or applied.
    Rejected(R),
    /// A step further down the chain failed: a policy's command rejected by
    /// its target, a malformed policy book, a gap, a chain too deep. `path`
    /// runs from the first reaction to the step that failed. Nothing was
    /// appended anywhere, the originating command's events included.
    Downstream { path: Vec<Step>, error: StepError },
    /// The book was malformed, or the kernel or the store failed at the
    /// originating stream or at the commit (a conflict on any stream the
    /// chain touched). Nothing was committed.
    Kernel(KernelError),
    /// The chain COMMITTED, then marked-up projectors (run after the
    /// commit and awaited) failed: each `(projector, error)` in
    /// `failures`, in registration order; a failing projector does not
    /// stop the others. `committed` is what the chain appended, every
    /// stream's book: those events stay committed. Its recovery is
    /// [`Recovery::Retry`]: a retry is answered DUPLICATE.
    Projection {
        committed: Vec<EventBook>,
        failures: Vec<(&'static str, Report)>,
    },
}

/// One step of a command chain: a policy's command delivered to `stream`,
/// caused by `cause`. Policies are pure, so the step's command is the
/// policy's output for that cause, recomputable from the log.
#[derive(Clone, PartialEq, Debug)]
pub struct Step {
    pub stream: Cover,
    pub cause: EventRef,
}

/// Why a step down the chain failed.
#[derive(Clone, PartialEq, Debug)]
pub enum StepError {
    /// The target's decide rejected the command: its rejection, as a
    /// report.
    Rejected(Report),
    /// A kernel fault at that step. A malformed policy book is a bug here,
    /// not the caller's: it is also written to the dead-letter log.
    Kernel(KernelError),
}

/// A fault outside any domain's rules.
#[derive(Clone, PartialEq, Debug)]
pub enum KernelError {
    /// The book was refused at step 1, before anything else ran.
    Malformed(Malformed),
    /// The stream is not at the sequence expected: a book's
    /// `expected_sequence` under STRICT (or past the head under any
    /// resolution), COMMUTATIVE's window overlapping the command, or a
    /// decision committed to a stream that moved. angzarr's ABORTED:
    /// re-read, then re-issue.
    Stale {
        stream: Cover,
        expected: u64,
        actual: u64,
    },
    /// A decide under AGGREGATE_HANDLES returned `Ok` without acknowledging
    /// the window with this delivery's nonce: its events were discarded,
    /// and the command fails as stale would. A handler bug, logged loudly.
    Unresolved {
        stream: Cover,
        expected: u64,
        actual: u64,
    },
    /// A command's or fact's payload type that the domain does not decode,
    /// after the upcaster: refused naming its type URL (angzarr's
    /// INVALID_ARGUMENT). Found at step 5, after deduplication.
    UnknownType {
        stream: Cover,
        type_url: String,
    },
    /// A command's or fact's payload that decodes as a type the domain
    /// reads, but is not a valid value of it (a required field absent, an
    /// undefined enum value): the sender's to fix, as `UnknownType` is.
    InvalidPayload {
        stream: Cover,
        type_url: String,
        reason: Report,
    },
    /// A command's or fact's upcast rule failed.
    Upcast {
        stream: Cover,
        error: UpcastError,
    },
    /// An ingest panicked: a bug, never a business rejection. The chain was
    /// aborted, nothing was written, and the dead-letter log has the fact.
    IngestPanicked {
        stream: Cover,
        command_id: Uuid,
    },
    /// A book or a log for `stream` was given to an `expected` domain's
    /// code.
    WrongDomain {
        stream: Cover,
        expected: &'static str,
    },
    /// A command chain went deeper than its limit: a reaction cycle, or a
    /// cascade longer than allowed.
    ChainTooDeep {
        limit: usize,
    },
    /// A decision on `stream` ran more continuation steps than its limit
    /// ([`STEP_LIMIT`](super::markup::STEP_LIMIT)): a step that always
    /// continues. Nothing it emitted is kept.
    TooManySteps {
        stream: Cover,
        limit: usize,
    },
    /// A dispatch from before a resumed stream's snapshot: those events are
    /// not held.
    BeforeSnapshot {
        stream: Cover,
        first: u64,
    },
    /// A stored stream of `domain` was replayed, resumed or dispatched, or
    /// its typed step run, but the registry does not hold `domain`: the
    /// application did not register it. A misconfiguration, unlike an
    /// incoming book for an unregistered domain
    /// ([`Malformed::UnknownDomain`], the caller's).
    UnregisteredDomain {
        domain: String,
    },
    /// Event `sequence` of `stream` is stored but cannot be replayed as a
    /// current event: a payload type nothing upcasts or decodes (replay is
    /// strict), or bytes that do not decode as it. `reason` names what
    /// could not be read (the type URL). A bug or a missing upcaster.
    Unreadable {
        stream: Cover,
        sequence: u64,
        reason: Report,
    },
    Replay(ReplayError),
    /// The store's own error, angzarr-ports' (codes `angzarr-ports-*`).
    Store(StoreError),
}

/// Why a log could not be replayed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplayError {
    /// The page at `index` claims sequence `found`.
    OutOfSequence { index: usize, found: u64 },
    /// Event `sequence` was recorded before the event before it.
    RecordedBackwards { sequence: u64 },
    /// Event `sequence`'s `EventMeta` lacks `field` (`causation`,
    /// `correlation_id` or `recorded_at`), or holds it malformed: every
    /// recorded event has all three.
    IncompleteMeta { sequence: u64, field: &'static str },
    /// The log is `found`'s, given to an `expected` domain's code.
    WrongDomain {
        found: String,
        expected: &'static str,
    },
    /// Event `sequence` was decided under rules the rulebook does not hold.
    UnknownRules { sequence: u64, version: String },
}

/// A payload that is not one of the types a domain decodes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PayloadError {
    /// No decoder for this type URL: not a current type of the domain's
    /// (strict: never skipped).
    UnknownType { type_url: String },
    /// The type is known, but its bytes do not decode as it.
    Decode {
        type_url: String,
        error: prost::DecodeError,
    },
    /// The bytes decode, but the message is not a valid domain value: a
    /// required field absent, an enum value the domain does not define
    /// ([`WireError`](super::WireError)), or a state's own invariant.
    Invalid { type_url: String, reason: Report },
}

/// An upcast rule that failed on one payload.
#[derive(Clone, PartialEq, Debug)]
pub enum UpcastError {
    /// The rule could not read its input.
    Payload(PayloadError),
    /// The rule from `source` failed for the domain's own reason.
    Rule { source: String, error: Report },
}

/// An upcaster whose rules cannot all run, found by
/// [`Registry::check`](super::Registry::check) before anything is read.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UpcastChainError {
    /// The domain, named in every error.
    pub domain: String,
    pub problem: ChainProblem,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ChainProblem {
    /// Two typed rules from the same type: the second never runs.
    Duplicate { source: String },
    /// A typed rule from `source` registered before the rule that produces
    /// `source`: rules run in registration order, so a payload reaching
    /// `source` stops there.
    OutOfOrder { source: String, produced_by: String },
    /// A chain from `source` ends at `ends_at`, which the domain does not
    /// decode: every payload of that type would be refused.
    DeadEnd { source: String, ends_at: String },
}

/// What to do about a failure: retry, fix the request, or give up, and
/// why. annals never retries anything itself.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Recovery {
    /// Run the command again, with the same command id, in a fresh unit of
    /// work: a store conflict, a stale or unresolved command (re-read and
    /// re-issue against the new head), the store unavailable, a gap (catch
    /// the target up first). Safe, because a retry starts at the
    /// deduplication lookup: if the first attempt committed after all, the
    /// retry is answered DUPLICATE.
    Retry,
    /// The domain said no: a rejection, at the origin or downstream. The
    /// same command will be refused the same way; tell the person.
    Business,
    /// The caller's book was malformed, or names a payload type the domain
    /// does not decode (angzarr's INVALID_ARGUMENT). Fix the request.
    Invalid,
    /// A bug or a misconfiguration: a malformed policy book, a panicking
    /// ingest, a discontinuous append, a key of the wrong domain, a chain
    /// too deep, unknown rules, a log that does not replay, an event the
    /// store cannot read back. Retrying will not help; an operator must.
    Bug,
}

impl<R> Failure<R> {
    pub fn recovery(&self) -> Recovery {
        match self {
            Failure::Rejected(_) => Recovery::Business,
            Failure::Downstream { error, .. } => error.recovery(),
            Failure::Kernel(error) => error.recovery(),
            Failure::Projection { .. } => Recovery::Retry,
        }
    }
}

impl StepError {
    pub fn recovery(&self) -> Recovery {
        match self {
            StepError::Rejected(_) => Recovery::Business,
            StepError::Kernel(KernelError::Malformed(_)) => Recovery::Bug,
            StepError::Kernel(error) => error.recovery(),
        }
    }
}

impl KernelError {
    pub fn recovery(&self) -> Recovery {
        match self {
            KernelError::Malformed(_)
            | KernelError::UnknownType { .. }
            | KernelError::InvalidPayload { .. } => Recovery::Invalid,
            KernelError::Stale { .. } | KernelError::Unresolved { .. } => Recovery::Retry,
            KernelError::Store(error) => store_recovery(error),
            KernelError::Upcast { .. }
            | KernelError::IngestPanicked { .. }
            | KernelError::WrongDomain { .. }
            | KernelError::ChainTooDeep { .. }
            | KernelError::TooManySteps { .. }
            | KernelError::BeforeSnapshot { .. }
            | KernelError::UnregisteredDomain { .. }
            | KernelError::Unreadable { .. }
            | KernelError::Replay(_) => Recovery::Bug,
        }
    }
}

/// A stream as an error names it: `domain/root`, then `@edition` outside
/// the main timeline.
pub(crate) fn cover_text(cover: &Cover) -> String {
    let root = cover.root.as_ref().map(Uuid::to_string).unwrap_or_default();
    match &cover.edition {
        Some(edition) => format!("{}/{root}@{edition}", cover.domain),
        None => format!("{}/{root}", cover.domain),
    }
}

fn stream(cover: &Cover) -> Arg {
    cover_text(cover).into()
}

// ---------------------------------------------------------------- Failure

impl<R: fmt::Display> fmt::Display for Failure<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Rejected(rejection) => rejection.fmt(f),
            Failure::Downstream { path, error } => write!(
                f,
                "a policy command down the chain failed, at depth {}: {error}",
                path.len()
            ),
            Failure::Kernel(error) => error.fmt(f),
            Failure::Projection { failures, .. } => match failures.as_slice() {
                [(projector, error)] => write!(
                    f,
                    "the command committed, then projector {projector} failed: {error}"
                ),
                [(projector, error), ..] => write!(
                    f,
                    "the command committed, then {} projectors failed, the first {projector}: {error}",
                    failures.len()
                ),
                [] => f.write_str("the command committed"),
            },
        }
    }
}

impl<R: Diagnostic> Diagnostic for Failure<R> {
    fn code(&self) -> &'static str {
        match self {
            Failure::Rejected(rejection) => rejection.code(),
            Failure::Downstream { .. } => "annals-next-downstream",
            Failure::Kernel(error) => error.code(),
            Failure::Projection { .. } => "annals-next-projection-failed",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            Failure::Rejected(rejection) => rejection.args(),
            Failure::Downstream { path, error } => {
                vec![("depth", path.len().into()), ("error", Arg::nested(error))]
            }
            Failure::Kernel(error) => error.args(),
            Failure::Projection { failures, .. } => {
                let mut args = vec![("failures", failures.len().into())];
                if let Some((projector, error)) = failures.first() {
                    args.push(("projector", (*projector).into()));
                    args.push(("error", Arg::nested(error)));
                }
                args
            }
        }
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StepError::Rejected(rejection) => rejection.fmt(f),
            StepError::Kernel(error) => error.fmt(f),
        }
    }
}

impl Diagnostic for StepError {
    fn code(&self) -> &'static str {
        match self {
            StepError::Rejected(rejection) => rejection.code(),
            StepError::Kernel(error) => error.code(),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            StepError::Rejected(rejection) => rejection.args(),
            StepError::Kernel(error) => error.args(),
        }
    }
}

// ---------------------------------------------------------------- KernelError

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KernelError::Malformed(m) => m.fmt(f),
            KernelError::Stale {
                stream,
                expected,
                actual,
            } => write!(
                f,
                "stream {} is at {actual}, not the {expected} expected: re-read and re-issue",
                cover_text(stream)
            ),
            KernelError::Unresolved {
                stream,
                expected,
                actual,
            } => write!(
                f,
                "decide never acknowledged the window of stream {} from {expected} to {actual}",
                cover_text(stream)
            ),
            KernelError::UnknownType { stream, type_url } => write!(
                f,
                "stream {}'s domain does not decode the type {type_url}",
                cover_text(stream)
            ),
            KernelError::InvalidPayload {
                stream,
                type_url,
                reason,
            } => write!(
                f,
                "a {type_url} for stream {} is not a valid value: {reason}",
                cover_text(stream)
            ),
            KernelError::Upcast { stream, error } => write!(
                f,
                "upcasting a payload for stream {} failed: {error}",
                cover_text(stream)
            ),
            KernelError::IngestPanicked { stream, command_id } => write!(
                f,
                "ingesting fact {command_id} into stream {} panicked: a bug",
                cover_text(stream)
            ),
            KernelError::WrongDomain { stream, expected } => write!(
                f,
                "stream {} was given to the {expected} domain's code",
                cover_text(stream)
            ),
            KernelError::ChainTooDeep { limit } => {
                write!(f, "the command chain went deeper than its limit of {limit}")
            }
            KernelError::TooManySteps { stream, limit } => write!(
                f,
                "the decision on stream {} ran more than its limit of {limit} continuation steps",
                cover_text(stream)
            ),
            KernelError::BeforeSnapshot { stream, first } => write!(
                f,
                "stream {} holds its events from {first} on only, after its snapshot",
                cover_text(stream)
            ),
            KernelError::UnregisteredDomain { domain } => write!(
                f,
                "a stored {domain} stream was read, but no domain \"{domain}\" is registered"
            ),
            KernelError::Replay(error) => error.fmt(f),
            KernelError::Unreadable {
                stream,
                sequence,
                reason,
            } => write!(
                f,
                "event {sequence} of stream {} cannot be replayed: {reason}",
                cover_text(stream)
            ),
            KernelError::Store(error) => error.fmt(f),
        }
    }
}

impl Diagnostic for KernelError {
    fn code(&self) -> &'static str {
        match self {
            KernelError::Malformed(m) => m.code(),
            KernelError::Stale { .. } => "annals-next-stale",
            KernelError::Unresolved { .. } => "annals-next-unresolved",
            KernelError::UnknownType { .. } => "annals-next-unknown-type",
            KernelError::InvalidPayload { .. } => "annals-next-invalid-payload",
            KernelError::Upcast { .. } => "annals-next-upcast",
            KernelError::IngestPanicked { .. } => "annals-next-ingest-panicked",
            KernelError::WrongDomain { .. } => "annals-next-wrong-domain",
            KernelError::ChainTooDeep { .. } => "annals-next-chain-too-deep",
            KernelError::TooManySteps { .. } => "annals-next-too-many-steps",
            KernelError::BeforeSnapshot { .. } => "annals-next-before-snapshot",
            KernelError::UnregisteredDomain { .. } => "annals-next-unregistered-domain",
            KernelError::Replay(error) => error.code(),
            KernelError::Unreadable { .. } => "annals-next-unreadable",
            KernelError::Store(error) => error.code(),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            KernelError::Malformed(m) => m.args(),
            KernelError::Stale {
                stream: s,
                expected,
                actual,
            }
            | KernelError::Unresolved {
                stream: s,
                expected,
                actual,
            } => vec![
                ("stream", stream(s)),
                ("expected", (*expected).into()),
                ("actual", (*actual).into()),
            ],
            KernelError::UnknownType {
                stream: s,
                type_url,
            } => vec![
                ("stream", stream(s)),
                ("type_url", type_url.as_str().into()),
            ],
            KernelError::InvalidPayload {
                stream: s,
                type_url,
                reason,
            } => vec![
                ("stream", stream(s)),
                ("type_url", type_url.as_str().into()),
                ("reason", Arg::nested(reason)),
            ],
            KernelError::Upcast { stream: s, error } => {
                vec![("stream", stream(s)), ("error", Arg::nested(error))]
            }
            KernelError::IngestPanicked {
                stream: s,
                command_id,
            } => vec![
                ("stream", stream(s)),
                ("command_id", command_id.to_string().into()),
            ],
            KernelError::WrongDomain {
                stream: s,
                expected,
            } => {
                vec![("stream", stream(s)), ("expected", (*expected).into())]
            }
            KernelError::ChainTooDeep { limit } => vec![("limit", (*limit).into())],
            KernelError::TooManySteps { stream: s, limit } => {
                vec![("stream", stream(s)), ("limit", (*limit).into())]
            }
            KernelError::BeforeSnapshot { stream: s, first } => {
                vec![("stream", stream(s)), ("first", (*first).into())]
            }
            KernelError::UnregisteredDomain { domain } => {
                vec![("domain", domain.as_str().into())]
            }
            KernelError::Replay(error) => error.args(),
            KernelError::Unreadable {
                stream: s,
                sequence,
                reason,
            } => vec![
                ("stream", stream(s)),
                ("sequence", (*sequence).into()),
                ("reason", Arg::nested(reason)),
            ],
            KernelError::Store(error) => store_args(error),
        }
    }
}

// ---------------------------------------------------------------- ReplayError

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplayError::OutOfSequence { index, found } => write!(
                f,
                "page {index} of the log claims sequence {found}, not the one due"
            ),
            ReplayError::RecordedBackwards { sequence } => write!(
                f,
                "event {sequence} was recorded before the event before it"
            ),
            ReplayError::IncompleteMeta { sequence, field } => write!(
                f,
                "event {sequence}'s metadata lacks a well-formed {field}"
            ),
            ReplayError::WrongDomain { found, expected } => {
                write!(f, "a {found} log was given to the {expected} domain's code")
            }
            ReplayError::UnknownRules { sequence, version } => write!(
                f,
                "event {sequence} was decided under rules {version}, which the rulebook does not hold"
            ),
        }
    }
}

impl Diagnostic for ReplayError {
    fn code(&self) -> &'static str {
        match self {
            ReplayError::OutOfSequence { .. } => "annals-next-replay-out-of-sequence",
            ReplayError::RecordedBackwards { .. } => "annals-next-replay-recorded-backwards",
            ReplayError::IncompleteMeta { .. } => "annals-next-replay-incomplete-meta",
            ReplayError::WrongDomain { .. } => "annals-next-replay-wrong-domain",
            ReplayError::UnknownRules { .. } => "annals-next-replay-unknown-rules",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            ReplayError::OutOfSequence { index, found } => {
                vec![("index", (*index).into()), ("found", (*found).into())]
            }
            ReplayError::RecordedBackwards { sequence } => vec![("sequence", (*sequence).into())],
            ReplayError::IncompleteMeta { sequence, field } => {
                vec![("sequence", (*sequence).into()), ("field", (*field).into())]
            }
            ReplayError::WrongDomain { found, expected } => vec![
                ("found", found.as_str().into()),
                ("expected", (*expected).into()),
            ],
            ReplayError::UnknownRules { sequence, version } => vec![
                ("sequence", (*sequence).into()),
                ("version", version.as_str().into()),
            ],
        }
    }
}

// ---------------------------------------------------------------- StoreError

/// What to do about a store's error: angzarr-ports' [`Recovery`]
/// (`angzarr_ports::Recovery`), one to one (`Retry`, `Invalid`, `Bug`).
pub(crate) fn store_recovery(error: &StoreError) -> Recovery {
    let _ = error;
    todo!("red: map angzarr-ports' Recovery onto annals'")
}

/// A store error's args as annals' diagnostics carry them: angzarr-ports'
/// args by name, unchanged (`StoreError::args`), a `Uint` as a `Uint`, a
/// `Str` as a `Str`, and a wrapped `Cause` (the adapter's own code and
/// English) as a nested report under that code.
pub(crate) fn store_args(error: &StoreError) -> Vec<(&'static str, Arg)> {
    let _ = error;
    todo!("red: forward angzarr-ports' args")
}

/// A stored page the kernel cannot give back as a current event: its
/// payload upcasts or decodes as nothing the domain knows, or its bytes do
/// not decode as that type. The kernel's, never the store's (angzarr-ports'
/// `StoreError::Unreadable` is the store's: bytes it cannot return).
pub(crate) fn unreadable(stream: &Cover, sequence: u64, reason: Report) -> KernelError {
    let _ = (stream, sequence, reason);
    todo!("red: KernelError::Unreadable")
}

// ---------------------------------------------------------------- payloads

impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadError::UnknownType { type_url } => {
                write!(f, "no decoder for the type {type_url}")
            }
            PayloadError::Decode { type_url, error } => {
                write!(f, "a payload of type {type_url} does not decode: {error}")
            }
            PayloadError::Invalid { type_url, reason } => {
                write!(
                    f,
                    "a payload of type {type_url} is not a valid value: {reason}"
                )
            }
        }
    }
}

impl Diagnostic for PayloadError {
    fn code(&self) -> &'static str {
        match self {
            PayloadError::UnknownType { .. } => "annals-next-payload-unknown-type",
            PayloadError::Decode { .. } => "annals-next-payload-decode",
            PayloadError::Invalid { .. } => "annals-next-payload-invalid",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            PayloadError::UnknownType { type_url } | PayloadError::Decode { type_url, .. } => {
                vec![("type_url", type_url.as_str().into())]
            }
            PayloadError::Invalid { type_url, reason } => vec![
                ("type_url", type_url.as_str().into()),
                ("reason", Arg::nested(reason)),
            ],
        }
    }
}

impl fmt::Display for UpcastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpcastError::Payload(error) => error.fmt(f),
            UpcastError::Rule { source, error } => {
                write!(f, "the upcast rule from {source} failed: {error}")
            }
        }
    }
}

impl Diagnostic for UpcastError {
    fn code(&self) -> &'static str {
        match self {
            UpcastError::Payload(error) => error.code(),
            UpcastError::Rule { .. } => "annals-next-upcast-rule",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            UpcastError::Payload(error) => error.args(),
            UpcastError::Rule { source, error } => vec![
                ("source", source.as_str().into()),
                ("error", Arg::nested(error)),
            ],
        }
    }
}

impl fmt::Display for UpcastChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let domain = &self.domain;
        match &self.problem {
            ChainProblem::Duplicate { source } => write!(
                f,
                "domain {domain}'s upcaster has two typed rules from {source}: the second never runs"
            ),
            ChainProblem::OutOfOrder {
                source,
                produced_by,
            } => write!(
                f,
                "domain {domain}'s upcaster registers the rule from {source} before the rule from {produced_by}, which produces it"
            ),
            ChainProblem::DeadEnd { source, ends_at } => write!(
                f,
                "domain {domain}'s upcaster takes {source} to {ends_at}, which the domain does not decode"
            ),
        }
    }
}

impl Diagnostic for UpcastChainError {
    fn code(&self) -> &'static str {
        match self.problem {
            ChainProblem::Duplicate { .. } => "annals-next-upcaster-duplicate",
            ChainProblem::OutOfOrder { .. } => "annals-next-upcaster-out-of-order",
            ChainProblem::DeadEnd { .. } => "annals-next-upcaster-dead-end",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        let mut args = vec![("domain", self.domain.as_str().into())];
        match &self.problem {
            ChainProblem::Duplicate { source } => args.push(("source", source.as_str().into())),
            ChainProblem::OutOfOrder {
                source,
                produced_by,
            } => {
                args.push(("source", source.as_str().into()));
                args.push(("produced_by", produced_by.as_str().into()));
            }
            ChainProblem::DeadEnd { source, ends_at } => {
                args.push(("source", source.as_str().into()));
                args.push(("ends_at", ends_at.as_str().into()));
            }
        }
        args
    }
}
