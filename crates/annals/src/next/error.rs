//! Errors, kept apart by whose they are: a domain's rejection is for the
//! person who gave the command; a [`Malformed`] book is the caller's; a
//! [`KernelError`] is a fault in delivery, storage or a log, for the
//! operator.
//!
//! Each becomes a [`Diagnostic`](annals_diagnostic::Diagnostic) when it is
//! implemented, with the codes `docs/design/audit.md` lists and messages in
//! `locales/en-US/annals.ftl`. Until then they have none, so the catalog
//! parity check is not asked to sample a `todo!()`. Every error about a
//! payload names its type URL.

use angzarr_proto::v1::{Cover, EventRef, Uuid};
use annals_diagnostic::Report;

use super::check::Malformed;
use super::handler::Gap;

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
    /// A delivery to `stream` skipped ahead of its position.
    Gap {
        stream: Cover,
        gap: Gap,
    },
    /// A reaction's target left the source's targets and came back: the
    /// targets of a source are not contiguous, so a backlog cannot be
    /// computed.
    TargetReturned {
        left: u64,
        returned: u64,
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
    /// A dispatch from before a resumed stream's snapshot: those events are
    /// not held.
    BeforeSnapshot {
        stream: Cover,
        first: u64,
    },
    Replay(ReplayError),
    Store(StoreError),
}

/// Why a log could not be replayed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplayError {
    /// The page at `index` claims sequence `found`.
    OutOfSequence { index: usize, found: u64 },
    /// Event `sequence`'s `causation.triggering_event` names an event of
    /// its own stream at or after itself.
    CauseNotEarlier { sequence: u64, cause: u64 },
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

/// Why a store refused or failed an append or a read. A conflict and an
/// infrastructure failure are different answers: a conflict means "re-read
/// and run again"; `Unavailable` means "the store could not say".
#[derive(Clone, PartialEq, Debug)]
pub enum StoreError {
    /// Someone else appended first: the stream is at `actual`, not
    /// `expected`. Nothing was appended. Running the command again starts
    /// at the deduplication lookup, which finds the winner's events if it
    /// was this same command.
    Conflict {
        stream: Cover,
        expected: u64,
        actual: u64,
    },
    /// The pages do not continue the stream: sequence `found` where
    /// `expected` was due. A caller bug; nothing was appended.
    Discontinuous {
        stream: Cover,
        expected: u64,
        found: u64,
    },
    /// The store failed: I/O, a timeout, a dropped connection. The adapter's
    /// own error, as a report. An append may or may not have landed.
    Unavailable(Report),
    /// The store holds event `sequence` of `stream` but it cannot be given
    /// back as a current event: a payload type nothing upcasts or decodes
    /// (strict replay), bytes that do not decode, metadata that is
    /// incomplete. A report naming what could not be read (the type URL).
    Unreadable {
        stream: Cover,
        sequence: u64,
        reason: Report,
    },
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
        todo!("proposal: Rejected Business; Downstream by StepError; Kernel by KernelError")
    }
}

impl StepError {
    pub fn recovery(&self) -> Recovery {
        todo!("proposal: Rejected Business; a malformed policy book Bug; else by KernelError")
    }
}

impl KernelError {
    pub fn recovery(&self) -> Recovery {
        todo!(
            "proposal: Malformed and UnknownType Invalid; Stale, Unresolved, Gap Retry; \
             Store by StoreError; the rest Bug"
        )
    }
}

impl StoreError {
    pub fn recovery(&self) -> Recovery {
        todo!("proposal: Conflict and Unavailable Retry; the rest Bug")
    }
}
