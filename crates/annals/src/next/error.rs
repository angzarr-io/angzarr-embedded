//! Errors, kept apart by whose they are: a domain's rejection is for the
//! person who gave the command; a [`KernelError`] is a fault in delivery,
//! storage or a log, for the operator.
//!
//! Each becomes a [`Diagnostic`](annals_diagnostic::Diagnostic) when it is
//! implemented, with the codes `docs/design/audit.md` lists and messages in
//! `locales/en-US/annals.ftl`. Until then they have none, so the catalog
//! parity check is not asked to sample a `todo!()`.

use annals_diagnostic::Report;

use super::handler::Gap;
use super::id::{CommandId, EventId, RulesVersion, Seq, StreamId};

use super::version::UpcastError;

/// Why a command did not commit.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Failure<R> {
    /// The domain refused it. Nothing was staged, appended or applied.
    Rejected(R),
    /// A step further down the chain failed: a policy's command rejected
    /// by its target, a translation's fact refused, a chain too deep.
    /// `path` runs from the first reaction to the step that failed. Nothing
    /// was appended anywhere, the originating command's events included.
    Downstream { path: Vec<Step>, error: StepError },
    /// The kernel or the store failed at the originating stream or at the
    /// commit (a conflict on any stream the chain touched). Nothing was
    /// committed: the unit of work was not.
    Kernel(KernelError),
}

/// One step of a command chain: a command or a fact delivered to `stream`,
/// caused by `cause`. Reactions are pure, so the step's command or fact is
/// the reaction's output for that cause, recomputable from the log.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Step {
    pub stream: StreamId,
    /// The target domain's [`Domain::NAME`](super::Domain::NAME).
    pub domain: &'static str,
    pub kind: StepKind,
    pub cause: EventId,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum StepKind {
    /// A policy's command.
    Command,
    /// A translation's fact.
    Fact,
}

/// Why a step down the chain failed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum StepError {
    /// The target's decide rejected the command: its rejection, as a
    /// report.
    Rejected(Report),
    /// A kernel fault at that step: a gap, a refused fact, the depth limit,
    /// a store read.
    Kernel(KernelError),
}

/// A fault outside any domain's rules.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum KernelError {
    /// A decision committed to a stream that is not the one, or not at the
    /// length, it was decided against.
    Stale {
        stream: StreamId,
        expected: Seq,
        actual: Seq,
    },
    /// A delivery to `stream` skipped ahead of its position.
    Gap {
        stream: StreamId,
        gap: Gap,
    },
    /// A reaction's target left the source's targets and came back: the
    /// targets of a source are not contiguous, so a backlog cannot be
    /// computed.
    TargetReturned {
        left: Seq,
        returned: Seq,
    },
    /// `stream` refused a fact for integrity: retired, or never created.
    /// The domain's own reason, as a report.
    FactRefused {
        stream: StreamId,
        reason: Report,
    },
    /// A command chain went deeper than its limit: a reaction cycle, or a
    /// cascade longer than allowed.
    ChainTooDeep {
        limit: usize,
    },
    /// A log names rules the rulebook does not hold.
    UnknownRules {
        seq: Seq,
        version: RulesVersion,
    },
    Replay(ReplayError),
    Upcast(UpcastError),
    Store(StoreError),
}

/// Why a log could not be replayed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplayError {
    /// Event at `index` claims seq `found`.
    OutOfSequence { index: usize, found: Seq },
    /// Event `seq` names an event of its own stream at or after itself as
    /// its cause.
    CauseNotEarlier { seq: Seq, cause: Seq },
    /// Event `seq` is at schema `found`, not the code's `current`: it needs
    /// upcasting (or the code is older than the log).
    SchemaNotCurrent { seq: Seq, found: u32, current: u32 },
    /// Event `seq` was decided under rules the rulebook does not hold.
    UnknownRules { seq: Seq, version: RulesVersion },
}

/// Why a store refused or failed an append or a read. A conflict and an
/// infrastructure failure are different answers: a conflict means "re-read
/// and decide again"; `Unavailable` means "the store could not say".
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum StoreError {
    /// Someone else appended first: the stream is at `actual`, not
    /// `expected`. Nothing was appended.
    Conflict {
        stream: StreamId,
        expected: Seq,
        actual: Seq,
    },
    /// The events do not continue the stream: event `found` where `expected`
    /// was due. A caller bug; nothing was appended.
    Discontinuous {
        stream: StreamId,
        expected: Seq,
        found: Seq,
    },
    /// The append's header is not the stream's. A caller bug; nothing was
    /// appended.
    HeaderChanged { stream: StreamId },
    /// The command that recorded `events` was already recorded in `stream`,
    /// as seqs `from..to` (see the idempotency note on
    /// [`EventStore::append`](super::EventStore::append)). Nothing was
    /// appended; the earlier result stands.
    Duplicate {
        stream: StreamId,
        command: CommandId,
        from: Seq,
        to: Seq,
    },
    /// The store failed: I/O, a timeout, a dropped connection. The adapter's
    /// own error, as a report. An append may or may not have landed.
    Unavailable(Report),
}

/// What a wrapping runtime (angzarr) should do about a failure: retry,
/// treat it as done, or give up, and why. annals never retries anything
/// itself.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Recovery {
    /// Run the command again from a fresh read: a store conflict (re-read
    /// and re-decide), the store unavailable (safe only under an
    /// idempotency key: an unavailable append may have landed), a gap
    /// (catch the target up, then retry).
    Retry,
    /// Already done: the store holds this command's events
    /// ([`StoreError::Duplicate`]). Read them instead.
    Done,
    /// The domain said no: a rejection, at the origin or downstream. The
    /// same command will be refused the same way; tell the person, or
    /// compensate.
    Business,
    /// A fact was refused for integrity: a retired or unknown target.
    Integrity,
    /// A bug or a misconfiguration: a stale decision, a discontinuous
    /// append, a changed header, a chain too deep, unknown rules, a log
    /// that does not replay or upcast. Retrying will not help; an operator
    /// must.
    Bug,
}

impl<R> Failure<R> {
    pub fn recovery(&self) -> Recovery {
        todo!("proposal: Rejected Business; Downstream by StepError; Kernel by KernelError")
    }
}

impl StepError {
    pub fn recovery(&self) -> Recovery {
        todo!("proposal: Rejected Business; Kernel by KernelError")
    }
}

impl KernelError {
    pub fn recovery(&self) -> Recovery {
        todo!("proposal: Gap Retry; FactRefused Integrity; Store by StoreError; the rest Bug")
    }
}

impl StoreError {
    pub fn recovery(&self) -> Recovery {
        todo!("proposal: Conflict and Unavailable Retry; Duplicate Done; the rest Bug")
    }
}
