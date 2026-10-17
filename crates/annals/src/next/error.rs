//! Errors, kept apart by whose they are: a domain's rejection is for the
//! person who gave the command; a [`KernelError`] is a fault in delivery,
//! storage or a log, for the operator.
//!
//! Each becomes a [`Diagnostic`](annals_diagnostic::Diagnostic) when it is
//! implemented, with the codes `docs/design/audit.md` lists and messages in
//! `locales/en-US/annals.ftl`. Until then they have none, so the catalog
//! parity check is not asked to sample a `todo!()`.

use annals_diagnostic::Report;

use super::book::Malformed;
use super::handler::Gap;
use super::id::{EventId, RulesVersion, Seq, StreamKey};
use super::meta::Origin;

/// Why a command did not commit.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Failure<R> {
    /// The domain refused it. Nothing was staged, appended or applied.
    Rejected(R),
    /// A step further down the chain failed: a policy's command rejected by
    /// its target, a gap, a chain too deep. `path` runs from the first
    /// reaction to the step that failed. Nothing was appended anywhere, the
    /// originating command's events included.
    Downstream { path: Vec<Step>, error: StepError },
    /// The kernel or the store failed at the originating stream or at the
    /// commit (a conflict on any stream the chain touched). Nothing was
    /// committed: the unit of work was not.
    Kernel(KernelError),
}

/// One step of a command chain: a policy's command delivered to `stream`,
/// caused by `cause`. Policies are pure, so the step's command is the
/// policy's output for that cause, recomputable from the log.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Step {
    /// The target stream; its domain is the target's
    /// [`Domain::NAME`](super::Domain::NAME).
    pub stream: StreamKey,
    pub cause: EventId,
}

/// Why a step down the chain failed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum StepError {
    /// The target's decide rejected the command: its rejection, as a
    /// report.
    Rejected(Report),
    /// A kernel fault at that step: a gap, the depth limit, a store read.
    Kernel(KernelError),
}

/// A fault outside any domain's rules.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum KernelError {
    /// The book was refused before business code ran.
    Malformed {
        stream: StreamKey,
        malformed: Malformed,
    },
    /// The stream is not at the length expected: a book's
    /// `expected_sequence` under STRICT (or past the head under any
    /// resolution), COMMUTATIVE's window overlapping the command, or a
    /// decision committed to a stream that moved. angzarr's ABORTED: re-read,
    /// then re-issue.
    Stale {
        stream: StreamKey,
        expected: Seq,
        actual: Seq,
    },
    /// A decide under AGGREGATE_HANDLES returned `Ok` without acknowledging
    /// the window with this delivery's nonce: its events were discarded,
    /// and the command fails as stale would. Also a handler bug, to log
    /// loudly.
    Unresolved {
        stream: StreamKey,
        expected: Seq,
        actual: Seq,
    },
    /// A delivery to `stream` skipped ahead of its position.
    Gap {
        stream: StreamKey,
        gap: Gap,
    },
    /// A reaction's target left the source's targets and came back: the
    /// targets of a source are not contiguous, so a backlog cannot be
    /// computed.
    TargetReturned {
        left: Seq,
        returned: Seq,
    },
    /// A book or a log for `stream` was given to an `expected` stream: a
    /// key of another domain. Caught at the origin of a chain and at
    /// replay; a reaction builds its target's key from the target's domain,
    /// so it cannot happen there.
    WrongDomain {
        stream: StreamKey,
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
        stream: StreamKey,
        first: Seq,
    },
    Replay(ReplayError),
    Store(StoreError),
}

/// Why a log could not be replayed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplayError {
    /// Event at `index` claims seq `found`.
    OutOfSequence { index: usize, found: Seq },
    /// Event `seq`'s causation names an event of its own stream at or after
    /// itself.
    CauseNotEarlier { seq: Seq, cause: Seq },
    /// Event `seq` was recorded before the event before it.
    RecordedBackwards { seq: Seq },
    /// The log is `found`'s, given to an `expected` stream.
    WrongDomain {
        found: String,
        expected: &'static str,
    },
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
        stream: StreamKey,
        expected: Seq,
        actual: Seq,
    },
    /// The events do not continue the stream: event `found` where `expected`
    /// was due. A caller bug; nothing was appended.
    Discontinuous {
        stream: StreamKey,
        expected: Seq,
        found: Seq,
    },
    /// The command keyed `origin` was already recorded in `stream`, as seqs
    /// `from..to` (see
    /// [`UnitOfWork::record_command`](super::UnitOfWork::record_command)).
    /// Nothing was committed; the earlier result stands: angzarr's
    /// DUPLICATE.
    Duplicate {
        stream: StreamKey,
        origin: Origin,
        from: Seq,
        to: Seq,
    },
    /// The key `origin` was already recorded in `stream` for a payload with
    /// another [`Fingerprint`](super::Fingerprint): the key was reused for a
    /// different command. Nothing was committed: angzarr's
    /// FAILED_PRECONDITION, never DUPLICATE. `from..to` is the earlier
    /// command's range, which the store reports for every key alike; the
    /// kernel answers with it only for a fact its domain configures
    /// [`FactMismatch::Duplicate`](super::FactMismatch::Duplicate) (as
    /// [`StoreError::DivergentDuplicate`]), never for a request or a
    /// causation.
    ///
    /// - **A client-supplied request id**: the caller reused it for another
    ///   command.
    /// - **A fact's `external_id`**, under the default
    ///   [`FactMismatch::Refuse`](super::FactMismatch::Refuse): the source
    ///   reused an id for another payload.
    /// - **A causation** (angzarr's KS-12: refuse as a framework bug): never
    ///   reached by the kernel, which records no causation key
    ///   ([`Origin::is_keyed`](super::Origin::is_keyed)); a store that is
    ///   handed one and finds it reused answers this too. annals has no
    ///   dead-letter queue: the chain fails loudly, back to its caller.
    ///
    /// All three are [`Recovery::Bug`].
    KeyReused {
        stream: StreamKey,
        origin: Origin,
        from: Seq,
        to: Seq,
    },
    /// A fact whose `external_id` was already recorded in `stream`, as seqs
    /// `from..to`, re-sent with another payload, and answered as a
    /// duplicate because its domain configures
    /// [`FactMismatch::Duplicate`](super::FactMismatch::Duplicate) for it.
    /// Nothing was committed; the earlier result stands, as for
    /// [`StoreError::Duplicate`] ([`Recovery::Done`]), and the application
    /// **raises an alert and a metric**: the kernel has neither, and this
    /// variant exists so the case cannot pass as an ordinary duplicate.
    /// Made by the kernel from the store's [`StoreError::KeyReused`]; a
    /// store never returns it.
    DivergentDuplicate {
        stream: StreamKey,
        origin: Origin,
        from: Seq,
        to: Seq,
    },
    /// The store failed: I/O, a timeout, a dropped connection. The adapter's
    /// own error, as a report. An append may or may not have landed.
    Unavailable(Report),
    /// The store holds event `seq` of `stream` but cannot give it back as
    /// an event: a payload type nothing decodes or upcasts (strict replay),
    /// bytes that do not decode. The adapter's error, as a report naming
    /// what it could not read (the type URL, for `annals-angzarr`).
    Unreadable {
        stream: StreamKey,
        seq: Seq,
        reason: Report,
    },
}

/// What to do about a failure: retry, treat it as done, or give up, and
/// why. annals never retries anything itself.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Recovery {
    /// Run the command again from a fresh read: a store conflict, a stale
    /// or unresolved command (re-read, and re-issue against the new head),
    /// the store unavailable (safe only under a recorded key: an
    /// unavailable commit may have landed), a gap (catch the target up,
    /// then retry).
    Retry,
    /// Already done: the store holds this command's events
    /// ([`StoreError::Duplicate`], or [`StoreError::DivergentDuplicate`],
    /// which the application also alerts on). Read them instead.
    Done,
    /// The domain said no: a rejection, at the origin or downstream. The
    /// same command will be refused the same way; tell the person.
    Business,
    /// A bug or a misconfiguration: a malformed book, a key reused for
    /// another payload ([`StoreError::KeyReused`]: a request id, a fact's
    /// external id, a causation), a discontinuous append, a key of the wrong domain, a chain too deep, unknown rules, a
    /// log that does not replay, an event the store cannot read back.
    /// Retrying will not help; an operator must.
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
        todo!("proposal: Stale, Unresolved, Gap Retry; Store by StoreError; the rest Bug")
    }
}

impl StoreError {
    pub fn recovery(&self) -> Recovery {
        todo!(
            "proposal: Conflict and Unavailable Retry; Duplicate and DivergentDuplicate Done; KeyReused and the rest Bug"
        )
    }
}
