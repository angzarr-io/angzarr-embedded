//! Dead letters: what the kernel gives up on, as angzarr's
//! `AngzarrDeadLetter`.
//!
//! angzarr dead-letters what it cannot deliver and cannot refuse to a
//! caller: a fact whose ingest failed (answered PARKED), a reactor's
//! malformed command. angzarr-embedded is in process, so there is no queue
//! and no relay. Every letter goes three ways:
//!
//! - **to the application's [`DeadLetterLog`]**, at once, as one line;
//! - **into the store, durably** (owner ruling Q5, 2026-10-31), when the
//!   step that raised it has no caller left: a policy's step, kept in the
//!   SAME write as the position it moves past (or in a write of its own
//!   when the position stays), a projector's, a parked fact's. A store
//!   that keeps them is an angzarr-ports
//!   [`DeadLetterStore`](super::DeadLetterStore); one that does not drops
//!   them. A letter is keyed by the stream it is about and its place there
//!   (its events' first sequence, its command's id, or else its step's
//!   trigger) and its component, so a step given up on again replaces it;
//! - **back to the caller**, in the call's answer
//!   ([`Handled::dead_letters`](super::Handled::dead_letters),
//!   [`Failure::dead_letters`](super::Failure::dead_letters)): every letter
//!   it kept.
//!
//! A command its caller waits for is the caller's (owner ruling,
//! 2026-10-31): its own failure is answered and its line logged, never
//! kept nor answered as a letter.
//!
//! A letter's reason is the key the spec spells and its args, as they were
//! answered (decision 14.48, as amended), never angzarr-embedded's own
//! code, and never English (decision 14.37); and it carries the one
//! `rejection_details` variant its reason chooses, by the mapping on
//! `AngzarrDeadLetter.rejection_details` in angzarr's `types.proto`:
//!
//! - a decide under AGGREGATE_HANDLES that never acknowledged its window
//!   ([`KernelError::Unresolved`](super::KernelError::Unresolved)):
//!   `COORDINATOR_REASON_CONFLICT_UNRESOLVED`, `sequence_mismatch` with the
//!   command's expected sequence and the head (as are a stale, an ahead
//!   and an overlapping sequence's keys);
//! - a policy's command its target refused with a typed refusal, which no
//!   OnRefused answers: `ROUTER_REASON_COMMAND_REFUSED` with its
//!   `refusal_type`, `command_refused` holding the refusal object whole;
//!   an untyped refusal, under the handler's own key, `processing_failed`;
//! - a policy's step or a fact given up at a cap (a command its caller
//!   waits for is the caller's, answered and never dead-lettered: owner
//!   ruling, 2026-10-31), every write lost to another write to its stream ([`KernelError::RetryCapExhausted`](super::KernelError::RetryCapExhausted),
//!   decision 14.46): `COORDINATOR_REASON_RETRY_CAP_EXHAUSTED` with its
//!   attempts and the cap, `processing_failed`, `retry_count` the retries
//!   (the attempts after the first, as `ProcessingFailedDetails.retry_count`
//!   counts them), transient;
//! - a fatal bug, `processing_failed`, never retried, not transient: a
//!   handler's ([`KernelError::Router`](super::KernelError::Router) under
//!   INTERNAL, UNKNOWN or DATA_LOSS), the router's
//!   `ROUTER_REASON_UNHANDLED_HANDLER_ERROR` with the handler's own code
//!   and args its `cause` (a panic's `ROUTER_REASON_HANDLER_PANICKED`), or
//!   whatever key a remote handler answered; the router's own error on
//!   malformed input; a policy's own error
//!   ([`StepError::PolicyFailed`](super::StepError::PolicyFailed)), keyed
//!   as the router keys a handler's bug; the kernel's refusal of a
//!   policy's answer (`COORDINATOR_REASON_POLICY_COMMAND_*`, an
//!   identity other than the stamp, a Fact, a `fact_origin` and a domain
//!   it cannot route, `COORDINATOR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN`,
//!   among them); a chain deeper than its limit
//!   (`COORDINATOR_REASON_CHAIN_TOO_DEEP`) and a decision past its step
//!   limit (`ROUTER_REASON_TOO_MANY_STEPS`), each with its `limit`
//!   (decision 14.49); a projector's failure; a book the kernel built
//!   malformed;
//! - a step whose retryable failure outlasted its retries
//!   ([`OnRetryable`](super::OnRetryable)): the failure's own key as last
//!   answered (`COORDINATOR_REASON_HANDLER_UNAVAILABLE`,
//!   `COORDINATOR_REASON_STORE_UNAVAILABLE`,
//!   `COORDINATOR_REASON_UPCASTER_UNAVAILABLE`, a handler's retryable bug, a
//!   policy step's ABORTED; every retryable failure carries a key, decision
//!   14.51), `processing_failed`, `retry_count` its retries, transient.
//!
//! Every letter names the component that could not take its payload and
//! its kind ([`Component`]: `source_component`, `source_component_kind`):
//! a policy, a projector, or a domain of the kind the registry holds it as,
//! UNSPECIFIED for a domain handled by a coordinator elsewhere.
//!
//! Every line starts with [`DEAD_LETTER_PREFIX`]: **anything arriving there
//! almost certainly indicates a problem**, never business as usual, so an
//! operator should alert on the prefix.

use std::sync::Mutex;

use angzarr_diagnostic::Diagnostic;
use angzarr_proto::v1::angzarr_dead_letter::{Payload, RejectionDetails};
use angzarr_proto::v1::{
    command_page, AngzarrDeadLetter, CommandRefusedDetails, ComponentKind, Cover,
    ProcessingFailedDetails, SequenceMismatchDetails,
};
use angzarr_router::error::CodedError;

use super::catalog::Catalog;
use super::check::{Identified, Malformed};
use super::error::{cover_text, KernelError};
use super::payload::type_name;
use super::route::{self, diagnostic, flat_args, Rejected};
use super::store::{DeadLetter, DeadLetterAt, StreamRef};
use angzarr_proto::v1::{reason_args as args, CoordinatorReason};

/// The start of every dead-letter line: a key to alert on.
pub const DEAD_LETTER_PREFIX: &str = "DEAD_LETTER";

/// Where dead letters go: the application's log. `&self`, so a shared
/// logger needs no lock of the kernel's.
pub trait DeadLetterLog: Send + Sync {
    /// One dead letter as its line ([`dead_letter_line`]).
    fn write(&self, line: &str);

    /// One dead letter, whole, as angzarr's `AngzarrDeadLetter`: its
    /// payload, its reason and its rejection details. Written as its line
    /// unless a log keeps more.
    fn letter(&self, letter: &AngzarrDeadLetter) {
        self.write(&dead_letter_line(letter));
    }
}

/// A dead letter as one line, keys and values only: [`DEAD_LETTER_PREFIX`],
/// the component that produced it, the reason's code and its args as
/// `name=value` in name order (a value quoted where it would not parse
/// back, `angzarr_diagnostic::quote`), then any metadata, a retried step's
/// `retry_count`, and the command's `command_id` and `stream`, or the
/// event's `sequence` and `stream` (the failed events', or an edition
/// removal's `EditionDeleted`), as the letter holds. Built from
/// angzarr's own `AngzarrDeadLetter`, so a line and an angzarr DLQ entry
/// say the same things.
pub fn dead_letter_line(letter: &AngzarrDeadLetter) -> String {
    let mut line = format!("{DEAD_LETTER_PREFIX} {}:", letter.source_component);
    let pairs = |map: &std::collections::HashMap<String, String>| {
        let mut pairs: Vec<(String, String)> =
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        pairs.sort();
        pairs
    };
    let mut fields = Vec::new();
    if let Some(reason) = &letter.reason {
        line.push_str(&format!(" {}", reason.code));
        fields.extend(pairs(&reason.args));
    }
    fields.extend(pairs(&letter.metadata));
    if let Some(RejectionDetails::ProcessingFailed(details)) = &letter.rejection_details {
        if details.retry_count > 0 {
            fields.push(("retry_count".to_string(), details.retry_count.to_string()));
        }
    }
    match &letter.payload {
        Some(Payload::RejectedEvents(book) | Payload::FailedEditionRemoval(book)) => {
            if let (Some(page), Some(cover)) = (book.pages.first(), &book.cover) {
                fields.push(("sequence".to_string(), page.sequence.to_string()));
                fields.push(("stream".to_string(), cover_text(cover)));
            }
        }
        Some(Payload::RejectedCommand(book)) => {
            if let Some(id) = book.page.as_ref().and_then(|p| p.command_id.as_ref()) {
                fields.push(("command_id".to_string(), id.to_string()));
            }
            if let Some(cover) = &book.cover {
                fields.push(("stream".to_string(), cover_text(cover)));
            }
        }
        None => {}
    }
    let mut named = std::collections::BTreeSet::new();
    for (name, value) in fields {
        // The payload's stream, where the reason names it already, once.
        if named.insert(name.clone()) {
            line.push_str(&format!(" {name}={}", angzarr_diagnostic::quote(&value)));
        }
    }
    line
}

/// The component a letter is from (`AngzarrDeadLetter.source_component`
/// and `source_component_kind`): the one that could not take its payload.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) struct Component<'a> {
    pub(crate) name: &'a str,
    pub(crate) kind: ComponentKind,
}

impl<'a> Component<'a> {
    /// The policy `name`.
    pub(crate) fn policy(name: &'a str) -> Self {
        Component {
            name,
            kind: ComponentKind::Policy,
        }
    }

    /// The projector `name`.
    pub(crate) fn projector(name: &'a str) -> Self {
        Component {
            name,
            kind: ComponentKind::Projector,
        }
    }

    /// The domain `name`, of the kind `registry` holds it as
    /// ([`Catalog::kind`]).
    pub(crate) fn domain(catalog: &Catalog, name: &'a str) -> Self {
        Component {
            name,
            kind: catalog.kind(name),
        }
    }
}

/// Why the kernel dead-letters a payload, as it holds it.
pub(crate) enum Cause<'a> {
    /// A kernel error: a bug, a conflict, a cap.
    Kernel(&'a KernelError),
    /// A book the kernel built (a policy's command, a refusal fact) that
    /// its own check refused: a bug.
    Malformed(&'a Malformed),
    /// A policy's command its target refused, which no OnRefused answers.
    Refused(&'a Rejected),
    /// A policy's own error: its `on_event`'s, its `on_refused`'s, or its
    /// source's `#[on_refused]` handler's, a handler's bug.
    PolicyFailed(&'a dyn Diagnostic),
}

/// Write the dead letter for `payload`, which `component` could not take
/// for `cause` ([`letter`]).
pub(crate) fn write(log: &dyn DeadLetterLog, payload: Payload, component: Component, cause: Cause) {
    log.letter(&letter(payload, component, cause));
}

/// Write the dead letter for `command`, which its domain could not take
/// for the kernel's `error`.
pub(crate) fn write_command(
    log: &dyn DeadLetterLog,
    catalog: &Catalog,
    command: &Identified,
    error: &KernelError,
) {
    write(
        log,
        Payload::RejectedCommand(command.to_book()),
        Component::domain(catalog, command.domain()),
        Cause::Kernel(error),
    );
}

/// Write the dead letter for `payload` (a command, or a projector's
/// events), whose step at `component` failed for the retryable `cause`
/// after `retries` retries ([`at_cap`]).
pub(crate) fn write_at_cap(
    log: &dyn DeadLetterLog,
    payload: Payload,
    component: Component,
    cause: Cause,
    retries: u32,
) {
    log.letter(&at_cap(payload, component, cause, retries));
}

/// The dead letter for `payload`, which `component` could not take for
/// `cause`: its reason ([`reason`]), and the one detail that reason
/// chooses (decision 14.48): `sequence_mismatch` for a sequence's key,
/// `command_refused` for a typed refusal, and otherwise
/// `processing_failed`, the same Diagnostic as its failure, transient with
/// its retries (the attempts after the first) at the retry cap, else never
/// retried and not transient. It names `component` and its kind.
pub(crate) fn letter(payload: Payload, component: Component, cause: Cause) -> AngzarrDeadLetter {
    let reason = reason(&cause, &payload);
    let details = match cause {
        Cause::Kernel(
            KernelError::Stale {
                expected, actual, ..
            }
            | KernelError::Overlap {
                expected, actual, ..
            }
            | KernelError::Unresolved {
                expected, actual, ..
            },
        ) => sequence_mismatch(*expected, *actual),
        Cause::Kernel(KernelError::ExpectedSequenceAhead { expected, head, .. }) => {
            sequence_mismatch(*expected, *head)
        }
        // The retries: the runs after the first.
        Cause::Kernel(KernelError::RetryCapExhausted { attempts, .. }) => {
            processing_failed(&reason, attempts.saturating_sub(1), true)
        }
        Cause::Refused(rejected) => match rejected.refusal_object() {
            Some(refusal) => RejectionDetails::CommandRefused(CommandRefusedDetails {
                refusal_type: type_name(&refusal.type_url).to_string(),
                refusal: Some(refusal.clone()),
            }),
            None => processing_failed(&reason, 0, false),
        },
        _ => processing_failed(&reason, 0, false),
    };
    AngzarrDeadLetter {
        payload: Some(payload),
        reason: Some(reason),
        rejection_details: Some(details),
        source_component: component.name.to_string(),
        source_component_kind: component.kind as i32,
        ..AngzarrDeadLetter::default()
    }
}

/// The dead letter for `payload` (a command, or a projector's events),
/// whose step at `component` failed for the retryable `cause` after
/// `retries` retries: its reason the failure's own key and args as last
/// answered, never the retry cap's (decision 14.48, as amended), and
/// `processing_failed`, transient, with its retry count. It names
/// `component` and its kind.
pub(crate) fn at_cap(
    payload: Payload,
    component: Component,
    cause: Cause,
    retries: u32,
) -> AngzarrDeadLetter {
    let reason = reason(&cause, &payload);
    AngzarrDeadLetter {
        payload: Some(payload),
        rejection_details: Some(processing_failed(&reason, retries, true)),
        reason: Some(reason),
        source_component: component.name.to_string(),
        source_component_kind: component.kind as i32,
        ..AngzarrDeadLetter::default()
    }
}

fn processing_failed(
    reason: &angzarr_proto::v1::Diagnostic,
    retry_count: u32,
    is_transient: bool,
) -> RejectionDetails {
    RejectionDetails::ProcessingFailed(ProcessingFailedDetails {
        failure: Some(reason.clone()),
        retry_count,
        is_transient,
        ..ProcessingFailedDetails::default()
    })
}

fn sequence_mismatch(expected: u64, actual: u64) -> RejectionDetails {
    RejectionDetails::SequenceMismatch(SequenceMismatchDetails {
        expected_sequence: expected,
        actual_sequence: actual,
    })
}

/// The reason a letter for `cause` records: the key the spec spells and
/// its args, as the wire carries them (decision 14.48). A kernel error is
/// its key on the wire ([`KernelError::as_coded`]); the router's error,
/// and a remote handler's, the key it answered; a policy's own error the
/// router's key for a handler's bug, its code and args the `cause`; a
/// refusal the refusing handler's key (a typed one's
/// `ROUTER_REASON_COMMAND_REFUSED`). An error the spec has no key for
/// keeps angzarr-embedded's code.
pub(crate) fn reason(cause: &Cause, payload: &Payload) -> angzarr_proto::v1::Diagnostic {
    match cause {
        Cause::Kernel(KernelError::Malformed(malformed)) => malformed_reason(malformed, payload),
        Cause::Malformed(malformed) => malformed_reason(malformed, payload),
        Cause::Kernel(KernelError::Router(error)) => coded(error),
        Cause::Kernel(error) => error
            .as_coded()
            .map_or_else(|| diagnostic(*error), |error| coded(&error)),
        Cause::Refused(rejected) => rejected.wire(),
        Cause::PolicyFailed(failure) => {
            coded(&CodedError::bug(failure.code(), flat_args(*failure)))
        }
    }
}

/// A book the kernel built that its check refused, by the coordinator's
/// key where the spec has one, naming the book's domain and its payload's
/// type: Handling on a policy's command
/// (`COORDINATOR_REASON_POLICY_COMMAND_WITH_HANDLING`), a policy's command
/// with no root (`COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT`), naming its
/// own command id or causation (`COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY`,
/// and the `field`) or correlation
/// (`COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION`), a fact
/// (`COORDINATOR_REASON_POLICY_COMMAND_IS_FACT`), a command carrying
/// `fact_origin` (`COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN`), a
/// refusal fact for another stream
/// (`COORDINATOR_REASON_REFUSAL_FACT_WRONG_STREAM`, its domain); any other
/// under angzarr-embedded's code.
fn malformed_reason(malformed: &Malformed, payload: &Payload) -> angzarr_proto::v1::Diagnostic {
    let book = match payload {
        Payload::RejectedCommand(book) => Some(book),
        _ => None,
    };
    let domain = book
        .and_then(|b| b.cover.as_ref())
        .map(|c| c.domain.clone())
        .unwrap_or_default();
    let command_type = book
        .and_then(|b| b.page.as_ref())
        .and_then(|p| match &p.body {
            Some(command_page::Body::Command(c)) => c.payload.as_ref(),
            Some(command_page::Body::Fact(f)) => f.payload.as_ref(),
            None => None,
        })
        .map(|p| type_name(&p.type_url).to_string())
        .unwrap_or_default();
    let named = || {
        [
            (args::DOMAIN.to_string(), domain.clone()),
            (args::COMMAND_TYPE.to_string(), command_type.clone()),
        ]
    };
    let naming = |reason: CoordinatorReason| coded(&route::internal(reason.as_str_name(), named()));
    match malformed {
        Malformed::HandlingOnPolicyCommand => naming(CoordinatorReason::PolicyCommandWithHandling),
        Malformed::NoRoot | Malformed::BadRoot { .. } => {
            naming(CoordinatorReason::PolicyCommandNoRoot)
        }
        Malformed::IdOnPolicyCommand {
            field: "correlation_id",
        } => naming(CoordinatorReason::PolicyCommandOtherCorrelation),
        Malformed::IdOnPolicyCommand { field } => coded(&route::internal(
            CoordinatorReason::PolicyCommandOtherIdentity.as_str_name(),
            named()
                .into_iter()
                .chain([(args::FIELD.to_string(), field.to_string())]),
        )),
        Malformed::FactFromPolicy => naming(CoordinatorReason::PolicyCommandIsFact),
        Malformed::CommandWithFactOrigin => naming(CoordinatorReason::PolicyCommandFactOrigin),
        Malformed::UnknownDomain { .. } => naming(CoordinatorReason::PolicyCommandUnknownDomain),
        Malformed::RefusalMisaddressed => coded(&route::internal(
            CoordinatorReason::RefusalFactWrongStream.as_str_name(),
            [(args::DOMAIN.to_string(), domain.clone())],
        )),
        other => diagnostic(other),
    }
}

/// `error` as angzarr's wire `Diagnostic`: its key and its metadata.
fn coded(error: &CodedError) -> angzarr_proto::v1::Diagnostic {
    angzarr_proto::v1::Diagnostic {
        code: error.code.clone(),
        args: error
            .extras
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}

/// Dead letters kept in memory, whole and as their lines: for tests.
#[derive(Default)]
pub struct MemoryDeadLetters {
    lines: Mutex<Vec<String>>,
    letters: Mutex<Vec<AngzarrDeadLetter>>,
}

impl MemoryDeadLetters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every line written so far.
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().map(|l| l.clone()).unwrap_or_default()
    }

    /// Every letter written so far, whole ([`DeadLetterLog::letter`]); a
    /// bare line ([`DeadLetterLog::write`]) is in [`Self::lines`] only.
    pub fn letters(&self) -> Vec<AngzarrDeadLetter> {
        self.letters.lock().map(|l| l.clone()).unwrap_or_default()
    }

    /// Every letter written since the last drain, taken, with every line:
    /// a test's per-call read.
    pub fn drain(&self) -> Vec<AngzarrDeadLetter> {
        if let Ok(mut lines) = self.lines.lock() {
            lines.clear();
        }
        self.letters
            .lock()
            .map(|mut l| std::mem::take(&mut *l))
            .unwrap_or_default()
    }
}

impl DeadLetterLog for MemoryDeadLetters {
    fn write(&self, line: &str) {
        if let Ok(mut lines) = self.lines.lock() {
            lines.push(line.to_string());
        }
    }

    fn letter(&self, letter: &AngzarrDeadLetter) {
        if let Ok(mut letters) = self.letters.lock() {
            letters.push(letter.clone());
        }
        self.write(&dead_letter_line(letter));
    }
}

/// One call's dead letters (owner ruling Q5, 2026-10-31: durable, and
/// answered by the call). Each letter raised goes to the kernel's log at
/// once, as its line, and is held until it is placed: with the position
/// the step that raised it moves (a policy's step, written in that one
/// write), or in a write of its own at the step's end. The originating
/// command's own letters are never placed: they come only with its
/// failure, which ends the call (answered to its caller, never kept),
/// and a parked fact's is placed before it is answered. Every letter
/// placed is the call's, whether its write lands or not (a step whose
/// write fails holds its position, so it runs again and raises the letter
/// again).
pub(crate) struct CallLetters<'k> {
    log: &'k dyn DeadLetterLog,
    held: Mutex<Held>,
}

#[derive(Default)]
struct Held {
    /// Raised since the last placement.
    raised: Vec<AngzarrDeadLetter>,
    /// Placed: the call's.
    placed: Vec<DeadLetter>,
}

impl<'k> CallLetters<'k> {
    pub(crate) fn new(log: &'k dyn DeadLetterLog) -> CallLetters<'k> {
        CallLetters {
            log,
            held: Mutex::default(),
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Held> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The letters raised since the last placement, each keyed
    /// ([`keyed`], `trigger` the step's trigger event), now the call's:
    /// what the step's write keeps.
    pub(crate) fn place(&self, trigger: Option<(&Cover, u64)>) -> Vec<DeadLetter> {
        let mut held = self.held();
        let placed: Vec<DeadLetter> = std::mem::take(&mut held.raised)
            .into_iter()
            .filter_map(|letter| keyed(letter, trigger))
            .collect();
        held.placed.extend(placed.iter().cloned());
        placed
    }

    /// Every letter placed so far, taken: what the call answers.
    pub(crate) fn take(&self) -> Vec<DeadLetter> {
        std::mem::take(&mut self.held().placed)
    }
}

impl DeadLetterLog for CallLetters<'_> {
    fn write(&self, line: &str) {
        self.log.write(line);
    }

    fn letter(&self, letter: &AngzarrDeadLetter) {
        self.held().raised.push(letter.clone());
        self.log.letter(letter);
    }
}

/// `letter`, keyed as the store keeps it: about the stream and place its
/// payload names (events: their stream and first sequence; a command: its
/// stream and id), or else `trigger`'s, the event its step ran on (a
/// policy's command the kernel booked malformed has neither stream nor
/// id). `None` for one about nothing it can name, which only the log
/// keeps.
fn keyed(letter: AngzarrDeadLetter, trigger: Option<(&Cover, u64)>) -> Option<DeadLetter> {
    let named = match &letter.payload {
        Some(Payload::RejectedEvents(book) | Payload::FailedEditionRemoval(book)) => {
            book.cover.clone().zip(
                book.pages
                    .first()
                    .map(|p| DeadLetterAt::Sequence(p.sequence)),
            )
        }
        Some(Payload::RejectedCommand(book)) => book.cover.clone().zip(
            book.page
                .as_ref()
                .and_then(|p| p.command_id.as_ref())
                .and_then(|id| angzarr_ports::Uuid::try_from(id).ok())
                .map(DeadLetterAt::Command),
        ),
        None => None,
    };
    let (stream, at) = named
        .filter(|(cover, _)| StreamRef::try_from(cover).is_ok())
        .or_else(|| {
            trigger.map(|(cover, sequence)| (cover.clone(), DeadLetterAt::Sequence(sequence)))
        })?;
    Some(DeadLetter { stream, at, letter })
}

#[cfg(test)]
#[path = "dead_letter.test.rs"]
mod tests;

// Dead letters kept in the store, and answered by every call (Q5).
#[cfg(test)]
#[path = "durable.test.rs"]
mod durable_tests;
