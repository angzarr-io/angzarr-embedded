//! angzarr-embedded's "dead-letter queue" is a **logger**.
//!
//! angzarr dead-letters what it cannot deliver and cannot refuse to a
//! caller: a fact whose ingest failed (answered PARKED), a reactor's
//! malformed command. angzarr-embedded is in process and atomic, so there is no
//! queue, no store and no replay: whatever angzarr would dead-letter fails
//! the chain back to its caller, nothing is written, and the kernel writes
//! one line to the application's [`DeadLetterLog`]. Cases:
//!
//! - a handler's bug ([`KernelError::Router`](super::KernelError::Router)
//!   under a status of the bug class: INTERNAL, UNKNOWN, DATA_LOSS): the
//!   router's ROUTER_REASON_UNHANDLED_HANDLER_ERROR, the handler's own code and args its
//!   `cause`, a panic's ROUTER_REASON_HANDLER_PANICKED; or the router's own error on
//!   malformed input. Never a business refusal;
//! - a policy's book the kernel built malformed (a policy or kernel bug);
//! - a decide under AGGREGATE_HANDLES that never acknowledged its window
//!   ([`KernelError::Unresolved`](super::KernelError::Unresolved));
//! - a policy that failed on its trigger
//!   ([`StepError::PolicyFailed`](super::StepError::PolicyFailed));
//! - a step whose retryable failure outlasted its retries
//!   ([`OnRetryable`](super::OnRetryable)), with how many there were.
//!
//! A letter's reason is its error's [`Diagnostic`]: its code and its args,
//! never English (angzarr decision 14.37). Every line starts with
//! [`DEAD_LETTER_PREFIX`]: **anything arriving there almost certainly
//! indicates a problem**, never business as usual, so an operator should
//! alert on the prefix.

use std::sync::Mutex;

use angzarr_embedded_diagnostic::Diagnostic;
use angzarr_proto::v1::angzarr_dead_letter::{Payload, RejectionDetails};
use angzarr_proto::v1::{AngzarrDeadLetter, CommandBook, EventProcessingFailedDetails};

use super::error::cover_text;
use super::route::diagnostic;

/// The start of every dead-letter line: a key to alert on.
pub const DEAD_LETTER_PREFIX: &str = "DEAD_LETTER";

/// Where dead-letter lines go: the application's log. `&self`, so a
/// shared logger needs no lock of the kernel's.
pub trait DeadLetterLog: Send + Sync {
    fn write(&self, line: &str);
}

/// A dead letter as one line, keys and values only: [`DEAD_LETTER_PREFIX`],
/// the component that produced it, the reason's code and its args as
/// `name=value` in name order (a value quoted where it would not parse
/// back, `angzarr_embedded_diagnostic::quote`), then any metadata, a retried step's
/// `retry_count`, and the command's `command_id` and `stream`, or the
/// event's `sequence` and `stream`, as the letter holds. Built from
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
    if let Some(RejectionDetails::EventProcessingFailed(details)) = &letter.rejection_details {
        fields.push(("retry_count".to_string(), details.retry_count.to_string()));
    }
    match &letter.payload {
        Some(Payload::RejectedEvents(book)) => {
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
            line.push_str(&format!(
                " {name}={}",
                angzarr_embedded_diagnostic::quote(&value)
            ));
        }
    }
    line
}

/// Write the dead letter for `payload`, which `component` could not take
/// for `reason`.
pub(crate) fn write_letter(
    log: &dyn DeadLetterLog,
    payload: Payload,
    component: &str,
    reason: &dyn Diagnostic,
) {
    log.write(&dead_letter_line(&AngzarrDeadLetter {
        payload: Some(payload),
        reason: Some(diagnostic(reason)),
        source_component: component.to_string(),
        ..AngzarrDeadLetter::default()
    }));
}

/// Write the dead letter for `book`, which `component` could not take for
/// `reason`.
pub(crate) fn write_dead_letter(
    log: &dyn DeadLetterLog,
    book: CommandBook,
    component: &str,
    reason: &dyn Diagnostic,
) {
    write_letter(log, Payload::RejectedCommand(book), component, reason);
}

/// Write the dead letter for `payload` (a command, or a projector's events),
/// whose step at `component` failed for
/// the retryable `failure` after `retries` retries ([`retries_exhausted`]).
pub(crate) fn write_retries_exhausted(
    log: &dyn DeadLetterLog,
    payload: Payload,
    component: &str,
    failure: &dyn Diagnostic,
    retries: u32,
) {
    log.write(&dead_letter_line(&retries_exhausted(
        payload, component, failure, retries,
    )));
}

/// The dead letter for `payload` (a command, or a projector's events), whose
/// step at `component` failed for the
/// retryable `failure` after `retries` retries: the reason, and the
/// processing failure's details, transient, with its retry count.
pub(crate) fn retries_exhausted(
    payload: Payload,
    component: &str,
    failure: &dyn Diagnostic,
    retries: u32,
) -> AngzarrDeadLetter {
    AngzarrDeadLetter {
        payload: Some(payload),
        reason: Some(diagnostic(failure)),
        rejection_details: Some(RejectionDetails::EventProcessingFailed(
            EventProcessingFailedDetails {
                failure: Some(diagnostic(failure)),
                retry_count: retries,
                is_transient: true,
                ..EventProcessingFailedDetails::default()
            },
        )),
        source_component: component.to_string(),
        ..AngzarrDeadLetter::default()
    }
}

/// Dead-letter lines kept in memory: for tests.
#[derive(Default)]
pub struct MemoryDeadLetters(Mutex<Vec<String>>);

impl MemoryDeadLetters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every line written so far.
    pub fn lines(&self) -> Vec<String> {
        self.0.lock().map(|l| l.clone()).unwrap_or_default()
    }
}

impl DeadLetterLog for MemoryDeadLetters {
    fn write(&self, line: &str) {
        if let Ok(mut lines) = self.0.lock() {
            lines.push(line.to_string());
        }
    }
}

#[cfg(test)]
#[path = "dead_letter.test.rs"]
mod tests;
