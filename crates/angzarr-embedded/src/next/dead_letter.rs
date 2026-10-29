//! angzarr-embedded's "dead-letter queue" is a **logger**.
//!
//! angzarr dead-letters what it cannot deliver and cannot refuse to a
//! caller: a fact whose ingest failed (answered PARKED), a reactor's
//! malformed command. angzarr-embedded is in process and atomic, so there is no
//! queue, no store and no replay: whatever angzarr would dead-letter fails
//! the chain back to its caller, nothing is written, and the kernel writes
//! one line to the application's [`DeadLetterLog`]. Cases:
//!
//! - an ingest that panicked ([`KernelError::IngestPanicked`](super::KernelError::IngestPanicked));
//! - a policy's book the kernel built malformed (a policy or kernel bug);
//! - a decide under AGGREGATE_HANDLES that never acknowledged its window
//!   ([`KernelError::Unresolved`](super::KernelError::Unresolved));
//! - a policy that failed on its trigger
//!   ([`StepError::PolicyFailed`](super::StepError::PolicyFailed)): its
//!   error's code is the reason and its args the letter's metadata, never
//!   its English (angzarr decision 14.37);
//! - a handler's bug ([`KernelError::Router`](super::KernelError::Router)):
//!   a code of its own under a status outside the business set
//!   (FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS,
//!   OUT_OF_RANGE, PERMISSION_DENIED, whose refusal is never
//!   dead-lettered), or the router's UNHANDLED_HANDLER_ERROR: its code and
//!   args, never its English.
//!
//! Every line starts with [`DEAD_LETTER_PREFIX`]: **anything arriving there
//! almost certainly indicates a problem**, never business as usual, so an
//! operator should alert on the prefix.

use std::sync::Mutex;

use angzarr_embedded_diagnostic::Diagnostic;
use angzarr_proto::v1::angzarr_dead_letter::Payload;
use angzarr_proto::v1::{AngzarrDeadLetter, CommandBook};
use angzarr_router::error::CodedError;

use super::route::flat_args;

/// The start of every dead-letter line.
pub const DEAD_LETTER_PREFIX: &str = "DEAD LETTER (almost certainly a bug; \
    nothing was written and nothing will retry):";

/// Where dead-letter lines go: the application's log. `&self`, so a
/// shared logger needs no lock of the kernel's.
pub trait DeadLetterLog: Send + Sync {
    fn write(&self, line: &str);
}

/// A dead letter as one line: [`DEAD_LETTER_PREFIX`], the component that
/// produced it, the reason, its metadata as `name=value` in name order, and
/// the command's id and stream when the letter holds a command, or the
/// event's sequence and stream when it holds events. Built from angzarr's
/// own `AngzarrDeadLetter`, so a line and an angzarr DLQ entry say the same
/// things.
pub fn dead_letter_line(letter: &AngzarrDeadLetter) -> String {
    let mut line = format!(
        "{DEAD_LETTER_PREFIX} {}: {}",
        letter.source_component, letter.rejection_reason
    );
    let mut metadata: Vec<_> = letter.metadata.iter().collect();
    metadata.sort();
    for (name, value) in metadata {
        line.push_str(&format!(" {name}={value}"));
    }
    if let Some(Payload::RejectedEvents(book)) = &letter.payload {
        if let (Some(page), Some(cover)) = (book.pages.first(), &book.cover) {
            line.push_str(&format!(" (event {} of {}", page.sequence, cover.domain));
            if let Some(root) = &cover.root {
                line.push_str(&format!(" {root}"));
            }
            if let Some(edition) = &cover.edition {
                line.push_str(&format!(" in {edition}"));
            }
            line.push(')');
        }
    }
    if let Some(Payload::RejectedCommand(book)) = &letter.payload {
        if let Some(id) = book.page.as_ref().and_then(|p| p.command_id.as_ref()) {
            line.push_str(&format!(" (command {id}"));
            if let Some(cover) = &book.cover {
                line.push_str(&format!(" to {}", cover.domain));
                if let Some(root) = &cover.root {
                    line.push_str(&format!(" {root}"));
                }
                if let Some(edition) = &cover.edition {
                    line.push_str(&format!(" in {edition}"));
                }
            }
            line.push(')');
        }
    }
    line
}

/// Write the dead letter for `book`, which `component` could not take for
/// `reason`.
pub(crate) fn write_dead_letter(
    log: &dyn DeadLetterLog,
    book: CommandBook,
    component: &str,
    reason: &str,
) {
    log.write(&dead_letter_line(&AngzarrDeadLetter {
        payload: Some(Payload::RejectedCommand(book)),
        rejection_reason: reason.to_string(),
        source_component: component.to_string(),
        ..AngzarrDeadLetter::default()
    }));
}

/// Write the dead letter for `component`'s `failure`, a policy's (its
/// `on_event`'s on the trigger event `payload` holds, or its source's
/// `#[on_refused]` handler's on the refusal fact): the failure's code as
/// the reason and its args as the metadata.
pub(crate) fn write_policy_failure(
    log: &dyn DeadLetterLog,
    payload: Payload,
    component: &str,
    failure: &dyn Diagnostic,
) {
    log.write(&dead_letter_line(&AngzarrDeadLetter {
        payload: Some(payload),
        rejection_reason: failure.code().to_string(),
        metadata: flat_args(failure).into_iter().collect(),
        source_component: component.to_string(),
        ..AngzarrDeadLetter::default()
    }));
}

/// Write the dead letter for `component`'s handler's own fault `error` (a
/// bug, never a business refusal) on `book`: its code as the reason and its metadata as the letter's, never
/// its English.
pub(crate) fn write_handler_fault(
    log: &dyn DeadLetterLog,
    book: CommandBook,
    component: &str,
    error: &CodedError,
) {
    log.write(&dead_letter_line(&AngzarrDeadLetter {
        payload: Some(Payload::RejectedCommand(book)),
        rejection_reason: error.code.clone(),
        metadata: error.extras.clone().into_iter().collect(),
        source_component: component.to_string(),
        ..AngzarrDeadLetter::default()
    }));
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
