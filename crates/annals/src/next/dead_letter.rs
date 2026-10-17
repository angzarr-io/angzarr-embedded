//! annals' "dead-letter queue" is a **logger**.
//!
//! angzarr dead-letters what it cannot deliver and cannot refuse to a
//! caller: a fact whose ingest failed (answered PARKED), a reactor's
//! malformed command. annals is in process and atomic, so there is no
//! queue, no store and no replay: whatever angzarr would dead-letter fails
//! the chain back to its caller, nothing is written, and the kernel writes
//! one line to the application's [`DeadLetterLog`]. Cases:
//!
//! - an ingest that panicked ([`KernelError::IngestPanicked`](super::KernelError::IngestPanicked));
//! - a policy's book the kernel built malformed (a policy or kernel bug);
//! - a decide under AGGREGATE_HANDLES that never acknowledged its window
//!   ([`KernelError::Unresolved`](super::KernelError::Unresolved)).
//!
//! Every line starts with [`DEAD_LETTER_PREFIX`]: **anything arriving there
//! almost certainly indicates a problem**, never business as usual, so an
//! operator should alert on the prefix.

use std::sync::Mutex;

use angzarr_proto::v1::{AngzarrDeadLetter, CommandBook};

/// The start of every dead-letter line.
pub const DEAD_LETTER_PREFIX: &str = "DEAD LETTER (almost certainly a bug; \
    nothing was written and nothing will retry):";

/// Where dead-letter lines go: the application's log. `&self`, so a
/// shared logger needs no lock of the kernel's.
pub trait DeadLetterLog: Send + Sync {
    fn write(&self, line: &str);
}

/// A dead letter as one line: [`DEAD_LETTER_PREFIX`], the component that
/// produced it, the reason, and the command's id and stream when the letter
/// holds a command. Built from angzarr's own `AngzarrDeadLetter`, so a
/// line and an angzarr DLQ entry say the same things.
pub fn dead_letter_line(letter: &AngzarrDeadLetter) -> String {
    use angzarr_proto::v1::angzarr_dead_letter::Payload;
    let mut line = format!(
        "{DEAD_LETTER_PREFIX} {}: {}",
        letter.source_component, letter.rejection_reason
    );
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
        payload: Some(angzarr_proto::v1::angzarr_dead_letter::Payload::RejectedCommand(book)),
        rejection_reason: reason.to_string(),
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
