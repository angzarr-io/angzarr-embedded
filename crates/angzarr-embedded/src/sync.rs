//! The SIMPLE command mode: a command handled as [`Kernel::handle_any`]
//! handles it, answered only once each projector the caller names has
//! projected its events, with what each answered
//! ([`Kernel::handle_sync`]; angzarr's `HandleSync`,
//! docs/design/command-handling.md §10).
//!
//! The kernel runs every registered projector inline: after a commit, each
//! is handed the committed books of its sources and awaited before the
//! call returns ([`Registry::projector_handler`]). So the wait is the
//! projector's own future, awaited in the call, and nothing is polled. A
//! named projector differs from the others in three ways:
//!
//! - what it answers is kept, as a [`Projection`] the kernel builds
//!   (decision 14.97: the coordinator builds it, once the projector has
//!   answered), one per named projector, for the command's own stream;
//! - its failure is the CALLER's, answered at once and never retried or
//!   dead-lettered: [`KernelError::SyncProjectionFailed`], UNAVAILABLE, the
//!   decision committed (decision 14.105). The caller resends with the same
//!   command id; the resend is a DUPLICATE, and the named projectors are
//!   handed the command's events again and waited for again;
//! - its run is bounded by the kernel's deadline
//!   ([`Kernel::sync_within`]), timed on the kernel's [`Wait`]: a named
//!   projector still running when it passes fails the call as a failure of
//!   its own would, its cause [`DeadlinePassed`].
//!
//! The projectors not named run as [`Kernel::handle_any`] runs them.

use std::fmt;
use std::time::Duration;

use angzarr_diagnostic::{code, Arg, Diagnostic};
use angzarr_proto::v1::Projection;

use super::handle::Handled;

#[cfg(doc)]
use super::error::KernelError;
#[cfg(doc)]
use super::handle::{Kernel, Wait};
#[cfg(doc)]
use super::registry::Registry;

/// How long a SIMPLE call waits, by default, for the projectors it names:
/// thirty seconds from the first one's run that does not answer at once.
pub const DEFAULT_SYNC_DEADLINE: Duration = Duration::from_secs(30);

/// What [`Kernel::handle_sync`] answers: the command's outcome, as
/// [`Kernel::handle_any`] answers it (its events and its delivery, APPLIED
/// or DUPLICATE), and each named projector's [`Projection`], one per name,
/// in the order first named.
pub struct HandledSync {
    pub handled: Handled,
    pub projections: Vec<Projection>,
}

/// A named projector was still running when the SIMPLE call's deadline
/// passed: the cause of its [`KernelError::SyncProjectionFailed`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeadlinePassed {
    /// The deadline that passed.
    pub deadline: Duration,
}

impl fmt::Display for DeadlinePassed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for DeadlinePassed {
    fn code(&self) -> &str {
        code!("sync-deadline-passed")
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let millis = u64::try_from(self.deadline.as_millis()).unwrap_or(u64::MAX);
        vec![("deadline_ms", millis.into())]
    }
}

#[cfg(test)]
#[path = "sync.test.rs"]
mod tests;
