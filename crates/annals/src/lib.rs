//! A small, deterministic event-sourcing engine.
//!
//! A [`Domain`] says what its state, commands and events are, how a command is
//! **decided** and how an event is **applied**. A [`Stream`] holds a domain's
//! event log and the state folded from it. Nothing else holds state: every
//! state is rebuilt by folding a log. annals-sim and annals-log are built on
//! this crate root; nothing else is.
//!
//! The rules the engine keeps:
//!
//! - **Decide reads, apply writes.** A decide reads state through a [`Tx`] and
//!   emits events; [`Tx::emit`] applies each one at once, so a decide sees the
//!   effects of its own events. Apply is the only thing that changes state.
//! - **Validate, then emit.** A decide may reject only before its first emit.
//!   Once it has emitted, it has committed.
//! - **Apply never sees the context.** Dice, clocks and anything else a decide
//!   may consult live in [`Domain::Ctx`], which apply is never given — so
//!   apply cannot roll a die. Outcomes are decided once and recorded in the
//!   events, and replaying a log never depends on how they were decided.
//! - **Deterministic.** No hash-map iteration, no clock, no I/O. Order is the
//!   order of the log.
//!
//! The crate knows nothing about any particular game.
//!
//! [`next`] is the kernel that is to replace this one, for auditability,
//! operating on angzarr's wire types (`io.angzarr.v1`, through
//! angzarr-proto): see `docs/design/audit.md`.

mod domain;
pub mod next;
mod stream;

pub use domain::{Domain, Tx};
pub use stream::{Envelope, Header, ReplayError, Seq, Stream, StreamId};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `crates/annals/locales/en-US/annals.ftl`.
pub const EN_US: (&str, &str) = ("annals", include_str!("../locales/en-US/annals.ftl"));

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test: each one's code must have a message in every locale,
/// using exactly its args. Its messages are in
/// `crates/annals/locales/en-US/annals.ftl`.
pub fn diagnostic_samples() -> Vec<Box<dyn annals_diagnostic::Diagnostic>> {
    let root: Vec<Box<dyn annals_diagnostic::Diagnostic>> = vec![
        Box::new(ReplayError::OutOfSequence { index: 1, found: 2 }),
        Box::new(ReplayError::CauseNotEarlier { seq: 1, cause: 1 }),
    ];
    root.into_iter().chain(next::diagnostic_samples()).collect()
}
