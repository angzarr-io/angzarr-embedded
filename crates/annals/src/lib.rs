//! A small, deterministic event-sourcing engine.
//!
//! A [`Domain`] says what its state, commands and events are, how a command is
//! **decided** and how an event is **applied**. A [`Stream`] holds a domain's
//! event log and the state folded from it. Nothing else holds state: every
//! state is rebuilt by folding a log.
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

mod domain;
mod projection;
mod store;
mod stream;

pub use domain::{Domain, Tx};
pub use projection::{project, Projection};
pub use store::{Conflict, EventStore, InMemoryStore};
pub use stream::{Envelope, Header, ReplayError, Seq, Stream, StreamId};
