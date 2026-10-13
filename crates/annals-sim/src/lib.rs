//! Deterministic simulation parts that sit on top of [`annals`].
//!
//! - [`rng`]: per-entity SplitMix64 streams, re-derivable from
//!   `(seed, owner, use index)`, so no generator position lives in a context.
//! - [`schedule`]: a tick [`Scheduler`] held in folded state, keyed
//!   `(tick, phase, side, seq)`, [`advance`], the clock a decide calls, and
//!   [`Ticked`], an event wrapper that carries its tick.
//! - [`job`]: scheduled work as data — `(kind, Value)` — run through a
//!   [`Registry`] each mechanic registers into.
//! - [`record`]: the self-describing [`Value`] a job's payload is written as.
//! - [`parallel()`]: an order-preserving map over scoped threads.
//! - [`gherkin`]: a small Gherkin runner for `harness = false` test targets.
//!
//! No dependencies beyond `annals`, no clock, no I/O in the simulation parts, and
//! nothing specific to any game.

pub mod gherkin;
pub mod job;
pub mod parallel;
pub mod record;
pub mod rng;
pub mod schedule;

pub use job::{Job, JobContext, JobRecord, Registry};
pub use parallel::parallel;
pub use record::{Rec, Value};
pub use rng::{derive, fnv1a, mix, Rng};
pub use schedule::{advance, Advance, Clock, Key, Scheduler, Tick, Ticked};
