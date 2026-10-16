//! **Proposal, not yet implemented**: the audited kernel that replaces the
//! crate root's [`Stream`](crate::Stream), [`Tx`](crate::Tx),
//! [`EventStore`](crate::EventStore), [`Projection`](crate::Projection) and
//! [`policy`](crate::policy) once the design in `docs/design/audit.md` is
//! reviewed. Bodies that are not trivial are `todo!("proposal: ...")`; the
//! tests that pin their behaviour are in `tests/next/` and are ignored until
//! they are implemented.
//!
//! annals is the in-process runtime: it does no I/O and keeps no
//! infrastructure, and has no dependencies. Its wire contract is angzarr's
//! (`io.angzarr.v1`), and the `annals-angzarr` crate maps it there: a
//! stream is a `Cover`'s (domain, edition, root) ([`StreamKey`]), an event
//! id is an `EventRef` ([`EventId`]), [`Meta`] is an `EventMeta`, a command
//! a one-page `CommandBook`, and what a chain recorded a set of
//! `EventBook`s. Its failures say whether to retry ([`Recovery`]), and its
//! command ids are idempotency keys ([`CommandId`]).
//!
//! One command, end to end ([`handle`]):
//!
//! 1. **Decide** is pure, with explicit inputs: state, command, the
//!    command's [`CommandMeta`] (instant, actor, ids, sampled by the
//!    handler) and the rules. It stages events in a [`Tx`] that folds them
//!    into a copy of the state, so it sees its own effects and the stream
//!    sees nothing; a rejection leaves nothing behind.
//! 2. **React**, in the same [`Chain`]: every [`Policy`] (event → commands,
//!    rejectable) and [`Translation`] (event → facts, refused only for
//!    integrity) routed for the source's domain runs on the staged events,
//!    and their targets' events set off their own reactions, depth first.
//!    Everything stays staged in memory.
//! 3. **Append, atomically**: once the whole chain has succeeded, every
//!    staged stream is appended through the application's unit of work,
//!    each checked against the length it was loaded at, and the unit
//!    committed. Any failure anywhere fails it all ([`Failure`], with the
//!    path to the failing [`Step`]).
//! 4. **Project**: [`Projection`]s run on the committed events only.
//!
//! Every event is stamped with [`Meta`], its correlation always set
//! ([`Correlation::generated`] when a command arrives without one); `apply`
//! never sees it. Payload versions are payload types, not a kernel field:
//! upcasting is the store adapter's, at read time. Every
//! reaction takes the same input, a [`Committed`] event (stream identity,
//! position, metadata, payload, state just after), under one [`Position`]
//! contract, through one dispatch ([`Handler`]).

// Proposal bodies are `todo!()`, so arguments and fields go unread until the
// implementation lands. Remove this with the last `todo!`.
#![allow(unused_variables, dead_code)]
// A failure names the path to the step that failed and carries reports, so
// it is large; failures are the cold path, and unboxed variants keep them
// matchable by pattern. Revisit (box `Downstream`) if profiling says so.
#![allow(clippy::result_large_err)]

mod domain;
mod error;
mod handle;
mod handler;
mod id;
mod meta;
mod reaction;
mod store;
mod stream;
mod version;

pub use domain::{Decision, Domain, Tx};
pub use error::{
    Failure, KernelError, Recovery, ReplayError, Step, StepError, StepKind, StoreError,
};
pub use handle::{
    handle, handle_async, AsyncRoutes, Chain, Project, ProjectionRoute, Recorded, Routes,
    DEFAULT_DEPTH_LIMIT,
};
pub use handler::{Admit, Committed, Gap, Handler, Position, Tracked};
pub use id::{
    Actor, CommandId, Correlation, Edition, EventId, RequestId, Root, RulesVersion, Seq, StreamKey,
    Timestamp,
};
pub use meta::{CommandMeta, Meta};
pub use reaction::{
    deliver, facts_owed, inject, orders_owed, policy, projection, target_key, translation, Backlog,
    Delivered, Fact, FactDelivery, Order, OrderDelivery, Owed, Policy, Projection, Projector,
    Reaction, Reactor, Translation, Translator,
};
pub use store::{
    AsyncEventStore, AsyncUnitOfWork, EventStore, InMemoryStore, InMemoryTx, Ready, Slice,
    UnitOfWork,
};
pub use stream::{Envelope, Stream};
pub use version::Rulebook;
