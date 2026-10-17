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
//! stream is a `Cover`'s (domain, edition, root) ([`StreamKey`], main =
//! no edition), an event id is an `EventRef` ([`EventId`]), a [`Book`] is a
//! one-page `CommandBook`, [`Meta`] is an `EventMeta`, and what a chain
//! recorded a set of `EventBook`s. annals is **not** angzarr: where their
//! execution models differ (the atomic in-process chain, above all) the
//! difference is deliberate and listed in the design's *Divergences*.
//!
//! One book, end to end ([`handle`]):
//!
//! 1. **Validate and resolve**, before business code: the body arm and the
//!    origin agree ([`Malformed`]); the expected sequence is checked or
//!    stamped, under the book's [`ConflictResolution`]; the origin is
//!    resolved (a request without a client-supplied id gets the
//!    application's minted UUIDv7), and so is the correlation.
//! 2. **Decide or ingest**, picked by the body arm ([`Body`]): a command is
//!    decided, purely ([`Domain::decide`], fallible, staging in a [`Tx`]);
//!    a fact ingested ([`Domain::ingest`], infallible). Business output is
//!    payloads only: the kernel stamps every event's [`Meta`].
//! 3. **React**, in the same [`Chain`]: every [`Policy`] (event →
//!    commands, rejectable) routed for the source's domain runs on the
//!    staged events, and their targets' events set off their own policies,
//!    depth first. Everything stays staged in memory.
//! 4. **Append, atomically**: once the whole chain has succeeded, every
//!    staged stream is appended through the application's unit of work,
//!    each checked against the length it was loaded at, the book's claim
//!    (key and payload fingerprint) recorded if it is keyed, and the unit
//!    committed. Any failure anywhere
//!    fails it all ([`Failure`], with the path to the failing [`Step`]).
//! 5. **Project**: [`Projection`]s run on the committed events only.
//!
//! Every reaction takes the same input, a [`Committed`] event (stream
//! identity, position, metadata, payload, state just after), under one
//! [`Position`] contract, through one dispatch ([`Handler`]).

// Proposal bodies are `todo!()`, so arguments and fields go unread until the
// implementation lands. Remove this with the last `todo!`.
#![allow(unused_variables, dead_code)]
// A failure names the path to the step that failed and carries reports, so
// it is large; failures are the cold path, and unboxed variants keep them
// matchable by pattern. Revisit (box `Downstream`) if profiling says so.
#![allow(clippy::result_large_err)]

mod book;
mod conflict;
mod domain;
mod error;
mod handle;
mod handler;
mod id;
mod meta;
mod reaction;
mod snapshot;
mod store;
mod stream;
mod version;

pub use book::{Body, Book, BookOf, ConflictResolution, Handling, Malformed, Sampled};
pub use conflict::{commutative, Changed, CommutativeState, Key, UnitPath, Units};
pub use domain::{Conflicts, Decision, Domain, Nonce, Tx};
pub use error::{Failure, KernelError, Recovery, ReplayError, Step, StepError, StoreError};
pub use handle::{
    handle, handle_async, AsyncRoutes, Chain, Project, ProjectionRoute, ProjectorFailure, Recorded,
    Routes, DEFAULT_DEPTH_LIMIT,
};
pub use handler::{Admit, Committed, Gap, Handler, Position, Tracked};
pub use id::{
    Actor, Correlation, DeriveIds, EditionName, EmptyEditionName, EventId, Fingerprint, RequestId,
    Root, RulesVersion, Seq, StreamKey, Timestamp, Uuid,
};
pub use meta::{Causation, FactOrigin, Meta, Origin, Provenance, RequestOrigin};
pub use reaction::{
    deliver, orders_owed, policy, projection, target_key, Backlog, Delivered, Order, OrderDelivery,
    Policy, Projection, Projector, Reaction, Reactor,
};
pub use snapshot::{load, Loaded, Snapshot, SnapshotStore};
pub use store::{
    AsyncEventStore, AsyncUnitOfWork, Claim, EventStore, InMemoryStore, InMemoryTx, Ready, Slice,
    UnitOfWork,
};
pub use stream::{Envelope, Stream};
pub use version::Rulebook;
