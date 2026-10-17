//! **Proposal, not yet implemented**: annals on angzarr's wire contract.
//! Bodies that are not trivial are `todo!("proposal: ...")`; the tests that
//! pin their behaviour are in `tests/` and are ignored until they are
//! implemented. See `docs/design/audit.md`.
//!
//! angzarr owns the wire contract (`io.angzarr.v1`, angzarr-proto); annals
//! is the in-process runtime, and its kernel stays dependency-free. This
//! crate is the one place the two meet:
//!
//! | annals | angzarr (angzarr-proto 7956dc8) |
//! |---|---|
//! | [`StreamKey`](annals::next::StreamKey) (domain, edition, root) | `Cover { domain, root, optional edition }` |
//! | `edition: None` / `Some(name)` | `Cover.edition` unset / set; a set `""` refused |
//! | [`EventId`](annals::next::EventId) | `EventRef { stream, sequence }` |
//! | [`Book`](annals::next::Book) | a one-page `CommandBook { cover, handling, page }` |
//! | [`Handling`](annals::next::Handling), [`ConflictResolution`](annals::next::ConflictResolution) | `Handling { expected_sequence, conflict_resolution }` |
//! | [`Provenance`](annals::next::Provenance) | `Provenance { actor, UUID correlation_id }` |
//! | [`Origin`](annals::next::Origin) | `Origin { request \| causation \| fact }`; `RequestOrigin { UUID request_id, minted }` (no index: a request id names one command) |
//! | a book's `origin: None` | `CommandPage.origin` unset: the request id is minted on receipt |
//! | [`Uuid`](annals::next::Uuid), [`RequestId`](annals::next::RequestId), [`Correlation`](annals::next::Correlation) | `UUID { bytes value }`, 16 bytes ([`ids`]) |
//! | [`DeriveIds`](annals::next::DeriveIds), derived roots | `angzarr_proto::ids::{fact_correlation, compute_root}` ([`ids::ANGZARR_IDS`], [`ids::compute_root`]) |
//! | [`Fingerprint`](annals::next::Fingerprint), [`Claim`](annals::next::Claim) | `IdempotencyClaim { stream, key, UUID fingerprint, first_sequence, event_count }` ([`fingerprint`]: `angzarr_proto::ids::fingerprint`, UUIDv5) |
//! | [`FactMismatch`](annals::next::FactMismatch) ([`Domain::fact_mismatch`](annals::next::Domain::fact_mismatch)) | the coordinator's fact-mismatch setting (configuration, not wire): refuse FAILED_PRECONDITION by default, or DUPLICATE plus an alert |
//! | [`CommutativeState`](annals::next::CommutativeState) | `changed()` generated from `(commutative_state)` and `(conflict)` (backlog CR-1) |
//! | [`Body`](annals::next::Body) | `CommandPage.body`: `Command` or `Fact` |
//! | [`Envelope`](annals::next::Envelope) | `EventPage { sequence, meta, payload }` |
//! | [`Meta`](annals::next::Meta) | `EventMeta { provenance, origin, recorded_at, base_sequence, rules_version }` |
//! | [`Snapshot`](annals::next::Snapshot) | `Snapshot { sequence, state, rules_version }` |
//! | what a chain recorded ([`Recorded`](annals::next::Recorded)) | `EventBook`s, one per stream, and an `Outcome` ([`outcome`]) |
//! | an event, command or fact | `Any`, packed by [`prost::Name`] ([`pack`], [`Payload`]) |
//! | the payload's version | its type URL (`pkg.v1.Foo` then `pkg.v2.Foo`) |
//! | upcasting | [`Upcaster`], a mirror of angzarr-router's `UpcasterDispatch` |
//! | the store port's stored form | [`Stored`]: type URL + page as written, the metadata as columns |
//!
//! - **Strict replay.** A type URL that nothing upcasts and nothing
//!   decodes fails the read with an error naming it ([`ReadError`]); an
//!   event is never skipped. [`Registry::check`] and golden replay fixtures
//!   ([`assert_replay_fixtures`]) catch it in CI first.
//! - **One home per value.** Correlation is in `Provenance` only, a
//!   command's cause in its `Origin` only: each goes on and off the wire
//!   through one pair of functions in [`wire`].
//! - Builds for `wasm32-unknown-unknown`, as angzarr-proto does.

// Proposal bodies are `todo!()`, so arguments and fields go unread until the
// implementation lands. Remove this with the last `todo!`.
#![allow(unused_variables, dead_code)]
// A read error names its stream and carries the kernel's errors and
// reports, so it is large; errors are the cold path, and unboxed variants
// keep them matchable by pattern, as in the kernel.
#![allow(clippy::result_large_err)]

mod book;
mod error;
mod fixture;
pub mod ids;
mod payload;
mod registry;
mod store;
mod upcast;
pub mod wire;

pub use book::{
    book_from_wire, command_book, event_book, event_page, fingerprint, fingerprinted, outcome,
};
pub use error::{
    BookError, ChainProblem, PayloadError, ReadError, ReadProblem, UpcastChainError, UpcastError,
};
pub use fixture::{assert_replay_fixtures, check_replay_fixtures, ReplayFixture};
pub use payload::{pack, type_name, Decoder, Payload};
pub use registry::Registry;
pub use store::{read_event, replay_stored, store_event, Opaque, Stored};
pub use upcast::{UpcastFn, Upcaster};

/// angzarr-proto, as this crate uses it: the `io.angzarr.v1` messages.
pub use angzarr_proto::v1 as pb;
