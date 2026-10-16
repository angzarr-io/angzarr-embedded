//! **Proposal, not yet implemented**: annals on angzarr's wire contract.
//! Bodies that are not trivial are `todo!("proposal: ...")`; the tests that
//! pin their behaviour are in `tests/` and are ignored until they are
//! implemented. See `docs/design/audit.md`.
//!
//! angzarr owns the wire contract (`io.angzarr.v1`, angzarr-proto); annals
//! is the in-process runtime, and its kernel stays dependency-free. This
//! crate is the one place the two meet:
//!
//! | annals | angzarr |
//! |---|---|
//! | [`StreamKey`](annals::next::StreamKey) (domain, edition, root) | `Cover` (domain, edition, root) |
//! | [`EventId`](annals::next::EventId) | `EventRef` |
//! | [`Meta`](annals::next::Meta) | `EventMeta`, on every page |
//! | a command and its [`CommandMeta`](annals::next::CommandMeta) ([`Command`]) | a one-page `CommandBook` with `CommandMeta` |
//! | what a chain recorded ([`Recorded`](annals::next::Recorded)) | `EventBook`s, one per stream |
//! | an event or command | `Any`, packed by [`prost::Name`] ([`pack`], [`Payload`]) |
//! | the payload's version | its type URL (`pkg.v1.Foo` then `pkg.v2.Foo`) |
//! | upcasting | [`Upcaster`], a mirror of angzarr-router's `UpcasterDispatch` |
//! | the store port's stored form | [`Stored`]: type URL + page as written, the metadata as columns |
//!
//! - **Strict replay.** A type URL that nothing upcasts and nothing
//!   decodes fails the read with an error naming it ([`ReadError`]); an
//!   event is never skipped. [`Registry::check`] and golden replay fixtures
//!   ([`assert_replay_fixtures`]) catch it in CI first.
//! - **Correlation and a command's cause** each go on and off the wire
//!   through one pair of functions ([`wire`]), because where angzarr keeps
//!   them is under review.
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
mod payload;
mod registry;
mod store;
mod upcast;
pub mod wire;

pub use book::{command_book, command_from_book, event_book, event_page, Command};
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
