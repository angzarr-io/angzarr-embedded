//! Books: a kernel [`Book`] is a one-page `CommandBook`; what a chain
//! recorded is a set of `EventBook`s, one per stream, each page carrying
//! `EventMeta`; a command handler's answer is an `Outcome`.

use angzarr_proto::v1::{self as pb, CommandBook, EventBook, EventPage};
use annals::next::{Book, BookOf, Domain, Envelope, Failure, Recorded, StreamKey};

use crate::error::BookError;
use crate::payload::Payload;

/// A book as a one-page `CommandBook`:
///
/// - `cover` is the stream ([`wire::cover`](crate::wire::cover)): domain,
///   root, and the edition, unset for main;
/// - `handling` as sent ([`wire::handling`](crate::wire::handling)): never
///   persisted;
/// - the page's `provenance` and `origin` as sent
///   ([`wire::provenance`](crate::wire::provenance),
///   [`wire::origin`](crate::wire::origin));
/// - the page's body is the arm the book's is: `Command { payload }` or
///   `Fact { payload }`, packed as `Any` ([`Payload::to_any`]).
pub fn command_book<C: Payload, F: Payload>(book: &Book<C, F>) -> CommandBook {
    todo!("proposal: cover, handling, page {{ provenance, origin, body }}")
}

/// A `CommandBook` as a book for `D`. Refuses a book with no cover, no page
/// or no body, for another domain, with an empty edition, a payload that
/// `D::Command` (for the command arm) or `D::Fact` (for the fact arm) does
/// not decode, or handling it cannot read. Whether its body and origin agree
/// is not checked here: the kernel does it before business code
/// ([`Book::validate`]), so a book built in Rust and one read off the wire
/// are refused the same way.
pub fn book_from_wire<D: Domain>(book: &CommandBook) -> Result<BookOf<D>, BookError>
where
    D::Command: Payload,
    D::Fact: Payload,
{
    todo!("proposal: the inverse of command_book")
}

/// One recorded event as an `EventPage { sequence, meta, payload }`: the
/// seq (64-bit, always definite), the metadata as `EventMeta`
/// ([`wire::event_meta`](crate::wire::event_meta)), and the payload packed
/// as `Any`. Business code wrote only the payload.
pub fn event_page<E: Payload>(envelope: &Envelope<E>) -> EventPage {
    todo!("proposal: sequence, meta, payload")
}

/// What a chain recorded in the `D` stream `key`, as an `EventBook`: the
/// cover ([`wire::cover`](crate::wire::cover)), a page per event
/// ([`event_page`]), `next_sequence` the stream's new length, and no
/// snapshot. `None` if the chain did not touch `key` or it is not a `D`
/// stream.
pub fn event_book<D: Domain>(recorded: &Recorded, key: &StreamKey) -> Option<EventBook>
where
    D::Event: Payload,
{
    todo!("proposal: recorded.events::<D>(key), then cover and pages")
}

/// What angzarr's command handler would answer with, as an `Outcome`, for
/// what [`handle`](annals::next::handle) returned:
///
/// - `Ok`: APPLIED, with each [`ProjectorFailure`](annals::next::ProjectorFailure)
///   as a `ProjectorFailure` (code INTERNAL);
/// - a recorded key ([`StoreError::Duplicate`](annals::next::StoreError::Duplicate)):
///   DUPLICATE (the caller re-reads the range it names);
/// - anything else: `None`. A rejection or a fault is an error status, never
///   an outcome. annals never answers PARKED: an ingest is infallible by
///   type, and in process a panicking one aborts the request with nothing
///   written (there is no dead-letter queue to park it in).
pub fn outcome<R>(result: &Result<Recorded, Failure<R>>) -> Option<pb::Outcome> {
    todo!("proposal: APPLIED with projector failures, DUPLICATE, else None")
}
