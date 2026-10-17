//! Books: a kernel [`Book`] is a one-page `CommandBook`; what a chain
//! recorded is a set of `EventBook`s, one per stream, each page carrying
//! `EventMeta`; a command handler's answer is an `Outcome`.

use angzarr_proto::v1::{self as pb, CommandBook, EventBook, EventPage};
use annals::next::{
    Book, BookOf, Domain, Envelope, Failure, Fingerprint, Recorded, StreamKey, Uuid,
};
use prost_types::Any;

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
///   [`wire::book_origin`](crate::wire::book_origin): unset for a book
///   whose request id the receiver mints);
/// - the page's body is the arm the book's is: `Command { payload }` or
///   `Fact { payload }`, packed as `Any` ([`Payload::to_any`]).
///
/// The book's fingerprint is not sent: a receiver takes its own over the
/// bytes it receives ([`book_from_wire`]).
pub fn command_book<C: Payload, F: Payload>(book: &Book<C, F>) -> CommandBook {
    todo!("proposal: cover, handling, page {{ provenance, origin, body }}")
}

/// A `CommandBook` as a book for `D`, **fingerprinted over the payload as
/// received** ([`fingerprint`] of the body's `Any`, its bytes exactly as
/// they arrived, never re-encoded), as angzarr's claim is. Refuses a book
/// with no cover, no page or no body, for another domain, with an empty
/// edition, a payload that `D::Command` (for the command arm) or `D::Fact`
/// (for the fact arm) does not decode, handling it cannot read, an id that
/// is not 16 bytes. Whether its
/// body and origin agree is not checked here: the kernel does it before
/// business code ([`Book::validate`]), so a book built in Rust and one read
/// off the wire are refused the same way.
pub fn book_from_wire<D: Domain>(book: &CommandBook) -> Result<BookOf<D>, BookError>
where
    D::Command: Payload,
    D::Fact: Payload,
{
    todo!("proposal: the inverse of command_book, fingerprinted over the received Any")
}

/// The fingerprint of a payload as received: angzarr's
/// `angzarr_proto::ids::fingerprint(type_url, value)`, a UUIDv5 under
/// `ANGZARR_FINGERPRINT_NAMESPACE` of the type URL's byte length (4 bytes,
/// big-endian), the type URL and the value bytes, exactly as they are
/// (never normalised or re-encoded). 16 bytes, stored with the claim, and
/// the same value angzarr's coordinator records for the same `Any`.
///
/// Misuse detection (a key reused for another payload), not tamper
/// protection; unrelated to the deferred event hash chain.
pub fn fingerprint(payload: &Any) -> Fingerprint {
    let id = angzarr_proto::ids::fingerprint(&payload.type_url, &payload.value);
    Fingerprint(Uuid::from_bytes(
        id.to_bytes().expect("angzarr-proto derives 16 bytes"),
    ))
}

/// A book built in Rust, fingerprinted over its payload's encoding
/// ([`Payload::to_any`]): what [`book_from_wire`] computes for the same
/// book sent by a client that encodes as prost does, so an in-process
/// retry and a wire retry of one payload agree.
///
/// **Deterministic within one prost version**, for payloads whose encoding
/// is: prost writes fields in tag order, but a proto `map` generated as a
/// `HashMap` (prost's default) is written in iteration order, which varies
/// between runs. A payload with map fields must be generated with
/// `btree_map` (sorted keys), or its retries are refused as a reused key.
/// A prost upgrade that changes the encoding changes fingerprints, which
/// matters only for a retry spanning the upgrade (within a claim's life).
pub fn fingerprinted<C: Payload, F: Payload>(book: Book<C, F>) -> Book<C, F> {
    todo!("proposal: fingerprint(body.to_any()), set on the book")
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
/// - a fact re-sent with another payload whose domain answers it as a
///   duplicate ([`StoreError::DivergentDuplicate`](annals::next::StoreError::DivergentDuplicate)):
///   DUPLICATE as well, as angzarr answers it under that setting; the
///   alert and the metric are the application's;
/// - anything else: `None`. A rejection or a fault is an error status, never
///   an outcome; a key reused for another payload
///   ([`StoreError::KeyReused`](annals::next::StoreError::KeyReused): a
///   request id, or a fact's external id by default) is
///   FAILED_PRECONDITION, never DUPLICATE. annals never answers PARKED: an ingest is infallible by
///   type, and in process a panicking one aborts the request with nothing
///   written (there is no dead-letter queue to park it in).
pub fn outcome<R>(result: &Result<Recorded, Failure<R>>) -> Option<pb::Outcome> {
    todo!("proposal: APPLIED with projector failures, DUPLICATE (divergent too), else None")
}
