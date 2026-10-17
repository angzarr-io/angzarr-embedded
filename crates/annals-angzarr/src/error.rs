//! The adapter's errors. Each becomes an
//! [`annals_diagnostic::Diagnostic`] when it is implemented, with the codes
//! `docs/design/audit.md` lists and messages in
//! `locales/en-US/annals-angzarr.ftl`; until then they have none, so the
//! catalog parity check is not asked to sample a `todo!()`.
//!
//! Every error that is about a payload names its type URL.

use std::fmt;

use annals::next::{Seq, StreamKey};
use annals_diagnostic::Report;

/// A book, a cover or a page that does not map onto annals.
#[derive(Clone, PartialEq, Debug)]
pub enum BookError {
    /// A book with no cover.
    NoCover,
    /// A cover with no root.
    NoRoot,
    /// A root that is not a UUID's 16 bytes.
    BadRoot {
        len: usize,
    },
    /// A set request or correlation id that is not a UUID's 16 bytes:
    /// angzarr refuses it INVALID_ARGUMENT.
    BadId {
        len: usize,
    },
    /// Text that is not a hyphenated 8-4-4-4-12 UUID, at an input boundary
    /// ([`ids::parse`](crate::ids::parse)).
    BadIdText,
    /// A domain containing `:`, which [`ids::compute_root`](crate::ids::compute_root)
    /// refuses: it separates the domain from the key.
    DomainContainsColon {
        domain: String,
    },
    /// A set but empty `Cover.edition`: the main timeline is the unset
    /// edition, never `""` (angzarr refuses it INVALID_ARGUMENT too).
    EmptyEdition,
    /// An `EventRef` with no stream.
    NoStream,
    /// A command book for another domain than the book's.
    WrongDomain {
        found: String,
        expected: &'static str,
    },
    /// A command book with no page.
    NoPage,
    /// A page with neither a command nor a fact.
    NoBody,
    /// A command, a fact or an event with no payload.
    NoPayload,
    /// A causation origin with no event.
    NoCausationEvent,
    /// Event meta with no `recorded_at`: angzarr always records it.
    NoRecordedAt,
    /// Event meta with no correlation: always set on a recorded event.
    NoCorrelation,
    /// A request origin with no request id on a recorded event: every
    /// recorded request has one, client-supplied or minted.
    NoRequestId,
    /// A command's request origin with no request id but a non-zero index:
    /// the kernel records such a request as minted, index 0, so an index
    /// would be lost. (angzarr keeps the index on its minted origin; see
    /// `docs/design/audit.md`, open questions.)
    IndexWithoutRequestId {
        index: u32,
    },
    /// A `ConflictResolution` value this build does not know.
    UnknownConflictResolution {
        value: i32,
    },
    /// A timestamp finer than the microsecond annals records.
    SubMicrosecond {
        seconds: i64,
        nanos: i32,
    },
    /// A stream whose domain the [`Registry`](crate::Registry) does not
    /// know.
    UnknownDomain {
        domain: String,
    },
    Payload(PayloadError),
}

/// A payload that is not one of the types a domain decodes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PayloadError {
    /// No decoder for this type URL: not a current type of the domain's
    /// (strict replay: never skipped).
    UnknownType { type_url: String },
    /// The type is known, but its bytes do not decode as it.
    Decode {
        type_url: String,
        error: prost::DecodeError,
    },
}

/// An upcast rule that failed on one event.
#[derive(Clone, PartialEq, Debug)]
pub enum UpcastError {
    /// The rule from `source` could not read its input.
    Payload(PayloadError),
    /// The rule from `source` failed for the domain's own reason.
    Rule { source: String, error: Report },
}

/// An upcaster whose rules cannot all run, found by
/// [`Registry::check`](crate::Registry::check) before anything is read.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UpcastChainError {
    /// The domain, named in every error.
    pub domain: String,
    pub problem: ChainProblem,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ChainProblem {
    /// Two typed rules from the same type: the second never runs.
    Duplicate { source: String },
    /// A typed rule from `source` registered before the rule that produces
    /// `source`: rules run in registration order, so an event reaching
    /// `source` stops there.
    OutOfOrder { source: String, produced_by: String },
    /// A chain from `source` ends at `ends_at`, which the domain does not
    /// decode: every event of that type would fail replay.
    DeadEnd { source: String, ends_at: String },
    /// An upcaster registered for `expected` names another domain.
    WrongDomain {
        found: String,
        expected: &'static str,
    },
}

/// A stored event that cannot be given back as a current event. Becomes a
/// [`StoreError::Unreadable`](annals::next::StoreError::Unreadable) at the
/// store port, as a report carrying the same args.
#[derive(Clone, PartialEq, Debug)]
pub struct ReadError {
    pub stream: StreamKey,
    pub seq: Seq,
    pub problem: ReadProblem,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ReadProblem {
    /// The stream's domain has no entry in the registry.
    UnknownDomain,
    /// The stored page bytes are not an `EventPage`.
    Page(prost::DecodeError),
    /// The page holds no payload, or meta that does not read.
    Book(BookError),
    /// Upcasting failed.
    Upcast(UpcastError),
    /// After upcasting, the payload is not a type the domain decodes: no
    /// upcaster and no decoder for its type URL.
    Payload(PayloadError),
}

impl ReadError {
    /// The type URL the error is about, if it is about a payload.
    pub fn type_url(&self) -> Option<&str> {
        todo!("proposal: from Payload and Upcast(Payload)")
    }

    /// This error as a store port's
    /// [`StoreError::Unreadable`](annals::next::StoreError::Unreadable):
    /// the stream, the seq, and this error as a report.
    pub fn into_store_error(self) -> annals::next::StoreError {
        todo!("proposal: Unreadable {{ stream, seq, reason: Report::of(&self) }}")
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("proposal: name the stream, the seq and the type URL")
    }
}

impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("proposal: name the type URL")
    }
}

impl From<PayloadError> for BookError {
    fn from(e: PayloadError) -> BookError {
        BookError::Payload(e)
    }
}
