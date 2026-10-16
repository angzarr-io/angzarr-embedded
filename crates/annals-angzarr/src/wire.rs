//! The small conversions between annals' identities and angzarr's
//! messages: a key and a `Cover`, an event id and an `EventRef`, a root and
//! a `UUID`, a timestamp, a `Meta` and an `EventMeta`.
//!
//! Two mappings are each kept behind one function per direction, because
//! where angzarr carries them is under review and may change in
//! angzarr-proto:
//!
//! - **correlation**: today on `Cover.correlation_id` and again per page in
//!   `EventMeta.correlation_id` / `CommandMeta.correlation_id`
//!   ([`correlation_to_wire`], [`correlation_from_wire`]);
//! - **a command's cause**: today in `CommandMeta.cause` and
//!   `command_index`, and again, for a reaction's command, in the page
//!   header's `AngzarrDeferredSequence` ([`command_cause_to_wire`],
//!   [`command_cause_from_wire`]).
//!
//! Switching either source is a change to its pair of functions and nothing
//! else.

use angzarr_proto::v1::{self as pb, Cover, EventRef, PageHeader, Uuid};
use annals::next::{CommandId, Correlation, Edition, EventId, Meta, Root, StreamKey, Timestamp};

use crate::error::BookError;

/// A root as angzarr's `UUID`: its 16 bytes, big-endian.
pub fn uuid(root: Root) -> Uuid {
    todo!("proposal: Uuid {{ value: root.to_bytes().to_vec() }}")
}

/// A `UUID` as a root. Refuses anything but 16 bytes.
pub fn root(uuid: &Uuid) -> Result<Root, BookError> {
    todo!("proposal: exactly 16 bytes, else BookError::BadRoot")
}

/// A key's edition as a `Cover`'s: unset for the main timeline (the wire
/// form angzarr's `Edition` documents), the name otherwise, with no
/// divergences.
pub fn edition(edition: &Edition) -> Option<pb::Edition> {
    todo!("proposal: None for MAIN, else Some(Edition {{ name, divergences: [] }})")
}

/// A `Cover` for `key`. Its correlation is set by [`correlation_to_wire`],
/// not here.
pub fn cover(key: &StreamKey) -> Cover {
    todo!("proposal: domain, root, edition; correlation and ext unset")
}

/// The key a `Cover` names, its edition normalised ("", "angzarr" and
/// unset are the main timeline). Refuses a cover with no root, a root that
/// is not 16 bytes, and explicit divergences, which annals does not model.
pub fn key(cover: &Cover) -> Result<StreamKey, BookError> {
    todo!("proposal: Edition::from(name), refuse divergences")
}

/// An event id as an `EventRef`: its stream's domain, edition (by its
/// canonical name: [`Edition::MAIN_NAME`] for the main timeline), root,
/// and its seq as the sequence.
pub fn event_ref(id: &EventId) -> EventRef {
    todo!("proposal: domain, edition.name(), uuid(root), seq")
}

/// The event id an `EventRef` names, its edition normalised.
pub fn event_id(event: &EventRef) -> Result<EventId, BookError> {
    todo!("proposal: the inverse of event_ref")
}

/// Microseconds since the epoch as a protobuf timestamp: exact.
pub fn timestamp(at: Timestamp) -> prost_types::Timestamp {
    todo!("proposal: seconds and nanos, flooring toward negative infinity")
}

/// A protobuf timestamp as microseconds since the epoch. Refuses a
/// timestamp finer than a microsecond, so nothing is silently rounded.
pub fn micros(at: &prost_types::Timestamp) -> Result<Timestamp, BookError> {
    todo!("proposal: seconds * 1e6 + nanos / 1e3, refusing a sub-microsecond remainder")
}

/// A recorded event's metadata as angzarr's `EventMeta`: `recorded_at`,
/// `actor` (empty for the system), `request_id`, `command_index`, the
/// correlation ([`correlation_to_wire`]), `cause` and `rules_version`.
///
/// A reaction's component ([`CommandId::Reaction`]'s `component`) has no
/// `EventMeta` field at angzarr-proto 9cfed59, so it is not on the page; it
/// is kept in storage, in the row's metadata (see [`crate::Stored`]).
pub fn event_meta(meta: &Meta) -> pb::EventMeta {
    todo!("proposal: field for field; correlation through correlation_to_wire")
}

/// Where a correlation goes on the wire.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WireCorrelation {
    /// For `Cover.correlation_id`.
    pub cover: String,
    /// For the page's `EventMeta.correlation_id` or
    /// `CommandMeta.correlation_id`.
    pub page: String,
}

/// **The one place a correlation is put on the wire.** angzarr carries it
/// on the cover and on each page's metadata today; which of them survives
/// is under review, so every writer goes through here.
pub fn correlation_to_wire(correlation: &Correlation) -> WireCorrelation {
    todo!("proposal: both fields, the correlation's text")
}

/// **The one place a correlation is read off the wire**, from a cover and
/// one page's correlation field: `None` when neither carries one (annals
/// then generates one from the request: [`Correlation::generated`]).
pub fn correlation_from_wire(cover: &Cover, page: &str) -> Option<Correlation> {
    todo!("proposal: the page's if set, else the cover's, else None")
}

/// Where a command's identity and cause go on a `CommandPage`.
#[derive(Clone, PartialEq, Debug)]
pub struct WireCommandCause {
    /// The page header: `AngzarrDeferredSequence { source, source_seq,
    /// source_component, command_index }` for a reaction's command, unset
    /// for a command the application issued.
    pub header: Option<PageHeader>,
    /// `CommandMeta.command_index`.
    pub command_index: u32,
    /// `CommandMeta.cause`.
    pub cause: Option<EventRef>,
}

/// **The one place a command's identity and cause are put on the wire.**
/// angzarr carries a reaction's cause twice today (`CommandMeta.cause` and
/// `command_index`, and the header's `AngzarrDeferredSequence`); which
/// survives is under review, so every writer goes through here.
pub fn command_cause_to_wire(command: &CommandId, cause: Option<&EventId>) -> WireCommandCause {
    todo!("proposal: Origin: no header, index, cause; Reaction: deferred header, index, cause")
}

/// **The one place a command's identity and cause are read off the
/// wire.** A deferred header makes it a [`CommandId::Reaction`]; no header,
/// or an explicit sequence, an [`CommandId::Origin`]. An external deferred
/// sequence (a webhook's fact) is refused: annals has no such commands.
pub fn command_cause_from_wire(
    page: &pb::CommandPage,
) -> Result<(CommandId, Option<EventId>), BookError> {
    todo!("proposal: the inverse of command_cause_to_wire")
}
