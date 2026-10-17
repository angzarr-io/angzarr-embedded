//! The small conversions between annals' identities and angzarr's
//! messages, one function per direction: a key and a `Cover`, an event id
//! and an `EventRef`, a root and a `UUID`, a timestamp, provenance, an
//! origin, handling, a `Meta` and an `EventMeta`, a snapshot.
//!
//! At angzarr-proto 279bc89 each value has exactly one home on the wire:
//! correlation lives only in `Provenance` (never on the `Cover`), a
//! command's cause only in its `Origin`, and the main timeline is the unset
//! `Cover.edition`.

use angzarr_proto::v1::{self as pb, Cover, EventRef, Uuid};
use annals::next::{
    ConflictResolution, EditionName, EventId, Handling, Meta, Origin, Provenance, Root, Snapshot,
    StreamKey, Timestamp,
};

use crate::error::BookError;
use crate::payload::Payload;

/// A root as angzarr's `UUID`: its 16 bytes, big-endian.
pub fn uuid(root: Root) -> Uuid {
    todo!("proposal: Uuid {{ value: root.to_bytes().to_vec() }}")
}

/// A `UUID` as a root. Refuses anything but 16 bytes.
pub fn root(uuid: &Uuid) -> Result<Root, BookError> {
    todo!("proposal: exactly 16 bytes, else BookError::BadRoot")
}

/// A key's edition as `Cover.edition` (`optional string`): unset for the
/// main timeline, the name for a named edition.
pub fn edition(edition: Option<&EditionName>) -> Option<String> {
    todo!("proposal: edition.map(|e| e.as_str().to_string())")
}

/// `Cover.edition` as a key's edition: unset is main; a set name is a named
/// edition; a set empty string is refused ([`BookError::EmptyEdition`]),
/// as angzarr refuses it (INVALID_ARGUMENT).
pub fn edition_name(edition: Option<&str>) -> Result<Option<EditionName>, BookError> {
    todo!("proposal: None => None, Some(\"\") => EmptyEdition, Some(name) => Some(EditionName)")
}

/// A `Cover` for `key`: domain, root, edition. Nothing else: a cover is
/// stream identity only.
pub fn cover(key: &StreamKey) -> Cover {
    todo!("proposal: domain, uuid(root), edition(key.edition)")
}

/// The key a `Cover` names. Refuses a cover with no root, a root that is
/// not 16 bytes, and a set empty edition.
pub fn key(cover: &Cover) -> Result<StreamKey, BookError> {
    todo!("proposal: root(cover.root), edition_name(cover.edition)")
}

/// An event id as an `EventRef { stream: cover, sequence }`.
pub fn event_ref(id: &EventId) -> EventRef {
    todo!("proposal: cover(&id.stream), id.seq")
}

/// The event id an `EventRef` names. Refuses one with no stream
/// ([`BookError::NoStream`]) or a cover [`key`] refuses.
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

/// A command's provenance as sent: an absent actor or correlation is the
/// empty string, as angzarr's "not supplied".
pub fn provenance(provenance: &Provenance) -> pb::Provenance {
    todo!("proposal: actor and correlation_id, empty for None")
}

/// The inverse of [`provenance`]: an empty field is `None`.
pub fn provenance_from_wire(provenance: Option<&pb::Provenance>) -> Provenance {
    todo!("proposal: empty strings are None; no provenance is the default")
}

/// An origin as angzarr's one `Origin` message: a keyless request is an
/// empty `RequestOrigin` (`request_id` ""); a causation's event an
/// `EventRef`.
pub fn origin(origin: &Origin) -> pb::Origin {
    todo!("proposal: one arm per variant")
}

/// The inverse of [`origin`]. No origin at all is a keyless request (as
/// angzarr records it). Refuses a causation with no event
/// ([`BookError::NoCausationEvent`]) or an event ref [`event_id`] refuses.
/// Whether the origin agrees with the body is the kernel's check
/// ([`Book::validate`](annals::next::Book::validate)), not this one's.
pub fn origin_from_wire(origin: Option<&pb::Origin>) -> Result<Origin, BookError> {
    todo!("proposal: the inverse of origin; unset is Origin::keyless()")
}

/// A resolution as angzarr's enum.
pub fn conflict_resolution(resolution: ConflictResolution) -> pb::ConflictResolution {
    todo!("proposal: one value each")
}

/// Handling as sent: `expected_sequence` as given, the resolution
/// UNSPECIFIED when the book leaves it to the domain.
pub fn handling(handling: &Handling) -> pb::Handling {
    todo!("proposal: expected_sequence, conflict_resolution or UNSPECIFIED")
}

/// The inverse of [`handling`]: no handling is the default; UNSPECIFIED is
/// `None` (the domain's default); a value the enum does not know is
/// refused ([`BookError::UnknownConflictResolution`]).
pub fn handling_from_wire(handling: Option<&pb::Handling>) -> Result<Handling, BookError> {
    todo!("proposal: the inverse of handling")
}

/// A recorded event's metadata as angzarr's `EventMeta`: `provenance`
/// (actor, empty for the system; correlation), `origin`, `recorded_at`,
/// `base_sequence`, `rules_version`. Field for field; nothing is left out.
pub fn event_meta(meta: &Meta) -> pb::EventMeta {
    todo!("proposal: field for field")
}

/// The inverse of [`event_meta`]: what an `EventPage` read back says.
/// Refuses meta with no `recorded_at` ([`BookError::NoRecordedAt`]) or an
/// empty correlation ([`BookError::NoCorrelation`]): angzarr always records
/// both.
pub fn meta(meta: &pb::EventMeta) -> Result<Meta, BookError> {
    todo!("proposal: the inverse of event_meta")
}

/// A snapshot as angzarr's `Snapshot`: `sequence`, `state` packed as `Any`
/// (its type URL is the state type compared on load), `rules_version`. Its
/// `recorded_at` has no field there; the adapter keeps it beside the row.
pub fn snapshot<S: Payload>(snapshot: &Snapshot<S>) -> pb::Snapshot {
    todo!("proposal: sequence, state.to_any(), rules_version")
}

/// A stored snapshot, if its state is a type `S` decodes: `None` when it is
/// not (the state type changed), so the load discards it and replays, never
/// an error. Whether its rules version is current is the kernel's check
/// ([`Snapshot::usable`]).
pub fn snapshot_from_wire<S: Payload>(
    snapshot: &pb::Snapshot,
    recorded_at: Timestamp,
) -> Option<Snapshot<S>> {
    todo!("proposal: S::from_any(state).ok(), with the sequence and rules version")
}
