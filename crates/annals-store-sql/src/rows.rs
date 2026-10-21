//! Stored values read back: what a backend's rows decode to.

use annals::next::v1::{Cover, EventPage, Snapshot, Uuid};
use annals::next::StoreError;
use prost::Message;

use crate::error::{unavailable, undecodable};
use crate::Dialect;

/// A page's stored bytes, decoded; `sequence` names it if they do not.
pub(crate) fn page(cover: &Cover, sequence: i64, bytes: &[u8]) -> Result<EventPage, StoreError> {
    EventPage::decode(bytes).map_err(|e| StoreError::Unreadable {
        stream: cover.clone(),
        sequence: sequence.unsigned_abs(),
        reason: undecodable(e),
    })
}

/// A snapshot's stored bytes, decoded.
pub(crate) fn snapshot(cover: &Cover, bytes: &[u8]) -> Result<Snapshot, StoreError> {
    Snapshot::decode(bytes).map_err(|e| StoreError::Unreadable {
        stream: cover.clone(),
        sequence: 0,
        reason: undecodable(e),
    })
}

/// A stream of `domain` from its stored edition and root.
pub(crate) fn cover<D: Dialect>(
    domain: &str,
    edition: String,
    root: &[u8],
) -> Result<Cover, StoreError> {
    let root: [u8; 16] = root
        .try_into()
        .map_err(|_| unavailable(format!("a stored root of {} bytes, not 16", root.len())))?;
    Ok(Cover {
        domain: domain.to_string(),
        root: Some(Uuid::from_bytes(root)),
        edition: D::cover_edition(edition),
    })
}
