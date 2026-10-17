//! annals' ids as angzarr's: the kernel's [`Uuid`] is the wire's 16-byte
//! `UUID`, and the ids annals derives are angzarr's own, computed by
//! `angzarr_proto::ids` rather than a second implementation:
//!
//! - **derived roots** ([`compute_root`]): UUIDv5 under
//!   `ANGZARR_ROOT_NAMESPACE` of `"{domain}:{key}"`, so a business key
//!   names the same stream under either runtime;
//! - **a fact's correlation** ([`fact_correlation`]): UUIDv5 under
//!   `ANGZARR_FACT_CORRELATION_NAMESPACE` of its `external_id`. The kernel
//!   needs it but has no SHA-1, so the application hands it
//!   [`ANGZARR_IDS`] in every [`Sampled`](annals::next::Sampled).
//!
//! Minted ids (a request id the caller did not supply) are UUIDv7s the
//! application mints; the kernel never mints. Text is for display and
//! input only ([`parse`], and the kernel's `Display`); everything else
//! passes the 16 bytes.

use angzarr_proto::v1 as pb;
use annals::next::{Correlation, DeriveIds, Root, Uuid};

use crate::error::BookError;

/// A kernel UUID as angzarr's wire `UUID`.
pub fn to_wire(id: Uuid) -> pb::Uuid {
    pb::Uuid::from_bytes(id.to_bytes())
}

/// A wire `UUID` as the kernel's. Refuses anything but 16 bytes
/// ([`BookError::BadId`]), as angzarr refuses a malformed id.
pub fn from_wire(id: &pb::Uuid) -> Result<Uuid, BookError> {
    id.to_bytes()
        .map(Uuid::from_bytes)
        .map_err(|_| BookError::BadId {
            len: id.value.len(),
        })
}

/// A UUID a person typed (a URL, a CLI argument): the hyphenated
/// 8-4-4-4-12 form, either case, exactly as angzarr-proto's `FromStr`
/// accepts it.
pub fn parse(text: &str) -> Result<Uuid, BookError> {
    let id: pb::Uuid = text.parse().map_err(|_| BookError::BadIdText)?;
    from_wire(&id)
}

/// The derived root of `key` in `domain`: angzarr's
/// `compute_root(domain, key)`. Refuses a domain containing `:`.
pub fn compute_root(domain: &str, key: &str) -> Result<Root, BookError> {
    let id = angzarr_proto::ids::compute_root(domain, key).map_err(|_| {
        BookError::DomainContainsColon {
            domain: domain.to_string(),
        }
    })?;
    from_wire(&id).map(|id| Root::from_bytes(id.to_bytes()))
}

/// A fact's correlation when its book supplies none: angzarr's
/// `fact_correlation(external_id)`.
pub fn fact_correlation(external_id: &str) -> Correlation {
    let id = angzarr_proto::ids::fact_correlation(external_id);
    Correlation(Uuid::from_bytes(
        id.to_bytes().expect("angzarr-proto derives 16 bytes"),
    ))
}

/// angzarr's derivations, for the kernel ([`DeriveIds`]).
#[derive(Copy, Clone, Debug, Default)]
pub struct AngzarrIds;

impl DeriveIds for AngzarrIds {
    fn fact_correlation(&self, external_id: &str) -> Correlation {
        fact_correlation(external_id)
    }
}

/// What an application puts in [`Sampled::derive`](annals::next::Sampled::derive).
pub static ANGZARR_IDS: AngzarrIds = AngzarrIds;
