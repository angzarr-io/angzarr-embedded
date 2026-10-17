//! The identities the kernel stamps and compares. The kernel never makes
//! one up: a clock, a UUID generator, a hash and an account service are
//! I/O or dependencies, and the kernel has neither. The application
//! supplies them ([`Sampled`](super::Sampled)): the instant, the UUIDv7 a
//! request without a client-supplied id is given, and the UUIDv5
//! derivations ([`DeriveIds`]).
//!
//! The shapes follow angzarr's wire contract (`io.angzarr.v1`), which
//! `annals-angzarr` maps onto: a stream is a `Cover`'s (domain, edition,
//! root), an event is an `EventRef`, request and correlation ids are the
//! wire's 16-byte `UUID`, and the rest are `EventMeta` and `Origin` fields.

use std::fmt;

/// An event's position in its stream, from 0. 64 bits, as angzarr's
/// sequences are.
pub type Seq = u64;

/// A UUID: its 16 bytes, most significant first (RFC 9562 byte order), as
/// angzarr's wire `UUID { bytes value }` holds one. The kernel's own type,
/// so it stays dependency-free; `annals-angzarr` maps it to and from
/// `angzarr_proto::v1::Uuid` with angzarr-proto's helpers.
///
/// **Operations use the bytes.** Ids are compared, ordered, hashed and
/// stored as their 16 bytes and never round-trip through text. Text is a
/// display concern: [`Display`](fmt::Display) writes the canonical form
/// (36 characters, lowercase, hyphenated 8-4-4-4-12), for logs and
/// messages; parsing text a person typed is the adapter's
/// (`annals_angzarr::ids::parse`). The kernel never inspects a version:
/// a client-supplied id may be any 16 bytes.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    pub const fn from_bytes(bytes: [u8; 16]) -> Uuid {
        Uuid(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    /// The RFC 9562 version: the high nibble of byte 6 (7 for a minted
    /// id, 5 for a derived one). For audit and tests only; the kernel
    /// never checks it.
    pub const fn version(self) -> u8 {
        self.0[6] >> 4
    }
}

/// The canonical text: lowercase hex, hyphenated 8-4-4-4-12. Display only.
impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, byte) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// The aggregate root a stream belongs to: 128 bits, so a UUID fits exactly
/// (big-endian, as [`Root::from_bytes`] reads it) and so does any `u64`.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Root(pub u128);

impl Root {
    /// A UUID's 16 bytes, in the order a UUID is written.
    pub const fn from_bytes(bytes: [u8; 16]) -> Root {
        Root(u128::from_be_bytes(bytes))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }
}

impl From<u64> for Root {
    fn from(id: u64) -> Root {
        Root(u128::from(id))
    }
}

/// The name of a non-main timeline (angzarr's editions). Never empty.
///
/// **The main timeline has no name.** A stream's edition is an
/// `Option<EditionName>`: `None` is the main timeline, `Some(name)` a named
/// edition. There is no magic string for main, so no spelling of it can be
/// mistaken for another, and an empty name is refused when it is built
/// ([`EditionName::new`]), as angzarr refuses a set empty `Cover.edition`.
///
/// annals keeps no edition registry: it does not model angzarr's explicit
/// create, divergence points or read-through from main (see
/// `docs/design/audit.md`, *Divergences*). A named edition's streams are
/// separate streams that start empty.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EditionName(String);

/// An edition name that was empty: the main timeline is `None`, never `""`.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct EmptyEditionName;

impl EditionName {
    /// The edition `name` names. Refuses `""`. Names are compared exactly,
    /// case included.
    pub fn new(name: &str) -> Result<EditionName, EmptyEditionName> {
        todo!("proposal: refuse \"\", else EditionName(name)")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EditionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Names a stream: its domain, its timeline and its root. Two keys are the
/// same stream exactly when all three are equal; the same root in another
/// domain or edition is another stream.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamKey {
    /// The domain's [`Domain::NAME`](super::Domain::NAME).
    pub domain: String,
    /// `None`: the main timeline.
    pub edition: Option<EditionName>,
    pub root: Root,
}

impl StreamKey {
    pub fn new(domain: &str, edition: Option<EditionName>, root: Root) -> StreamKey {
        StreamKey {
            domain: domain.to_string(),
            edition,
            root,
        }
    }

    /// `domain`'s stream `root`, on the main timeline.
    pub fn main(domain: &str, root: Root) -> StreamKey {
        StreamKey::new(domain, None, root)
    }

    /// `D`'s stream `root` in `edition`.
    pub fn of<D: super::Domain>(edition: Option<EditionName>, root: Root) -> StreamKey {
        StreamKey::new(D::NAME, edition, root)
    }

    pub fn is_main(&self) -> bool {
        self.edition.is_none()
    }
}

/// One event, anywhere: its stream and its seq in it (angzarr's
/// `EventRef { stream, sequence }`). Unique for as long as stream keys are,
/// never minted, and the same on every replay. It is the event id, and
/// what a causation points at.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EventId {
    pub stream: StreamKey,
    pub seq: Seq,
}

/// The request a command came in as: one command's id, so a batch of
/// commands has one each, grouped by correlation. A UUID. **Always
/// assigned**: either the caller's (client-supplied: any 16 bytes, reused
/// verbatim on every retry of that command, and alone its idempotency key
/// at the addressed stream), or, when the caller supplied none, the UUIDv7 the
/// application minted for the chain ([`Sampled::request`](super::Sampled::request)),
/// recorded with [`RequestOrigin::minted`](super::RequestOrigin::minted)
/// set and never deduplicating anything.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RequestId(pub Uuid);

/// Groups every event that follows from one workflow, across streams,
/// reaction hops and requests. A UUID. **Always set on a recorded event**:
/// the caller's, else derived from the command's origin
/// ([`Correlation::derived`]). A reaction's commands keep their trigger's.
/// Nothing is minted for it: a request always has a request id by then.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Correlation(pub Uuid);

impl Correlation {
    /// The correlation of a book that supplied none, from its resolved
    /// origin (angzarr's rule, `Provenance` in `types.proto`):
    ///
    /// - a request: its request id verbatim, the same 16 bytes, whether
    ///   client-supplied (every retry lands in one correlation) or minted;
    /// - a fact: UUIDv5(`FACT_CORRELATION` namespace, `external_id`), which
    ///   the kernel cannot compute and asks `derive` for (every redelivery
    ///   of one external id lands in one correlation);
    /// - a causation: `None`. A reaction's command is stamped with its
    ///   trigger's correlation, never derived.
    pub fn derived(origin: &super::Origin, derive: &dyn DeriveIds) -> Option<Correlation> {
        todo!("proposal: the request id's bytes, derive.fact_correlation(external_id), else None")
    }
}

/// The UUIDv5 derivations the kernel needs but cannot compute: UUIDv5 is
/// SHA-1, and the kernel has no dependencies. The application supplies
/// them in [`Sampled::derive`](super::Sampled::derive);
/// `annals_angzarr::ids::AngzarrIds` implements this over
/// `angzarr_proto::ids`, so annals derives exactly angzarr's ids.
///
/// Derived roots (`compute_root`) are not here: the kernel never derives a
/// root. Domain code that keys a stream by a business key
/// ([`Reaction::root`](super::Reaction::root), an API handler) calls
/// `annals_angzarr::ids::compute_root(domain, key)`.
pub trait DeriveIds: Sync {
    /// UUIDv5(`ANGZARR_FACT_CORRELATION_NAMESPACE`, `external_id`): a fact's
    /// correlation when its book supplies none.
    fn fact_correlation(&self, external_id: &str) -> Correlation;
}

/// A fingerprint of a keyed command's payload **as received**, recorded
/// with its key ([`Claim`](super::Claim)) so a key reused for a different
/// payload is caught ([`StoreError::KeyReused`](super::StoreError::KeyReused)),
/// as angzarr's claim is. A UUID (`IdempotencyClaim.fingerprint`, 16
/// bytes): angzarr's UUIDv5 under `ANGZARR_FINGERPRINT_NAMESPACE` of the
/// payload `Any`'s type URL, length-prefixed, and its value bytes. Opaque to
/// the kernel, which only compares it (it has no SHA-1): the adapter
/// computes it (`annals_angzarr::fingerprint`, over
/// `angzarr_proto::ids::fingerprint`).
///
/// **Misuse detection, not tamper protection**: it catches an honest
/// caller (or source) reusing a key for another payload. It makes no claim
/// against an adversary, and it is unrelated to the deferred event hash
/// chain.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Fingerprint(pub Uuid);

/// Who asked: opaque to the kernel. Make it a **pseudonymous** id, never a
/// name or address, so erasing a person deletes the mapping outside the log
/// and never rewrites history (which a hash chain would forbid anyway).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Actor(pub String);

/// When an event was recorded: microseconds since the Unix epoch, UTC.
/// Audit time: the kernel stamps it from the application's sampled instant
/// and never lets it go backwards within a stream
/// ([`Meta::recorded_at`](super::Meta::recorded_at)). Business time (a
/// deadline, a cooldown's start) belongs in the event's payload, where
/// `apply` can see it.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Timestamp(pub i64);

/// Names one version of a domain's rules: the reference data `apply` and
/// `decide` read. Declared at registration (the [`Rulebook`]'s current
/// version), stamped on every event by the kernel, and **load-bearing**:
/// replay folds each event with the rules its version names, and a
/// snapshot is used only under the version it was folded with.
///
/// [`Rulebook`]: super::Rulebook
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RulesVersion(pub String);

impl From<&str> for RulesVersion {
    fn from(version: &str) -> RulesVersion {
        RulesVersion(version.to_string())
    }
}
