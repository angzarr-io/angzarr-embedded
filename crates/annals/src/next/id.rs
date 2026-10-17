//! The identities the kernel stamps and compares. All opaque to the kernel,
//! which never makes up a random one: the application supplies them (a
//! clock, a UUID generator and an account service are I/O, and the kernel
//! does none), including the fresh correlation a keyless request is given
//! ([`Sampled::minted`](super::Sampled::minted)).
//!
//! The shapes follow angzarr's wire contract (`io.angzarr.v1`), which
//! `annals-angzarr` maps onto: a stream is a `Cover`'s (domain, edition,
//! root), an event is an `EventRef`, and the rest are `EventMeta` and
//! `Origin` fields.

use std::fmt;

/// An event's position in its stream, from 0. 64 bits, as angzarr's
/// sequences are.
pub type Seq = u64;

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

/// The inbound request a command came in on: one API call, one job run.
/// Opaque; chosen by the caller and reused verbatim on every retry. With
/// the command's index in it, the command's key
/// ([`RequestOrigin`](super::RequestOrigin)).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RequestId(pub String);

impl From<&str> for RequestId {
    fn from(id: &str) -> RequestId {
        RequestId(id.to_string())
    }
}

/// Groups every event that follows from one workflow, across streams,
/// reaction hops and requests. Opaque. **Always set on a recorded event**:
/// the caller's, else derived from the command's key
/// ([`Correlation::derived`]), else the one the application minted for the
/// command ([`Sampled::minted`](super::Sampled::minted)). A reaction's
/// commands keep their trigger's.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Correlation(pub String);

impl Correlation {
    /// The correlation derived from a command's key, so every retry of one
    /// request (or one redelivered fact) lands in one correlation: the
    /// request id's text for a keyed request, the external id's for a fact.
    /// `None` for a keyless request (one is minted) and for a causation (a
    /// reaction keeps its trigger's).
    pub fn derived(origin: &super::Origin) -> Option<Correlation> {
        todo!("proposal: the request id's or the external id's text, else None")
    }
}

impl From<&str> for Correlation {
    fn from(id: &str) -> Correlation {
        Correlation(id.to_string())
    }
}

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
