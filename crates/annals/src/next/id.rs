//! The identities the kernel stamps and compares. All opaque to the kernel,
//! which never mints a random one: the application supplies them (a clock,
//! a UUID generator and an account service are I/O, and the kernel does
//! none). The one value the kernel makes up, a correlation for a command
//! that arrives without one, it derives from the request
//! ([`Correlation::generated`]), so replaying or retrying gives the same.
//!
//! The shapes follow angzarr's wire contract (`io.angzarr.v1`), which
//! `annals-angzarr` maps onto: a stream is a `Cover`'s (domain, edition,
//! root), an event is an `EventRef`, and the rest are `EventMeta` fields.

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

/// A timeline. Every stream is in one: the main timeline, or a named
/// edition that diverged from it (angzarr's editions).
///
/// The main timeline has three spellings on the wire, all the same
/// timeline: an empty name, its canonical name [`Edition::MAIN_NAME`]
/// (`"angzarr"`), and none at all. [`Edition::new`] and the `From` impls
/// normalise all three to [`Edition::MAIN`], so keys built from any of them
/// are equal and order together; nothing outside this type can build a
/// second main timeline.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Edition(Option<String>);

impl Edition {
    /// The main timeline.
    pub const MAIN: Edition = Edition(None);

    /// The main timeline's canonical name, as angzarr writes it.
    pub const MAIN_NAME: &'static str = "angzarr";

    /// The edition `name` names: [`Edition::MAIN`] for `""` and
    /// [`Edition::MAIN_NAME`], a named edition otherwise. Names are compared
    /// exactly, case included.
    pub fn new(name: &str) -> Edition {
        todo!("proposal: \"\" and MAIN_NAME are MAIN, anything else is Some(name)")
    }

    /// The edition's name: [`Edition::MAIN_NAME`] for the main timeline.
    pub fn name(&self) -> &str {
        todo!("proposal: MAIN_NAME for None, else the name")
    }

    pub fn is_main(&self) -> bool {
        self.0.is_none()
    }
}

impl From<&str> for Edition {
    fn from(name: &str) -> Edition {
        Edition::new(name)
    }
}

/// An unset edition is the main timeline.
impl From<Option<&str>> for Edition {
    fn from(name: Option<&str>) -> Edition {
        name.map_or(Edition::MAIN, Edition::new)
    }
}

/// Names a stream: its domain, its timeline and its root. Two keys are the
/// same stream exactly when all three are equal, the edition normalised
/// ([`Edition`]); the same root in another domain or edition is another
/// stream.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamKey {
    /// The domain's [`Domain::NAME`](super::Domain::NAME).
    pub domain: String,
    pub edition: Edition,
    pub root: Root,
}

impl StreamKey {
    pub fn new(domain: &str, edition: Edition, root: Root) -> StreamKey {
        StreamKey {
            domain: domain.to_string(),
            edition,
            root,
        }
    }

    /// `domain`'s stream `root`, on the main timeline.
    pub fn main(domain: &str, root: Root) -> StreamKey {
        StreamKey::new(domain, Edition::MAIN, root)
    }

    /// `D`'s stream `root` in `edition`.
    pub fn of<D: super::Domain>(edition: Edition, root: Root) -> StreamKey {
        StreamKey::new(D::NAME, edition, root)
    }
}

/// One event, anywhere: its stream and its seq in it (angzarr's
/// `EventRef`). Unique for as long as stream keys are, never minted, and
/// the same on every replay. It is the event id, and what a cause points at.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EventId {
    pub stream: StreamKey,
    pub seq: Seq,
}

/// The request a command came in on: one API call, one job run. Opaque;
/// supplied by the application (a UUID's text fits). One request may issue
/// several commands, and every command a chain issues in reaction keeps its
/// origin's request.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RequestId(pub String);

impl From<&str> for RequestId {
    fn from(id: &str) -> RequestId {
        RequestId(id.to_string())
    }
}

/// Names one command within its request, deterministically, so that with
/// the request it serves as an **idempotency key**: a retried command keeps
/// its id, and a store that records it can recognise the retry
/// ([`StoreError::Duplicate`](super::StoreError::Duplicate)).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum CommandId {
    /// A command the application issued: the `index`th of its request.
    Origin { index: u32 },
    /// A command (or fact) a reaction issued: the `index`th output of the
    /// reaction named `component` (its [`Reaction::NAME`]) for the source
    /// event `source`. Recomputable from the log, since reactions are pure.
    ///
    /// [`Reaction::NAME`]: super::Reaction::NAME
    Reaction {
        source: EventId,
        component: String,
        index: u32,
    },
}

/// Groups every event that follows from one originating request, across
/// streams, across reaction hops and across commands (a workflow keeps one
/// correlation over many requests). Opaque. Always set on a recorded event:
/// a command that arrives without one gets [`Correlation::generated`]; a
/// reaction's outputs keep their source's.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Correlation(pub String);

impl Correlation {
    /// The correlation of a command that arrived without one: derived from
    /// its request, so the same request always gets the same correlation
    /// and the kernel needs no randomness. It is the request id's text.
    pub fn generated(request: &RequestId) -> Correlation {
        todo!("proposal: the request id's text")
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
/// Audit time, supplied by the application per command (angzarr's
/// server-stamped `recorded_at`). Business time (a deadline, a cooldown's
/// start) belongs in the event's payload, where `apply` can see it.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Timestamp(pub i64);

/// Names one version of a domain's rules: the reference data `apply` and
/// `decide` read. Recorded on every event, so replay can find the rules each
/// event was decided under.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RulesVersion(pub String);

impl From<&str> for RulesVersion {
    fn from(version: &str) -> RulesVersion {
        RulesVersion(version.to_string())
    }
}
