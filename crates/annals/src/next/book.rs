//! One command addressed to one stream, as angzarr's one-page
//! `CommandBook` carries it: the stream, how to handle it, its provenance
//! and origin, and a body that is either a decision-requesting command or
//! an already-happened fact.

use super::domain::Domain;
use super::id::{Correlation, Seq, StreamKey, Timestamp};
use super::meta::{Origin, Provenance};

/// What the command asks for: a decision (fallible, [`Domain::decide`]) or
/// the recording of a fact (infallible, [`Domain::ingest`]). The arm picks
/// the pipeline (angzarr's reflective dispatch on the `CommandPage` body).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Body<C, F> {
    Command(C),
    Fact(F),
}

/// How a stale `expected_sequence` is treated (angzarr's
/// `ConflictResolution`). Matters only when the sender gave one.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub enum ConflictResolution {
    /// Any mismatch fails as [`KernelError::Stale`](super::KernelError::Stale),
    /// before decide runs. The default.
    #[default]
    Strict,
    /// decide runs against the head, is handed the window (the events from
    /// `expected_sequence` on) with a [`Nonce`](super::Nonce), and must
    /// acknowledge it ([`Tx::resolve_conflicts`](super::Tx::resolve_conflicts));
    /// unacknowledged, the command fails as stale
    /// ([`KernelError::Unresolved`](super::KernelError::Unresolved)).
    AggregateHandles,
    /// As `AggregateHandles`, but the kernel acknowledges for decide: it
    /// keeps the command's events iff the fields the window changed and the
    /// fields the command's events change are disjoint
    /// ([`Domain::changed`]); otherwise the command is stale. Opt-in.
    Commutative,
}

/// How to handle one command (angzarr's `Handling`): instructions, never
/// persisted. A policy's command carries none.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Handling {
    /// The stream length the sender issued the command against. `None`:
    /// the kernel uses the head it decides against (never stale). Forbidden
    /// on a fact.
    pub expected_sequence: Option<Seq>,
    /// `None`: the domain's own default for the command
    /// ([`Domain::conflict_resolution`]).
    pub conflict_resolution: Option<ConflictResolution>,
}

/// One command to one stream: angzarr's `CommandBook { cover, handling,
/// page { provenance, origin, body } }`, typed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Book<C, F> {
    /// `CommandBook.cover`.
    pub stream: StreamKey,
    pub handling: Handling,
    pub provenance: Provenance,
    pub origin: Origin,
    pub body: Body<C, F>,
}

/// A book for `D`.
pub type BookOf<D> = Book<<D as Domain>::Command, <D as Domain>::Fact>;

impl<C, F> Book<C, F> {
    /// A command, with no handling (no expected sequence, the domain's
    /// default resolution).
    pub fn command(stream: StreamKey, cmd: C, origin: Origin, provenance: Provenance) -> Self {
        Book {
            stream,
            handling: Handling::default(),
            provenance,
            origin,
            body: Body::Command(cmd),
        }
    }

    /// A fact, keyed by `origin` (which must be [`Origin::Fact`]).
    pub fn fact(stream: StreamKey, fact: F, origin: Origin, provenance: Provenance) -> Self {
        Book {
            stream,
            handling: Handling::default(),
            provenance,
            origin,
            body: Body::Fact(fact),
        }
    }

    /// Expecting the stream at `seq`, resolved by `resolution` if stale.
    pub fn expecting(mut self, seq: Seq, resolution: ConflictResolution) -> Self {
        self.handling = Handling {
            expected_sequence: Some(seq),
            conflict_resolution: Some(resolution),
        };
        self
    }

    /// Refuse a book that is malformed, before business code: a body and an
    /// origin that disagree, a fact with an empty external id or an
    /// expected sequence, a causation with any handling.
    pub fn validate(&self) -> Result<(), Malformed> {
        todo!("proposal: the checks Malformed lists, in its order")
    }

    /// The correlation the book's events are stamped with: the given one,
    /// else [`Correlation::derived`] from the origin, else `minted`.
    pub fn correlation(&self, minted: &Correlation) -> Correlation {
        todo!("proposal: given, else derived, else minted")
    }
}

/// Why a book was refused before any business code ran (angzarr's
/// INVALID_ARGUMENT, or a reactor bug). Never repaired.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Malformed {
    /// A fact without a fact origin.
    FactWithoutFactOrigin,
    /// A command with a fact origin.
    CommandWithFactOrigin,
    /// A fact origin whose external id is empty: it is the fact's key.
    EmptyExternalId,
    /// A fact with an expected sequence: facts are never checked, always
    /// stamped at the head.
    FactWithExpectedSequence,
    /// A causation (a policy's command) with handling: the target stamps
    /// its own head.
    HandlingOnReaction,
    /// COMMUTATIVE for a domain that cannot say which fields an event
    /// changed ([`Domain::changed`] is `None`).
    CommutativeUnsupported,
}

/// What the application sampled for one command and its whole chain: the
/// kernel has no clock and no randomness.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Sampled {
    /// The instant: every event of the chain is recorded at it, or at its
    /// stream's previous `recorded_at` if that is later.
    pub now: Timestamp,
    /// A fresh correlation (a UUID's text, say), used only when the book
    /// carries none and its origin derives none (a keyless request).
    pub minted: Correlation,
}
