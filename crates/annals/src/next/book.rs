//! One command addressed to one stream, as angzarr's one-page
//! `CommandBook` carries it: the stream, how to handle it, its provenance
//! and origin, and a body that is either a decision-requesting command or
//! an already-happened fact.

use super::domain::Domain;
use super::id::{Correlation, DeriveIds, Fingerprint, RequestId, Seq, StreamKey, Timestamp};
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
    /// keeps the command's events iff the conflict units the window changed
    /// and the units the command's events change do not overlap
    /// ([`Units::overlaps`](super::Units::overlaps)), each computed by the
    /// state's generated [`CommutativeState::changed`](super::CommutativeState::changed);
    /// otherwise the command is stale. Opt-in, and only for a domain whose
    /// state is a `commutative_state` message ([`Domain::COMMUTATIVE`]).
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
/// page { provenance, origin, body } }`, typed, with the fingerprint of
/// its payload as received.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Book<C, F> {
    /// `CommandBook.cover`.
    pub stream: StreamKey,
    pub handling: Handling,
    pub provenance: Provenance,
    /// `None`: a request whose caller supplied no request id (angzarr's
    /// unset `CommandPage.origin`). The kernel records it as
    /// [`Origin::minted`] with the application's [`Sampled::request`]
    /// ([`Book::origin`]). Every recorded origin has a request id.
    pub origin: Option<Origin>,
    pub body: Body<C, F>,
    /// The fingerprint of the body's payload as received
    /// ([`Fingerprint`]), recorded with the key. Required for a keyed book
    /// (a client-supplied request or a fact: [`Malformed::Unfingerprinted`]);
    /// ignored for any other. Not on the wire: the adapter computes it from
    /// the bytes it received (`annals_angzarr::book_from_wire`), or from the
    /// payload's encoding for a book built in Rust
    /// (`annals_angzarr::fingerprinted`).
    pub fingerprint: Option<Fingerprint>,
}

/// A book for `D`.
pub type BookOf<D> = Book<<D as Domain>::Command, <D as Domain>::Fact>;

impl<C, F> Book<C, F> {
    /// A command, with no handling (no expected sequence, the domain's
    /// default resolution) and no fingerprint yet.
    pub fn command(stream: StreamKey, cmd: C, origin: Origin, provenance: Provenance) -> Self {
        Book {
            stream,
            handling: Handling::default(),
            provenance,
            origin: Some(origin),
            body: Body::Command(cmd),
            fingerprint: None,
        }
    }

    /// A command whose caller supplied no request id: the kernel records
    /// the application's minted one ([`Book::origin`]). Never deduplicated
    /// and needing no fingerprint.
    pub fn minting(stream: StreamKey, cmd: C, provenance: Provenance) -> Self {
        Book {
            stream,
            handling: Handling::default(),
            provenance,
            origin: None,
            body: Body::Command(cmd),
            fingerprint: None,
        }
    }

    /// A fact, keyed by `origin` (which must be [`Origin::Fact`]), with no
    /// fingerprint yet.
    pub fn fact(stream: StreamKey, fact: F, origin: Origin, provenance: Provenance) -> Self {
        Book {
            stream,
            handling: Handling::default(),
            provenance,
            origin: Some(origin),
            body: Body::Fact(fact),
            fingerprint: None,
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

    /// With the payload's fingerprint.
    pub fn fingerprinted(mut self, fingerprint: Fingerprint) -> Self {
        self.fingerprint = Some(fingerprint);
        self
    }

    /// Refuse a book that is malformed, before business code: a body and an
    /// origin that disagree, a fact with an empty external id or an
    /// expected sequence, a causation with any handling or with no
    /// correlation, a request marked minted by its sender, a keyed book
    /// with no fingerprint.
    pub fn validate(&self) -> Result<(), Malformed> {
        todo!("proposal: the checks Malformed lists, in its order")
    }

    /// The origin the book's events are recorded with: the book's, else
    /// [`Origin::minted`] with `sampled.request`. Always has a request id.
    pub fn origin(&self, sampled: &Sampled) -> Origin {
        todo!("proposal: the book's origin, else Origin::minted(sampled.request)")
    }

    /// The correlation the book's events are stamped with: the given one,
    /// else [`Correlation::derived`] from the resolved origin
    /// ([`Book::origin`]), through `sampled.derive`. A valid book always
    /// has one (a causation must carry its trigger's).
    pub fn correlation(&self, sampled: &Sampled) -> Correlation {
        todo!("proposal: given, else derived from self.origin(sampled)")
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
    /// A causation with no correlation: a reaction's command carries its
    /// trigger's, and the kernel never derives or mints one for it.
    CausationWithoutCorrelation,
    /// A request origin with `minted` set: only the kernel mints (angzarr
    /// refuses it INVALID_ARGUMENT).
    MintedBySender,
    /// A keyed book (a client-supplied request, or a fact) with no
    /// fingerprint: its key could not be checked against a reuse.
    Unfingerprinted,
    /// COMMUTATIVE (the book's, or the domain's default) for a domain whose
    /// state is not a `commutative_state` message ([`Domain::COMMUTATIVE`]
    /// is `None`). angzarr refuses it INVALID_ARGUMENT before decide.
    CommutativeUnsupported,
}

/// What the application supplies for one book and its whole chain: the
/// kernel has no clock, no randomness and no hash, so it stays
/// deterministic.
#[derive(Clone, Copy)]
pub struct Sampled {
    /// The instant: every event of the chain is recorded at it, or at its
    /// stream's previous `recorded_at` if that is later.
    pub now: Timestamp,
    /// A fresh UUIDv7 (RFC 9562), minted by the application for this book:
    /// its request id if the book has no origin ([`Book::origin`]), and so
    /// its correlation if it supplied none. Unused otherwise.
    pub request: RequestId,
    /// The UUIDv5 derivations ([`DeriveIds`]):
    /// `annals_angzarr::ids::ANGZARR_IDS` for angzarr's.
    pub derive: &'static dyn DeriveIds,
}

impl std::fmt::Debug for Sampled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sampled")
            .field("now", &self.now)
            .field("request", &self.request)
            .finish_non_exhaustive()
    }
}
