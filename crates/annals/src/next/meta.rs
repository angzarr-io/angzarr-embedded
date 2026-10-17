//! Provenance: who, which workflow, why, when, decided against what, under
//! which rules. Field for field what angzarr's `Provenance`, `Origin` and
//! `EventMeta` carry, so `annals-angzarr` maps them without inventing
//! anything.

use super::id::{Actor, Correlation, EventId, RequestId, RulesVersion, Seq, Timestamp};

/// Who issued a command and which workflow it belongs to, as sent (angzarr's
/// `Provenance` on a `CommandPage`). Empty means "not supplied".
///
/// A reaction's command is never given one: the kernel stamps the
/// trigger's actor and correlation on it.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Provenance {
    /// `None` for the system itself: a scheduled job, a migration.
    pub actor: Option<Actor>,
    /// `None`: derived from the command's origin ([`Correlation::derived`]).
    pub correlation: Option<Correlation>,
}

/// Why a command exists, and therefore why its events exist: exactly one of
/// three (angzarr's `Origin`). The same value is the command's origin, every
/// one of its events' origin ([`Meta::origin`]) and, where it is keyed
/// ([`Origin::is_keyed`]), the command's idempotency key.
///
/// A request's id is always set: a book whose caller supplied none has no
/// origin ([`Book::origin`](super::Book::origin) is `None`), and the kernel
/// records it as a request with the application's minted id
/// ([`Origin::minted`]).
///
/// It must agree with the command's body ([`Body`](super::Body)): a fact
/// requires [`Origin::Fact`]; a command requires a request or a causation.
/// The kernel refuses a disagreement before business code runs
/// ([`Malformed`](super::Malformed)).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Origin {
    /// Issued directly by a caller.
    Request(RequestOrigin),
    /// Issued by a policy reacting to an event. Only the kernel builds one,
    /// in a chain or a backlog.
    Causation(Causation),
    /// Something that already happened outside the system.
    Fact(FactOrigin),
}

/// A command issued directly by a caller. `request` is always set;
/// `minted` says whose it is.
///
/// A request id names **one command** (angzarr pass 10 deleted
/// `RequestOrigin.index`): a caller that sends several commands together
/// (a batch, one API call fanning out) gives each its own request id and
/// groups them by a shared [`Provenance::correlation`].
///
/// - **Client-supplied** (`minted: false`): the caller's id, reused on
///   every retry of that command. The request id alone is the command's
///   idempotency key at the addressed stream.
/// - **Minted** (`minted: true`): the caller supplied none, and the kernel
///   recorded the application's UUIDv7 ([`Sampled::request`](super::Sampled::request)).
///   For audit only: never recorded as a key, never deduplicating (a
///   retry gets another). Only the kernel sets it: a book that arrives
///   with `minted` set is [`Malformed::MintedBySender`](super::Malformed::MintedBySender).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RequestOrigin {
    pub request: RequestId,
    pub minted: bool,
}

/// A policy's command: the `index`th the policy named `component`
/// ([`Reaction::NAME`](super::Reaction::NAME)) issued for `event`, the
/// triggering event. Recomputable from the log, since policies are pure.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Causation {
    pub event: EventId,
    pub component: String,
    pub index: u32,
}

/// Where a fact came from. `external_id` is required (non-empty): it is the
/// fact's key, unique per stream (see
/// [`UnitOfWork::record_command`](super::UnitOfWork::record_command)).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct FactOrigin {
    /// The external system's id for it (`pi_1234` from Stripe, say).
    pub external_id: String,
    /// Human-readable (`"Stripe webhook"`); not part of the key.
    pub description: String,
}

impl Origin {
    /// The command whose client-supplied request id is `request`.
    pub fn request(request: RequestId) -> Origin {
        Origin::Request(RequestOrigin {
            request,
            minted: false,
        })
    }

    /// The origin the kernel records for a book with none: the request id
    /// `request`, minted by the application.
    pub fn minted(request: RequestId) -> Origin {
        Origin::Request(RequestOrigin {
            request,
            minted: true,
        })
    }

    /// A fact from `description`'s `external_id`.
    pub fn fact(external_id: &str, description: &str) -> Origin {
        Origin::Fact(FactOrigin {
            external_id: external_id.to_string(),
            description: description.to_string(),
        })
    }

    /// The triggering event, for a causation.
    pub fn cause(&self) -> Option<&EventId> {
        match self {
            Origin::Causation(c) => Some(&c.event),
            _ => None,
        }
    }

    /// Whether this origin is a key the store may record
    /// ([`UnitOfWork::record_command`](super::UnitOfWork::record_command)):
    /// a request with a client-supplied id, or a fact. Never a minted
    /// request, and never a causation: annals records only a chain's
    /// originating book, so a causation's fingerprint is never compared
    /// (see [`StoreError::KeyReused`](super::StoreError::KeyReused)).
    pub fn is_keyed(&self) -> bool {
        todo!("proposal: Request with !minted, or Fact")
    }
}

/// One stored event's metadata (angzarr's `EventMeta`). The kernel stamps
/// it; business code never sets it and `apply` never sees it. Every event
/// one command produced shares one `Meta`.
///
/// The event's own id is not stored: it is [`EventId`] `{ stream, seq }`,
/// which the stream's key and the envelope's seq already give. There is no
/// schema version: an event's payload type names its version (a protobuf
/// type URL, for `annals-angzarr`), and upcasting is the store adapter's.
/// There is no request field (angzarr's D2): a reaction's event records its
/// causation and its correlation, and its request is found by walking
/// causation or by correlation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Meta {
    /// `EventMeta.provenance.actor`.
    pub actor: Option<Actor>,
    /// `EventMeta.provenance.correlation_id`. Always set.
    pub correlation: Correlation,
    /// The producing command's origin, unchanged.
    pub origin: Origin,
    /// When the event was recorded: the application's sampled instant, or
    /// the stream's previous event's if that is later, so it never goes
    /// backwards within a stream and as-of-time reads are prefixes.
    pub recorded_at: Timestamp,
    /// The stream's length decide (or ingest) saw: the version decided
    /// against. The head, under every conflict resolution.
    pub base_sequence: Seq,
    /// The rules the event was decided under, and that `apply` folds it
    /// with: the rulebook's current version at the time.
    pub rules_version: RulesVersion,
}
