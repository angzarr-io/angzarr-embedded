//! Snapshots: a framework optimisation, never business data (angzarr's
//! `Snapshot`, D8).
//!
//! A snapshot records what produced it: the rules version it was folded
//! under, and (for an adapter's stored form) the state's type. On load it
//! is used only if both match the current registration: the rulebook's
//! current version, and a state the adapter can decode as `D::State`. On
//! any mismatch it is **discarded**, never migrated, and the stream is
//! replayed from its events, so a rules change or a state-schema change
//! never needs a snapshot migration.

use super::domain::Domain;
use super::error::{KernelError, StoreError};
use super::id::{RulesVersion, Seq, StreamKey, Timestamp};
use super::store::EventStore;
use super::stream::Stream;
use super::version::Rulebook;

/// A stream's state after the event at `sequence`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Snapshot<S> {
    /// The seq of the last event folded into `state`.
    pub sequence: Seq,
    pub state: S,
    /// The rulebook's current version when it was folded.
    pub rules_version: RulesVersion,
    /// The `recorded_at` of the event at `sequence`: the floor for the next
    /// event's. Not on angzarr's `Snapshot`; an adapter reads it off that
    /// event's row.
    pub recorded_at: Timestamp,
}

impl<S> Snapshot<S> {
    /// Whether a load may start from this snapshot: it was folded under the
    /// rulebook's current version.
    pub fn usable<R>(&self, rules: &Rulebook<R>) -> bool {
        todo!("proposal: self.rules_version == *rules.current().0")
    }
}

/// Where snapshots are kept: beside the events, in the same database. A
/// store keeps the newest snapshot per stream (angzarr's default policy).
/// An adapter that cannot decode a stored snapshot as `S` (its state type
/// changed) answers `Ok(None)`: a snapshot of another type is discarded,
/// never an error.
pub trait SnapshotStore<S> {
    fn read_snapshot(&mut self, key: &StreamKey) -> Result<Option<Snapshot<S>>, StoreError>;

    fn write_snapshot(&mut self, key: &StreamKey, snapshot: &Snapshot<S>)
        -> Result<(), StoreError>;
}

/// A stream as loaded, and whether a snapshot was used.
pub struct Loaded<D: Domain> {
    pub stream: Stream<D>,
    /// The snapshot's seq, if one was used; `None` if there was none or it
    /// was discarded.
    pub from_snapshot: Option<Seq>,
}

/// Load the stream `key` names: from its snapshot when one is
/// [`usable`](Snapshot::usable), reading only the events after it;
/// otherwise from seq 0, the snapshot discarded. `None`: no such stream.
pub fn load<D, S>(
    store: &mut S,
    key: &StreamKey,
    rules: &Rulebook<D::Rules>,
) -> Result<Option<Loaded<D>>, KernelError>
where
    D: Domain,
    S: EventStore<D::Event> + SnapshotStore<D::State>,
{
    todo!("proposal: read_snapshot, usable? read_from(seq + 1) and resume : read and replay")
}
