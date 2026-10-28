//! The storage ports are angzarr-ports': one async trait set, shared with
//! angzarr, held to one conformance suite (`angzarr_ports::contract`), with
//! an in-memory backend (`angzarr_ports::memory::MemoryStore`, feature
//! `memory`) and a SQLite one (angzarr-store-sqlite). annals has no store
//! trait of its own, and re-exports the shared ones here.
//!
//! **There is no unit of work.** Each handling step the kernel runs makes
//! ONE atomic [`EventStore::write`]: one stream's events at the length the
//! step loaded, with the step's positions, landing together or not at all.
//! A race is caught by that length ([`StoreError::Conflict`]); nothing is
//! held open between a step's reads and its write. A step's dedup lookup
//! comes before its write; a snapshot is its own call.
//!
//! **What the kernel needs of a store** is the base ports, [`Store`]: it
//! reads snapshots, streams and positions, looks commands up, and writes.
//! The rest (purge, read units, watch, the outbox, editions) is the
//! application's, through the store directly.
//!
//! **Opaque.** A store keeps angzarr's pages as written and never decodes a
//! payload; upcasting is the kernel's, on every read. A page the kernel
//! cannot replay is the kernel's [`KernelError::Unreadable`](super::KernelError::Unreadable);
//! a store's [`StoreError::Unreadable`] is bytes the store cannot return.
//!
//! **Absent is empty.** A stream with no events reads as an empty
//! [`Slice`] at 0: "no such stream" and "empty" are one answer, so a
//! snapshot whose stream has no events is never used.
//!
//! **Editions** are per-stream forks (14.32), the store's to enforce: a
//! command to an edition stream that was never forked is refused by the
//! store at the write ([`StoreError::EditionNotForked`]). The kernel
//! creates no edition yet.

use std::future::Future;

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EventPage, Snapshot, Uuid};

pub use angzarr_ports::{
    Append, ChangeToken, EventStore, Head, ListStreams, Outbox, OutboxEntry, PositionKey,
    PositionStore, Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice,
    SnapshotStore, Store, StoreError, StreamRef, Watch, Write, Written,
};

/// Whether `page` was produced by the command `command_id`: what
/// deduplication looks for among a step's pages.
pub(crate) fn produced_by(page: &EventPage, command_id: &Uuid) -> bool {
    matches!(
        page.meta.as_ref().and_then(|m| m.causation.as_ref()),
        Some(Causation::CommandId(id)) if id == command_id
    )
}

/// The store operations a step makes, keyed by the wire `Cover` the kernel
/// carries. `read_from` answers `None` for a stream with no events.
pub(crate) trait Port {
    /// One step's write: `pages` to `stream` at `expected`, with the step's
    /// `positions` ((handler, the stream it consumes, its next sequence)),
    /// all of it or none. Answers the stream's new length.
    fn write(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
        positions: &[(&str, &Cover, u64)],
    ) -> impl Future<Output = Result<u64, StoreError>>;

    fn read_from(
        &mut self,
        stream: &Cover,
        from: u64,
    ) -> impl Future<Output = Result<Option<Slice>, StoreError>>;

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>>;

    fn read_snapshot(
        &mut self,
        stream: &Cover,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>>;
}

/// An angzarr-ports store as the kernel's [`Port`]: each `Cover` as a
/// [`StreamRef`] (a malformed one is the store's
/// [`StoreError::MalformedStream`]), a read of a stream with no events as
/// `None`, a step's write as one [`Write`].
pub(crate) struct StorePort<'s, S>(pub(crate) &'s S);

impl<S: Store> Port for StorePort<'_, S> {
    async fn write(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
        positions: &[(&str, &Cover, u64)],
    ) -> Result<u64, StoreError> {
        let _ = (self.0, stream, expected, pages, positions);
        todo!("red: a step's one write to the angzarr-ports store")
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        let _ = (self.0, stream, from);
        todo!("red: read through the angzarr-ports store")
    }

    async fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let _ = (self.0, stream, command_id);
        todo!("red: look the command up through the angzarr-ports store")
    }

    async fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        let _ = (self.0, stream);
        todo!("red: read the snapshot through the angzarr-ports store")
    }
}
