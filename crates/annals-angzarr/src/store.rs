//! The storage shape: **opaque rows**. A database holds, per event, the
//! type URL it was written with, the `EventPage` encoded exactly as
//! written, and the metadata as typed columns:
//!
//! | column | from |
//! |---|---|
//! | `domain`, `edition`, `root` | the stream key; `edition` NULL for the main timeline |
//! | `sequence` | the envelope's seq (64-bit) |
//! | `type_url` | the payload's type URL as written (before any upcast) |
//! | `page` | the `EventPage` bytes as written: `sequence`, `meta`, `payload` |
//! | `actor`, `correlation` | [`Meta`](annals::next::Meta)'s provenance |
//! | `origin_kind`, `request_id`, `request_index`, `cause_domain`, `cause_edition`, `cause_root`, `cause_sequence`, `cause_component`, `cause_index`, `external_id` | [`Meta::origin`](annals::next::Meta::origin), one set of columns per arm |
//! | `recorded_at`, `base_sequence`, `rules_version` | the rest of [`Meta`](annals::next::Meta) |
//!
//! Keys, for [`UnitOfWork::record_command`]: a unique index on (domain,
//! edition, root, `external_id`) where `external_id` is set refuses a
//! redelivered webhook fact; one on (domain, edition, root, `request_id`,
//! `request_index`) a retried HTTP request, if the server wants that. They
//! can be partial indexes on the events table itself, but a command that
//! emits no events leaves no row to index, so a separate commands table
//! (stream, origin, first sequence, count) is the general form.
//!
//! The application's database implements the kernel's own store port over
//! these rows, `EventStore<Stored>` (and `AsyncEventStore<Stored>`, and
//! `UnitOfWork`): each [`Envelope<Stored>`] is one row, its `meta` the
//! metadata columns and its [`Stored`] the type URL and page. It never
//! decodes a payload. [`Opaque`] turns that port into a typed one for the
//! kernel, one blanket impl for every domain: it encodes on append and, on
//! read, decodes the page, upcasts the payload by type URL and unpacks it,
//! strictly.
//!
//! The columns and the page's `EventMeta` hold the same metadata; the page
//! is what an angzarr reader is served. Nothing stored is ever rewritten:
//! upcasting happens on every read.

use annals::next::{
    AsyncEventStore, AsyncUnitOfWork, Domain, Envelope, EventStore, Origin, Rulebook, Seq, Slice,
    StoreError, Stream, StreamKey, UnitOfWork,
};

use crate::error::ReadError;
use crate::payload::Payload;
use crate::registry::Registry;

/// One stored event's opaque part: what it was written as.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Stored {
    /// The payload's type URL when written: indexable, so the rows still
    /// at an old type can be found.
    pub type_url: String,
    /// The `EventPage`, encoded as written.
    pub page: Vec<u8>,
}

/// One event, stored: its page encoded ([`event_page`](crate::event_page))
/// and its type URL, the seq and metadata kept as the envelope's.
pub fn store_event<E: Payload>(envelope: &Envelope<E>) -> Envelope<Stored> {
    todo!("proposal: event_page, encode, keep seq and meta")
}

/// One stored event of the stream `key`, read back as a current event:
/// decode the page, upcast its payload with the domain's upcaster, unpack
/// it as `E`. A type nothing upcasts or decodes is a [`ReadError`] naming
/// its type URL: replay is strict.
pub fn read_event<E: Payload>(
    registry: &Registry,
    key: &StreamKey,
    stored: Envelope<Stored>,
) -> Result<Envelope<E>, ReadError> {
    todo!("proposal: decode EventPage, upcaster.upcast_event, E::from_any")
}

/// A stored log, read back ([`read_event`], each event) and replayed with
/// each event's own rules: what a read model does on every read.
pub fn replay_stored<D: Domain>(
    registry: &Registry,
    key: StreamKey,
    stored: Vec<Envelope<Stored>>,
    rules: &Rulebook<D::Rules>,
) -> Result<Stream<D>, annals::next::KernelError>
where
    D::Event: Payload,
{
    todo!("proposal: read_event each (as Store(Unreadable)), then Stream::replay")
}

/// The application's opaque store, typed for the kernel: what
/// [`Registry::store`] returns. One blanket impl serves every registered
/// domain's events, so a command chain across several domains runs over one
/// unit of work.
pub struct Opaque<'r, S> {
    pub(crate) stored: S,
    pub(crate) registry: &'r Registry,
}

impl<S> Opaque<'_, S> {
    /// The application's store underneath.
    pub fn stored(&mut self) -> &mut S {
        &mut self.stored
    }
}

impl<S: EventStore<Stored>, E: Payload> EventStore<E> for Opaque<'_, S> {
    /// Each event as [`store_event`], appended as one row.
    fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> Result<Seq, StoreError> {
        todo!("proposal: store_event each, then self.stored.append")
    }

    /// Each row as [`read_event`]; a row that cannot be read is
    /// [`StoreError::Unreadable`] ([`ReadError::into_store_error`]).
    fn read_from(&mut self, key: &StreamKey, from: Seq) -> Result<Option<Slice<E>>, StoreError> {
        todo!("proposal: self.stored.read_from, then read_event each")
    }
}

impl<S: UnitOfWork> UnitOfWork for Opaque<'_, S> {
    fn commit(self) -> Result<(), StoreError> {
        self.stored.commit()
    }

    fn record_command(
        &mut self,
        stream: &StreamKey,
        origin: &Origin,
        from: Seq,
        to: Seq,
    ) -> Result<(), StoreError> {
        self.stored.record_command(stream, origin, from, to)
    }
}

impl<S, E> AsyncEventStore<E> for Opaque<'_, S>
where
    S: AsyncEventStore<Stored> + Send,
    E: Payload + Send + Sync,
{
    async fn append(
        &mut self,
        key: &StreamKey,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> Result<Seq, StoreError> {
        todo!("proposal: as the sync append, awaiting the store")
    }

    async fn read_from(
        &mut self,
        key: &StreamKey,
        from: Seq,
    ) -> Result<Option<Slice<E>>, StoreError> {
        todo!("proposal: as the sync read_from, awaiting the store")
    }
}

impl<S: AsyncUnitOfWork + Send> AsyncUnitOfWork for Opaque<'_, S> {
    async fn commit(self) -> Result<(), StoreError> {
        self.stored.commit().await
    }

    async fn record_command(
        &mut self,
        stream: &StreamKey,
        origin: &Origin,
        from: Seq,
        to: Seq,
    ) -> Result<(), StoreError> {
        self.stored.record_command(stream, origin, from, to).await
    }
}
