//! A stream: an append-only log of stamped events and the state folded from
//! it.

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{
    CommandBook, Cover, EventMeta, EventPage, EventRef, FactOrigin, Snapshot, Uuid,
};
use prost_types::Timestamp;

use super::check::{Malformed, Sampled};
use super::dead_letter::DeadLetterLog;
use super::domain::{Decision, Domain};
use super::error::{Failure, KernelError};
use super::handler::Handler;
use super::payload::Payload;
use super::registry::Registry;
use super::upcast::Upcaster;

/// One recorded event, checked and decoded: an [`EventPage`] whose
/// `EventMeta` had every field a recorded event always has (so its
/// accessors are not optional) and whose payload, upcast, decoded as `E`.
/// Built only by the kernel: by stamping a decision, or by
/// [`Envelope::decode`].
#[derive(Clone, PartialEq, Debug)]
pub struct Envelope<E> {
    pub(crate) sequence: u64,
    pub(crate) causation: Causation,
    pub(crate) correlation_id: Uuid,
    pub(crate) fact: Option<FactOrigin>,
    pub(crate) recorded_at: Timestamp,
    pub(crate) rules_version: String,
    pub(crate) event: E,
}

impl<E> Envelope<E> {
    /// `EventPage.sequence`: always definite.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Its direct cause (angzarr's `EventMeta.causation`): the producing
    /// command's id, or (for a process manager's own process event) the
    /// event it reacted to. Exactly one; never absent on a recorded event.
    ///
    /// For an event of a policy's command this is the command, never the
    /// policy's trigger: events record their direct cause only (angzarr
    /// decision 13.3). The trigger is the command's own causation
    /// ([`Identified::causation_id`](super::Identified::causation_id)),
    /// which goes to tracing, not to the event.
    pub fn causation(&self) -> &Causation {
        &self.causation
    }

    /// The producing command's id, what deduplication finds; `None` for a
    /// process manager's process event.
    pub fn command_id(&self) -> Option<&Uuid> {
        match &self.causation {
            Causation::CommandId(id) => Some(id),
            Causation::TriggeringEvent(_) => None,
        }
    }

    /// The event a process manager reacted to in writing this one, what its
    /// deduplication finds within its stream; `None` for a command's event.
    pub fn triggering_event(&self) -> Option<&EventRef> {
        match &self.causation {
            Causation::TriggeringEvent(trigger) => Some(trigger),
            Causation::CommandId(_) => None,
        }
    }

    /// Never absent on a recorded event: for a process event, its
    /// trigger's.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// The source, for a fact's event.
    pub fn fact(&self) -> Option<&FactOrigin> {
        self.fact.as_ref()
    }

    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// The rules the event was decided under, and that apply folds it with.
    pub fn rules_version(&self) -> &str {
        &self.rules_version
    }

    pub fn event(&self) -> &E {
        &self.event
    }

    /// The metadata, as angzarr's `EventMeta`.
    pub fn meta(&self) -> EventMeta {
        EventMeta {
            causation: Some(self.causation.clone()),
            fact: self.fact.clone(),
            correlation_id: Some(self.correlation_id.clone()),
            recorded_at: Some(self.recorded_at),
            rules_version: self.rules_version.clone(),
        }
    }

    /// As an `EventPage { sequence, meta, payload }`: what the store
    /// appends and an angzarr reader is served.
    pub fn page(&self) -> EventPage
    where
        E: Payload,
    {
        EventPage {
            payload: Some(self.event.to_any()),
            meta: Some(self.meta()),
            sequence: self.sequence,
        }
    }

    /// A stored page of `stream`, read back: its meta checked complete
    /// ([`ReplayError::IncompleteMeta`](super::ReplayError::IncompleteMeta) otherwise), its payload run through
    /// `upcaster` and decoded as `E`. A type nothing upcasts or decodes is
    /// [`StoreError::Unreadable`](super::StoreError::Unreadable) naming its type
    /// URL: replay is strict.
    pub fn decode(
        stream: &Cover,
        page: &EventPage,
        upcaster: &Upcaster,
    ) -> Result<Envelope<E>, KernelError>
    where
        E: Payload,
    {
        todo!("proposal: check the meta, upcast_event, E::from_any")
    }
}

/// What [`Stream::execute`] did.
#[derive(Debug, PartialEq)]
pub enum Executed<'a, E> {
    /// Business code ran; these are the events it committed (possibly
    /// none).
    Applied(&'a [Envelope<E>]),
    /// The stream already held this command's events: these. Nothing ran.
    Duplicate(&'a [Envelope<E>]),
}

/// A domain's event log and the state folded from it, under one cover
/// whose domain is `D::NAME`. Its state changes only in [`Stream::commit`].
///
/// A stream resumed from a snapshot ([`Stream::resume`]) holds only the
/// events after it: [`Stream::events`] starts at [`Stream::first`], and
/// [`Stream::len`] counts from 0 all the same.
pub struct Stream<D: Domain> {
    cover: Cover,
    /// The sequence of the first held event: 0, or the snapshot's + 1.
    first: u64,
    events: Vec<Envelope<D::Event>>,
    state: D::State,
    /// The last event's `recorded_at`: the floor for the next.
    last_recorded_at: Option<Timestamp>,
}

impl<D: Domain> Stream<D> {
    /// An empty `D` stream: `root` in `edition` (`None`: main). Refuses a
    /// root that is not 16 bytes and a set empty edition, as [`check`](super::check)
    /// would.
    pub fn new(root: Uuid, edition: Option<String>) -> Result<Stream<D>, Malformed> {
        if !root.is_well_formed() {
            return Err(Malformed::BadRoot {
                len: root.value.len(),
            });
        }
        if edition.as_deref() == Some("") {
            return Err(Malformed::EmptyEdition);
        }
        Ok(Stream {
            cover: Cover {
                domain: D::NAME.to_string(),
                root: Some(root),
                edition,
            },
            first: 0,
            events: Vec::new(),
            state: D::State::default(),
            last_recorded_at: None,
        })
    }

    /// The whole pipeline on this stream, in memory, with no store: for
    /// tests and in-memory use ([`Kernel::handle`](super::Kernel::handle)
    /// is the full loop, with a store and a chain). [`check`](super::check),
    /// identify, deduplicate against this stream's own events, resolve the
    /// handling, decode, decide or ingest, commit, then hand each new event
    /// to `handlers`. A book for another stream is
    /// [`KernelError::WrongDomain`] or [`KernelError::Stale`].
    pub fn execute(
        &mut self,
        book: CommandBook,
        registry: &Registry,
        sampled: &Sampled,
        dead_letters: &dyn DeadLetterLog,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<Executed<'_, D::Event>, Failure<D::Rejection>> {
        todo!("proposal: check, identify, find_command, resolve, decode, decide, commit")
    }

    /// Fold a decision into the stream, then hand each of its events to
    /// every handler in order, with the state just after it. Refuses a
    /// decision made against another stream or another head
    /// ([`KernelError::Stale`]). Returns the new length.
    pub fn commit(
        &mut self,
        decision: Decision<D>,
        registry: &Registry,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<u64, KernelError> {
        todo!("proposal: check stream and head, apply each event, push it, call the handlers")
    }

    /// Rebuild a stream from its whole stored log, each page decoded
    /// ([`Envelope::decode`], upcast) and folded with the rules its
    /// `rules_version` names, from the registry's rulebook; the caller does
    /// not choose them. Refuses a cover of another domain, a rules version
    /// the rulebook lacks, a sequence out of order, a triggering event in
    /// this stream that is not earlier, and a `recorded_at` earlier than the
    /// event before.
    pub fn replay(
        cover: Cover,
        pages: &[EventPage],
        registry: &Registry,
    ) -> Result<Stream<D>, KernelError> {
        todo!("proposal: check the domain, decode and check each page, resolve its rules, fold")
    }

    /// Rebuild a stream from a snapshot and the pages after it, checked as
    /// [`Stream::replay`] checks. The caller has checked the snapshot is
    /// usable ([`Registry::snapshot_usable`]); `last_recorded_at` is the
    /// snapshot's last event's, the floor for the next.
    pub fn resume(
        cover: Cover,
        snapshot: &Snapshot,
        last_recorded_at: Option<Timestamp>,
        pages: &[EventPage],
        registry: &Registry,
    ) -> Result<Stream<D>, KernelError> {
        todo!("proposal: decode the state, start at its sequence + 1, then as replay")
    }

    /// The stream's state as an angzarr `Snapshot`: the last sequence, the
    /// state packed as `Any` (its type URL is what a load compares), the
    /// current rules version. `None` for an empty stream.
    pub fn snapshot(&self, registry: &Registry) -> Option<Snapshot> {
        todo!("proposal: the last sequence, pack(state), the current rules version")
    }

    /// Hand every held event from sequence `from` on to the handlers, with
    /// the state just after it: rebuilding a projection (read models are
    /// replayed on read), or a reaction's catch-up. One pass, however many
    /// handlers. A stream resumed from a snapshot cannot hand out events
    /// before [`Stream::first`] ([`KernelError::BeforeSnapshot`]).
    pub fn dispatch(
        &self,
        from: u64,
        registry: &Registry,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<(), KernelError> {
        todo!("proposal: fold from first, call the handlers from `from`")
    }

    /// The held events command `command_id` produced: what deduplication
    /// finds in memory. Empty if none (never applied, or applied with no
    /// events).
    pub fn find_command(&self, command_id: &Uuid) -> &[Envelope<D::Event>] {
        todo!("proposal: the contiguous run of events with this command id")
    }

    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    /// The events held, from [`Stream::first`].
    pub fn events(&self) -> &[Envelope<D::Event>] {
        &self.events
    }

    /// The sequence of the first held event: 0 unless resumed from a
    /// snapshot.
    pub fn first(&self) -> u64 {
        self.first
    }

    pub fn state(&self) -> &D::State {
        &self.state
    }

    /// The number of events, which is also the next event's sequence (the
    /// head).
    pub fn len(&self) -> u64 {
        self.first + self.events.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The last event's `recorded_at`: no later event is recorded before it.
    pub fn last_recorded_at(&self) -> Option<Timestamp> {
        self.last_recorded_at
    }

    pub fn into_parts(self) -> (Cover, Vec<Envelope<D::Event>>, D::State) {
        (self.cover, self.events, self.state)
    }
}
