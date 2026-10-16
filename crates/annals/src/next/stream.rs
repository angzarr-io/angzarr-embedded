//! A stream: an append-only log of stamped events and the state folded from
//! it.

use super::domain::{Decision, Domain};
use super::error::{Failure, KernelError, ReplayError};
use super::handler::Handler;
use super::id::{Seq, StreamId};
use super::meta::{CommandMeta, Meta};
use super::version::Rulebook;

/// What a stream is, stamped on it once and never changed: a store refuses
/// an append under a different header. Versions are per event now, in
/// [`Meta`], so a stream may outlive a schema or rules change.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Header {
    pub id: StreamId,
    /// Which domain the events belong to.
    pub domain: String,
}

/// One stored event: its position, its metadata and its payload.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Envelope<E> {
    pub seq: Seq,
    pub meta: Meta,
    pub event: E,
}

/// A domain's event log and the state folded from it. Its state changes
/// only in [`Stream::commit`].
pub struct Stream<D: Domain> {
    header: Header,
    events: Vec<Envelope<D::Event>>,
    state: D::State,
}

impl<D: Domain> Stream<D> {
    pub fn new(header: Header) -> Stream<D> {
        Stream {
            header,
            events: Vec::new(),
            state: D::State::default(),
        }
    }

    /// Decide `cmd` under the rulebook's current rules. Pure: the stream is
    /// untouched whatever the outcome, so a rejection leaves nothing behind.
    /// Every staged event is stamped from `meta`, with [`Domain::SCHEMA`]
    /// and the current rules version.
    pub fn decide(
        &self,
        cmd: &D::Command,
        meta: &CommandMeta,
        rules: &Rulebook<D::Rules>,
    ) -> Result<Decision<D>, D::Rejection> {
        todo!("proposal: run D::decide in a staging Tx over &self.state")
    }

    /// Fold a decision into the stream, then hand each of its events to
    /// every handler in order, with the state just after it. Refuses a
    /// decision made against another stream or another length
    /// ([`KernelError::Stale`]). Returns the new length.
    ///
    /// Each event is applied again here, to the live state, rather than the
    /// decide's staged copy kept: the handlers need the state after every
    /// event, and apply is pure, so the result is the same.
    pub fn commit(
        &mut self,
        decision: Decision<D>,
        rules: &Rulebook<D::Rules>,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<Seq, KernelError> {
        todo!("proposal: check stream and base, apply each event, push it, call the handlers")
    }

    /// Decide and commit with no store between: for tests and in-memory
    /// use. [`handle`](super::handle) is the full loop, with a store.
    pub fn execute(
        &mut self,
        cmd: &D::Command,
        meta: &CommandMeta,
        rules: &Rulebook<D::Rules>,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<&[Envelope<D::Event>], Failure<D::Rejection>> {
        todo!("proposal: decide, then commit")
    }

    /// Stage `fact`, an event that already happened upstream, with no
    /// decide: a [`Reaction`](super::Reaction)'s translation. Refused only
    /// for integrity ([`Domain::admit_fact`], as
    /// [`KernelError::FactRefused`]), never on business grounds. Stamped
    /// from `meta` like any event; committed like any decision.
    pub fn admit(
        &self,
        fact: D::Event,
        meta: &CommandMeta,
        rules: &Rulebook<D::Rules>,
    ) -> Result<Decision<D>, KernelError> {
        todo!("proposal: D::admit_fact, then stage one stamped event")
    }

    /// Rebuild a stream from its log. Each event is folded with the rules
    /// its own metadata names, found in `rules`; the caller does not choose
    /// them. Refuses an event not at [`Domain::SCHEMA`] (upcast it first),
    /// a rules version the rulebook lacks, a seq out of order and an
    /// in-stream cause that is not earlier.
    pub fn replay(
        header: Header,
        events: Vec<Envelope<D::Event>>,
        rules: &Rulebook<D::Rules>,
    ) -> Result<Stream<D>, ReplayError> {
        todo!("proposal: check each envelope, resolve its rules, fold")
    }

    /// Fold the log from the start and hand every event from seq `from` on
    /// to the handlers, with the state just after it: rebuilding a
    /// projection, or a policy's catch-up. One pass, however many handlers.
    pub fn dispatch(
        &self,
        from: Seq,
        rules: &Rulebook<D::Rules>,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<(), KernelError> {
        todo!("proposal: fold from 0, call the handlers from `from`")
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn id(&self) -> StreamId {
        self.header.id
    }

    pub fn events(&self) -> &[Envelope<D::Event>] {
        &self.events
    }

    pub fn state(&self) -> &D::State {
        &self.state
    }

    /// The number of events, which is also the next event's seq.
    pub fn len(&self) -> Seq {
        self.events.len() as Seq
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn into_parts(self) -> (Header, Vec<Envelope<D::Event>>, D::State) {
        (self.header, self.events, self.state)
    }
}
