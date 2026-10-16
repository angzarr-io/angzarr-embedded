//! Reacting to committed events: one callback shape for projections,
//! policies and the application's own reactions, and one position contract
//! for all of them.

use std::collections::BTreeMap;

use super::domain::Domain;
use super::id::{EventId, Seq, StreamId};
use super::meta::Meta;
use super::stream::{Envelope, Header};

/// One committed event as a handler sees it: which stream, where in it,
/// its metadata, its payload, and the stream's state just after it.
pub struct Committed<'a, D: Domain> {
    pub header: &'a Header,
    pub envelope: &'a Envelope<D::Event>,
    pub state: &'a D::State,
}

impl<'a, D: Domain> Committed<'a, D> {
    /// The event's id: its stream and seq.
    pub fn id(&self) -> EventId {
        EventId {
            stream: self.header.id,
            seq: self.envelope.seq,
        }
    }

    pub fn stream(&self) -> StreamId {
        self.header.id
    }

    pub fn seq(&self) -> Seq {
        self.envelope.seq
    }

    pub fn meta(&self) -> &'a Meta {
        &self.envelope.meta
    }

    pub fn event(&self) -> &'a D::Event {
        &self.envelope.event
    }
}

/// Anything that reacts to a domain's committed events: a projection, a
/// policy's [`Reactor`](super::Reactor), an application callback. Called
/// after commit, in log order, once per event. It cannot change the stream
/// it is reading; it can only record, or hand off commands for others.
///
/// Any `FnMut(&Committed<D>)` is a handler. A handler over several domains
/// implements this once per domain.
///
/// Delivery to an in-process handler is exactly once per commit. Anything
/// further away (a bus, another process) may lose or repeat events; it keeps
/// a [`Position`] per stream and catches up from the store, because streams
/// replay.
pub trait Handler<D: Domain> {
    fn handle(&mut self, committed: &Committed<'_, D>);
}

impl<D: Domain, F: FnMut(&Committed<'_, D>)> Handler<D> for F {
    fn handle(&mut self, committed: &Committed<'_, D>) {
        self(committed)
    }
}

/// How far a reader of one stream has got: the last seq it handled, or
/// none yet. The one contract for every reader: a projection, an
/// application handler, a policy target (whose position is a fold of its
/// own stream).
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Position(Option<Seq>);

impl Position {
    /// Nothing handled: seq 0 is next.
    pub const START: Position = Position(None);

    /// `last` handled: `last + 1` is next.
    pub const fn at(last: Seq) -> Position {
        Position(Some(last))
    }

    pub const fn last(self) -> Option<Seq> {
        self.0
    }

    /// The seq due next: where a catch-up reads from.
    pub fn next(self) -> Seq {
        todo!("proposal: 0 at the start, else last + 1")
    }

    /// Where `seq` falls: the next one ([`Admit::Next`]), one already
    /// handled ([`Admit::Redelivery`]: do nothing), or past the next one
    /// (a [`Gap`]: catch up from [`Position::next`] first).
    pub fn place(self, seq: Seq) -> Result<Admit, Gap> {
        todo!("proposal: compare seq with next()")
    }

    /// [`Position::place`], moving on to `seq` when it is the next one.
    pub fn advance(&mut self, seq: Seq) -> Result<Admit, Gap> {
        todo!("proposal: place, and on Next move to seq")
    }
}

/// Where a delivered seq falls against a [`Position`].
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Admit {
    /// The next one: handle it.
    Next,
    /// Already handled: do nothing.
    Redelivery,
}

/// A delivery skipped ahead: `expected` was due and `found` came.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Gap {
    pub expected: Seq,
    pub found: Seq,
}

/// A handler that keeps a [`Position`] per stream, so it may be fed from
/// anywhere (a commit, a dispatch, a bus) and still sees each event once,
/// in order: a redelivery is dropped, and a gap is recorded and the event
/// dropped, for the application to catch up from the store.
pub struct Tracked<H> {
    inner: H,
    positions: BTreeMap<StreamId, Position>,
    gaps: BTreeMap<StreamId, Gap>,
}

impl<H> Tracked<H> {
    /// Every stream at [`Position::START`].
    pub fn new(inner: H) -> Tracked<H> {
        Tracked {
            inner,
            positions: BTreeMap::new(),
            gaps: BTreeMap::new(),
        }
    }

    /// Resume `stream` from a saved position.
    pub fn resume(mut self, stream: StreamId, position: Position) -> Tracked<H> {
        self.positions.insert(stream, position);
        self
    }

    pub fn position(&self, stream: StreamId) -> Position {
        self.positions.get(&stream).copied().unwrap_or_default()
    }

    /// The first gap seen on `stream` since it last advanced, if any.
    pub fn gap(&self, stream: StreamId) -> Option<&Gap> {
        self.gaps.get(&stream)
    }

    pub fn inner(&self) -> &H {
        &self.inner
    }

    pub fn into_inner(self) -> H {
        self.inner
    }
}

impl<D: Domain, H: Handler<D>> Handler<D> for Tracked<H> {
    fn handle(&mut self, committed: &Committed<'_, D>) {
        todo!("proposal: advance the stream's position; Next goes to inner, a gap is recorded")
    }
}
