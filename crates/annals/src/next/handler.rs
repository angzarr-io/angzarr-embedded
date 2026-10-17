//! Reacting to committed events: one callback shape for projections,
//! policies and the application's own reactions, and one position contract
//! for all of them.

use std::collections::HashMap;
use std::fmt;

use annals_diagnostic::{Arg, Diagnostic};

use angzarr_proto::v1::{Cover, EventRef};

use super::domain::Domain;
use super::stream::Envelope;

/// One committed event as a handler sees it: which stream, where in it, its
/// metadata and payload, and the stream's state just after it.
pub struct Committed<'a, D: Domain> {
    pub cover: &'a Cover,
    pub envelope: &'a Envelope<D::Event>,
    pub state: &'a D::State,
}

impl<'a, D: Domain> Committed<'a, D> {
    /// The event's reference: its stream and sequence (angzarr's
    /// `EventRef`), what a policy's command names as its causation (on the
    /// command only; the command's events name the command).
    pub fn event_ref(&self) -> EventRef {
        EventRef {
            stream: Some(self.cover.clone()),
            sequence: self.envelope.sequence(),
        }
    }

    pub fn cover(&self) -> &'a Cover {
        self.cover
    }

    pub fn sequence(&self) -> u64 {
        self.envelope.sequence()
    }

    pub fn envelope(&self) -> &'a Envelope<D::Event> {
        self.envelope
    }

    pub fn event(&self) -> &'a D::Event {
        self.envelope.event()
    }
}

/// Anything that reacts to a domain's committed events: a projection, a
/// policy's [`Reactor`](super::Reactor), an application callback. Called
/// after commit, in log order, once per event. It cannot change the stream
/// it is reading; it can only record, or hand off commands for others.
///
/// Any `FnMut(&Committed<D>)` is a handler.
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

/// How far a reader of one stream has got: the last sequence it handled,
/// or none yet. The one contract for every reader: a projection, an
/// application handler, a policy target (whose position is a fold of its
/// own stream).
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Position(Option<u64>);

impl Position {
    /// Nothing handled: sequence 0 is next.
    pub const START: Position = Position(None);

    /// `last` handled: `last + 1` is next.
    pub const fn at(last: u64) -> Position {
        Position(Some(last))
    }

    pub const fn last(self) -> Option<u64> {
        self.0
    }

    /// The sequence due next: where a catch-up reads from.
    pub fn next(self) -> u64 {
        self.0.map_or(0, |last| last + 1)
    }

    /// Where `sequence` falls: the next one ([`Admit::Next`]), one already
    /// handled ([`Admit::Redelivery`]: do nothing), or past the next one
    /// (a [`Gap`]: catch up from [`Position::next`] first).
    pub fn place(self, sequence: u64) -> Result<Admit, Gap> {
        let next = self.next();
        match sequence.cmp(&next) {
            std::cmp::Ordering::Less => Ok(Admit::Redelivery),
            std::cmp::Ordering::Equal => Ok(Admit::Next),
            std::cmp::Ordering::Greater => Err(Gap {
                expected: next,
                found: sequence,
            }),
        }
    }

    /// [`Position::place`], moving on to `sequence` when it is the next
    /// one.
    pub fn advance(&mut self, sequence: u64) -> Result<Admit, Gap> {
        let admit = self.place(sequence)?;
        if admit == Admit::Next {
            self.0 = Some(sequence);
        }
        Ok(admit)
    }
}

/// Where a delivered sequence falls against a [`Position`].
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
    pub expected: u64,
    pub found: u64,
}

impl fmt::Display for Gap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "event {} came where {} was due: catch up first",
            self.found, self.expected
        )
    }
}

impl Diagnostic for Gap {
    fn code(&self) -> &'static str {
        "annals-next-position-gap"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![
            ("expected", self.expected.into()),
            ("found", self.found.into()),
        ]
    }
}

/// A handler that keeps a [`Position`] per stream, so it may be fed from
/// anywhere (a commit, a dispatch, a bus) and still sees each event once,
/// in order: a redelivery is dropped, and a gap is recorded and the event
/// dropped, for the application to catch up from the store. Its maps are
/// only looked up, never iterated, so their order cannot leak.
pub struct Tracked<H> {
    inner: H,
    positions: HashMap<Cover, Position>,
    gaps: HashMap<Cover, Gap>,
}

impl<H> Tracked<H> {
    /// Every stream at [`Position::START`].
    pub fn new(inner: H) -> Tracked<H> {
        Tracked {
            inner,
            positions: HashMap::new(),
            gaps: HashMap::new(),
        }
    }

    /// Resume `stream` from a saved position.
    pub fn resume(mut self, stream: Cover, position: Position) -> Tracked<H> {
        self.positions.insert(stream, position);
        self
    }

    pub fn position(&self, stream: &Cover) -> Position {
        self.positions.get(stream).copied().unwrap_or_default()
    }

    /// The first gap seen on `stream` since it last advanced, if any.
    pub fn gap(&self, stream: &Cover) -> Option<&Gap> {
        self.gaps.get(stream)
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
        let cover = committed.cover;
        let position = self.positions.entry(cover.clone()).or_default();
        match position.advance(committed.sequence()) {
            Ok(Admit::Next) => {
                self.gaps.remove(cover);
                self.inner.handle(committed);
            }
            Ok(Admit::Redelivery) => {}
            Err(gap) => {
                self.gaps.entry(cover.clone()).or_insert(gap);
            }
        }
    }
}
