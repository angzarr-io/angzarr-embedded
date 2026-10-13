//! A stream: an append-only log of events and the state folded from it.

use crate::domain::{Domain, Tx};

/// An event's position in its stream, from 0.
pub type Seq = u64;

/// Names a stream, so another stream can refer to it.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamId(pub u64);

/// What a stream is, stamped on it once.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Header {
    pub id: StreamId,
    /// Which domain the events belong to.
    pub domain: String,
    /// The event schema version — what an upcaster reads.
    pub schema: u32,
    /// The rules and content the events were decided under.
    pub rules: String,
}

/// One stored event.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Envelope<E> {
    pub seq: Seq,
    /// The earlier event this one follows from, if any.
    pub cause: Option<Seq>,
    pub event: E,
}

/// Why a log could not be replayed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReplayError {
    /// Event at `index` claims sequence number `found`.
    OutOfSequence { index: usize, found: Seq },
    /// Event `seq` names a cause at or after itself.
    CauseNotEarlier { seq: Seq, cause: Seq },
}

/// A domain's event log and the state folded from it.
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

    /// Decide a command. On success, returns the events it emitted.
    ///
    /// A decide that rejects after emitting is a bug: it panics in debug
    /// builds. In release builds the emitted events stand — they have already
    /// been applied, and the log must match the state — and the rejection is
    /// returned.
    pub fn execute(
        &mut self,
        cmd: &D::Command,
        rules: &D::Rules,
        ctx: &mut D::Ctx,
    ) -> Result<&[Envelope<D::Event>], D::Rejection> {
        self.execute_caused(cmd, None, rules, ctx)
    }

    /// As `execute`, with every emitted event caused by `cause` unless the
    /// decide says otherwise.
    pub fn execute_caused(
        &mut self,
        cmd: &D::Command,
        cause: Option<Seq>,
        rules: &D::Rules,
        ctx: &mut D::Ctx,
    ) -> Result<&[Envelope<D::Event>], D::Rejection> {
        let start = self.events.len();
        let mut tx = Tx::new(&mut self.state, rules, ctx, &mut self.events, cause);
        let result = D::decide(cmd, &mut tx);
        let emitted = tx.emitted();
        match result {
            Ok(()) => Ok(&self.events[start..]),
            Err(rejection) => {
                debug_assert!(
                    emitted == 0,
                    "a decide rejected after emitting {emitted} event(s): validate before you emit"
                );
                Err(rejection)
            }
        }
    }

    /// Rebuild a stream from its log.
    pub fn replay(
        header: Header,
        events: Vec<Envelope<D::Event>>,
        rules: &D::Rules,
    ) -> Result<Stream<D>, ReplayError> {
        check(&events)?;
        let mut state = D::State::default();
        for e in &events {
            D::apply(&mut state, &e.event, rules);
        }
        Ok(Stream {
            header,
            events,
            state,
        })
    }

    /// The state after the first `n` events — before event `n`.
    pub fn state_at(&self, n: Seq, rules: &D::Rules) -> D::State {
        let mut state = D::State::default();
        for e in self.events.iter().take(n as usize) {
            D::apply(&mut state, &e.event, rules);
        }
        state
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn events(&self) -> &[Envelope<D::Event>] {
        &self.events
    }

    pub fn state(&self) -> &D::State {
        &self.state
    }

    /// The number of events, which is also the next event's sequence number.
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

/// A log is well formed when sequence numbers run 0, 1, 2… and every cause
/// points backwards.
fn check<E>(events: &[Envelope<E>]) -> Result<(), ReplayError> {
    for (index, e) in events.iter().enumerate() {
        if e.seq != index as Seq {
            return Err(ReplayError::OutOfSequence { index, found: e.seq });
        }
        if let Some(cause) = e.cause {
            if cause >= e.seq {
                return Err(ReplayError::CauseNotEarlier { seq: e.seq, cause });
            }
        }
    }
    Ok(())
}
