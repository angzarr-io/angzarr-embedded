//! The storage port. A store holds event streams and nothing else.

use std::collections::BTreeMap;

use crate::stream::{Envelope, Header, Seq, StreamId};

/// Someone else appended first: the stream is at `actual`, not `expected`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Conflict {
    pub id: StreamId,
    pub expected: Seq,
    pub actual: Seq,
}

/// Where streams are kept. Games implement this for real storage; the engine
/// ships an in-memory one for tests.
pub trait EventStore<E> {
    /// Append to a stream, creating it with `header` if it doesn't exist yet.
    /// `expected` is the stream's length the caller last saw — optimistic
    /// concurrency.
    fn append(
        &mut self,
        header: &Header,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> Result<(), Conflict>;

    /// A stream's header and every event in it.
    fn read(&self, id: StreamId) -> Option<(Header, Vec<Envelope<E>>)>;
}

/// Streams held in memory, in id order.
pub struct InMemoryStore<E> {
    streams: BTreeMap<StreamId, (Header, Vec<Envelope<E>>)>,
}

impl<E> Default for InMemoryStore<E> {
    fn default() -> Self {
        InMemoryStore {
            streams: BTreeMap::new(),
        }
    }
}

impl<E> InMemoryStore<E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every stream id, in order.
    pub fn ids(&self) -> Vec<StreamId> {
        self.streams.keys().copied().collect()
    }
}

impl<E: Clone> EventStore<E> for InMemoryStore<E> {
    fn append(
        &mut self,
        header: &Header,
        expected: Seq,
        events: &[Envelope<E>],
    ) -> Result<(), Conflict> {
        let (_, log) = self
            .streams
            .entry(header.id)
            .or_insert_with(|| (header.clone(), Vec::new()));
        let actual = log.len() as Seq;
        if actual != expected {
            return Err(Conflict {
                id: header.id,
                expected,
                actual,
            });
        }
        log.extend_from_slice(events);
        Ok(())
    }

    fn read(&self, id: StreamId) -> Option<(Header, Vec<Envelope<E>>)> {
        self.streams.get(&id).cloned()
    }
}
