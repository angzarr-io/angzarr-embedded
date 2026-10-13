//! Read models: folds over one or more streams, never stored.

use crate::stream::Envelope;

/// A read model, fed events in log order. A projection over several streams
/// implements this once per event type.
pub trait Projection<E> {
    fn on(&mut self, envelope: &Envelope<E>);
}

/// Feed a run of events to a projection.
pub fn project<E, P: Projection<E>>(projection: &mut P, events: &[Envelope<E>]) {
    for e in events {
        projection.on(e);
    }
}
