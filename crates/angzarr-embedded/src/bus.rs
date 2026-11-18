//! The bus: projectors served from the store an event at a time.

use std::num::NonZeroUsize;
use std::time::Duration;

use angzarr_proto::v1::Cover;
use angzarr_router::router::{DynProjectorHandler, ProjectorHandler};

use super::catalog::Catalog;
use super::error::KernelError;
use super::handle::Wait;
use super::misconfigured::Misconfigured;
use super::store::{CommitNotify, Processed, ReadStore, Store, Watch};

/// The most events of one stream a subscription has in flight at once.
pub const MAX_PER_STREAM: usize = 64;

/// One projector on a [`Bus`].
pub struct Subscription {
    _projector: Box<dyn DynProjectorHandler>,
}

impl Subscription {
    /// `projector` over the streams of `sources`, run as `instances`.
    pub fn new<P: ProjectorHandler + 'static>(
        _sources: &[&str],
        projector: P,
        _instances: NonZeroUsize,
    ) -> Result<Subscription, Misconfigured> {
        Ok(Subscription {
            _projector: Box::new(projector),
        })
    }

    /// Up to `in_flight` events of one stream at once.
    pub fn per_stream(self, _in_flight: NonZeroUsize) -> Subscription {
        self
    }
}

/// Subscriptions served from one store.
#[derive(Default)]
pub struct Bus {
    subscriptions: Vec<Subscription>,
}

/// What the bus served since it was last found idle.
#[derive(Debug, Default)]
pub struct Served {
    handled: u64,
    failures: Vec<(String, Cover, KernelError)>,
}

impl Served {
    /// How many events (and gone books) its handlers returned handled.
    pub fn handled(&self) -> u64 {
        self.handled
    }

    /// The events whose handler failed: the subscription's name, the
    /// stream, and why.
    pub fn failures(&self) -> &[(String, Cover, KernelError)] {
        &self.failures
    }
}

impl Bus {
    /// A bus with no subscription.
    pub fn new() -> Bus {
        Bus::default()
    }

    /// Serve `subscription` too.
    pub fn subscribe(&mut self, subscription: Subscription) -> Result<(), Misconfigured> {
        self.subscriptions.push(subscription);
        Ok(())
    }

    /// Serve every subscription from `store`, polling its change token.
    pub async fn follow<S: ReadStore + Store + Processed + Watch>(
        &mut self,
        _store: &S,
        _catalog: &Catalog,
        _wait: &dyn Wait,
        _every: Duration,
        _more: impl FnMut(&Served) -> bool,
    ) -> Result<(), KernelError> {
        Ok(())
    }

    /// [`Bus::follow`], woken by the store's commits.
    pub async fn follow_commits<S: ReadStore + Store + Processed + Watch + CommitNotify>(
        &mut self,
        _store: &S,
        _catalog: &Catalog,
        _wait: &dyn Wait,
        _every: Duration,
        _more: impl FnMut(&Served) -> bool,
    ) -> Result<(), KernelError> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "bus.test.rs"]
mod tests;
