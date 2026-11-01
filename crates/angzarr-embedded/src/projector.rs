//! Projectors: ONE port, run two ways.
//!
//! A projector is angzarr-router's [`ProjectorHandler`]: handed a book of
//! one stream's events, it answers a `Projection`. A hand-written one, a
//! marked-up one (`#[projector]`, through [`markup::Projected`](super::markup::Projected)) and a
//! remote one (a child's `ProjectorService`, the host's) are the same
//! port, and run either way:
//!
//! - **inline**, registered in the [`Registry`] ([`Registry::projector`],
//!   [`Registry::projector_handler`]): the kernel hands it each step's
//!   committed book of its sources and awaits it before the call returns
//!   ([`Kernel::handle`](super::Kernel::handle));
//! - **catch-up**, a [`CatchUp`] the application runs, a step at a time
//!   ([`CatchUp::step`], [`CatchUp::step_if_changed`]) or as a loop
//!   ([`CatchUp::follow`]), on its own runtime: no thread of its own.
//!
//! What a projector is handed, either way, is per stream; no order crosses
//! streams, and nothing numbers across them:
//!
//! - a book of one stream's events, in sequence order, after the last it
//!   was handed of that stream, `next_sequence` the stream's length;
//! - a book whose first page is sequence 0 starts that stream afresh:
//!   whatever it held of a stream on the same cover is gone (one purged,
//!   and written again);
//! - in catch-up, a book with no page and `next_sequence` 0: the stream is
//!   gone (purged), and the projector forgets it.

use std::collections::BTreeMap;

use angzarr_proto::v1::Cover;
use angzarr_router::router::ProjectorHandler;

use super::error::KernelError;
use super::handle::Wait;
use super::registry::Registry;
use super::store::{ChangeToken, ReadStore, Store, Watch};

/// A projector caught up from a store: every main-timeline stream of its
/// sources (an edition is a what-if fork, not the application's timeline)
/// handed what it has not yet been handed, one book per stream, from where
/// the projector is on that stream.
///
/// Where it is on each stream is kept one of two ways:
/// - **held** ([`CatchUp::new`]): in memory, for a projection held in
///   memory, rebuilt by its first step from each stream's start. A stream
///   purged and written again is told from the one it replaced by its
///   first event (its cause and when it was recorded), never by its
///   sequence, since each starts at 0: it is handed again from its start.
/// - **stored** ([`CatchUp::stored`]): in the store's [`PositionStore`](super::PositionStore),
///   under the projector's name, as the kernel keeps a policy's, so a
///   durable projection resumes where it left off in a new process. A
///   purge removes the positions on its stream, so one written again is
///   handed from its start.
///
/// A step reads in one read-only unit ([`ReadStore`]): what it hands is
/// one moment of the store, and a write landing meanwhile is the next
/// step's. A book the projector fails (or that does not upcast) keeps the
/// stream where it was, so the next step hands it again; the other
/// streams go on. Delivery is at least once: a stored position is written
/// after its book is handed.
pub struct CatchUp<P> {
    projector: P,
    sources: Vec<String>,
    kept: Kept,
    seen: BTreeMap<Vec<u8>, ()>,
    token: Option<ChangeToken>,
}

/// Where a [`CatchUp`] keeps its positions.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kept {
    Held,
    Stored,
}

/// What one catch-up step did.
#[derive(Debug, Default)]
pub struct Caught {
    handed: Vec<Cover>,
    failures: Vec<(Cover, KernelError)>,
}

impl Caught {
    /// The streams handed a book this step, in the order handed: a gone
    /// stream's empty book included.
    pub fn handed(&self) -> &[Cover] {
        &self.handed
    }

    /// The streams whose book failed (the projector's error, or an upcast
    /// that failed), each with why: held where they were, for the next
    /// step.
    pub fn failures(&self) -> &[(Cover, KernelError)] {
        &self.failures
    }
}

impl<P: ProjectorHandler> CatchUp<P> {
    /// `projector` over the streams of `sources`, its positions held in
    /// memory. PANICS for no source, as registering one does.
    pub fn new(sources: &[&str], projector: P) -> CatchUp<P> {
        Self::kept(sources, projector, Kept::Held)
    }

    /// `projector` over the streams of `sources`, its positions in the
    /// store, under its name. PANICS for no source.
    pub fn stored(sources: &[&str], projector: P) -> CatchUp<P> {
        Self::kept(sources, projector, Kept::Stored)
    }

    fn kept(sources: &[&str], projector: P, kept: Kept) -> CatchUp<P> {
        let _ = (sources, kept);
        let _ = projector;
        todo!()
    }

    /// The projector.
    pub fn projector(&self) -> &P {
        &self.projector
    }

    /// One step: every stream of the sources the store holds handed what
    /// it is owed, then every stream handed before and no longer held
    /// handed as gone. A store error ends the step; what was handed before
    /// it stays handed.
    pub async fn step<S: ReadStore + Store>(
        &mut self,
        store: &S,
        registry: &Registry,
    ) -> Result<Caught, KernelError> {
        let _ = (store, registry, &self.sources, &self.kept, &self.seen);
        todo!()
    }

    /// [`CatchUp::step`], unless nothing was committed since the last step
    /// that handed everything it owed (the store's [`Watch`] token is
    /// unchanged): then `None`, and nothing is read. Tokens compare only
    /// from one handle, so every call is given the same store handle.
    pub async fn step_if_changed<S: ReadStore + Store + Watch>(
        &mut self,
        store: &S,
        registry: &Registry,
    ) -> Result<Option<Caught>, KernelError> {
        let _ = (store, registry, &self.token);
        todo!()
    }

    /// Follow the store: [`CatchUp::step_if_changed`], then wait `every`
    /// on `wait` (the caller's timer), again and again while `more`, asked
    /// after each poll with what that step did (`None`: nothing changed),
    /// says so. Returns when it does not, or at a store error.
    pub async fn follow<S: ReadStore + Store + Watch>(
        &mut self,
        store: &S,
        registry: &Registry,
        wait: &dyn Wait,
        every: std::time::Duration,
        more: impl FnMut(Option<&Caught>) -> bool,
    ) -> Result<(), KernelError> {
        let _ = (store, registry, wait, every, more);
        todo!()
    }
}

#[cfg(test)]
#[path = "projector.test.rs"]
mod tests;
