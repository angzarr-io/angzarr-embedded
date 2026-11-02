//! Projectors: ONE port, run two ways.
//!
//! A projector is angzarr-router's [`ProjectorHandler`]: handed a book of
//! one stream's events, it answers a `Projection`. A hand-written one, a
//! marked-up one (`#[projector]`, through [`markup::Projected`](super::markup::Projected)) and a
//! remote one (a child's `ProjectorService`, the host's) are the same
//! port, and run either way:
//!
//! - **inline**, registered in the [`Registry`](super::Registry)
//!   ([`Registry::projector`](super::Registry::projector),
//!   [`Registry::projector_handler`](super::Registry::projector_handler)):
//!   the kernel hands it each step's
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

use std::collections::{BTreeMap, BTreeSet};

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EventBook, EventPage};
use angzarr_router::router::ProjectorHandler;
use prost_types::Timestamp;

use super::catalog::{Catalog, Misconfigured};
use super::error::KernelError;
use super::handle::Wait;
use super::store::{
    ChangeToken, ListPositions, PositionKey, ReadStore, ReadUnit, Store, StoreError, StreamRef,
    Watch, Write,
};
use super::upcast::upcast_pages;

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
///   handed from its start. Its first step reads every position the
///   projector holds ([`ListPositions`]): a stream it was handed before it
///   started is known from the start, so one purged since is handed gone,
///   and its position is put back to 0 (a purge landing between a step's
///   read and its position's write leaves that position behind).
///
/// A step reads in one read-only unit ([`ReadStore`]): what it hands is
/// one moment of the store, and a write landing meanwhile is the next
/// step's. Each page is upcast as a policy's trigger is, through its
/// domain's upcast port. A book the projector fails (or that does not
/// upcast) keeps the stream where it was, so the next step hands it again;
/// the other streams go on. Delivery is at least once: a stored position
/// is written after its book is handed.
pub struct CatchUp<P> {
    projector: P,
    sources: Vec<String>,
    kept: Kept,
    /// Every stream handed a book and still held when last read, by
    /// [`key`]: how far it is (where positions are held), and what tells
    /// it from a stream written again on its cover.
    seen: BTreeMap<(String, Vec<u8>), Seen>,
    /// The store's change token when a step last handed everything it
    /// owed ([`CatchUp::step_if_changed`]).
    token: Option<ChangeToken>,
}

/// Where a [`CatchUp`] keeps its positions.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kept {
    Held,
    /// In the store; `listed` once its positions have been read.
    Stored {
        listed: bool,
    },
}

/// One stream a catch-up has handed a book.
struct Seen {
    cover: Cover,
    /// Its first event's fingerprint.
    first: Fingerprint,
    /// The next sequence it is owed.
    next: u64,
}

/// What tells one stream on a cover from another: its first event's cause
/// (the command that wrote it) and when it was recorded. The sequence
/// cannot tell them apart (each starts at 0), nor the payload (a stream
/// written again may hold the same).
#[derive(Clone, PartialEq, Debug)]
struct Fingerprint {
    cause: Option<Causation>,
    recorded_at: Option<Timestamp>,
}

impl Fingerprint {
    fn of(page: Option<&EventPage>) -> Fingerprint {
        let meta = page.and_then(|page| page.meta.as_ref());
        Fingerprint {
            cause: meta.and_then(|meta| meta.causation.clone()),
            recorded_at: meta.and_then(|meta| meta.recorded_at),
        }
    }
}

/// A stream as a catch-up tells streams apart: its domain and root (it
/// reads main only).
fn key(cover: &Cover) -> (String, Vec<u8>) {
    let root = cover.root.as_ref().map(|r| r.value.to_vec());
    (cover.domain.clone(), root.unwrap_or_default())
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
    /// memory. Refused for no source, as registering one is
    /// ([`Misconfigured::ProjectorNoInputDomain`]).
    pub fn new(sources: &[&str], projector: P) -> Result<CatchUp<P>, Misconfigured> {
        Self::kept(sources, projector, Kept::Held)
    }

    /// `projector` over the streams of `sources`, its positions in the
    /// store, under its name. Refused for no source.
    pub fn stored(sources: &[&str], projector: P) -> Result<CatchUp<P>, Misconfigured> {
        Self::kept(sources, projector, Kept::Stored { listed: false })
    }

    fn kept(sources: &[&str], projector: P, kept: Kept) -> Result<CatchUp<P>, Misconfigured> {
        if sources.is_empty() {
            return Err(Misconfigured::ProjectorNoInputDomain {
                component: projector.name().to_string(),
            });
        }
        Ok(CatchUp {
            projector,
            sources: sources.iter().map(|source| source.to_string()).collect(),
            kept,
            seen: BTreeMap::new(),
            token: None,
        })
    }

    /// The projector.
    pub fn projector(&self) -> &P {
        &self.projector
    }

    /// One step: every stream of the sources the store holds handed what
    /// it is owed, then every stream handed before and no longer held
    /// handed as gone (stored, "before" counts from the positions the
    /// store holds when the first step begins: [`ListPositions`]). A store
    /// error ends the step; what was handed before it stays handed.
    pub async fn step<S: ReadStore + Store + ListPositions>(
        &mut self,
        store: &S,
        catalog: &Catalog,
    ) -> Result<Caught, KernelError> {
        let mut caught = Caught::default();
        self.listed(store).await.map_err(KernelError::Store)?;
        let mut reads = store.begin_read().await.map_err(KernelError::Store)?;
        let mut held = BTreeSet::new();
        for source in self.sources.clone() {
            let covers = reads.covers(&source).await.map_err(KernelError::Store)?;
            for cover in covers.into_iter().filter(|c| c.edition.is_none()) {
                held.insert(key(&cover));
                self.catch_stream(store, &mut reads, catalog, cover, &mut caught)
                    .await?;
            }
        }
        self.hand_gone(store, &held, &mut caught).await;
        Ok(caught)
    }

    /// [`CatchUp::step`], unless nothing was committed since the last step
    /// that handed everything it owed (the store's [`Watch`] token is
    /// unchanged): then `None`, and nothing is read. Tokens compare only
    /// from one handle, so every call is given the same store handle.
    pub async fn step_if_changed<S: ReadStore + Store + ListPositions + Watch>(
        &mut self,
        store: &S,
        catalog: &Catalog,
    ) -> Result<Option<Caught>, KernelError> {
        let token = store.change_token().await.map_err(KernelError::Store)?;
        if self.token == Some(token) {
            return Ok(None);
        }
        let caught = self.step(store, catalog).await?;
        self.token = caught.failures.is_empty().then_some(token);
        Ok(Some(caught))
    }

    /// Follow the store: [`CatchUp::step_if_changed`], then wait `every`
    /// on `wait` (the caller's timer), again and again while `more`, asked
    /// after each poll with what that step did (`None`: nothing changed),
    /// says so. Returns when it does not, or at a store error.
    pub async fn follow<S: ReadStore + Store + ListPositions + Watch>(
        &mut self,
        store: &S,
        catalog: &Catalog,
        wait: &dyn Wait,
        every: std::time::Duration,
        mut more: impl FnMut(Option<&Caught>) -> bool,
    ) -> Result<(), KernelError> {
        loop {
            let caught = self.step_if_changed(store, catalog).await?;
            if !more(caught.as_ref()) {
                return Ok(());
            }
            wait.wait(every).await;
        }
    }

    /// Stored, before its first step: every main-timeline stream of its
    /// sources it holds a position on, known as handed to there.
    async fn listed<S: ListPositions>(&mut self, store: &S) -> Result<(), StoreError> {
        if self.kept != (Kept::Stored { listed: false }) {
            return Ok(());
        }
        let ours = |cover: &Cover| cover.edition.is_none() && self.sources.contains(&cover.domain);
        let listed = store.list_positions(self.projector.name()).await?;
        for (cover, next) in listed.into_iter().filter(|(cover, _)| ours(cover)) {
            let first = Fingerprint::of(None);
            self.seen.insert(key(&cover), Seen { cover, first, next });
        }
        self.kept = Kept::Stored { listed: true };
        Ok(())
    }

    /// Hand `cover`'s stream what it is owed, read through `reads`: its
    /// pages from where the projector is on it, upcast, as one book.
    async fn catch_stream<S: Store, U: ReadUnit>(
        &mut self,
        store: &S,
        reads: &mut U,
        catalog: &Catalog,
        cover: Cover,
        caught: &mut Caught,
    ) -> Result<(), KernelError> {
        let stream = StreamRef::try_from(&cover).map_err(KernelError::Store)?;
        let (from, first) = self
            .owed(store, reads, &cover)
            .await
            .map_err(KernelError::Store)?;
        let slice = reads
            .read(stream, from, None)
            .await
            .map_err(KernelError::Store)?;
        if slice.pages.is_empty() {
            return Ok(());
        }
        let first = first.unwrap_or_else(|| Fingerprint::of(slice.pages.first()));
        let pages = match catalog.upcast_port(&cover.domain) {
            None => slice.pages,
            Some(port) => match upcast_pages(port, &cover.domain, &slice.pages).await {
                Ok(pages) => pages,
                Err(error) => {
                    let stream = cover.clone();
                    caught
                        .failures
                        .push((cover, KernelError::Upcast { stream, error }));
                    return Ok(());
                }
            },
        };
        let book = EventBook {
            cover: Some(cover.clone()),
            pages,
            next_sequence: slice.next_sequence,
            snapshot: None,
        };
        if let Err(error) = self.projector.dispatch(&book).await {
            caught.failures.push((cover, KernelError::Router(error)));
            return Ok(());
        }
        self.handed(store, &cover, first, slice.next_sequence)
            .await
            .map_err(KernelError::Store)?;
        caught.handed.push(cover);
        Ok(())
    }

    /// Where the projector is on `cover`'s stream: the next sequence it is
    /// owed, and, past its start, the fingerprint of the stream it was
    /// handed. Held, a stream seen before is owed from where it was
    /// handed to, unless its first event is not the one seen first: then
    /// it is a stream written again, owed from its start. Stored, the
    /// store's position.
    async fn owed<S: Store, U: ReadUnit>(
        &self,
        store: &S,
        reads: &mut U,
        cover: &Cover,
    ) -> Result<(u64, Option<Fingerprint>), StoreError> {
        let stream = StreamRef::try_from(cover)?;
        let seen = self.seen.get(&key(cover));
        let from = match (self.kept, seen) {
            (Kept::Stored { .. }, _) => {
                let handler = self.projector.name();
                store.position(PositionKey { handler, stream }).await?
            }
            (Kept::Held, None) => 0,
            (Kept::Held, Some(seen)) => {
                let start = reads.read(stream, 0, Some(1)).await?;
                match Fingerprint::of(start.pages.first()) == seen.first {
                    true => seen.next,
                    false => 0,
                }
            }
        };
        let first = seen.filter(|_| from > 0).map(|seen| seen.first.clone());
        Ok((from, first))
    }

    /// Keep that `cover`'s stream, whose first event is `first`, was
    /// handed up to `next`: in memory, and, stored, in the store.
    async fn handed<S: Store>(
        &mut self,
        store: &S,
        cover: &Cover,
        first: Fingerprint,
        next: u64,
    ) -> Result<(), StoreError> {
        if self.kept != Kept::Held {
            self.store_position(store, cover, next).await?;
        }
        let cover = cover.clone();
        self.seen.insert(key(&cover), Seen { cover, first, next });
        Ok(())
    }

    /// Put the projector's position on `cover`'s stream at `next`, in the
    /// store.
    async fn store_position<S: Store>(
        &self,
        store: &S,
        cover: &Cover,
        next: u64,
    ) -> Result<(), StoreError> {
        let handler = self.projector.name();
        let stream = StreamRef::try_from(cover)?;
        store
            .write(Write::positions(&[(PositionKey { handler, stream }, next)]))
            .await
            .map(drop)
    }

    /// Hand every stream seen before and not `held` now its gone book: no
    /// page, at 0; stored, its position put back to 0. One the projector
    /// fails (or whose position is not put back) stays seen, to be handed
    /// again.
    async fn hand_gone<S: Store>(
        &mut self,
        store: &S,
        held: &BTreeSet<(String, Vec<u8>)>,
        caught: &mut Caught,
    ) {
        let gone: Vec<_> = self
            .seen
            .keys()
            .filter(|key| !held.contains(*key))
            .cloned()
            .collect();
        for key in gone {
            let cover = self.seen[&key].cover.clone();
            let book = EventBook {
                cover: Some(cover.clone()),
                ..EventBook::default()
            };
            if let Err(error) = self.projector.dispatch(&book).await {
                caught.failures.push((cover, KernelError::Router(error)));
                continue;
            }
            if self.kept != Kept::Held {
                if let Err(error) = self.store_position(store, &cover, 0).await {
                    caught.failures.push((cover, KernelError::Store(error)));
                    continue;
                }
            }
            self.seen.remove(&key);
            caught.handed.push(cover);
        }
    }
}

#[cfg(test)]
#[path = "projector.test.rs"]
mod tests;
