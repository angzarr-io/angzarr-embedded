//! The bus: projectors served from the store an event at a time, each by a
//! pool of instances.
//!
//! An embedded-only way to run a projector beside [`CatchUp`](super::CatchUp):
//! where a catch-up hands each stream a book of everything it is owed, one
//! stream after another, the bus hands each of a subscription's instances
//! one event, and when that instance returns handled, records it and hands
//! it the next owed event of any stream of the sources. Every event is
//! read from the store when it is handed, never kept by the bus: the bus
//! holds its instances' slots and its positions, nothing else.
//!
//! - **Per stream, never across streams.** Positions are per (subscription,
//!   stream); nothing orders events of two streams, and they are handled in
//!   whatever order the bus hands them.
//! - **One event of a stream in flight**, by default, so a stream's events
//!   are handled one at a time, in order. A subscription whose events are
//!   independent work items asks for more ([`Subscription::per_stream`]),
//!   up to [`MAX_PER_STREAM`]: of a stream, only events within that many of
//!   its first unhandled one are handed.
//! - **A position is a mark and the handled set above it.** Every event
//!   below the mark is handled, and of those within the window above it,
//!   the set handled, so instances finish out of order and a crash hands
//!   again only what was not handled. Both are written after the handler
//!   returns, as one write: delivery is at least once.
//! - **Woken by a commit** ([`Bus::follow_commits`]: the store's
//!   [`CommitNotify`], armed before each read), polling the store's
//!   [`Watch`] token on the caller's timer as the fallback, which a store
//!   without push has alone ([`Bus::follow`]).
//! - **A failure holds its event**: its handler's error, or an upcast that
//!   failed, is reported ([`Served::failures`]), and the event is handed
//!   again at the next poll; a stream with one event in flight is held
//!   there, the others go on.
//! - **A purged stream** the subscription handled is handed as gone (its
//!   cover, no page, at 0) and forgotten, and one whose head is below its
//!   mark (purged and written again) is handed again from its start, as a
//!   stored catch-up's are.
//!
//! Runtime-agnostic: the instances run concurrently inside the follow's own
//! future, on whatever runtime polls it, with no thread or task of its own.
//! A handler that blocks runs its work where its runtime allows blocking.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::future::{poll_fn, Future};
use std::num::NonZeroUsize;
use std::pin::{pin, Pin};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;

use angzarr_ports::CommitSignal;
use angzarr_proto::v1::Cover;
use angzarr_router::router::{DynProjectorHandler, ProjectorHandler};

use super::catalog::Catalog;
use super::error::KernelError;
use super::handle::Wait;
use super::misconfigured::Misconfigured;
use super::projector::{gone_request, key, project_request};
use super::store::{
    CommitNotify, PositionKey, Processed, ReadStore, ReadUnit, Store, StoreError, StreamRef, Watch,
    Write,
};

/// The most events of one stream a subscription has in flight at once:
/// the handled set above a mark is kept as one 64-bit word.
pub const MAX_PER_STREAM: usize = 64;

/// What a subscription's name is followed by to name where it keeps the
/// handled set above each mark, a position of its own beside the mark's.
const HANDLED_ABOVE: &str = "+handled";

/// A stream as the bus tells streams apart ([`key`]).
type Key = (String, Vec<u8>, Option<String>);

/// One projector on a [`Bus`]: its sources, how many instances run it, and
/// how many events of one stream may be in flight at once.
pub struct Subscription {
    projector: Box<dyn DynProjectorHandler>,
    sources: Vec<String>,
    instances: usize,
    per_stream: u64,
}

impl Subscription {
    /// `projector` over the streams of `sources`, run as `instances` at
    /// once, one event each, one event of a stream in flight. Its positions
    /// are the store's, under its name. Refused for no source, as
    /// registering one is ([`Misconfigured::NoInputDomain`]).
    pub fn new<P: ProjectorHandler + 'static>(
        sources: &[&str],
        projector: P,
        instances: NonZeroUsize,
    ) -> Result<Subscription, Misconfigured> {
        if sources.is_empty() {
            return Err(Misconfigured::NoInputDomain {
                component: projector.name().to_string(),
                kind: angzarr_proto::v1::ComponentKind::Projector
                    .as_str_name()
                    .to_string(),
            });
        }
        Ok(Subscription {
            projector: Box::new(projector),
            sources: sources.iter().map(|source| source.to_string()).collect(),
            instances: instances.get(),
            per_stream: 1,
        })
    }

    /// Up to `in_flight` events of one stream at once (at most
    /// [`MAX_PER_STREAM`]), for events that are independent work items:
    /// their handler is then handed a stream's events out of order.
    pub fn per_stream(mut self, in_flight: NonZeroUsize) -> Subscription {
        self.per_stream = in_flight.get().min(MAX_PER_STREAM) as u64;
        self
    }
}

/// Subscriptions served from one store, each by its own instances. See the
/// module.
#[derive(Default)]
pub struct Bus {
    pools: Vec<Pool>,
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

    /// The events that failed (the handler's error, or an upcast that
    /// failed): the subscription's name, the stream, and why. Each is held,
    /// to be handed again at the next poll.
    pub fn failures(&self) -> &[(String, Cover, KernelError)] {
        &self.failures
    }
}

impl Bus {
    /// A bus with no subscription.
    pub fn new() -> Bus {
        Bus::default()
    }

    /// Serve `subscription` too. Refused for a name another subscription
    /// on the bus has ([`Misconfigured::ComponentRegistered`]): the name
    /// keys its positions.
    pub fn subscribe(&mut self, subscription: Subscription) -> Result<(), Misconfigured> {
        let name = DynProjectorHandler::name(&*subscription.projector).to_string();
        if self.pools.iter().any(|pool| pool.name == name) {
            return Err(Misconfigured::ComponentRegistered { component: name });
        }
        self.pools.push(Pool {
            handled_above: format!("{name}{HANDLED_ABOVE}"),
            name,
            sources: subscription.sources,
            projector: subscription.projector,
            instances: subscription.instances,
            per_stream: subscription.per_stream,
            state: Mutex::default(),
        });
        Ok(())
    }

    /// Serve every subscription from `store`, polling its change token
    /// every `every` on `wait` (the caller's timer). `more` is asked, with
    /// what was served since it was last asked, each time the bus is found
    /// idle (nothing in flight, nothing owed but what failed); the follow
    /// returns when it says no, or at a store error. Tokens compare only
    /// from one handle, so every follow is given the same store handle.
    pub async fn follow<S: ReadStore + Store + Processed + Watch>(
        &mut self,
        store: &S,
        catalog: &Catalog,
        wait: &dyn Wait,
        every: Duration,
        more: impl FnMut(&Served) -> bool,
    ) -> Result<(), KernelError> {
        self.serve(store, catalog, wait, every, None, more).await
    }

    /// [`Bus::follow`], woken by every commit the store pushes as well as
    /// by the poll, which still sees what the push does not (another
    /// process's commits).
    pub async fn follow_commits<S: ReadStore + Store + Processed + Watch + CommitNotify>(
        &mut self,
        store: &S,
        catalog: &Catalog,
        wait: &dyn Wait,
        every: Duration,
        more: impl FnMut(&Served) -> bool,
    ) -> Result<(), KernelError> {
        self.serve(store, catalog, wait, every, Some(store), more)
            .await
    }

    /// Run every pool's instances and the steering in one future, until
    /// the steering returns or an instance meets a store error.
    async fn serve<S: ReadStore + Store + Processed + Watch>(
        &mut self,
        store: &S,
        catalog: &Catalog,
        wait: &dyn Wait,
        every: Duration,
        push: Option<&dyn CommitNotify>,
        mut more: impl FnMut(&Served) -> bool,
    ) -> Result<(), KernelError> {
        for pool in &self.pools {
            pool.restart();
        }
        let signals = Signals::default();
        let pools = &self.pools;
        let mut instances: Vec<_> = pools
            .iter()
            .flat_map(|pool| (0..pool.instances).map(move |_| pool))
            .map(|pool| Box::pin(pool.instance(store, catalog, &signals)))
            .collect();
        let mut steering = pin!(steer(pools, store, wait, every, push, &signals, &mut more));
        poll_fn(|cx| {
            if let Poll::Ready(done) = steering.as_mut().poll(cx) {
                return Poll::Ready(done);
            }
            for instance in &mut instances {
                match instance.as_mut().poll(cx) {
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(never)) => match never {},
                    Poll::Pending => {}
                }
            }
            Poll::Pending
        })
        .await
    }
}

/// How the instances and the steering wake each other.
#[derive(Default)]
struct Signals {
    /// An event may be owed: the instances waiting for one look again.
    work: CommitSignal,
    /// An instance found nothing owed: the steering looks again.
    idle: CommitSignal,
}

/// What wakes the steering.
enum Woken {
    Commit,
    Idle,
    Tick,
}

/// Steer the pools: read each one's streams when it has an instance free
/// and something may have been committed since its last read, ask `more`
/// whenever every pool is idle, and wait for a commit, an idle instance or
/// the timer. Returns when `more` says no.
async fn steer<S: ReadStore + Store + Processed + Watch>(
    pools: &[Pool],
    store: &S,
    wait: &dyn Wait,
    every: Duration,
    push: Option<&dyn CommitNotify>,
    signals: &Signals,
    more: &mut impl FnMut(&Served) -> bool,
) -> Result<(), KernelError> {
    let mut timer = wait.wait(every);
    let mut token = None;
    loop {
        // Armed before reading: a commit after this resolves it.
        let mut commit = push.map(CommitNotify::next_commit);
        let mut idle = signals.idle.wait();
        let stale: Vec<&Pool> = pools.iter().filter(|pool| pool.stale()).collect();
        if !stale.is_empty() {
            token = Some(store.change_token().await.map_err(KernelError::Store)?);
            for pool in stale {
                pool.scan(store).await.map_err(KernelError::Store)?;
            }
            signals.work.notify();
        }
        if pools.iter().all(Pool::quiet) {
            let mut served = Served::default();
            for pool in pools {
                pool.served(&mut served);
            }
            if !more(&served) {
                return Ok(());
            }
        }
        let woken = poll_fn(|cx| {
            if let Some(commit) = commit.as_mut() {
                if Pin::new(commit).poll(cx).is_ready() {
                    return Poll::Ready(Woken::Commit);
                }
            }
            if Pin::new(&mut idle).poll(cx).is_ready() {
                return Poll::Ready(Woken::Idle);
            }
            timer.as_mut().poll(cx).map(|()| Woken::Tick)
        })
        .await;
        match woken {
            Woken::Commit => pools.iter().for_each(Pool::changed),
            Woken::Idle => {}
            Woken::Tick => {
                timer = wait.wait(every);
                let now = store.change_token().await.map_err(KernelError::Store)?;
                if token != Some(now) {
                    pools.iter().for_each(Pool::changed);
                }
                let mut released = false;
                for pool in pools {
                    released |= pool.release();
                }
                if released {
                    signals.work.notify();
                }
            }
        }
    }
}

/// One subscription as the bus serves it.
struct Pool {
    name: String,
    /// The handler name its handled sets are kept under.
    handled_above: String,
    sources: Vec<String>,
    projector: Box<dyn DynProjectorHandler>,
    instances: usize,
    per_stream: u64,
    state: Mutex<Lanes>,
}

/// A pool's streams and its instances' bookkeeping.
#[derive(Default)]
struct Lanes {
    lanes: BTreeMap<Key, Lane>,
    /// Its stored positions have been read into `lanes`.
    loaded: bool,
    /// Something may have been committed since its streams were last read.
    stale: bool,
    /// How many of its instances hold an event (or a gone book).
    busy: usize,
    handled: u64,
    failures: Vec<(Cover, KernelError)>,
}

/// Where a pool is on one stream. Every bit set is of a sequence within
/// the window above the mark: bit `i` is `mark + i`.
struct Lane {
    cover: Cover,
    /// The stream's next sequence, as last read.
    head: u64,
    /// Every sequence below it is handled; it is not.
    mark: u64,
    /// Handled above the mark (bit 0 never: the mark is not handled).
    handled: u64,
    /// Held by an instance (bit 0 also for the gone book).
    in_flight: u64,
    /// Failed, held until the next poll (bit 0 also for the gone book).
    held: u64,
    /// Gone from the store's listing with something handled: owed its
    /// gone book.
    gone: bool,
    /// An instance is writing its position; `rewrite` says it changed
    /// meanwhile, so that instance writes it again: one writer per lane, so
    /// a stored position never moves back.
    writing: bool,
    rewrite: bool,
}

impl Lane {
    fn new(cover: Cover) -> Lane {
        Lane {
            cover,
            head: 0,
            mark: 0,
            handled: 0,
            in_flight: 0,
            held: 0,
            gone: false,
            writing: false,
            rewrite: false,
        }
    }

    /// What it is owed first: its gone book, once nothing of it is in
    /// flight, failed or being written; else the first sequence within the
    /// window that is below the head and neither handled, in flight nor
    /// held.
    fn owed(&self, per_stream: u64) -> Option<Owed> {
        if self.gone {
            let free = self.in_flight == 0 && self.held == 0 && !self.writing;
            return free.then_some(Owed::Gone);
        }
        let taken = self.handled | self.in_flight | self.held;
        (0..per_stream)
            .take_while(|&i| self.mark + i < self.head)
            .find(|&i| taken & (1 << i) == 0)
            .map(|i| Owed::Event(self.mark + i))
    }

    /// Move the mark past every handled sequence at it.
    fn advance(&mut self) {
        while self.handled & 1 == 1 {
            self.handled >>= 1;
            self.in_flight >>= 1;
            self.held >>= 1;
            self.mark += 1;
        }
    }

    /// Start the stream afresh: purged and written again.
    fn restart(&mut self) {
        self.mark = 0;
        self.handled = 0;
        self.held = 0;
    }

    /// Whether it holds anything the store keeps for it.
    fn kept(&self) -> bool {
        self.mark > 0 || self.handled != 0
    }
}

/// What a lane is owed next.
enum Owed {
    Event(u64),
    Gone,
}

/// An instance's hold on one owed item of a lane.
struct Claim {
    key: Key,
    cover: Cover,
    owed: Owed,
}

/// `sequence`'s bit in `lane`: in flight, it is within the window above
/// the mark.
fn bit(lane: &Lane, sequence: u64) -> u64 {
    1 << (sequence - lane.mark)
}

impl Pool {
    fn lock(&self) -> MutexGuard<'_, Lanes> {
        // Nothing panics under the lock with the state half-changed.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A new follow: no instance holds anything, what failed is handed
    /// again, and the streams are read again.
    fn restart(&self) {
        let mut state = self.lock();
        for lane in state.lanes.values_mut() {
            lane.in_flight = 0;
            lane.held = 0;
            lane.writing = false;
            lane.rewrite = false;
        }
        state.busy = 0;
        state.stale = true;
    }

    /// Something may have been committed.
    fn changed(&self) {
        self.lock().stale = true;
    }

    /// Whether its streams are to be read now: something may have been
    /// committed, and an instance is free to be handed what that owes.
    fn stale(&self) -> bool {
        let state = self.lock();
        state.stale && state.busy < self.instances
    }

    /// Whether it is idle: nothing in flight, nothing owed but what
    /// failed, and nothing committed it has not read.
    fn quiet(&self) -> bool {
        let state = self.lock();
        let owed = |lane: &Lane| lane.owed(self.per_stream).is_some();
        state.busy == 0 && !state.stale && !state.lanes.values().any(owed)
    }

    /// Add what it served since last asked to `served`, and start again.
    fn served(&self, served: &mut Served) {
        let mut state = self.lock();
        served.handled += std::mem::take(&mut state.handled);
        let failures = std::mem::take(&mut state.failures);
        let named = failures.into_iter();
        let named = named.map(|(cover, error)| (self.name.clone(), cover, error));
        served.failures.extend(named);
    }

    /// Hand again what failed. Whether anything had.
    fn release(&self) -> bool {
        let mut state = self.lock();
        let held = state.lanes.values().any(|lane| lane.held != 0);
        for lane in state.lanes.values_mut() {
            lane.held = 0;
        }
        held
    }

    /// Read its sources' streams and their heads, as one read unit saw the
    /// store, and its stored positions the first time: a stream it kept
    /// something for that is no longer listed is owed its gone book, and
    /// one whose head is below its mark starts afresh.
    async fn scan<S: ReadStore + Processed>(&self, store: &S) -> Result<(), StoreError> {
        let stored = {
            let mut state = self.lock();
            state.stale = false;
            !state.loaded
        };
        let stored = match stored {
            true => Some((
                store.processed(&self.name).await?,
                store.processed(&self.handled_above).await?,
            )),
            false => None,
        };
        let mut listed = BTreeMap::new();
        let mut reads = store.begin_read().await?;
        for source in &self.sources {
            for cover in reads.covers(source).await? {
                let head = reads.head(StreamRef::try_from(&cover)?).await?;
                listed.insert(key(&cover), (cover, head.next_sequence));
            }
        }
        drop(reads);
        let mut state = self.lock();
        if let Some((marks, handled)) = stored {
            let ours = |cover: &Cover| self.sources.contains(&cover.domain);
            for (cover, mark) in marks.into_iter().filter(|(cover, _)| ours(cover)) {
                let lane = state.lanes.entry(key(&cover)).or_insert(Lane::new(cover));
                lane.mark = mark;
            }
            for (cover, bits) in handled.into_iter().filter(|(cover, _)| ours(cover)) {
                let lane = state.lanes.entry(key(&cover)).or_insert(Lane::new(cover));
                lane.handled = bits;
            }
            state.loaded = true;
        }
        let unlisted: BTreeSet<Key> = state
            .lanes
            .keys()
            .filter(|key| !listed.contains_key(*key))
            .cloned()
            .collect();
        for key in unlisted {
            let Some(lane) = state.lanes.get_mut(&key) else {
                continue;
            };
            match lane.kept() || lane.writing {
                true => lane.gone = true,
                false if lane.in_flight == 0 => {
                    state.lanes.remove(&key);
                }
                false => {}
            }
        }
        for (key, (cover, head)) in listed {
            let lane = state.lanes.entry(key).or_insert(Lane::new(cover));
            lane.gone = false;
            lane.head = head;
            if head < lane.mark && lane.in_flight == 0 && !lane.writing {
                lane.restart();
            }
        }
        Ok(())
    }

    /// Hold the first owed item of any of its streams, if any.
    fn claim(&self) -> Option<Claim> {
        let mut guard = self.lock();
        let state = &mut *guard;
        let (key, lane, owed) = state.lanes.iter_mut().find_map(|(key, lane)| {
            let owed = lane.owed(self.per_stream)?;
            Some((key.clone(), lane, owed))
        })?;
        lane.in_flight |= match owed {
            Owed::Event(sequence) => bit(lane, sequence),
            Owed::Gone => 1,
        };
        let cover = lane.cover.clone();
        state.busy += 1;
        Some(Claim { key, cover, owed })
    }

    /// One instance: hand it each owed item in turn, waiting for one when
    /// nothing is owed. Ends only at a store error.
    async fn instance<S: Store>(
        &self,
        store: &S,
        catalog: &Catalog,
        signals: &Signals,
    ) -> Result<Infallible, KernelError> {
        loop {
            // Armed before looking: work owed after this resolves it.
            let work = signals.work.wait();
            match self.claim() {
                Some(claim) => {
                    self.hand(store, catalog, claim).await?;
                    signals.work.notify();
                }
                None => {
                    signals.idle.notify();
                    work.await;
                }
            }
        }
    }

    /// Hand `claim`'s item to the projector, read from the store now, and
    /// record what came of it.
    async fn hand<S: Store>(
        &self,
        store: &S,
        catalog: &Catalog,
        claim: Claim,
    ) -> Result<(), KernelError> {
        let Claim { key, cover, owed } = claim;
        let sequence = match owed {
            Owed::Gone => return self.hand_gone(store, key, cover).await,
            Owed::Event(sequence) => sequence,
        };
        let stream = StreamRef::try_from(&cover).map_err(KernelError::Store)?;
        let slice = store
            .read(stream, sequence, Some(sequence + 1))
            .await
            .map_err(KernelError::Store)?;
        let next_sequence = slice.next_sequence;
        if slice.pages.is_empty() {
            self.missing(&key, sequence, next_sequence);
            return Ok(());
        }
        let request = match project_request(catalog, &cover, slice.pages, next_sequence).await {
            Ok(request) => request,
            Err(error) => {
                self.failed(&key, Some(sequence), cover, error);
                return Ok(());
            }
        };
        let fork = request
            .events
            .as_ref()
            .is_none_or(|book| book.pages.is_empty());
        if !fork {
            if let Err(error) = DynProjectorHandler::dispatch(&*self.projector, &request).await {
                self.failed(&key, Some(sequence), cover, KernelError::Router(error));
                return Ok(());
            }
        }
        // A fork page is never handed, but its sequence counts.
        self.handled(store, &key, sequence, !fork)
            .await
            .map_err(KernelError::Store)
    }

    /// Hand `cover`'s gone book; handled, forget the stream, its positions
    /// put back to 0.
    async fn hand_gone<S: Store>(
        &self,
        store: &S,
        key: Key,
        cover: Cover,
    ) -> Result<(), KernelError> {
        if let Err(error) =
            DynProjectorHandler::dispatch(&*self.projector, &gone_request(&cover)).await
        {
            self.failed(&key, None, cover, KernelError::Router(error));
            return Ok(());
        }
        let written = self.write(store, &cover, 0, 0).await;
        let mut state = self.lock();
        state.busy -= 1;
        written.map_err(KernelError::Store)?;
        state.lanes.remove(&key);
        state.handled += 1;
        Ok(())
    }

    /// `sequence` of `key`'s stream is not there to read: the stream ends
    /// at `next_sequence`, so it is not owed until the next read of the
    /// streams says otherwise.
    fn missing(&self, key: &Key, sequence: u64, next_sequence: u64) {
        let mut state = self.lock();
        state.busy -= 1;
        state.stale = true;
        if let Some(lane) = state.lanes.get_mut(key) {
            lane.in_flight &= !bit(lane, sequence);
            lane.head = next_sequence;
        }
    }

    /// `sequence` of `key`'s stream (`None`: its gone book) failed: held
    /// until the next poll, and reported.
    fn failed(&self, key: &Key, sequence: Option<u64>, cover: Cover, error: KernelError) {
        let mut state = self.lock();
        state.busy -= 1;
        state.failures.push((cover, error));
        if let Some(lane) = state.lanes.get_mut(key) {
            let at = sequence.map_or(1, |sequence| bit(lane, sequence));
            lane.in_flight &= !at;
            lane.held |= at;
        }
    }

    /// `sequence` of `key`'s stream is handled (`counted`: by the
    /// projector, not a fork page passed over): move the lane on and write
    /// its position, unless another instance is writing it, which then
    /// writes it again.
    async fn handled<S: Store>(
        &self,
        store: &S,
        key: &Key,
        sequence: u64,
        counted: bool,
    ) -> Result<(), StoreError> {
        let mut next = self.record(key, sequence, counted);
        while let Some((cover, mark, handled)) = next {
            let written = self.write(store, &cover, mark, handled).await;
            next = self.wrote(key, written.is_ok());
            written?;
        }
        Ok(())
    }

    /// Record `sequence` of `key`'s stream handled: what to write, if this
    /// instance is to write it (its cover, mark and handled set). Else the
    /// instance is done.
    fn record(&self, key: &Key, sequence: u64, counted: bool) -> Option<(Cover, u64, u64)> {
        let mut guard = self.lock();
        let state = &mut *guard;
        state.handled += u64::from(counted);
        let Some(lane) = state.lanes.get_mut(key) else {
            state.busy -= 1;
            return None;
        };
        let at = bit(lane, sequence);
        lane.in_flight &= !at;
        lane.handled |= at;
        lane.advance();
        if lane.writing {
            lane.rewrite = true;
            state.busy -= 1;
            return None;
        }
        lane.writing = true;
        Some((lane.cover.clone(), lane.mark, lane.handled))
    }

    /// The write of `key`'s position landed (`ok`) or failed: what to
    /// write again, if the lane changed meanwhile. Else the instance is
    /// done, and the lane has no writer.
    fn wrote(&self, key: &Key, ok: bool) -> Option<(Cover, u64, u64)> {
        let mut guard = self.lock();
        let state = &mut *guard;
        let lane = state.lanes.get_mut(key);
        match lane {
            Some(lane) if ok && lane.rewrite => {
                lane.rewrite = false;
                Some((lane.cover.clone(), lane.mark, lane.handled))
            }
            lane => {
                if let Some(lane) = lane {
                    lane.writing = false;
                }
                state.busy -= 1;
                None
            }
        }
    }

    /// Put its position on `cover`'s stream at `mark`, with the handled set
    /// above it, as one write.
    async fn write<S: Store>(
        &self,
        store: &S,
        cover: &Cover,
        mark: u64,
        handled: u64,
    ) -> Result<(), StoreError> {
        let stream = StreamRef::try_from(cover)?;
        let positions = [
            (
                PositionKey {
                    handler: &self.name,
                    stream,
                },
                mark,
            ),
            (
                PositionKey {
                    handler: &self.handled_above,
                    stream,
                },
                handled,
            ),
        ];
        store.write(Write::positions(&positions)).await.map(drop)
    }
}

#[cfg(test)]
#[path = "bus.test.rs"]
mod tests;
