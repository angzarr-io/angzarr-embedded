//! The bus: angzarr core's bus interface ([`angzarr_bus::Bus`],
//! angzarr-project's `docs/design/bus-interface.md`) over the store,
//! hosted on a kernel ([`Host`]): projector and policy subscriptions, each
//! served an event at a time by a pool of instances. The trait is the
//! interface, the distributed coordinator's as well; angzarr core's
//! conformance suite holds this bus to it (`bus_contract.test.rs`).
//!
//! The bus hands each of a subscription's instances one event, and when
//! that instance returns handled, records it and hands it the next owed
//! event of any stream of its scope. Every event is read from the store
//! when it is handed, never kept by the bus: the bus holds its instances'
//! slots and its positions, nothing else.
//!
//! - **Per stream, never across streams.** Positions are per (subscription,
//!   stream); nothing orders events of two streams.
//! - **Credit**: at most its instances in its handler at once, and one
//!   event of a stream by default, so a stream's events are handled one at
//!   a time, in order. A projector whose events are independent work items
//!   asks for more ([`angzarr_bus::Credit::per_stream`]), up to
//!   [`MAX_PER_STREAM`]; a policy is always served one at a time.
//! - **A position is a mark and the handled set above it**
//!   ([`angzarr_bus::Window`]): instances finish out of order and a crash
//!   hands again only what was not handled. Both are written after the
//!   handler returns, as one write: delivery is at least once.
//! - **Woken by a commit** ([`Host::pushed_by`]: the store's
//!   [`CommitNotify`], armed before each read), polling the store's
//!   [`Watch`] token on the host's timer as the fallback, which a store
//!   without push has alone.
//! - **A projector's failure** is the kernel's, as an inline projector's
//!   is: a retryable one retried as the kernel retries, then dead-lettered
//!   at its cap; any other dead-lettered at once. Dead-lettered (its letter
//!   kept in the store, placed at the event), the event is passed; else
//!   (retries off, or the letter not kept) it is held, and handed again at
//!   the next poll, its stream held there with one event in flight. An
//!   upcast that fails holds its event too. Each failure is reported.
//! - **A purged stream** a projector handled is handed as gone (its cover,
//!   no page, at 0) and forgotten; a policy is never called for it, and
//!   forgets it. One whose head is below its mark (purged and written
//!   again) is handed again from its start.
//!
//! **A policy on the bus** ([`BusPolicy`]) is one the registry does not
//! hold, never both: no chain runs it, and
//! [`Kernel::resume`](super::Kernel::resume) and
//! [`Kernel::forget_gone`](super::Kernel::forget_gone) never touch it. Each
//! event is a chain's policy step: the kernel reacts, routes the outputs
//! (their ids `ids::policy_command_id`'s), writes the policy's position
//! past the event with the last of them, then settles what they wrote (the
//! targets' inline policies and the inline projectors). Its position is an
//! inline policy's (the next sequence, under its name), so a policy moved
//! between the two keeps its place. Its failure is the kernel's
//! (dead-lettered, the position past it under
//! [`OnPolicyBug::MovePast`](super::OnPolicyBug), or held), reported;
//! held, it is handed again at the next poll.
//!
//! Runtime-agnostic: the instances run concurrently inside the serve's own
//! future, on whatever runtime polls it, with no thread or task of its own.
//! A handler that blocks runs its work where its runtime allows blocking.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::future::{poll_fn, Future};
use std::pin::{pin, Pin};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;

use angzarr_bus::{Failed, Handler, Served};
use angzarr_diagnostic::Report;
use angzarr_ports::CommitSignal;
use angzarr_proto::v1::{Cover, EventBook};
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::router::{DynProjectorHandler, PolicyComponent};
use prost_types::Timestamp;

use super::error::{Failure, KernelError};
use super::handle::{Kernel, Wait};
use super::misconfigured::Misconfigured;
use super::projector::{gone_request, key, project_request};
use super::registry::PolicyEntry;
use super::route::Rejected;
use super::store::{
    CommitNotify, PositionKey, Processed, ReadStore, ReadUnit, Store, StoreError, StreamRef, Watch,
    Write,
};

/// The most events of one stream a subscription has in flight at once:
/// the handled set above a mark is kept as one 64-bit word.
pub const MAX_PER_STREAM: usize = angzarr_bus::MAX_PER_STREAM;

/// A stream as the bus tells streams apart ([`key`]).
type Key = (String, Vec<u8>, Option<String>);

/// A policy a bus serves, never the registry: written by hand or marked up
/// ([`BusPolicy::new`]), as angzarr-router's dispatch table
/// ([`BusPolicy::dispatch`]) or its `PolicyComponent`
/// ([`BusPolicy::component`]), as [`Registry::policy`](super::Registry::policy)
/// and its twins take one.
pub struct BusPolicy {
    entry: PolicyEntry,
}

impl BusPolicy {
    /// `policy`, as the router's table for it.
    pub fn new<P: super::Policy>(policy: P) -> BusPolicy {
        BusPolicy::dispatch(super::policy::dispatch(policy))
    }

    /// A policy built as angzarr-router's dispatch table: its source its
    /// input domain and its target its output domain.
    pub fn dispatch(dispatch: PolicyDispatch) -> BusPolicy {
        let output_domain = dispatch.output_domain().to_string();
        BusPolicy {
            entry: PolicyEntry {
                dispatch: Box::new(dispatch),
                output_domain,
            },
        }
    }

    /// A policy written as angzarr-router's `PolicyComponent`, its outputs
    /// held to `output_domain`.
    pub fn component(policy: impl PolicyComponent + 'static, output_domain: &str) -> BusPolicy {
        BusPolicy {
            entry: PolicyEntry {
                dispatch: Box::new(policy),
                output_domain: output_domain.to_string(),
            },
        }
    }

    /// Its name: its outputs' component, and its position's handler.
    pub fn name(&self) -> &str {
        self.entry.name()
    }

    /// The domain whose events it handles.
    pub fn source(&self) -> &str {
        self.entry.source()
    }
}

/// Why an item failed.
#[derive(Debug)]
enum Fault {
    /// A projector's upcast failed.
    Upcast(KernelError),
    /// A handler or a policy's step failed, as the kernel reports it.
    Reported(Report),
}

impl Fault {
    fn report(self) -> Report {
        match self {
            Fault::Upcast(error) => Report::of(&error),
            Fault::Reported(report) => report,
        }
    }
}

/// A served policy's step's failure, as a report.
fn step_report(failure: Failure<std::convert::Infallible>) -> Report {
    let failure: Failure<Rejected> = match failure {
        Failure::Rejected(never) => match never {},
        Failure::Downstream {
            path,
            error,
            dead_letters,
        } => Failure::Downstream {
            path,
            error,
            dead_letters,
        },
        Failure::Kernel(error) => Failure::Kernel(error),
        Failure::Projection {
            committed,
            failures,
            dead_letters,
        } => Failure::Projection {
            committed,
            failures,
            dead_letters,
        },
    };
    Report::of(&failure)
}

/// What a bus is served on: the store, the kernel its handlers' failures
/// and its policies' steps go through, the clock they are stamped by, the
/// timer, and the push. See [`Bus::new`].
pub struct Host<'h, S> {
    store: &'h S,
    kernel: &'h Kernel<'h>,
    clock: &'h Clock,
    wait: &'h dyn Wait,
    every: Duration,
    push: Option<&'h dyn CommitNotify>,
}

/// The instant a served policy's step is recorded at, and a served
/// handler's dead letters stamped with, as the application samples it.
pub type Clock = dyn Fn() -> Timestamp + Send + Sync;

impl<'h, S> Host<'h, S> {
    /// Served from `store`, through `kernel` (whose registry's catalog
    /// upcasts, and whose retry and bug settings hold), at `clock`'s instant,
    /// polling the store's change token every `every` on `wait` (the
    /// caller's timer).
    pub fn new(
        store: &'h S,
        kernel: &'h Kernel<'h>,
        clock: &'h Clock,
        wait: &'h dyn Wait,
        every: Duration,
    ) -> Host<'h, S> {
        Host {
            store,
            kernel,
            clock,
            wait,
            every,
            push: None,
        }
    }

    /// Woken by every commit `notify` pushes as well as by the poll, which
    /// still sees what the push does not (another process's commits).
    pub fn pushed_by(mut self, notify: &'h dyn CommitNotify) -> Host<'h, S> {
        self.push = Some(notify);
        self
    }
}

/// Subscriptions served from one store on a kernel, each by its own
/// instances: angzarr core's [`angzarr_bus::Bus`], serving projector
/// subscriptions (a `DynProjectorHandler`, boxed) and policy subscriptions
/// ([`BusPolicy`]). See the module.
pub struct Bus<'h, S> {
    pools: Vec<Pool>,
    host: Host<'h, S>,
}

impl<'h, S> Bus<'h, S> {
    /// A bus with no subscription, served on `host`.
    pub fn new(host: Host<'h, S>) -> Bus<'h, S> {
        Bus {
            pools: Vec::new(),
            host,
        }
    }
}

impl<S: ReadStore + Store + Processed + Watch> angzarr_bus::Bus for Bus<'_, S> {
    type Projector = Box<dyn DynProjectorHandler>;
    type Policy = BusPolicy;
    type Refused = Misconfigured;
    type Error = KernelError;

    /// Refused for a name another subscription on the bus has
    /// ([`Misconfigured::ComponentRegistered`]: the name keys its
    /// positions), and for a remote source ([`Misconfigured::RemoteSource`]:
    /// its events are its coordinator's); a policy also for a name the
    /// kernel's registry runs inline (never both).
    fn subscribe(
        &mut self,
        subscription: angzarr_bus::Subscription<Self::Projector, BusPolicy>,
    ) -> Result<(), Misconfigured> {
        let catalog = self.host.kernel.registry().catalog();
        let (handler, credit) = subscription.into_parts();
        let (name, sources, serves) = match handler {
            Handler::Projector { scope, projector } => {
                let name = projector.name().to_string();
                (name, scope.domains().to_vec(), Serves::Projector(projector))
            }
            Handler::Policy(policy) => {
                let name = policy.name().to_string();
                if catalog.policy_names().any(|inline| inline == name) {
                    return Err(Misconfigured::ComponentRegistered { component: name });
                }
                let source = policy.source().to_string();
                (name, vec![source], Serves::Policy(policy.entry))
            }
        };
        if let Some(remote) = sources.iter().find(|source| catalog.is_remote(source)) {
            return Err(Misconfigured::RemoteSource {
                domain: remote.clone(),
                component: name,
            });
        }
        if self.pools.iter().any(|pool| pool.name == name) {
            return Err(Misconfigured::ComponentRegistered { component: name });
        }
        self.pools.push(Pool {
            handled_above: angzarr_bus::handled_key(&name),
            name,
            sources,
            serves,
            instances: credit.instances().get(),
            per_stream: credit.in_flight_per_stream().get() as u64,
            state: Mutex::default(),
        });
        Ok(())
    }

    fn serve<M: FnMut(&Served) -> bool + angzarr_bus::MaybeSend>(
        &mut self,
        more: M,
    ) -> impl Future<Output = Result<(), KernelError>> + angzarr_bus::MaybeSend {
        serve(&self.pools, &self.host, more)
    }
}

/// Run every pool's instances and the steering in one future, until the
/// steering returns or an instance meets a store error.
async fn serve<S: ReadStore + Store + Processed + Watch>(
    pools: &[Pool],
    host: &Host<'_, S>,
    mut more: impl FnMut(&Served) -> bool,
) -> Result<(), KernelError> {
    for pool in pools {
        pool.restart();
    }
    let signals = Signals::default();
    let mut instances: Vec<_> = (pools.iter())
        .flat_map(|pool| (0..pool.instances).map(move |_| pool))
        .map(|pool| Box::pin(pool.instance(host, &signals)))
        .collect();
    let mut steering = pin!(steer(pools, host, &signals, &mut more));
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
    host: &Host<'_, S>,
    signals: &Signals,
    more: &mut impl FnMut(&Served) -> bool,
) -> Result<(), KernelError> {
    let (store, wait, every, push) = (host.store, host.wait, host.every, host.push);
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
        if pools.iter().all(|pool| pool.quiet()) {
            let mut served = Served::new();
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
            Woken::Commit => pools.iter().for_each(|pool| pool.changed()),
            Woken::Idle => {}
            Woken::Tick => {
                timer = wait.wait(every);
                let now = store.change_token().await.map_err(KernelError::Store)?;
                if token != Some(now) {
                    pools.iter().for_each(|pool| pool.changed());
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
    serves: Serves,
    instances: usize,
    per_stream: u64,
    state: Mutex<Lanes>,
}

/// What a pool's instances are handed to.
enum Serves {
    Projector(Box<dyn DynProjectorHandler>),
    Policy(PolicyEntry),
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
    failures: Vec<(Cover, Option<u64>, Fault)>,
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

    fn is_policy(&self) -> bool {
        matches!(self.serves, Serves::Policy(_))
    }

    /// A new serve: no instance holds anything, what failed is handed
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
        served.add_handled(std::mem::take(&mut state.handled));
        for (stream, sequence, error) in std::mem::take(&mut state.failures) {
            served.push(Failed {
                subscription: self.name.clone(),
                stream,
                sequence,
                cause: error.report(),
            });
        }
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
        host: &Host<'_, S>,
        signals: &Signals,
    ) -> Result<Infallible, KernelError> {
        loop {
            // Armed before looking: work owed after this resolves it.
            let work = signals.work.wait();
            match self.claim() {
                Some(claim) => {
                    self.hand(host, claim).await?;
                    signals.work.notify();
                }
                None => {
                    signals.idle.notify();
                    work.await;
                }
            }
        }
    }

    /// Hand `claim`'s item to the handler, read from the store now, and
    /// record what came of it.
    async fn hand<S: Store>(&self, host: &Host<'_, S>, claim: Claim) -> Result<(), KernelError> {
        let Claim { key, cover, owed } = claim;
        let sequence = match owed {
            Owed::Gone => return self.hand_gone(host, key, cover).await,
            Owed::Event(sequence) => sequence,
        };
        if let Serves::Policy(policy) = &self.serves {
            return self.hand_policy(host, policy, key, cover, sequence).await;
        }
        let store = host.store;
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
        let catalog = host.kernel.registry().catalog();
        let request = match project_request(catalog, &cover, slice.pages, next_sequence).await {
            Ok(request) => request,
            Err(error) => {
                self.failed(&key, Some(sequence), cover, Fault::Upcast(error));
                return Ok(());
            }
        };
        let book = request.events.unwrap_or_default();
        // A fork page is never handed, but its sequence counts.
        let fork = book.pages.is_empty();
        if !fork {
            let at = Some((&cover, sequence));
            if let Err((failure, lettered)) = self.project(host, &book, at).await {
                if !lettered {
                    self.failed(&key, Some(sequence), cover, Fault::Reported(failure));
                    return Ok(());
                }
                self.report(cover, Some(sequence), Fault::Reported(failure));
                return self
                    .handled(store, &key, sequence, false)
                    .await
                    .map_err(KernelError::Store);
            }
        }
        self.handled(store, &key, sequence, !fork)
            .await
            .map_err(KernelError::Store)
    }

    /// `book` through the kernel to this pool's projector, its failure
    /// retried and dead-lettered as the kernel's inline projectors' are.
    async fn project<S: Store>(
        &self,
        host: &Host<'_, S>,
        book: &EventBook,
        at: Option<(&Cover, u64)>,
    ) -> Result<(), (Report, bool)> {
        let Serves::Projector(projector) = &self.serves else {
            return Ok(());
        };
        let now = (host.clock)();
        let kernel = host.kernel;
        kernel
            .project_served(host.store, &self.name, &**projector, book, at, now)
            .await
    }

    /// Report `error` of `cover`'s `sequence`, not held.
    fn report(&self, cover: Cover, sequence: Option<u64>, error: Fault) {
        self.lock().failures.push((cover, sequence, error));
    }

    /// Policy `policy`'s step on `sequence` of `cover`'s stream, read from
    /// the store now, through the kernel, which writes the position: done
    /// when it is past the event (the step, or its bug dead-lettered),
    /// else held. A failure is reported either way.
    async fn hand_policy<S: Store>(
        &self,
        host: &Host<'_, S>,
        policy: &PolicyEntry,
        key: Key,
        cover: Cover,
        sequence: u64,
    ) -> Result<(), KernelError> {
        let (store, kernel) = (host.store, host.kernel);
        let stream = StreamRef::try_from(&cover).map_err(KernelError::Store)?;
        let slice = store
            .read(stream, sequence, Some(sequence + 1))
            .await
            .map_err(KernelError::Store)?;
        let Some(page) = slice.pages.first() else {
            self.missing(&key, sequence, slice.next_sequence);
            return Ok(());
        };
        let failure = match kernel
            .react_served(store, policy, &cover, page, (host.clock)())
            .await
        {
            Ok(_) => None,
            Err(failure) => Some(Fault::Reported(step_report(failure))),
        };
        let Some(failure) = failure else {
            self.stepped(&key, sequence, None);
            return Ok(());
        };
        let position = PositionKey {
            handler: &self.name,
            stream,
        };
        let at = store.position(position).await.map_err(KernelError::Store)?;
        match at > sequence {
            true => self.stepped(&key, sequence, Some((cover, failure))),
            false => self.failed(&key, Some(sequence), cover, failure),
        }
        Ok(())
    }

    /// A policy's step on `sequence` of `key`'s stream moved its position
    /// past it, as the kernel wrote it, `failed` along the way or not.
    fn stepped(&self, key: &Key, sequence: u64, failed: Option<(Cover, Fault)>) {
        let mut guard = self.lock();
        let state = &mut *guard;
        state.busy -= 1;
        state.handled += 1;
        if let Some((cover, error)) = failed {
            state.failures.push((cover, Some(sequence), error));
        }
        if let Some(lane) = state.lanes.get_mut(key) {
            let at = bit(lane, sequence);
            lane.in_flight &= !at;
            lane.handled |= at;
            lane.advance();
        }
    }

    /// Hand `cover`'s gone book (never to a policy, which is called for
    /// events alone); handled or dead-lettered, forget the stream, its
    /// positions put back to 0.
    async fn hand_gone<S: Store>(
        &self,
        host: &Host<'_, S>,
        key: Key,
        cover: Cover,
    ) -> Result<(), KernelError> {
        let gone = gone_request(&cover).events.unwrap_or_default();
        let projected = self.project(host, &gone, None).await;
        if let Err((failure, lettered)) = projected {
            if !lettered {
                self.failed(&key, None, cover, Fault::Reported(failure));
                return Ok(());
            }
            self.report(cover.clone(), None, Fault::Reported(failure));
        }
        let written = self.write(host.store, &cover, 0, 0).await;
        let mut state = self.lock();
        state.busy -= 1;
        written.map_err(KernelError::Store)?;
        state.lanes.remove(&key);
        state.handled += u64::from(!self.is_policy());
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
    fn failed(&self, key: &Key, sequence: Option<u64>, cover: Cover, error: Fault) {
        let mut state = self.lock();
        state.busy -= 1;
        state.failures.push((cover, sequence, error));
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
    /// above it, as one write: a policy's mark alone, as the kernel keeps
    /// it.
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
        let positions = match self.serves {
            Serves::Projector(_) => &positions[..],
            Serves::Policy(_) => &positions[..1],
        };
        store.write(Write::positions(positions)).await.map(drop)
    }
}

#[cfg(test)]
#[path = "bus.test.rs"]
mod tests;
