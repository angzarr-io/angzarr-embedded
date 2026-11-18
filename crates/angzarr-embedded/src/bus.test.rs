//! The bus: every event of a subscription's sources handed to one of its
//! instances once it is owed, recorded once handled, and after a crash only
//! what was not handled handed again; one event of a stream in flight
//! unless more are asked for; several subscriptions on one bus; each event
//! read once per handling; woken by a commit, or by the poll without push;
//! a failure held and handed again; a purged stream handed as gone.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use angzarr_ports::memory::MemoryRead;
use angzarr_router::error::CodedError;
use angzarr_router::router::ProjectorHandler;

use crate::angzarr_ports::Uuid;
use crate::angzarr_router::hpb::{ProjectRequest, ProjectResponse};
use crate::toys::prelude::*;
use crate::toys::{at, command, cover, opened, registry, Account, AccountCmd, Edit};
use crate::v1::{Cover, EventPage, Snapshot};
use crate::{
    Bus, ChangeToken, KernelError, Misconfigured, Payload as _, Processed, ReadStore, ReadUnit,
    Registry, Served, Slice, StoreError, StreamRef, Subscription, Wait, Watch, Written,
};

/// One book as a worker was handed it: its stream and its pages'
/// sequences.
type Handed = (Cover, Vec<u64>);

/// What a [`Worker`] keeps.
#[derive(Default)]
struct Log {
    /// Every book handed, in the order handed.
    handed: Vec<Handed>,
    /// Every book handled, in the order its dispatch returned.
    done: Vec<Handed>,
    /// How many books of each stream are in its dispatch now.
    running: BTreeMap<String, usize>,
    /// The most books of one stream in its dispatch at once.
    most_of_one: usize,
    /// The most books in its dispatch at once.
    most: usize,
    /// Events whose dispatch never returns.
    stuck: Vec<(Cover, u64)>,
    /// Events whose dispatch fails, once each.
    failing: Vec<(Cover, u64)>,
    /// How many times each dispatch yields before it returns.
    yields: usize,
}

/// A projector that logs what it is handed and handles it, as its log
/// says: some never, some failing once, each after some yields.
#[derive(Clone)]
struct Worker {
    name: &'static str,
    log: Arc<Mutex<Log>>,
}

impl Worker {
    fn new(name: &'static str) -> Worker {
        Worker {
            name,
            log: Arc::default(),
        }
    }

    fn log(&self) -> std::sync::MutexGuard<'_, Log> {
        self.log.lock().unwrap()
    }

    fn stick(self, stream: Cover, sequence: u64) -> Worker {
        self.log().stuck.push((stream, sequence));
        self
    }

    fn fail(self, stream: Cover, sequence: u64) -> Worker {
        self.log().failing.push((stream, sequence));
        self
    }

    fn yielding(self, yields: usize) -> Worker {
        self.log().yields = yields;
        self
    }

    fn handed(&self) -> Vec<Handed> {
        self.log().handed.clone()
    }

    fn done(&self) -> Vec<Handed> {
        self.log().done.clone()
    }

    /// What it handled of `stream`, in the order handled.
    fn done_of(&self, stream: &Cover) -> Vec<Vec<u64>> {
        let done = self.done();
        let of = done.into_iter().filter(|(cover, _)| cover == stream);
        of.map(|(_, sequences)| sequences).collect()
    }
}

impl ProjectorHandler for Worker {
    fn name(&self) -> &str {
        self.name
    }

    async fn dispatch(&self, request: &ProjectRequest) -> Result<ProjectResponse, CodedError> {
        let book = request.events.clone().unwrap_or_default();
        let cover = book.cover.clone().unwrap_or_default();
        let sequences: Vec<u64> = book.pages.iter().map(|page| page.sequence).collect();
        let this = sequences.first().map(|&sequence| (cover.clone(), sequence));
        let stream = format!("{cover:?}");
        let (stuck, fails, yields) = {
            let mut log = self.log();
            log.handed.push((cover.clone(), sequences.clone()));
            *log.running.entry(stream.clone()).or_default() += 1;
            let of_one = log.running[&stream];
            log.most_of_one = log.most_of_one.max(of_one);
            let all = log.running.values().sum();
            log.most = log.most.max(all);
            let stuck = this.as_ref().is_some_and(|this| log.stuck.contains(this));
            let failing = log.failing.iter().position(|f| Some(f) == this.as_ref());
            let fails = failing.map(|at| log.failing.remove(at)).is_some();
            (stuck, fails, log.yields)
        };
        if stuck {
            std::future::pending::<()>().await;
        }
        for _ in 0..yields {
            tokio::task::yield_now().await;
        }
        let mut log = self.log();
        *log.running.get_mut(&stream).unwrap() -= 1;
        if fails {
            return Err(CodedError::bug("toy-worker-failed", []));
        }
        log.done.push((cover, sequences));
        Ok(ProjectResponse::default())
    }
}

/// A timer that never fires: only a commit wakes the bus.
struct Never;

impl Wait for Never {
    fn wait(&self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::pending())
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// A timer that fires as soon as the runtime has run everything else.
struct Soon;

impl Wait for Soon {
    fn wait(&self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::task::yield_now())
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

const EVERY: Duration = Duration::from_secs(3600);

fn n(instances: usize) -> NonZeroUsize {
    NonZeroUsize::new(instances).unwrap()
}

/// The bus with `worker` alone, as `instances`.
fn bus_of(worker: &Worker, instances: usize) -> Bus {
    let mut bus = Bus::new();
    let subscription = Subscription::new(&["account"], worker.clone(), n(instances)).unwrap();
    bus.subscribe(subscription).unwrap();
    bus
}

/// Command `id` depositing `cents` into account `n`.
fn deposit(store: &impl crate::Store, registry: &Registry, n: u128, id: u128, cents: i64) {
    crate::toys::handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Deposit(cents).to_any()).id(id),
        &at(20 + id as i64),
    )
    .unwrap();
}

/// `future`, failing the test if it takes longer than a test should.
async fn soon<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("the bus kept waiting")
}

/// Run `follow` until `until` holds, then drop it: a crash, whatever it
/// had in flight.
async fn crash_when(
    follow: impl Future<Output = Result<(), KernelError>>,
    until: impl Fn() -> bool,
) {
    let until = async {
        while !until() {
            tokio::task::yield_now().await;
        }
    };
    soon(async {
        tokio::select! {
            ended = follow => panic!("the bus stopped before its crash: {ended:?}"),
            () = until => {}
        }
    })
    .await;
}

/// Serve until the bus is idle, then stop: what it served.
async fn drain<S>(bus: &mut Bus, store: &S, registry: &Registry) -> Vec<(u64, Vec<(String, Cover)>)>
where
    S: ReadStore + crate::Store + Processed + Watch + crate::CommitNotify,
{
    let mut asked = Vec::new();
    let more = |served: &Served| {
        asked.push(summary(served));
        false
    };
    soon(bus.follow_commits(store, registry.catalog(), &Never, EVERY, more))
        .await
        .unwrap();
    asked
}

fn summary(served: &Served) -> (u64, Vec<(String, Cover)>) {
    let failures = served.failures().iter();
    let failures = failures.map(|(name, cover, _)| (name.clone(), cover.clone()));
    (served.handled(), failures.collect())
}

/// Two instances, one stream's first event never handled: the other
/// stream is handled meanwhile. After a crash, a new bus on the store
/// hands only the unhandled: the stuck event, and what came after it on
/// its stream.
#[tokio::test]
async fn out_of_order_completion_then_a_crash_re_serves_only_the_unhandled() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let first = Worker::new("worker").stick(cover("account", 1), 0);
    let mut bus = bus_of(&first, 2);
    let follow = bus.follow_commits(&store, registry.catalog(), &Never, EVERY, |_| true);
    crash_when(follow, || first.done().len() == 2).await;
    assert_eq!(first.done_of(&cover("account", 2)), [vec![0], vec![1]]);

    let second = Worker::new("worker");
    drain(&mut bus_of(&second, 2), &store, &registry).await;
    assert_eq!(
        second.handed(),
        [
            (cover("account", 1), vec![0]),
            (cover("account", 1), vec![1])
        ]
    );
}

/// Three events of one stream in flight at once, the first never handled:
/// the two above it are, and after a crash only the first is handed again.
#[tokio::test]
async fn handled_events_above_an_unhandled_one_are_not_re_served() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    deposit(&store, &registry, 1, 3, 4);
    let first = Worker::new("worker").stick(cover("account", 1), 0);
    let mut bus = Bus::new();
    let subscription = Subscription::new(&["account"], first.clone(), n(3)).unwrap();
    bus.subscribe(subscription.per_stream(n(3))).unwrap();
    let follow = bus.follow_commits(&store, registry.catalog(), &Never, EVERY, |_| true);
    crash_when(follow, || first.done().len() == 2).await;
    assert_eq!(first.done_of(&cover("account", 1)), [vec![1], vec![2]]);

    let second = Worker::new("worker");
    let mut bus = Bus::new();
    let subscription = Subscription::new(&["account"], second.clone(), n(3)).unwrap();
    bus.subscribe(subscription.per_stream(n(3))).unwrap();
    drain(&mut bus, &store, &registry).await;
    assert_eq!(second.handed(), [(cover("account", 1), vec![0])]);
}

/// By default a stream has one event in flight: each stream's events are
/// handled one at a time, in order, while the instances run several
/// streams at once.
#[tokio::test]
async fn a_stream_has_one_event_in_flight_by_default() {
    let registry = registry();
    let store = MemoryStore::new();
    for n in [1, 2] {
        opened(&store, &registry, n, 5);
        deposit(&store, &registry, n, 3, 4);
    }
    let worker = Worker::new("worker").yielding(3);
    drain(&mut bus_of(&worker, 3), &store, &registry).await;
    for n in [1, 2] {
        let done = worker.done_of(&cover("account", n));
        assert_eq!(done, [vec![0], vec![1], vec![2]], "in order");
    }
    assert_eq!(worker.log().most_of_one, 1, "one of a stream at once");
    assert_eq!(worker.log().most, 2, "both streams at once");
}

/// Asked for, several events of one stream are in flight at once.
#[tokio::test]
async fn several_events_of_a_stream_are_in_flight_when_asked_for() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    deposit(&store, &registry, 1, 3, 4);
    let worker = Worker::new("worker").yielding(3);
    let mut bus = Bus::new();
    let subscription = Subscription::new(&["account"], worker.clone(), n(3)).unwrap();
    bus.subscribe(subscription.per_stream(n(3))).unwrap();
    drain(&mut bus, &store, &registry).await;
    assert_eq!(worker.log().most_of_one, 3);
    let mut done = worker.done_of(&cover("account", 1));
    done.sort();
    assert_eq!(done, [vec![0], vec![1], vec![2]]);
}

/// Two subscriptions on one bus, each with its own instances: each is
/// handed every event once, and keeps its own positions.
#[tokio::test]
async fn several_subscriptions_share_one_bus() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let (a, b) = (Worker::new("a"), Worker::new("b"));
    let mut bus = Bus::new();
    for (worker, instances) in [(&a, 2), (&b, 1)] {
        let subscription = Subscription::new(&["account"], worker.clone(), n(instances));
        bus.subscribe(subscription.unwrap()).unwrap();
    }
    drain(&mut bus, &store, &registry).await;
    for worker in [&a, &b] {
        let mut done = worker.done();
        done.sort_by_key(|(cover, sequences)| (format!("{cover:?}"), sequences.clone()));
        let all = [(1, 0), (1, 1), (2, 0), (2, 1)].map(|(n, s)| (cover("account", n), vec![s]));
        assert_eq!(done, all, "{}", worker.name);
        for n in [1, 2] {
            assert_eq!(position(&store, worker.name, &cover("account", n)), 2);
        }
    }
}

/// Each event is read from the store once per handling, and a bus with
/// nothing owed reads no event.
#[tokio::test]
async fn no_event_is_read_twice_per_handling() {
    let registry = registry();
    let store = Counted::default();
    opened(&store.inner, &registry, 1, 5);
    opened(&store.inner, &registry, 2, 7);
    let worker = Worker::new("worker");
    let mut bus = bus_of(&worker, 2);
    let follow = bus.follow(&store, registry.catalog(), &Soon, EVERY, |_| false);
    soon(follow).await.unwrap();
    assert_eq!(worker.done().len(), 4);
    assert_eq!(
        store.pages.load(Ordering::SeqCst),
        4,
        "each event read once"
    );

    let follow = bus.follow(&store, registry.catalog(), &Soon, EVERY, |_| false);
    soon(follow).await.unwrap();
    assert_eq!(
        store.pages.load(Ordering::SeqCst),
        4,
        "nothing owed, nothing read"
    );
}

/// Idle, the bus is woken by a commit, with a timer that never fires.
#[tokio::test]
async fn a_commit_wakes_the_bus_with_no_poll() {
    let registry = registry();
    let store = MemoryStore::new();
    let worker = Worker::new("worker");
    let mut bus = bus_of(&worker, 1);
    let more = |_: &Served| worker.done().len() < 2;
    let follow = bus.follow_commits(&store, registry.catalog(), &Never, EVERY, more);
    let write = async {
        tokio::task::yield_now().await;
        opened(&store, &registry, 1, 5);
    };
    let (followed, ()) = soon(async { tokio::join!(follow, write) }).await;
    followed.unwrap();
    assert_eq!(worker.done_of(&cover("account", 1)), [vec![0], vec![1]]);
}

/// A store that does not push is polled: its change token, on the timer.
#[tokio::test]
async fn a_store_that_does_not_push_is_polled() {
    let registry = registry();
    let store = Counted::default();
    let worker = Worker::new("worker");
    let mut bus = bus_of(&worker, 1);
    let more = |_: &Served| worker.done().len() < 2;
    let follow = bus.follow(&store, registry.catalog(), &Soon, EVERY, more);
    let write = async {
        tokio::task::yield_now().await;
        opened(&store.inner, &registry, 1, 5);
    };
    let (followed, ()) = soon(async { tokio::join!(follow, write) }).await;
    followed.unwrap();
    assert_eq!(worker.done_of(&cover("account", 1)), [vec![0], vec![1]]);
}

/// A failing event holds its stream: it is reported, nothing after it on
/// its stream is handed, the other streams go on, and it is handed again
/// at the next poll.
#[tokio::test]
async fn a_failed_event_holds_its_stream_and_is_handed_again() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let worker = Worker::new("worker").fail(cover("account", 1), 0);
    let mut bus = bus_of(&worker, 2);
    let mut asked = Vec::new();
    let more = |served: &Served| {
        asked.push(summary(served));
        !served.failures().is_empty()
    };
    let follow = bus.follow_commits(&store, registry.catalog(), &Soon, EVERY, more);
    soon(follow).await.unwrap();
    let failed = (2, vec![("worker".to_string(), cover("account", 1))]);
    assert_eq!(
        asked.first(),
        Some(&failed),
        "reported, the other stream handled"
    );
    assert_eq!(
        asked.last(),
        Some(&(2, vec![])),
        "handed again, and handled"
    );
    let of_one: Vec<Handed> = worker
        .handed()
        .into_iter()
        .filter(|(c, _)| c == &cover("account", 1))
        .collect();
    assert_eq!(
        of_one,
        [0, 0, 1].map(|s| (cover("account", 1), vec![s])),
        "nothing after it handed before it"
    );
}

/// A stream handled and purged since is handed as gone (a book with no
/// page at 0) and forgotten, by the bus that handled it and by a new one
/// on the store.
#[tokio::test]
async fn a_purged_stream_is_handed_as_gone_once() {
    let registry = registry();
    let store = MemoryStore::new();
    for n in [1, 2] {
        opened(&store, &registry, n, 5);
    }
    let worker = Worker::new("worker");
    let mut bus = bus_of(&worker, 1);
    drain(&mut bus, &store, &registry).await;

    store.purge(sref(&cover("account", 1)), 2).ready().unwrap();
    drain(&mut bus, &store, &registry).await;
    assert_eq!(worker.handed()[4..], [(cover("account", 1), vec![])]);
    assert_eq!(position(&store, "worker", &cover("account", 1)), 0);
    drain(&mut bus, &store, &registry).await;
    assert_eq!(worker.handed().len(), 5, "forgotten");

    store.purge(sref(&cover("account", 2)), 2).ready().unwrap();
    let again = Worker::new("worker");
    drain(&mut bus_of(&again, 1), &store, &registry).await;
    assert_eq!(again.handed(), [(cover("account", 2), vec![])]);
}

/// A subscription reads some domain, and its name is its own on a bus.
#[test]
fn a_subscription_names_a_source_and_a_name_of_its_own() {
    let worker = Worker::new("worker");
    let refused = Subscription::new(&[], worker.clone(), n(1)).err();
    assert!(matches!(refused, Some(Misconfigured::NoInputDomain { .. })));

    let mut bus = Bus::new();
    let subscribe = |bus: &mut Bus| {
        bus.subscribe(Subscription::new(&["account"], worker.clone(), n(1)).unwrap())
    };
    subscribe(&mut bus).unwrap();
    assert_eq!(
        subscribe(&mut bus),
        Err(Misconfigured::ComponentRegistered {
            component: "worker".to_string()
        })
    );
}

/// A follow can be spawned on a multi-threaded runtime.
#[test]
fn a_follow_is_send() {
    fn send<T: Send>(_: T) {}
    let store = MemoryStore::new();
    let registry = registry();
    let mut bus = Bus::new();
    send(bus.follow_commits(&store, registry.catalog(), &Never, EVERY, |_| false));
}

// ------------------------------------------------------- a store that counts

/// A store that does not push, counting the pages its reads hand back.
#[derive(Clone, Default)]
struct Counted {
    inner: MemoryStore,
    pages: Arc<AtomicUsize>,
}

impl Counted {
    fn counted(&self, slice: Slice) -> Slice {
        self.pages.fetch_add(slice.pages.len(), Ordering::SeqCst);
        slice
    }
}

impl EventStore for Counted {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        self.inner.write(write).await
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        Ok(self.counted(self.inner.read(stream, from, to).await?))
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let pages = self.inner.find_command(stream, command_id).await?;
        self.pages.fetch_add(pages.len(), Ordering::SeqCst);
        Ok(pages)
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(domain).await
    }
}

impl SnapshotStore for Counted {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        self.inner.read_snapshot(stream).await
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.inner.write_snapshot(stream, snapshot).await
    }
}

impl PositionStore for Counted {
    async fn position(&self, key: crate::PositionKey<'_>) -> Result<u64, StoreError> {
        self.inner.position(key).await
    }
}

impl Processed for Counted {
    async fn processed(&self, handler: &str) -> Result<Vec<(Cover, u64)>, StoreError> {
        self.inner.processed(handler).await
    }
}

impl Watch for Counted {
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        self.inner.change_token().await
    }
}

impl ReadStore for Counted {
    type Reads<'s> = CountedReads;

    async fn begin_read(&self) -> Result<CountedReads, StoreError> {
        Ok(CountedReads {
            inner: self.inner.begin_read().await?,
            pages: Arc::clone(&self.pages),
        })
    }
}

/// [`Counted`]'s read unit, counting as it does.
struct CountedReads {
    inner: MemoryRead,
    pages: Arc<AtomicUsize>,
}

impl ReadUnit for CountedReads {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(domain).await
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        let slice = self.inner.read(stream, from, to).await?;
        self.pages.fetch_add(slice.pages.len(), Ordering::SeqCst);
        Ok(slice)
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        self.inner.read_snapshot(stream).await
    }
}
