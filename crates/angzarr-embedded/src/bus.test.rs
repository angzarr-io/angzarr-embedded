//! The bus, as angzarr-embedded runs it beyond angzarr core's conformance
//! suite (`bus_contract.test.rs`): several streams at once, each event read
//! once per handling, a store without push polled, a retryable failure
//! held with retries off and dead-lettered at the kernel's cap with them
//! on, a purged stream handed as gone to a new bus too, and policies served
//! as the kernel's chain steps them: never both inline and on the bus, a
//! remote source refused, a policy moved from inline keeping its place, and
//! `Kernel::resume` leaving it alone.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use angzarr_bus::Bus as _;
use angzarr_ports::memory::MemoryRead;
use angzarr_router::error::{CodedError, RetryableStatus};
use angzarr_router::router::{DynProjectorHandler, ProjectorHandler};

use crate::angzarr_ports::Uuid;
use crate::angzarr_router::hpb::{ProjectRequest, ProjectResponse};
use crate::toys::prelude::*;
use crate::toys::{at, command, cover, no_retries, opened, registry, Account, AccountCmd, Edit};
use crate::v1::{Cover, EventPage, Snapshot};
use crate::{
    Bus, BusPolicy, ChangeToken, DeadLetterStore, Host, Kernel, Misconfigured, Payload as _,
    Processed, ReadStore, ReadUnit, Registry, Slice, StoreError, StreamRef, Wait, Watch, Written,
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
    /// Events whose dispatch fails retryably, once per entry.
    failing: Vec<(Cover, u64)>,
    /// How many times each dispatch yields before it returns.
    yields: usize,
    /// Its failures are bugs.
    buggy: bool,
}

/// A projector that logs what it is handed and handles it, as its log
/// says: some failing, each after some yields.
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

    fn fail(self, stream: Cover, sequence: u64) -> Worker {
        self.log().failing.push((stream, sequence));
        self
    }

    /// Failing with a bug, not retryably.
    fn buggy(self) -> Worker {
        self.log().buggy = true;
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
        let this = Some((cover.clone(), sequences.first().copied().unwrap_or(GONE)));
        let stream = format!("{cover:?}");
        let (fails, yields, buggy) = {
            let mut log = self.log();
            log.handed.push((cover.clone(), sequences.clone()));
            *log.running.entry(stream.clone()).or_default() += 1;
            let of_one = log.running[&stream];
            log.most_of_one = log.most_of_one.max(of_one);
            let all = log.running.values().sum();
            log.most = log.most.max(all);
            let failing = log.failing.iter().position(|f| Some(f) == this.as_ref());
            let fails = failing.map(|at| log.failing.remove(at)).is_some();
            (fails, log.yields, log.buggy)
        };
        for _ in 0..yields {
            tokio::task::yield_now().await;
        }
        let mut log = self.log();
        *log.running.get_mut(&stream).unwrap() -= 1;
        if fails && buggy {
            return Err(CodedError::bug("toy-worker-bug", []));
        }
        if fails {
            return Err(CodedError::retryable_bug(
                RetryableStatus::Unavailable,
                "toy-worker-failed",
                [],
            ));
        }
        log.done.push((cover, sequences));
        Ok(ProjectResponse::default())
    }
}

/// The sequence a worker's script names a gone book by.
const GONE: u64 = u64::MAX;

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

/// The instant the bus's steps and letters are stamped with.
fn clock() -> crate::Timestamp {
    crate::toys::instant(50)
}

/// The kernel the tests' buses run on: retries off, unless a test says.
fn kernel(registry: &Registry) -> Kernel<'_> {
    Kernel::new(registry).on_retryable(no_retries())
}

/// A bus over `store` on `kernel`, woken by its commits and by `wait`.
fn bus_on<'h>(
    store: &'h MemoryStore,
    kernel: &'h Kernel<'h>,
    wait: &'h dyn Wait,
) -> Bus<'h, MemoryStore> {
    Bus::new(Host::new(store, kernel, &clock, wait, EVERY).pushed_by(store))
}

/// `worker` on `bus`, over the account domain, as `instances` at once,
/// `per_stream` of a stream in flight.
fn work<S: ReadStore + crate::Store + Processed + Watch>(
    bus: &mut Bus<'_, S>,
    worker: &Worker,
    instances: usize,
    per_stream: usize,
) -> Result<(), Misconfigured> {
    let projector: Box<dyn DynProjectorHandler> = Box::new(worker.clone());
    let credit = angzarr_bus::Credit::new(n(instances)).per_stream(n(per_stream));
    let scope = angzarr_bus::Scope::domain("account");
    bus.subscribe(angzarr_bus::Subscription::projector(
        scope, credit, projector,
    ))
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

/// Serve until the bus is idle, then stop: what it served, each time asked.
async fn drain<S: ReadStore + crate::Store + Processed + Watch>(
    bus: &mut Bus<'_, S>,
) -> Vec<angzarr_bus::Served> {
    let mut asked = Vec::new();
    let more = |served: &angzarr_bus::Served| {
        asked.push(served.clone());
        false
    };
    soon(bus.serve(more)).await.unwrap();
    asked
}

/// How many items were handled over every ask.
fn handled(asked: &[angzarr_bus::Served]) -> u64 {
    asked.iter().map(angzarr_bus::Served::handled).sum()
}

/// Every failure over every ask: its subscription, stream and sequence.
fn failures(asked: &[angzarr_bus::Served]) -> Vec<(String, Cover, Option<u64>)> {
    let failures = asked.iter().flat_map(angzarr_bus::Served::failures);
    let at = |f: &angzarr_bus::Failed| (f.subscription.clone(), f.stream.clone(), f.sequence);
    failures.map(at).collect()
}

/// Several streams are in their handler at once, each one event at a time
/// and in order.
#[tokio::test]
async fn several_streams_are_served_at_once_each_in_order() {
    let registry = registry();
    let store = MemoryStore::new();
    for n in [1, 2] {
        opened(&store, &registry, n, 5);
        deposit(&store, &registry, n, 3, 4);
    }
    let worker = Worker::new("worker").yielding(3);
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    work(&mut bus, &worker, 3, 1).unwrap();
    drain(&mut bus).await;
    for n in [1, 2] {
        let done = worker.done_of(&cover("account", n));
        assert_eq!(done, [vec![0], vec![1], vec![2]], "in order");
    }
    assert_eq!(worker.log().most_of_one, 1, "one of a stream at once");
    assert_eq!(worker.log().most, 2, "both streams at once");
}

/// Each event is read from the store once per handling, and a bus with
/// nothing owed reads no event; a store that does not push is served on
/// its poll.
#[tokio::test]
async fn no_event_is_read_twice_per_handling() {
    let registry = registry();
    let store = Counted::default();
    opened(&store.inner, &registry, 1, 5);
    opened(&store.inner, &registry, 2, 7);
    let worker = Worker::new("worker");
    let kernel = kernel(&registry);
    let mut bus = Bus::new(Host::new(&store, &kernel, &clock, &Soon, EVERY));
    work(&mut bus, &worker, 2, 1).unwrap();
    drain(&mut bus).await;
    assert_eq!(worker.done().len(), 4);
    assert_eq!(
        store.pages.load(Ordering::SeqCst),
        4,
        "each event read once"
    );
    drain(&mut bus).await;
    assert_eq!(
        store.pages.load(Ordering::SeqCst),
        4,
        "nothing owed, nothing read"
    );
}

/// A store that does not push is polled: its change token, on the timer.
#[tokio::test]
async fn a_store_that_does_not_push_is_polled() {
    let registry = registry();
    let store = Counted::default();
    let worker = Worker::new("worker");
    let kernel = kernel(&registry);
    let mut bus = Bus::new(Host::new(&store, &kernel, &clock, &Soon, EVERY));
    work(&mut bus, &worker, 1, 1).unwrap();
    let more = |_: &angzarr_bus::Served| worker.done().len() < 2;
    let serve = bus.serve(more);
    let write = async {
        tokio::task::yield_now().await;
        opened(&store.inner, &registry, 1, 5);
    };
    let (served, ()) = soon(async { tokio::join!(serve, write) }).await;
    served.unwrap();
    assert_eq!(worker.done_of(&cover("account", 1)), [vec![0], vec![1]]);
}

/// With retries off, a retryable failure holds its stream: it is reported,
/// nothing after it on its stream is handed, the other streams go on, and
/// it is handed again at the next poll.
#[tokio::test]
async fn a_failed_event_holds_its_stream_and_is_handed_again() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let worker = Worker::new("worker").fail(cover("account", 1), 0);
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Soon);
    work(&mut bus, &worker, 2, 1).unwrap();
    let mut asked = Vec::new();
    let more = |served: &angzarr_bus::Served| {
        asked.push(served.clone());
        !served.failures().is_empty()
    };
    soon(bus.serve(more)).await.unwrap();
    let first = asked.first().unwrap();
    assert_eq!(first.handled(), 2, "the other stream handled");
    assert_eq!(
        failures(std::slice::from_ref(first)),
        [("worker".to_string(), cover("account", 1), Some(0))]
    );
    assert_eq!(handled(&asked), 4, "handed again, and handled");
    let of_one: Vec<Handed> = (worker.handed().into_iter())
        .filter(|(c, _)| c == &cover("account", 1))
        .collect();
    assert_eq!(
        of_one,
        [0, 0, 1].map(|s| (cover("account", 1), vec![s])),
        "nothing after it handed before it"
    );
    assert!(store
        .dead_letters(sref(&cover("account", 1)))
        .ready()
        .unwrap()
        .is_empty());
}

/// With retries on, a retryable failure is retried within the step, as the
/// kernel retries an inline projector, and at the cap dead-lettered, its
/// letter kept at the event, and the event passed.
#[tokio::test]
async fn a_retryable_failure_is_retried_then_dead_lettered_at_the_cap() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let at_cap = cover("account", 1);
    let worker = Worker::new("worker")
        .fail(at_cap.clone(), 0)
        .fail(at_cap.clone(), 0)
        .fail(at_cap.clone(), 0);
    let retry = crate::OnRetryable {
        max_retries: 2,
        ..crate::OnRetryable::default()
    };
    let kernel = Kernel::new(&registry).on_retryable(retry).wait_with(&Soon);
    let mut bus = bus_on(&store, &kernel, &Never);
    work(&mut bus, &worker, 1, 1).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(
        failures(&asked),
        [("worker".to_string(), at_cap.clone(), Some(0))]
    );
    let handed: Vec<Handed> = worker.handed();
    assert_eq!(handed, [0, 0, 0, 1].map(|s| (at_cap.clone(), vec![s])));
    assert_eq!(worker.done_of(&at_cap), [vec![1]]);
    assert_eq!(position(&store, "worker", &at_cap), 2, "passed");
    assert_eq!(store.dead_letters(sref(&at_cap)).ready().unwrap().len(), 1);
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
    let kernel = kernel(&registry);
    let mut first = bus_on(&store, &kernel, &Never);
    work(&mut first, &worker, 1, 1).unwrap();
    drain(&mut first).await;

    store.purge(sref(&cover("account", 1)), 2).ready().unwrap();
    drain(&mut first).await;
    assert_eq!(worker.handed()[4..], [(cover("account", 1), vec![])]);
    assert_eq!(position(&store, "worker", &cover("account", 1)), 0);
    drain(&mut first).await;
    assert_eq!(worker.handed().len(), 5, "forgotten");

    store.purge(sref(&cover("account", 2)), 2).ready().unwrap();
    let again = Worker::new("worker");
    let mut second = bus_on(&store, &kernel, &Never);
    work(&mut second, &again, 1, 1).unwrap();
    drain(&mut second).await;
    assert_eq!(again.handed(), [(cover("account", 2), vec![])]);
}

/// A gone book that fails retryably, with retries off, is held: the
/// stream is not forgotten until it is handed again and handled.
#[tokio::test]
async fn a_failed_gone_book_is_held_and_handed_again() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let gone = cover("account", 1);
    let worker = Worker::new("worker").fail(gone.clone(), GONE);
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Soon);
    work(&mut bus, &worker, 1, 1).unwrap();
    drain(&mut bus).await;
    store.purge(sref(&gone), 2).ready().unwrap();
    let mut asked = Vec::new();
    let more = |served: &angzarr_bus::Served| {
        asked.push(served.clone());
        handled(&asked) == 0
    };
    soon(bus.serve(more)).await.unwrap();
    assert_eq!(
        failures(&asked),
        [("worker".to_string(), gone.clone(), None)]
    );
    assert_eq!(handled(&asked), 1, "handed again, and handled");
    let books = worker.handed().into_iter().filter(|(_, s)| s.is_empty());
    assert_eq!(books.count(), 2, "the gone book twice");
    assert_eq!(position(&store, "worker", &gone), 0);
}

/// A gone book that fails with a bug is dead-lettered, and the stream
/// forgotten all the same.
#[tokio::test]
async fn a_gone_books_bug_is_dead_lettered_and_the_stream_forgotten() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let gone = cover("account", 1);
    let worker = Worker::new("worker").fail(gone.clone(), GONE).buggy();
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    work(&mut bus, &worker, 1, 1).unwrap();
    drain(&mut bus).await;
    store.purge(sref(&gone), 2).ready().unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(
        failures(&asked),
        [("worker".to_string(), gone.clone(), None)]
    );
    assert_eq!(position(&store, "worker", &gone), 0, "forgotten");
    drain(&mut bus).await;
    let books = worker.handed().into_iter().filter(|(_, s)| s.is_empty());
    assert_eq!(books.count(), 1, "not handed again");
}

/// A name is its own on a bus: a second subscription of it is refused.
#[test]
fn a_subscriptions_name_is_its_own() {
    let registry = registry();
    let store = MemoryStore::new();
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    let worker = Worker::new("worker");
    work(&mut bus, &worker, 1, 1).unwrap();
    assert_eq!(
        work(&mut bus, &worker, 1, 1),
        Err(Misconfigured::ComponentRegistered {
            component: "worker".to_string()
        })
    );
}

/// The ledger's records, as their source sequences.
fn ledger_records(store: &MemoryStore) -> Vec<u64> {
    let events = pages(store, &ledger());
    let recorded = events.iter().filter_map(|page| match &page.body {
        Some(crate::v1::event_page::Body::Event(any)) => {
            <crate::toys::ledger::Recorded as prost::Message>::decode(&any.value[..]).ok()
        }
        _ => None,
    });
    recorded.map(|record| record.source_sequence).collect()
}

fn ledger() -> Cover {
    Cover {
        domain: "ledger".to_string(),
        root: Some(crate::toys::ledger_root()),
        edition: None,
    }
}

fn bookkeeping() -> angzarr_bus::Subscription<Box<dyn DynProjectorHandler>, BusPolicy> {
    angzarr_bus::Subscription::policy(n(1), BusPolicy::new(crate::toys::Bookkeeping))
}

/// A policy is inline or on a bus, never both: a bus refuses one the
/// kernel's registry runs, one whose source is remote, and a second
/// subscription of one name; a projector of a remote source is refused
/// too.
#[test]
fn a_bus_refuses_a_policy_run_inline_or_of_a_remote_source() {
    let store = MemoryStore::new();
    let mut inline = registry();
    inline.policy(crate::toys::Bookkeeping).unwrap();
    let kernel = Kernel::new(&inline);
    let mut bus = bus_on(&store, &kernel, &Never);
    assert_eq!(
        bus.subscribe(bookkeeping()).err(),
        Some(Misconfigured::ComponentRegistered {
            component: "bookkeeping".to_string()
        })
    );

    let mut remote = registry();
    remote.remote("bank", Nowhere).unwrap();
    let kernel = Kernel::new(&remote);
    let mut bus = bus_on(&store, &kernel, &Never);
    let banking = BusPolicy::component(Banking, "ledger");
    assert_eq!(banking.source(), "bank");
    let refused = Some(Misconfigured::RemoteSource {
        domain: "bank".to_string(),
        component: "banking".to_string(),
    });
    let policy = angzarr_bus::Subscription::policy(n(1), banking);
    assert_eq!(bus.subscribe(policy).err(), refused);
    let projector: Box<dyn DynProjectorHandler> = Box::new(Worker::new("banking"));
    let credit = angzarr_bus::Credit::new(n(1));
    let scope = angzarr_bus::Scope::of("account", &["bank"]);
    let projector = angzarr_bus::Subscription::projector(scope, credit, projector);
    assert_eq!(bus.subscribe(projector).err(), refused);

    let plain = registry();
    let kernel = Kernel::new(&plain);
    let mut bus = bus_on(&store, &kernel, &Never);
    bus.subscribe(bookkeeping()).unwrap();
    assert_eq!(
        bus.subscribe(bookkeeping()).err(),
        Some(Misconfigured::ComponentRegistered {
            component: "bookkeeping".to_string()
        })
    );
}

/// A policy on the bus is a chain's step: its outputs land with the ids an
/// inline policy's would have, the inline projectors are run on what they
/// wrote, its position is an inline policy's, and `Kernel::resume`, which
/// runs the registry's policies, owes it nothing.
#[tokio::test]
async fn a_policy_on_the_bus_steps_as_an_inline_one_and_resume_leaves_it_alone() {
    let store = MemoryStore::new();
    let mut registry = registry();
    let books = Worker::new("books");
    registry
        .projector_handler(&["ledger"], books.clone())
        .unwrap();
    opened(&store, &crate::toys::registry(), 1, 5);
    let kernel = Kernel::new(&registry);
    let mut bus = Bus::new(Host::new(&store, &kernel, &clock, &Never, EVERY));
    bus.subscribe(bookkeeping()).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(handled(&asked), 2);
    assert_eq!(ledger_records(&store), [0, 1]);
    assert_eq!(
        books.done().len(),
        2,
        "the inline projector saw each record"
    );
    let account = cover("account", 1);
    for (page, sequence) in pages(&store, &ledger()).iter().zip(0..) {
        let trigger = crate::v1::EventRef {
            stream: Some(account.clone()),
            sequence,
        };
        let derived = crate::ids::policy_command_id(&trigger, "bookkeeping", 0).unwrap();
        let caused = page.meta.as_ref().and_then(|meta| meta.causation.clone());
        let id = match caused {
            Some(crate::v1::event_meta::Causation::Command(cause)) => cause.command_id,
            other => panic!("a command caused the record: {other:?}"),
        };
        assert_eq!(id, Some(derived));
    }
    assert_eq!(position(&store, "bookkeeping", &account), 2);

    let resumed = kernel.resume(&store, &at(60)).await.ok().unwrap();
    assert!(
        resumed.steps().is_empty(),
        "resume owes the bus's policy nothing"
    );
    assert_eq!(ledger_records(&store), [0, 1]);
}

/// A policy registered inline, then served by a bus, keeps its place: the
/// bus hands it only what the inline runs had not.
#[tokio::test]
async fn a_policy_moved_from_inline_to_the_bus_keeps_its_place() {
    let store = MemoryStore::new();
    let mut inline = registry();
    inline.policy(crate::toys::Bookkeeping).unwrap();
    opened(&store, &inline, 1, 5);
    assert_eq!(ledger_records(&store), [0, 1]);

    let plain = registry();
    deposit(&store, &plain, 1, 3, 4);
    let kernel = Kernel::new(&plain);
    let mut bus = bus_on(&store, &kernel, &Never);
    bus.subscribe(bookkeeping()).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(handled(&asked), 1);
    assert_eq!(ledger_records(&store), [0, 1, 2]);
    assert_eq!(position(&store, "bookkeeping", &cover("account", 1)), 3);
}

/// A purged stream a policy handled is forgotten without a call: nothing
/// is handled, and its position is put back to 0.
#[tokio::test]
async fn a_policy_forgets_a_purged_stream_uncalled() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 5);
    let kernel = Kernel::new(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    bus.subscribe(bookkeeping()).unwrap();
    assert_eq!(handled(&drain(&mut bus).await), 2);
    store.purge(sref(&cover("account", 1)), 2).ready().unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(handled(&asked), 0, "the policy is not called for it");
    assert!(failures(&asked).is_empty());
    assert_eq!(position(&store, "bookkeeping", &cover("account", 1)), 0);
    assert_eq!(ledger_records(&store), [0, 1]);
}

/// An edition's fork page is never a policy's trigger, but its sequence
/// counts: the policy's position moves past it.
#[tokio::test]
async fn a_policy_passes_an_editions_fork_page() {
    let store = MemoryStore::new();
    let registry = registry();
    let edition = crate::toys::cover_in("account", 1, "what-if");
    crate::toys::fork(&store, &edition);
    let kernel = Kernel::new(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    bus.subscribe(bookkeeping()).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(handled(&asked), 1);
    assert_eq!(position(&store, "bookkeeping", &edition), 1);
}

/// A policy whose step fails is reported, named by its subscription,
/// stream and sequence; under `OnPolicyBug::Stay` its position stays.
#[tokio::test]
async fn a_policys_failed_step_is_reported() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 13);
    let kernel = Kernel::new(&registry).on_policy_bug(crate::OnPolicyBug::Stay);
    let mut bus = bus_on(&store, &kernel, &Never);
    let unlucky = BusPolicy::new(crate::toys::Unlucky);
    bus.subscribe(angzarr_bus::Subscription::policy(n(1), unlucky))
        .unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(
        failures(&asked).first(),
        Some(&("unlucky".to_string(), cover("account", 1), Some(1)))
    );
    assert_eq!(position(&store, "unlucky", &cover("account", 1)), 1, "held");
}

/// A projector subscription's credit holds: two of a stream at once when
/// asked for.
#[tokio::test]
async fn several_events_of_a_stream_are_in_flight_when_asked_for() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 5);
    deposit(&store, &registry, 1, 3, 4);
    let kernel = kernel(&registry);
    let worker = Worker::new("worker").yielding(3);
    let mut bus = bus_on(&store, &kernel, &Never);
    work(&mut bus, &worker, 3, 3).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(worker.log().most_of_one, 3, "three of a stream at once");
    assert_eq!(handled(&asked), 3);
}

/// A bus's serve can be spawned on a multi-threaded runtime.
#[test]
fn a_serve_is_send() {
    fn send<T: Send>(_: T) {}
    let store = MemoryStore::new();
    let registry = registry();
    let kernel = Kernel::new(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    send(bus.serve(|_| false));
}

/// A coordinator that is never reached.
struct Nowhere;

impl crate::CoordinatorPort for Nowhere {
    fn handle(&self, _: crate::v1::CommandBook) -> crate::Coordinated<'_> {
        Box::pin(async { Err(CodedError::bug("toy-nowhere", [])) })
    }

    fn deliver(&self, _: crate::v1::ReactionDelivery) -> crate::Coordinated<'_> {
        Box::pin(async { Err(CodedError::bug("toy-nowhere", [])) })
    }
}

/// A policy of the remote bank's events.
struct Banking;

impl angzarr_router::router::PolicyComponent for Banking {
    fn name(&self) -> &str {
        "banking"
    }

    fn input_domain(&self) -> &str {
        "bank"
    }

    async fn react(
        &self,
        _: &crate::angzarr_router::hpb::ReactRequest,
    ) -> Result<crate::angzarr_router::hpb::ReactResponse, CodedError> {
        Ok(Default::default())
    }

    async fn translate(
        &self,
        _: &crate::angzarr_router::hpb::TranslateRequest,
    ) -> Result<crate::angzarr_router::hpb::TranslateResponse, CodedError> {
        Ok(Default::default())
    }
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
