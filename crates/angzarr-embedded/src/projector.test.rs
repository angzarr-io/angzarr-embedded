//! The projector port's catch-up: each stream of the projector's sources
//! handed what it is owed, once, from where the projector is on it; held
//! or stored positions; purges, editions, failures, the change token and
//! the loop.

use std::sync::{Arc, Mutex};

use angzarr_router::error::CodedError;
use angzarr_router::router::ProjectorHandler;

use crate::toys::prelude::*;
use crate::toys::{at, command, cover, cover_in, opened, registry, Account, AccountCmd, Edit};
use crate::v1::{Cover, EventBook, EventPage, Projection};
use crate::{CatchUp, KernelError, Payload as _, Registry};

/// One book as the projector was handed it: its stream, its pages'
/// sequences, and its `next_sequence`.
type Handed = (Cover, Vec<u64>, u64);

/// What a [`Recorder`] runs on its first book.
type Meanwhile = Box<dyn FnOnce() + Send>;

/// A projector that records every book it is handed, and fails the books
/// of `failing` while it is set.
#[derive(Clone, Default)]
struct Recorder {
    books: Arc<Mutex<Vec<Handed>>>,
    failing: Arc<Mutex<Option<Cover>>>,
    /// Run on the first book handed, once: a write landing mid-step.
    meanwhile: Arc<Mutex<Option<Meanwhile>>>,
}

impl Recorder {
    /// Every book handed since the last call.
    fn take(&self) -> Vec<Handed> {
        std::mem::take(&mut *self.books.lock().unwrap())
    }

    fn fail(&self, stream: Option<Cover>) {
        *self.failing.lock().unwrap() = stream;
    }
}

impl ProjectorHandler for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }

    async fn dispatch(&self, events: &EventBook) -> Result<Projection, CodedError> {
        if let Some(write) = self.meanwhile.lock().unwrap().take() {
            write();
        }
        let cover = events.cover.clone().unwrap_or_default();
        if self.failing.lock().unwrap().as_ref() == Some(&cover) {
            return Err(CodedError::bug("toy-recorder-failed", []));
        }
        let sequences = events.pages.iter().map(|p| p.sequence).collect();
        self.books
            .lock()
            .unwrap()
            .push((cover, sequences, events.next_sequence));
        Ok(Projection::default())
    }
}

/// Command `id` depositing `cents` into account `n`.
fn deposit(store: &MemoryStore, registry: &Registry, n: u128, id: u128, cents: i64) {
    crate::toys::handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Deposit(cents).to_any()).id(id),
        &at(20 + id as i64),
    )
    .unwrap();
}

/// Purge account `n` at its head.
fn purge(store: &MemoryStore, n: u128) {
    let stream = cover("account", n);
    let head = slice(store, &stream).next_sequence;
    store.purge(sref(&stream), head).ready().unwrap();
}

fn step(catch_up: &mut CatchUp<Recorder>, store: &MemoryStore, registry: &Registry) {
    let caught = catch_up.step(store, registry).ready().unwrap();
    assert!(caught.failures().is_empty(), "{:?}", caught.failures());
}

/// The first step hands each stream its whole log; a later one only what
/// was written since, and nothing for a stream with nothing new.
#[test]
fn each_stream_is_handed_what_it_is_owed_once() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());

    step(&mut catch_up, &store, &registry);
    assert_eq!(
        recorder.take(),
        [
            (cover("account", 1), vec![0, 1], 2),
            (cover("account", 2), vec![0, 1], 2)
        ]
    );

    deposit(&store, &registry, 2, 3, 4);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 2), vec![2], 3)]);

    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [], "nothing new, nothing handed");
}

/// Only the sources' streams are handed: a ledger stream is not an
/// account's.
#[test]
fn only_the_sources_streams_are_handed() {
    let mut registry = registry();
    registry.policy(crate::toys::Bookkeeping);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let ledger = Cover {
        domain: "ledger".to_string(),
        root: Some(crate::toys::ledger_root()),
        edition: None,
    };
    assert!(
        !slice(&store, &ledger).pages.is_empty(),
        "the policy wrote the ledger"
    );
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());
    step(&mut catch_up, &store, &registry);
    let handed: Vec<Cover> = recorder.take().into_iter().map(|b| b.0).collect();
    assert_eq!(handed, [cover("account", 1)]);
}

/// A stream purged and written again to the same length between two steps
/// is a new stream: told by its first event, it is handed from its start.
#[test]
fn a_stream_purged_and_written_again_is_handed_from_its_start() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());
    step(&mut catch_up, &store, &registry);
    recorder.take();

    purge(&store, 1);
    // Written again by other commands, at other instants: the same length.
    crate::toys::handle::<Account>(
        &store,
        &registry,
        command(cover("account", 1), AccountCmd::Open.to_any()).id(8),
        &at(40),
    )
    .unwrap();
    deposit(&store, &registry, 1, 9, 6);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
}

/// A stream handed before and purged since is handed as gone: a book with
/// no page at 0. Once: it is forgotten after.
#[test]
fn a_purged_stream_is_handed_as_gone_once() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());
    step(&mut catch_up, &store, &registry);
    recorder.take();

    purge(&store, 1);
    let caught = catch_up.step(&store, &registry).ready().unwrap();
    assert_eq!(caught.handed(), [cover("account", 1)]);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![], 0)]);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [], "forgotten");
}

/// An edition is a what-if fork, not the timeline: its stream is not
/// handed, main's is.
#[test]
fn an_edition_stream_is_not_handed() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let main = cover("account", 1);
    let fork = cover_in("account", 1, "what-if");
    let head = slice(&store, &main).next_sequence;
    let created = EventPage {
        sequence: head,
        payload: Some(crate::Any {
            type_url: crate::angzarr_ports::EDITION_CREATED_TYPE_URL.to_string(),
            value: prost::Message::encode_to_vec(&crate::v1::meta::EditionCreated {
                edition: "what-if".to_string(),
                parent_sequence: head,
                ..crate::v1::meta::EditionCreated::default()
            }),
        }),
        ..pages(&store, &main).pop().unwrap()
    };
    store
        .write(Write::append(sref(&fork), head, &[created]))
        .ready()
        .unwrap();
    assert!(store.covers("account").ready().unwrap().contains(&fork));

    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());
    step(&mut catch_up, &store, &registry);
    let handed: Vec<Cover> = recorder.take().into_iter().map(|b| b.0).collect();
    assert_eq!(handed, [main]);
}

/// A book the projector fails is reported, and keeps its stream where it
/// was: the next step hands it again. The other streams go on.
#[test]
fn a_failed_book_is_handed_again_at_the_next_step() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let recorder = Recorder::default();
    recorder.fail(Some(cover("account", 1)));
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());

    let caught = catch_up.step(&store, &registry).ready().unwrap();
    assert_eq!(caught.failures().len(), 1);
    let (stream, error) = &caught.failures()[0];
    assert_eq!(stream, &cover("account", 1));
    assert!(
        matches!(error, KernelError::Router(e) if e.extras.get("cause").map(String::as_str) == Some("toy-recorder-failed")),
        "{error:?}"
    );
    assert_eq!(recorder.take(), [(cover("account", 2), vec![0, 1], 2)]);

    recorder.fail(None);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
}

/// One moment: a write landing while a step runs (forced here, from the
/// projector's first book) is not handed by that step, whose reads all see
/// the store as it began, but by the next.
#[test]
fn a_write_landing_during_a_step_is_the_next_steps() {
    let registry = Arc::new(registry());
    let store = Arc::new(MemoryStore::new());
    opened(&*store, &registry, 1, 5);
    opened(&*store, &registry, 2, 7);
    let recorder = Recorder::default();
    let (writer, writes) = (Arc::clone(&store), Arc::clone(&registry));
    *recorder.meanwhile.lock().unwrap() = Some(Box::new(move || {
        deposit(&writer, &writes, 2, 3, 4);
    }));
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());

    step(&mut catch_up, &store, &registry);
    assert_eq!(
        recorder.take(),
        [
            (cover("account", 1), vec![0, 1], 2),
            (cover("account", 2), vec![0, 1], 2)
        ],
        "account 2 as the step began"
    );
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 2), vec![2], 3)]);
}

/// Stored positions are the store's: a new catch-up of the same projector
/// (a new process) resumes where the last left off, handing only what was
/// written since; they are kept under the projector's name.
#[test]
fn stored_positions_resume_in_a_new_catch_up() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    let mut first = CatchUp::stored(&["account"], recorder.clone());
    step(&mut first, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
    assert_eq!(position(&store, "recorder", &cover("account", 1)), 2);

    deposit(&store, &registry, 1, 3, 4);
    let mut again = CatchUp::stored(&["account"], recorder.clone());
    step(&mut again, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![2], 3)]);
    assert_eq!(position(&store, "recorder", &cover("account", 1)), 3);
}

/// A purge removes the stored positions on its stream, so a stored
/// catch-up hands a stream written again from its start.
#[test]
fn a_stored_catch_up_hands_a_stream_written_again_from_its_start() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    step(
        &mut CatchUp::stored(&["account"], recorder.clone()),
        &store,
        &registry,
    );
    recorder.take();
    purge(&store, 1);
    crate::toys::handle::<Account>(
        &store,
        &registry,
        command(cover("account", 1), AccountCmd::Open.to_any()).id(8),
        &at(40),
    )
    .unwrap();
    step(
        &mut CatchUp::stored(&["account"], recorder.clone()),
        &store,
        &registry,
    );
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0], 1)]);
}

/// A failed book's stored position stays: the next step hands it again.
#[test]
fn a_failed_books_stored_position_stays() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    recorder.fail(Some(cover("account", 1)));
    let mut catch_up = CatchUp::stored(&["account"], recorder.clone());
    let caught = catch_up.step(&store, &registry).ready().unwrap();
    assert_eq!(caught.failures().len(), 1);
    assert_eq!(position(&store, "recorder", &cover("account", 1)), 0);
    recorder.fail(None);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
}

/// With the change token, a step with nothing committed since the last
/// reads nothing and hands nothing (`None`); one after a commit runs.
#[test]
fn a_step_if_changed_skips_while_nothing_was_committed() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());
    let ran = catch_up.step_if_changed(&store, &registry).ready().unwrap();
    assert_eq!(
        ran.map(|c| c.handed().to_vec()),
        Some(vec![cover("account", 1)])
    );
    recorder.take();

    let skipped = catch_up.step_if_changed(&store, &registry).ready().unwrap();
    assert!(skipped.is_none(), "unchanged: not run");

    deposit(&store, &registry, 1, 3, 4);
    let ran = catch_up.step_if_changed(&store, &registry).ready().unwrap();
    assert!(ran.is_some(), "changed: run");
    assert_eq!(recorder.take(), [(cover("account", 1), vec![2], 3)]);
}

/// A step that failed a book does not keep the token: the next step runs
/// again, though nothing was committed.
#[test]
fn a_step_with_a_failure_runs_again_unchanged() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    recorder.fail(Some(cover("account", 1)));
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());
    let failed = catch_up.step_if_changed(&store, &registry).ready().unwrap();
    assert_eq!(failed.map(|c| c.failures().len()), Some(1));
    recorder.fail(None);
    let ran = catch_up.step_if_changed(&store, &registry).ready().unwrap();
    assert!(ran.is_some(), "run again");
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
}

/// The caller's timer: waits not at all, and counts the waits it is asked
/// for.
#[derive(Default)]
struct Counting(Mutex<Vec<std::time::Duration>>);

impl crate::Wait for Counting {
    fn wait(
        &self,
        backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0.lock().unwrap().push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// The loop: a step whenever something was committed, a wait of `every`
/// on the caller's timer after each poll, until the caller says stop.
#[test]
fn follow_polls_waits_and_stops_when_told() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone());
    let wait = Counting::default();
    let every = std::time::Duration::from_millis(250);
    let mut polls = Vec::new();
    catch_up
        .follow(&store, &registry, &wait, every, |caught| {
            polls.push(caught.map(|c| c.handed().len()));
            if polls.len() == 2 {
                deposit(&store, &registry, 1, 3, 4);
            }
            polls.len() < 4
        })
        .ready()
        .unwrap();
    assert_eq!(polls, [Some(1), None, Some(1), None]);
    assert_eq!(*wait.0.lock().unwrap(), [every, every, every]);
    assert_eq!(
        recorder.take(),
        [
            (cover("account", 1), vec![0, 1], 2),
            (cover("account", 1), vec![2], 3)
        ]
    );
}

#[test]
#[should_panic(expected = "recorder")]
fn a_catch_up_with_no_source_panics() {
    let _ = CatchUp::new(&[], Recorder::default());
}
