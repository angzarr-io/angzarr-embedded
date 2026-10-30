//! Same-stream concurrency is optimistic (red, design-by-test; the owner's
//! ruling of 2026-10-30): two commands to one stream at once both load and
//! both decide; the store takes the first write and refuses the second
//! (`StoreError::Conflict`). What the loser comes to depends on whether it
//! said what it expected:
//!
//! - **No expected sequence** (`CommandBook.handling.expected_sequence`
//!   unset): the kernel stamped the head it loaded, so the loss is the
//!   kernel's own race. It re-loads and decides again on the new head,
//!   under the retry policy, and succeeds.
//! - **An expected sequence**: the caller decided against a head that is
//!   gone. Its conflict is an immediate business failure: a business-fatal
//!   rejection under a coordinator reason key, never retried.
//!
//! Each test forces the race: both commands' handlers wait at a barrier
//! that only the two of them together can pass, so both have loaded the
//! same head before either writes. No sleep decides the outcome.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};

use angzarr_router::error::CodedError;
use angzarr_router::pb::CommandHandlerResponse;
use angzarr_router::router::CommandHandler;

use crate::next::toys::prelude::*;
use crate::next::toys::{at, command, command_id, cover};
use crate::next::v1::{
    CommandBook, ComponentDescriptor, ComponentKind, ConflictResolution, ContextualCommand,
    EventBook, EventPage, Handling, StateDescriptor, StateOptions,
};
use crate::next::{Any, Failure, Handled, Kernel, MemoryDeadLetters, Recovery, Registry, Rejected};
use angzarr_embedded_diagnostic::Diagnostic;

/// The reason key a lost race is refused under when the command set an
/// expected sequence. PROPOSED, for review: no key of angzarr-project's
/// catalog (`catalog/reasons.toml`) fits (CONFLICT_UNRESOLVED is
/// AGGREGATE_HANDLES's, CONFLICT_OVERLAP COMMUTATIVE's), so this needs a
/// new coordinator key there, status FAILED_PRECONDITION (business-fatal),
/// args `expected` and `actual`, and its constant in `next::reasons`.
const EXPECTED_SEQUENCE_STALE: &str = "EXPECTED_SEQUENCE_STALE";

/// How many of the first dispatches meet at the barrier: the two racing
/// commands' first decisions. Any later dispatch (a retry) passes.
const RACERS: usize = 2;

/// An aggregate, `till`, whose first [`RACERS`] dispatches wait for each
/// other at a barrier: both have loaded before either decides. It records
/// how many events each dispatch saw, and answers one `Rung` event.
struct Racing {
    barrier: Barrier,
    dispatched: AtomicUsize,
    /// The length of the stream each dispatch was handed, in the order
    /// they came out of the barrier (or, past it, entered).
    saw: Mutex<Vec<usize>>,
}

impl Racing {
    fn new() -> Racing {
        Racing {
            barrier: Barrier::new(RACERS),
            dispatched: AtomicUsize::new(0),
            saw: Mutex::new(Vec::new()),
        }
    }
}

impl CommandHandler for &'static Racing {
    fn name(&self) -> &str {
        "Till"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.till.v1.Ring".to_string()]
    }

    fn dispatch(&self, command: &ContextualCommand) -> Result<CommandHandlerResponse, CodedError> {
        if self.dispatched.fetch_add(1, Ordering::SeqCst) < RACERS {
            self.barrier.wait();
        }
        let seen = command.events.as_ref().map_or(0, |e| e.pages.len());
        self.saw.lock().unwrap().push(seen);
        Ok(CommandHandlerResponse {
            conflict: None,
            events: Some(EventBook {
                pages: vec![EventPage {
                    payload: Some(any("/toy.till.v1.Rung")),
                    ..EventPage::default()
                }],
                ..EventBook::default()
            }),
            rules_version: "till-1".to_string(),
        })
    }
}

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

fn till_descriptor() -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Till".to_string(),
        kind: ComponentKind::Aggregate as i32,
        rules_version: "till-1".to_string(),
        state: Some(StateDescriptor {
            type_url: "/toy.till.v1.State".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }),
        ..ComponentDescriptor::default()
    }
}

/// Command `n` to the till's stream 1, expecting `expected` if given.
fn ring(n: u128, expected: Option<u64>) -> CommandBook {
    let mut book = command(cover("till", 1), any("/toy.till.v1.Ring"));
    if let Some(page) = book.page.as_mut() {
        page.command_id = Some(command_id(n));
    }
    book.handling = expected.map(|expected| Handling {
        expected_sequence: Some(expected),
        conflict_resolution: ConflictResolution::Strict as i32,
    });
    book
}

/// `books` handled at once, each on a thread of its own with a runtime
/// of its own, through one kernel over one store: their results, in the
/// order given.
fn race(
    registry: &Registry,
    log: &MemoryDeadLetters,
    store: &MemoryStore,
    books: [CommandBook; 2],
) -> Vec<Result<Handled, Failure<Rejected>>> {
    let kernel = Kernel::new(registry, log);
    std::thread::scope(|scope| {
        let racers: Vec<_> = books
            .into_iter()
            .enumerate()
            .map(|(k, book)| {
                let kernel = &kernel;
                scope.spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap()
                        .block_on(kernel.handle_any(store, book, &at(k as i64 + 1), &mut ()))
                })
            })
            .collect();
        racers.into_iter().map(|r| r.join().unwrap()).collect()
    })
}

fn registry_of(racing: &'static Racing) -> Registry {
    let mut registry = Registry::new();
    registry
        .command_handler("till", racing, till_descriptor())
        .unwrap();
    registry
}

/// Neither command set an expected sequence: both decide on the empty
/// stream, one write wins, and the loser is re-loaded and decided again on
/// the new head, so both are applied. The stream holds both events,
/// gapless; the loser's handler ran twice, the second time on the
/// winner's event.
#[test]
fn a_lost_race_without_an_expected_sequence_is_retried_on_the_new_head() {
    let racing: &'static Racing = Box::leak(Box::new(Racing::new()));
    let registry = registry_of(racing);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let results = race(&registry, &log, &store, [ring(1, None), ring(2, None)]);

    for (k, result) in results.iter().enumerate() {
        assert!(
            matches!(result, Ok(Handled::Applied(_))),
            "command {}: {:?}",
            k + 1,
            result.as_ref().err()
        );
    }
    let sequences: Vec<_> = pages(&store, &cover("till", 1))
        .iter()
        .map(|p| p.sequence)
        .collect();
    assert_eq!(sequences, [0, 1]);
    assert_eq!(
        *racing.saw.lock().unwrap(),
        [0, 0, 1],
        "both raced on the empty stream; the loser decided again on the winner's event"
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// Both commands expected the empty stream (sequence 0): both pass the
/// check, both decide, one write wins. The loser decided against a head
/// that is gone, so it is refused at once: a business-fatal rejection
/// under its reason key, its expected and actual sequences as args, never
/// retried (its handler ran once) and never dead-lettered. The stream
/// holds the winner's event only.
#[test]
fn a_lost_race_with_an_expected_sequence_is_an_immediate_business_failure() {
    let racing: &'static Racing = Box::leak(Box::new(Racing::new()));
    let registry = registry_of(racing);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let results = race(
        &registry,
        &log,
        &store,
        [ring(1, Some(0)), ring(2, Some(0))],
    );

    let applied = results
        .iter()
        .filter(|r| matches!(r, Ok(Handled::Applied(_))))
        .count();
    let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(applied, 1, "one write wins: {errors:?}");
    let lost = results
        .iter()
        .find_map(|r| r.as_ref().err())
        .expect("one loses");
    let Failure::Rejected(rejected) = lost else {
        panic!("a business rejection, not {lost:?}");
    };
    assert_eq!(rejected.code(), EXPECTED_SEQUENCE_STALE);
    let args: Vec<_> = rejected
        .args()
        .into_iter()
        .map(|(name, arg)| (name, format!("{arg:?}")))
        .collect();
    assert!(
        args.iter()
            .any(|(n, v)| *n == "expected" && v.contains('0'))
            && args.iter().any(|(n, v)| *n == "actual" && v.contains('1')),
        "its expected and actual sequences: {args:?}"
    );
    assert_eq!(lost.recovery(), Recovery::Business);
    assert_eq!(
        racing.dispatched.load(Ordering::SeqCst),
        RACERS,
        "the loser is not retried"
    );
    assert_eq!(pages(&store, &cover("till", 1)).len(), 1);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}
