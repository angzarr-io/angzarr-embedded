//! The coordinator's conflict-resolution contract at load and at the
//! write (angzarr-project `features/coordinator-contract/conflict_resolution.feature`;
//! decisions 14.45, 14.46, 14.47), on an `order` stream holding events 0
//! to 2 (its head at 3), in every resolution its state supports (STRICT,
//! AGGREGATE_HANDLES, COMMUTATIVE).
//!
//! - **Ahead** (C-0727): an expected sequence past the head is malformed
//!   under every resolution, refused INVALID_ARGUMENT before business code.

use std::collections::BTreeMap;
use std::sync::Mutex;

use angzarr_router::error::{CodedError, GrpcCode};
use angzarr_router::pb::CommandHandlerResponse;
use angzarr_router::router::CommandHandler;

use crate::next::reasons::CoordinatorReason;
use crate::next::toys::prelude::*;
use crate::next::toys::{at, command, cover, Edit};
use crate::next::v1::{
    ComponentDescriptor, ComponentKind, ConflictResolution, ContextualCommand, Cover, EventBook,
    EventPage, StateDescriptor, StateOptions,
};
use crate::next::{
    class, Any, Class, Failure, Kernel, KernelError, MemoryDeadLetters, Recovery, Registry,
    Rejected,
};

const RESOLUTIONS: [ConflictResolution; 3] = [
    ConflictResolution::Strict,
    ConflictResolution::AggregateHandles,
    ConflictResolution::Commutative,
];

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

/// The order stream every test runs on.
fn order() -> Cover {
    cover("order", 1)
}

/// What business code was handed on one delivery: the expected sequence,
/// the head (the window runs from the one to the other) and the nonce.
#[derive(Clone, Debug, PartialEq)]
struct Seen {
    expected: u64,
    head: u64,
    nonce: Vec<u8>,
}

/// The `order` aggregate, hand-written as the router's `CommandHandler`:
/// every delivery it is handed, in order, answered with one `ItemAdded`.
struct Orders {
    seen: Mutex<Vec<Seen>>,
}

impl Orders {
    fn leaked() -> &'static Orders {
        Box::leak(Box::new(Orders {
            seen: Mutex::new(Vec::new()),
        }))
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

impl CommandHandler for &'static Orders {
    fn name(&self) -> &str {
        "Order"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.order.v1.AddItem".to_string()]
    }

    async fn dispatch(
        &self,
        command: &ContextualCommand,
    ) -> Result<CommandHandlerResponse, CodedError> {
        let head = command.events.as_ref().map_or(0, |e| e.next_sequence);
        let expected = command
            .command
            .as_ref()
            .and_then(|book| book.handling.as_ref())
            .and_then(|handling| handling.expected_sequence)
            .unwrap_or(head);
        self.seen.lock().unwrap().push(Seen {
            expected,
            head,
            nonce: command.conflict_nonce.to_vec(),
        });
        Ok(CommandHandlerResponse {
            conflict: None,
            events: Some(EventBook {
                pages: vec![EventPage {
                    payload: Some(any("/toy.order.v1.ItemAdded")),
                    ..EventPage::default()
                }],
                ..EventBook::default()
            }),
            rules_version: "order-1".to_string(),
        })
    }
}

fn order_descriptor() -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Order".to_string(),
        kind: ComponentKind::Aggregate as i32,
        rules_version: "order-1".to_string(),
        state: Some(StateDescriptor {
            type_url: "/toy.order.v1.State".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: RESOLUTIONS.iter().map(|r| *r as i32).collect(),
            }),
        }),
        ..ComponentDescriptor::default()
    }
}

fn registry_of(orders: &'static Orders) -> Registry {
    let mut registry = Registry::new();
    registry
        .command_handler("order", orders, order_descriptor())
        .unwrap();
    registry
}

/// A note at `sequence` of the order stream: an event no command of the
/// test's wrote.
fn note(sequence: u64) -> EventPage {
    EventPage {
        sequence,
        payload: Some(any("/toy.order.v1.OrderNoteAdded")),
        ..EventPage::default()
    }
}

/// The background: the order stream holds events 0 to 2, its head at 3.
fn store_at_three() -> MemoryStore {
    let store = MemoryStore::new();
    store
        .write(Write::append(
            sref(&order()),
            0,
            &[note(0), note(1), note(2)],
        ))
        .ready()
        .unwrap();
    store
}

/// An AddItem to the order stream, command id `n`.
fn add_item(n: u128) -> crate::next::v1::CommandBook {
    command(order(), any("/toy.order.v1.AddItem")).id(n)
}

/// `failure` is never answered the stale expected sequence, STRICT's key.
fn never_stale(failure: &Failure<Rejected>) {
    if let Failure::Kernel(error) = failure {
        if let Some(coded) = error.as_coded() {
            assert_ne!(
                coded.code,
                CoordinatorReason::ExpectedSequenceStale.as_str_name(),
                "{failure:?}"
            );
        }
    }
}

/// C-0727: a command whose expected sequence is ahead of the head (4 or 7,
/// the head at 3) is malformed under every resolution: refused
/// INVALID_ARGUMENT with `COORDINATOR_REASON_EXPECTED_SEQUENCE_AHEAD`, args
/// `expected` and `head`, a final business refusal (never retried); business
/// code is not invoked, nothing is persisted, and it is never answered the
/// stale key.
#[test]
fn a_command_ahead_of_the_stream_is_malformed_under_every_resolution() {
    for resolution in RESOLUTIONS {
        for expected in [4, 7] {
            let orders = Orders::leaked();
            let registry = registry_of(orders);
            let log = MemoryDeadLetters::new();
            let store = store_at_three();
            let failure = Kernel::new(&registry, &log)
                .handle_any(
                    &store,
                    add_item(1).expecting(expected, resolution),
                    &at(1),
                    &mut (),
                )
                .ready()
                .err()
                .expect("refused");
            let case = format!("{resolution:?} expecting {expected}");
            assert_eq!(
                failure,
                Failure::Kernel(KernelError::ExpectedSequenceAhead {
                    stream: order(),
                    expected,
                    head: 3,
                }),
                "{case}"
            );
            let Failure::Kernel(error) = &failure else {
                unreachable!()
            };
            let coded = error.as_coded().expect("one of the coordinator's answers");
            assert_eq!(
                coded.code,
                CoordinatorReason::ExpectedSequenceAhead.as_str_name(),
                "{case}"
            );
            assert_eq!(coded.grpc, GrpcCode::InvalidArgument, "{case}");
            assert_eq!(class(coded.grpc as i32), Class::BusinessFatal, "{case}");
            assert_eq!(
                coded.extras,
                BTreeMap::from([
                    ("expected".to_string(), expected.to_string()),
                    ("head".to_string(), "3".to_string()),
                ]),
                "{case}"
            );
            assert_eq!(failure.recovery(), Recovery::Invalid, "{case}");
            never_stale(&failure);
            assert!(orders.seen().is_empty(), "{case}: business code ran");
            assert_eq!(pages(&store, &order()).len(), 3, "{case}");
            assert!(log.lines().is_empty(), "{case}: {:?}", log.lines());
        }
    }
}
