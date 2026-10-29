//! The host's remote domains through angzarr-embedded's kernel: a command
//! for the remote domain handled by `Kernel::handle_any`, its events
//! written to the store; and a policy step from an in-process domain into
//! the remote one.

use angzarr_embedded::next::angzarr_router::error::CodedError;
use angzarr_embedded::next::angzarr_router::policy::PolicyDispatch;
use angzarr_embedded::next::angzarr_router::router::CommandHandler;
use angzarr_embedded::next::v1::{
    command_page, Command, CommandBook, CommandPage, ComponentDescriptor, ComponentKind,
    ConflictResolution, ContextualCommand, Cover, EventBook, EventPage, StateDescriptor,
    StateOptions, Uuid,
};
use angzarr_embedded::next::{
    Any, EventStore, Handled, Kernel, MemoryDeadLetters, Misconfigured, Registry, Sampled,
    StreamRef, Timestamp,
};
use angzarr_ports::memory::MemoryStore;
use angzarr_router::pb::CommandHandlerResponse;

use super::Host;
use crate::fake_child::{config, domain, mode, noted};

fn root() -> Uuid {
    Uuid::from_bytes([0x0a; 16])
}

fn cover(domain: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(root()),
        edition: None,
    }
}

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

/// Command `n` to `domain`'s stream, as a client sends it.
fn send(domain: &str, n: u8, payload: Any) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain)),
        page: Some(CommandPage {
            body: Some(command_page::Body::Command(Command {
                payload: Some(payload),
            })),
            command_id: Some(Uuid::from_bytes([n; 16])),
            ..CommandPage::default()
        }),
        handling: None,
    }
}

fn sampled(n: u8) -> Sampled {
    Sampled {
        now: Timestamp {
            seconds: i64::from(n),
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0x70 | n; 16]),
    }
}

fn pages(store: &MemoryStore, stream: &Cover) -> Vec<EventPage> {
    let stream = StreamRef::try_from(stream).unwrap();
    block_on(store.read(stream, 0, None)).unwrap().pages
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn state(type_url: &str) -> Option<StateDescriptor> {
    Some(StateDescriptor {
        type_url: type_url.to_string(),
        options: Some(StateOptions {
            conflict_resolutions: vec![
                ConflictResolution::Strict as i32,
                ConflictResolution::AggregateHandles as i32,
            ],
        }),
    })
}

/// An in-process Rust domain, `table`, hand-written as the router's
/// `CommandHandler`: every command is answered with one `Seated` event.
struct Table;

const SEATED: &str = "test.table.v1.Seated";

impl CommandHandler for Table {
    fn name(&self) -> &str {
        "Table"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["test.table.v1.Seat".to_string()]
    }

    fn dispatch(&self, _: &ContextualCommand) -> Result<CommandHandlerResponse, CodedError> {
        Ok(CommandHandlerResponse {
            events: Some(EventBook {
                pages: vec![EventPage {
                    payload: Some(any(&format!("/{SEATED}"))),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            rules_version: "table-1".to_string(),
        })
    }

    fn component_descriptor(&self) -> Result<ComponentDescriptor, CodedError> {
        Ok(ComponentDescriptor {
            name: "Table".to_string(),
            kind: ComponentKind::Aggregate as i32,
            rules_version: "table-1".to_string(),
            state: state("/test.table.v1.State"),
            ..Default::default()
        })
    }
}

fn host() -> Host {
    Host::start(config(vec![domain(
        "player",
        "PlayerAggregate",
        mode::SERVE,
    )]))
    .expect("the host starts")
}

/// A command for the remote domain, handled through the kernel: the
/// child decides, and its events land on the domain's stream, stamped with
/// its rules version; the next command reaches the child with them.
#[test]
fn a_remote_command_round_trips_through_the_kernel_into_the_store() {
    let host = host();
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let rules_version = host
        .handler("player")
        .unwrap()
        .component_descriptor()
        .unwrap()
        .rules_version;

    let handled = block_on(kernel.handle_any(
        &store,
        send("player", 1, any("/test.player.v1.SitDown")),
        &sampled(1),
        &mut (),
    ))
    .expect("handled");
    assert!(matches!(handled, Handled::Applied(_)));
    block_on(kernel.handle_any(
        &store,
        send("player", 2, any("/test.player.v1.Bet")),
        &sampled(2),
        &mut (),
    ))
    .expect("handled");

    let written = pages(&store, &cover("player"));
    let payloads: Vec<_> = written.iter().map(|p| p.payload.clone()).collect();
    assert_eq!(payloads, [Some(noted(0)), Some(noted(1))]);
    assert_eq!(
        written[0].meta.as_ref().map(|m| m.rules_version.as_str()),
        Some(rules_version.as_str())
    );
    host.shutdown();
}

/// A policy from the in-process `table` into the remote `player`: the
/// table's event sets off a step the kernel dispatches through the child,
/// and both streams are written.
#[test]
fn a_policy_step_from_an_in_process_domain_reaches_the_remote_one() {
    let host = host();
    let mut registry = Registry::new();
    registry.command_handler("table", Table).unwrap();
    host.register(&mut registry).unwrap();
    registry.policy_dispatch(
        PolicyDispatch::new("table-player", "table", "player").on_event(SEATED, |_, source| {
            Ok(vec![CommandBook {
                cover: Some(Cover {
                    domain: "player".to_string(),
                    root: source.and_then(|c| c.root.clone()),
                    edition: None,
                }),
                page: Some(CommandPage {
                    body: Some(command_page::Body::Command(Command {
                        payload: Some(any("/test.player.v1.SitDown")),
                    })),
                    ..Default::default()
                }),
                handling: None,
            }])
        }),
    );
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);

    let handled = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.Seat")),
        &sampled(1),
        &mut (),
    ))
    .expect("handled");
    assert_eq!(
        handled.books().len(),
        2,
        "the table's step and the player's"
    );

    let table: Vec<_> = pages(&store, &cover("table"))
        .into_iter()
        .map(|p| p.payload)
        .collect();
    assert_eq!(table, [Some(any(&format!("/{SEATED}")))]);
    let player: Vec<_> = pages(&store, &cover("player"))
        .into_iter()
        .map(|p| p.payload)
        .collect();
    assert_eq!(player, [Some(noted(0))]);
    host.shutdown();
}

/// Registering is by domain, refused as the registry refuses: a domain
/// registered already.
#[test]
fn register_is_refused_for_a_domain_registered_already() {
    let host = host();
    let mut registry = Registry::new();
    registry.command_handler("player", Table).unwrap();
    assert_eq!(
        host.register(&mut registry),
        Err(Misconfigured::AlreadyRegistered {
            domain: "player".to_string(),
        })
    );
    host.shutdown();
}
