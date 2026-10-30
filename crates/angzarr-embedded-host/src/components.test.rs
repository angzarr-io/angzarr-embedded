//! Red tests (feat/host-admit-policies, the owner's rulings of 2026-10-29):
//! the host's remote process managers apply their start rule, and the host
//! carries policy and projector children beside its domains.
//!
//! - A remote process manager's start rule is the host's, from the child's
//!   descriptor (`ComponentDescriptor.starting_types`), applied in
//!   `RemoteCommandHandler::admit` with no call to the child: admit is
//!   coordinator work, and the coordinator upcasts inputs before it
//!   (decision 14.39; the proposal is 14.41), so the payload admit sees is
//!   already current. There is no admit RPC.
//! - A `[[domain]]` of `kind = "policy"` is a `PolicyService` child, run by
//!   the kernel as any policy of its source; one of `kind = "projector"` a
//!   `ProjectorService` child, run after each commit of its source's
//!   events, its failures following the four classes (decision 14.37).

use std::path::{Path, PathBuf};
use std::time::Duration;

use angzarr_embedded::next::angzarr_router::error::{CodedError, GrpcCode};
use angzarr_embedded::next::angzarr_router::router::CommandHandler;
use angzarr_embedded::next::v1::{
    command_page, Command, CommandBook, CommandPage, ComponentDescriptor, ComponentKind,
    ConflictResolution, ContextualCommand, Cover, EventBook, EventPage, Fact, FactOrigin,
    StateDescriptor, StateOptions, Uuid,
};
use angzarr_embedded::next::{
    Any, EventStore, Failure, Handled, Kernel, MemoryDeadLetters, OnRetryable, Registry, Sampled,
    StreamRef, Timestamp,
};
use angzarr_embedded_diagnostic::Diagnostic;
use angzarr_ports::memory::MemoryStore;
use angzarr_router::pb::CommandHandlerResponse;

use super::Host;
use crate::config::{ComponentKindConfig, DomainConfig};
use crate::fake_child::{
    config, domain, kind, mode, noted, HOLD, HOLD_TOO_MUCH, INPUT_ENV, KIND_ENV, LOG_ENV,
    SEAT_RELEASED, STARTING_ENV, TARGET_ENV,
};

fn root() -> Uuid {
    Uuid::from_bytes([0x0b; 16])
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

/// Fact `n` to `domain`'s stream, from an outside source.
fn tell(domain: &str, n: u8, payload: Any) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain)),
        page: Some(CommandPage {
            body: Some(command_page::Body::Fact(Fact {
                payload: Some(payload),
            })),
            fact_origin: Some(FactOrigin {
                external_id: format!("outside-{n}"),
                description: "a test's fact".to_string(),
            }),
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
        command_id: Uuid::from_bytes([0x60 | n; 16]),
    }
}

fn payloads(store: &MemoryStore, stream: &Cover) -> Vec<Option<Any>> {
    let stream = StreamRef::try_from(stream).unwrap();
    block_on(store.read(stream, 0, None))
        .unwrap()
        .pages
        .into_iter()
        .map(|p| p.payload)
        .collect()
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

fn descriptor(name: &str, state_type: &str) -> ComponentDescriptor {
    ComponentDescriptor {
        name: name.to_string(),
        kind: ComponentKind::Aggregate as i32,
        rules_version: format!("{name}-1"),
        state: state(state_type),
        ..Default::default()
    }
}

/// The payload of a command or a fact.
fn input(req: &ContextualCommand) -> Any {
    let page = req.command.as_ref().and_then(|c| c.page.as_ref());
    match page.and_then(|p| p.body.as_ref()) {
        Some(command_page::Body::Command(c)) => c.payload.clone().unwrap_or_default(),
        Some(command_page::Body::Fact(f)) => f.payload.clone().unwrap_or_default(),
        None => Any::default(),
    }
}

fn one_event(payload: Any, rules_version: &str) -> CommandHandlerResponse {
    CommandHandlerResponse {
        events: Some(EventBook {
            pages: vec![EventPage {
                payload: Some(payload),
                ..Default::default()
            }],
            ..Default::default()
        }),
        rules_version: rules_version.to_string(),
    }
}

/// An in-process `table`: a command `/test.table.v1.Seat<X>` records
/// `/test.table.v1.Seated<X>`; a fact is recorded as it arrives.
struct Table;

impl CommandHandler for Table {
    fn name(&self) -> &str {
        "Table"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["test.table.v1.Seat".to_string()]
    }

    fn dispatch(&self, req: &ContextualCommand) -> Result<CommandHandlerResponse, CodedError> {
        let sent = input(req);
        let is_fact = matches!(
            req.command
                .as_ref()
                .and_then(|c| c.page.as_ref())
                .and_then(|p| p.body.as_ref()),
            Some(command_page::Body::Fact(_))
        );
        let event = match is_fact {
            true => sent,
            false => any(&sent.type_url.replacen("Seat", "Seated", 1)),
        };
        Ok(one_event(event, "Table-1"))
    }

    fn component_descriptor(&self) -> Result<ComponentDescriptor, CodedError> {
        Ok(descriptor("Table", "/test.table.v1.State"))
    }
}

/// An in-process `wallet`: `Hold` records `Held`; `HoldTooMuch` is
/// refused FAILED_PRECONDITION, `INSUFFICIENT_FUNDS`, with the typed
/// refusal [`INSUFFICIENT`].
struct Wallet;

const HELD: &str = "/test.wallet.v1.Held";
const INSUFFICIENT: &str = "/test.wallet.v1.Insufficient";

impl CommandHandler for Wallet {
    fn name(&self) -> &str {
        "Wallet"
    }

    fn command_types(&self) -> Vec<String> {
        vec![HOLD[1..].to_string(), HOLD_TOO_MUCH[1..].to_string()]
    }

    fn dispatch(&self, req: &ContextualCommand) -> Result<CommandHandlerResponse, CodedError> {
        if input(req).type_url == HOLD_TOO_MUCH {
            return Err(CodedError {
                code: "INSUFFICIENT_FUNDS".to_string(),
                extras: Default::default(),
                grpc: GrpcCode::FailedPrecondition,
                refusal: Some(Box::new(any(INSUFFICIENT))),
            });
        }
        Ok(one_event(any(HELD), "Wallet-1"))
    }

    fn component_descriptor(&self) -> Result<ComponentDescriptor, CodedError> {
        Ok(descriptor("Wallet", "/test.wallet.v1.State"))
    }
}

/// A fresh call log for a fake policy or projector, by test.
fn log_file(test: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("angzarr-fake-{}-{test}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

fn log_lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn with_env(mut domain: DomainConfig, env: &[(&str, &str)]) -> DomainConfig {
    for (k, v) in env {
        domain.env.insert(k.to_string(), v.to_string());
    }
    domain
}

// ---------------------------------------------------------------------
// A remote process manager's start rule.
// ---------------------------------------------------------------------

/// A remote process manager `audit` whose child lists `starting` as its
/// starting types.
fn process_manager(starting: &str) -> Host {
    let pm = with_env(
        domain("audit", "AuditProcess", mode::SERVE),
        &[(KIND_ENV, kind::PROCESS_MANAGER), (STARTING_ENV, starting)],
    );
    Host::start(config(vec![pm])).expect("the host starts")
}

/// A non-starting COMMAND for a root with no instance is refused before
/// the child decides: FAILED_PRECONDITION, PM_NO_INSTANCE, a business
/// rejection, nothing recorded (process_manager.proto, Starting an
/// instance). The child would have answered `Noted`.
#[test]
fn a_remote_process_managers_non_starting_command_with_no_instance_is_refused() {
    let host = process_manager("test.pm.v1.Open");
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);

    let refused = block_on(kernel.handle_any(
        &store,
        send("audit", 1, any("/test.pm.v1.Close")),
        &sampled(1),
        &mut (),
    ));

    let written = payloads(&store, &cover("audit"));
    assert!(
        written.is_empty(),
        "no instance is started by a non-starting command: {written:?}"
    );
    let Err(Failure::Rejected(rejected)) = &refused else {
        panic!("refused PM_NO_INSTANCE: {:?}", refused.err());
    };
    assert_eq!(rejected.code(), "PM_NO_INSTANCE");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

/// A non-starting FACT for a root with no instance is skipped silently:
/// APPLIED, no events, nothing dead-lettered.
#[test]
fn a_remote_process_managers_non_starting_fact_with_no_instance_is_skipped() {
    let host = process_manager("test.pm.v1.Open");
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);

    let handled = block_on(kernel.handle_any(
        &store,
        tell("audit", 1, any("/test.pm.v1.Closed")),
        &sampled(1),
        &mut (),
    ))
    .map_err(|e| format!("{e:?}"))
    .expect("a skipped fact is applied");

    let written = payloads(&store, &cover("audit"));
    assert!(
        written.is_empty(),
        "a non-starting fact starts no instance: {written:?}"
    );
    assert!(matches!(handled, Handled::Applied(_)), "applied");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

/// A starting command starts the instance (the child decides), and once it
/// exists any input reaches the child.
#[test]
fn a_remote_process_managers_starting_command_starts_an_instance_that_then_takes_any_input() {
    let host = process_manager("test.pm.v1.Open");
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);

    block_on(kernel.handle_any(
        &store,
        send("audit", 1, any("/test.pm.v1.Open")),
        &sampled(1),
        &mut (),
    ))
    .expect("a starting command is delivered");
    block_on(kernel.handle_any(
        &store,
        send("audit", 2, any("/test.pm.v1.Close")),
        &sampled(2),
        &mut (),
    ))
    .expect("the instance exists: any command is delivered");

    assert_eq!(
        payloads(&store, &cover("audit")),
        [Some(noted(0)), Some(noted(1))]
    );
    host.shutdown();
}

/// The start rule matches by type NAME: a descriptor may list a starting
/// type as a full type URL (`type.googleapis.com/...`, as
/// component.proto calls them) or as a bare name (as angzarr-router's
/// `DomainDispatch` advertises them), and a payload under `/...` (angzarr's
/// canonical prefix) matches either. Nothing else starts an instance.
#[test]
fn a_remote_process_managers_start_rule_matches_by_type_name_whatever_the_prefix() {
    let host = process_manager("type.googleapis.com/test.pm.v1.Open,test.pm.v1.Reopen");
    let mut registry = Registry::new();
    host.register(&mut registry).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);

    let closed = MemoryStore::new();
    let refused = block_on(kernel.handle_any(
        &closed,
        send("audit", 1, any("/test.pm.v1.Close")),
        &sampled(1),
        &mut (),
    ));
    assert!(
        matches!(&refused, Err(Failure::Rejected(r)) if r.code() == "PM_NO_INSTANCE"),
        "an unlisted type does not start one: {:?}",
        refused.err()
    );

    for (n, starting) in [(2, "/test.pm.v1.Open"), (3, "/test.pm.v1.Reopen")] {
        let store = MemoryStore::new();
        block_on(kernel.handle_any(
            &store,
            send("audit", n, any(starting)),
            &sampled(n),
            &mut (),
        ))
        .unwrap_or_else(|e| panic!("{starting} starts an instance: {e:?}"));
        assert_eq!(payloads(&store, &cover("audit")), [Some(noted(0))]);
    }
    host.shutdown();
}

// ---------------------------------------------------------------------
// A remote policy.
// ---------------------------------------------------------------------

/// A remote policy `table-wallet`, `kind = "policy"`, from `table` to
/// `wallet`, logging its calls to `log`.
fn policy_domain(log: &Path) -> DomainConfig {
    let mut policy = with_env(
        domain("table-wallet", "TableWalletPolicy", mode::SERVE),
        &[
            (KIND_ENV, kind::POLICY),
            (INPUT_ENV, "table"),
            (TARGET_ENV, "wallet"),
            (LOG_ENV, log.to_str().unwrap()),
        ],
    );
    policy.kind = ComponentKindConfig::Policy;
    policy
}

/// `kind = "policy"` launches a `PolicyService` child, handed back as a
/// policy (named by its descriptor, its source its descriptor's one input),
/// never as a domain.
#[test]
fn a_policy_child_comes_up_as_a_policy_not_a_domain() {
    let log = log_file("policy-comes-up");
    let host = Host::start(config(vec![policy_domain(&log)])).expect("the host starts");

    let policy = host
        .policy("table-wallet")
        .expect("handed back as a policy");
    assert_eq!(policy.name(), "TableWalletPolicy");
    assert_eq!(policy.input_domain(), "table");
    assert!(
        host.handler("table-wallet").is_none(),
        "a policy is not a domain"
    );
    host.shutdown();
}

/// The kernel runs the remote policy on its source's events: the table's
/// `Seated` reaches the child, whose `Hold` the wallet records.
#[test]
fn a_remote_policy_turns_its_sources_events_into_commands_through_the_kernel() {
    let log_path = log_file("policy-commands");
    let host = Host::start(config(vec![policy_domain(&log_path)])).expect("the host starts");
    let mut registry = Registry::new();
    registry.command_handler("table", Table).unwrap();
    registry.command_handler("wallet", Wallet).unwrap();
    host.register(&mut registry)
        .expect("a policy child registers as a policy");
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);

    block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.Seat")),
        &sampled(1),
        &mut (),
    ))
    .expect("handled");

    assert_eq!(
        log_lines(&log_path),
        ["handle /test.table.v1.Seated"],
        "the child saw the event"
    );
    assert_eq!(payloads(&store, &cover("wallet")), [Some(any(HELD))]);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

/// The wallet refuses the remote policy's command: the kernel calls the
/// child's OnRefused with the refusal (the typed refusal included), and
/// the fact it answers lands on the table's stream, the trigger's. The
/// wallet records nothing.
#[test]
fn a_refusal_of_a_remote_policys_command_is_answered_by_its_on_refused() {
    let log_path = log_file("policy-refused");
    let host = Host::start(config(vec![policy_domain(&log_path)])).expect("the host starts");
    let mut registry = Registry::new();
    registry.command_handler("table", Table).unwrap();
    registry.command_handler("wallet", Wallet).unwrap();
    host.register(&mut registry)
        .expect("a policy child registers as a policy");
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);

    let handled = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.SeatBig")),
        &sampled(1),
        &mut (),
    ));

    assert!(
        log_lines(&log_path).contains(&format!("refused {INSUFFICIENT}")),
        "OnRefused got the typed refusal: {:?}",
        log_lines(&log_path)
    );
    let released = Any {
        type_url: SEAT_RELEASED.to_string(),
        value: HOLD_TOO_MUCH.as_bytes().to_vec(),
    };
    assert_eq!(
        payloads(&store, &cover("table")),
        [Some(any("/test.table.v1.SeatedBig")), Some(released)]
    );
    assert!(payloads(&store, &cover("wallet")).is_empty());
    assert!(
        handled.is_ok(),
        "the refusal was answered: {:?}",
        handled.err()
    );
    host.shutdown();
}

// ---------------------------------------------------------------------
// A remote projector.
// ---------------------------------------------------------------------

/// A remote projector `table-view`, `kind = "projector"`, of `table`,
/// logging its calls to `log`.
fn projector_domain(log: &Path) -> DomainConfig {
    let mut projector = with_env(
        domain("table-view", "TableView", mode::SERVE),
        &[
            (KIND_ENV, kind::PROJECTOR),
            (INPUT_ENV, "table"),
            (LOG_ENV, log.to_str().unwrap()),
        ],
    );
    projector.kind = ComponentKindConfig::Projector;
    projector
}

/// The host, the in-process table and the remote projector, registered.
fn projected(log_path: &Path) -> (Host, Registry) {
    let host = Host::start(config(vec![projector_domain(log_path)])).expect("the host starts");
    let mut registry = Registry::new();
    registry.command_handler("table", Table).unwrap();
    host.register(&mut registry)
        .expect("a projector child registers as a projector");
    (host, registry)
}

/// Retrying quickly, so the tests are.
fn quick() -> OnRetryable {
    OnRetryable {
        max_retries: 3,
        initial_backoff: Duration::from_millis(5),
        multiplier: 1.0,
        max_backoff: Duration::from_millis(5),
        jitter: 0.0,
    }
}

/// `kind = "projector"` launches a `ProjectorService` child, handed back as
/// a projector of its descriptor's one input, never as a domain.
#[test]
fn a_projector_child_comes_up_as_a_projector_not_a_domain() {
    let log = log_file("projector-comes-up");
    let host = Host::start(config(vec![projector_domain(&log)])).expect("the host starts");

    let projector = host
        .projector("table-view")
        .expect("handed back as a projector");
    assert_eq!(projector.name(), "TableView");
    assert_eq!(projector.source(), "table");
    assert!(
        host.handler("table-view").is_none(),
        "a projector is not a domain"
    );
    host.shutdown();
}

/// The kernel runs the remote projector after the commit, on its source's
/// events.
#[test]
fn a_remote_projector_receives_its_sources_committed_events() {
    let log_path = log_file("projector-receives");
    let (host, registry) = projected(&log_path);
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);

    block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.Seat")),
        &sampled(1),
        &mut (),
    ))
    .expect("handled");

    assert_eq!(log_lines(&log_path), ["handle /test.table.v1.Seated"]);
    host.shutdown();
}

/// A projector's bug (INTERNAL: bug, fatal) is never retried: the call
/// fails after the commit (the events stay), naming the projector and the
/// child's code, and the bug is dead-lettered.
#[test]
fn a_remote_projectors_bug_is_fatal_not_retried_and_dead_lettered() {
    let log_path = log_file("projector-bug");
    let (host, registry) = projected(&log_path);
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).on_retryable(quick());

    let failed = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.SeatBreak")),
        &sampled(1),
        &mut (),
    ));

    assert_eq!(
        log_lines(&log_path),
        ["handle /test.table.v1.SeatedBreak"],
        "called once"
    );
    let Err(Failure::Projection {
        committed,
        failures,
    }) = &failed
    else {
        panic!("a projection failure: {:?}", failed.err());
    };
    assert!(!committed.is_empty(), "the events stay committed");
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].0, "TableView");
    assert!(
        failures[0].1.to_string().contains("PROJECTOR_BROKE"),
        "{}",
        failures[0].1
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "dead-lettered: {lines:?}");
    assert!(lines[0].contains("PROJECTOR_BROKE"), "{}", lines[0]);
    host.shutdown();
}

/// A projector's business status (FAILED_PRECONDITION: business, fatal)
/// is returned, never retried, and not dead-lettered.
#[test]
fn a_remote_projectors_business_status_is_returned_and_not_retried() {
    let log_path = log_file("projector-business");
    let (host, registry) = projected(&log_path);
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).on_retryable(quick());

    let failed = block_on(kernel.handle_any(
        &store,
        send("table", 1, any("/test.table.v1.SeatRefuse")),
        &sampled(1),
        &mut (),
    ));

    assert_eq!(
        log_lines(&log_path),
        ["handle /test.table.v1.SeatedRefuse"],
        "called once"
    );
    let Err(Failure::Projection { failures, .. }) = &failed else {
        panic!("a projection failure: {:?}", failed.err());
    };
    assert!(
        failures[0].1.to_string().contains("VIEW_REFUSED"),
        "{}",
        failures[0].1
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    host.shutdown();
}

/// A projector's retryable bug (UNAVAILABLE) and its immediate-return
/// status (ABORTED, re-read and re-decide, the runtime being the caller)
/// are both retried under the kernel's backoff: the second call succeeds,
/// and the call is answered.
#[test]
fn a_remote_projectors_retryable_and_aborted_statuses_are_retried() {
    for (n, event) in [(1, "Flaky"), (2, "Stale")] {
        let log_path = log_file(&format!("projector-retry-{event}"));
        let (host, registry) = projected(&log_path);
        let store = MemoryStore::new();
        let log = MemoryDeadLetters::new();
        let kernel = Kernel::new(&registry, &log).on_retryable(quick());

        let handled = block_on(kernel.handle_any(
            &store,
            send("table", n, any(&format!("/test.table.v1.Seat{event}"))),
            &sampled(n),
            &mut (),
        ));

        let line = format!("handle /test.table.v1.Seated{event}");
        assert_eq!(
            log_lines(&log_path),
            [line.clone(), line],
            "{event}: retried once"
        );
        assert!(handled.is_ok(), "{event}: {:?}", handled.err());
        assert!(log.lines().is_empty(), "{event}: {:?}", log.lines());
        host.shutdown();
    }
}
