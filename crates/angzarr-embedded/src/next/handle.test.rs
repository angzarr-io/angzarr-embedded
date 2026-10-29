//! The chain as a sequence of committed steps: atomicity covers one
//! handling step, never a chain. A command commits on its own stream; each
//! policy step after it is one atomic write to one stream, committed with
//! the policy's position on its source; a step that fails keeps every commit before it, and a step left
//! undone (a crash, a failed store) is resumed by position, deduplicated by
//! its derived command id. The call waits for the chain to settle.

use crate::next::recording::Recording;
use crate::next::toys::prelude::*;
use crate::next::v1::event_meta::Causation;
use crate::next::v1::{CommandBook, EventMeta, EventRef};
use crate::next::{
    ids, Failure, Kernel, KernelError, MemoryDeadLetters, Payload, Recovery, Step, StepError,
};

use crate::next::toys::{
    at, command, cover, cover_in, ledger, ledger_root, opened, payloads, registry, Account,
    AccountCmd, Bookkeeping, Edit, Ledger, LEDGER_LIMIT,
};

fn deposit(n: u128, cents: i64) -> CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

fn ledger_cover() -> crate::next::v1::Cover {
    crate::next::v1::Cover {
        domain: "ledger".to_string(),
        root: Some(ledger_root()),
        edition: None,
    }
}

#[test]
fn a_policys_events_carry_the_derived_command_id_as_causation_and_no_trigger() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);

    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25).correlated(8), &at(30), &mut ())
        .ready()
        .unwrap();
    let books = handled.books();
    assert_eq!(books.len(), 2, "the account, then the ledger");
    assert_eq!(books[1].cover, Some(ledger_cover()));
    assert_eq!(
        payloads::<ledger::Recorded>(&books[1..]),
        [ledger::Recorded {
            source_sequence: 2,
            cents: 25
        }]
    );

    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    let meta: EventMeta = books[1].pages[0].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(Causation::CommandId(
            ids::policy_command_id(&trigger, "bookkeeping", 0).unwrap()
        )),
        "the direct cause is the command, UUIDv5 of (trigger, component, \
         index) as angzarr derives it; never the trigger (decision 13.3)"
    );
    assert_eq!(
        meta.correlation_id,
        Some(crate::next::toys::correlation(8)),
        "the trigger's"
    );
    assert_eq!(
        meta.recorded_at.unwrap().seconds,
        30,
        "one instant per chain"
    );
}

/// A policy step its target rejects fails the call with the path to the
/// step, but rolls nothing back: the command's commit stands. The rejection
/// is a business answer, so the step is dead-lettered and its position moves
/// past the trigger, and nothing runs it again: a retry of the command is a
/// duplicate, and owes nothing.
#[test]
fn a_rejected_policy_step_keeps_the_commands_commit_and_is_dead_lettered() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let before = slice(&store, &cover("account", 1)).next_sequence;

    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30), &mut ())
        .ready()
        .err()
        .unwrap();
    let Failure::Downstream { path, error } = &failure else {
        panic!("a downstream failure: {failure:?}");
    };
    assert_eq!(
        path,
        &[Step {
            stream: ledger_cover(),
            cause: EventRef {
                stream: Some(cover("account", 1)),
                sequence: 2,
            },
        }]
    );
    assert!(
        matches!(error, StepError::Rejected(r) if angzarr_embedded_diagnostic::Diagnostic::code(r) == "toy-ledger-too-large")
    );
    assert_eq!(failure.recovery(), Recovery::Business);
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        before + 1,
        "the command's deposit committed, and stays"
    );
    assert_eq!(
        pages(&store, &ledger_cover()),
        [],
        "the step landed nothing"
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "the rejected step, dead-lettered");
    assert!(lines[0].starts_with(crate::next::DEAD_LETTER_PREFIX));
    assert_eq!(
        position(&store, "bookkeeping", &cover("account", 1)),
        before + 1,
        "past the rejected trigger"
    );

    let retried = kernel
        .handle::<Account, _, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(31), &mut ())
        .ready()
        .unwrap();
    assert!(matches!(retried, crate::next::Handled::Duplicate(_)));
    assert_eq!(pages(&store, &ledger_cover()), [], "nothing run again");
    assert_eq!(log.lines().len(), 1);
}

#[test]
fn a_chain_deeper_than_its_limit_is_refused() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).limit(0);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, 1), &at(30), &mut ())
        .ready()
        .err()
        .unwrap();
    assert!(matches!(
        failure,
        Failure::Downstream {
            error: StepError::Kernel(KernelError::ChainTooDeep { limit: 0 }),
            ..
        }
    ));
    assert_eq!(failure.recovery(), Recovery::Bug);
    assert_eq!(
        pages(&store, &cover("account", 1)).len(),
        3,
        "the command's commit stands"
    );
}

/// Editions are per-stream forks (14.32): a command to a stream not forked
/// into its edition is outside input to it, refused by the store at the
/// write (`EDITION_NOT_FORKED`), and nothing is written. angzarr-embedded creates no
/// edition yet.
#[test]
fn a_command_to_an_edition_is_refused_and_writes_nothing() {
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    let stream = cover_in("account", 1, "what-if");
    let open = command(stream.clone(), AccountCmd::Open.to_any());
    let failure = kernel
        .handle::<Account, _, _>(&store, open, &at(30), &mut ())
        .ready()
        .err()
        .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Store(
            crate::next::StoreError::EditionNotForked {
                stream: Box::new(stream)
            }
        ))
    );
    assert_eq!(failure.recovery(), Recovery::Invalid);
    assert_eq!(streams(&store), [], "nothing written");
}

#[test]
fn a_policys_target_is_in_its_sources_edition() {
    let target = crate::next::target_cover::<Bookkeeping>(
        &cover_in("account", 1, "what-if"),
        &ledger_root(),
    );
    assert_eq!(
        target.edition.as_deref(),
        Some("what-if"),
        "a policy never crosses timelines"
    );
}

/// A hand-written policy's refusal answer is angzarr's pass-through
/// `CommandRefused` unless it says otherwise.
#[test]
fn a_hand_written_policy_passes_a_refusal_through() {
    use crate::next::markup::{self, v1::Refusal};
    let refusal = Refusal {
        event: Some(crate::next::v1::EventBook {
            cover: Some(cover("account", 1)),
            ..Default::default()
        }),
        ..Default::default()
    };
    let answer = crate::next::Policy::on_refused(&Bookkeeping, &refusal);
    assert_eq!(answer, markup::__pass_through(&refusal));
}

#[test]
fn a_book_for_another_domain_is_refused() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    let failure = kernel
        .handle::<Ledger, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "ledger"
        })
    );
}

/// Each step loads what every step before it committed: the second
/// deposit's ledger step finds the first's record. What a chain answers is
/// every stream it committed to, in the order first touched.
#[test]
fn each_step_loads_what_the_steps_before_it_committed() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ())
        .ready()
        .unwrap();
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(4, 5), &at(31), &mut ())
        .ready()
        .unwrap();
    let crate::next::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    assert_eq!(
        recorded
            .books()
            .iter()
            .map(|b| (b.cover.clone().unwrap(), b.pages.len(), b.next_sequence))
            .collect::<Vec<_>>(),
        [(cover("account", 1), 1, 4), (ledger_cover(), 1, 2)],
        "each stream once, in the order first touched"
    );
    let ledger = recorded.events::<Ledger>(&ledger_cover()).unwrap();
    assert_eq!(ledger[0].sequence(), 1, "after the first deposit's record");
    assert!(recorded.events::<Ledger>(&cover("account", 1)).is_none());
    assert!(recorded.events::<Account>(&cover("account", 9)).is_none());
}

/// Every deposit's amount, projected after commit.
#[derive(Default)]
struct Deposits(Vec<i64>);

impl crate::next::Projection<Account> for Deposits {
    fn project(&mut self, c: &crate::next::Committed<'_, Account>) {
        if let crate::next::toys::AccountEvent::Deposited(cents) = c.event() {
            self.0.push(*cents);
        }
    }
}

/// Projections run on what each step committed: the command's events and
/// every policy step's.
#[test]
fn projections_run_after_commit_over_their_domains_streams() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let mut deposits = Deposits::default();
    let mut ledger_seen = 0;
    let mut route = crate::next::ProjectionRoute {
        name: "deposits",
        projection: &mut deposits,
        domain: std::marker::PhantomData,
    };
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut route)
        .ready()
        .unwrap();
    assert!(handled.outcome().projector_failures.is_empty());
    assert_eq!(deposits.0, [25], "only the new events, only the account's");

    let crate::next::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    recorded
        .dispatch::<Ledger>(
            &ledger_cover(),
            &registry,
            &mut [&mut |_: &crate::next::Committed<'_, Ledger>| ledger_seen += 1],
        )
        .unwrap();
    assert_eq!(ledger_seen, 1);
    assert_eq!(
        recorded.dispatch::<Ledger>(&cover("account", 1), &registry, &mut []),
        Err(KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "ledger"
        })
    );
    assert_eq!(
        recorded.dispatch::<Ledger>(&cover("ledger", 9), &registry, &mut []),
        Ok(()),
        "a stream the chain did not touch: nothing recorded"
    );
}

#[test]
fn a_duplicate_runs_no_projection_and_commits_nothing() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let mut deposits = Deposits::default();
    let mut route = crate::next::ProjectionRoute {
        name: "deposits",
        projection: &mut deposits,
        domain: std::marker::PhantomData,
    };
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    let handled = kernel
        .handle::<Account, _, _>(&store, open, &at(30), &mut route)
        .ready()
        .unwrap();
    assert!(matches!(handled, crate::next::Handled::Duplicate(_)));
    assert_eq!(handled.outcome().projector_failures, []);
    assert!(deposits.0.is_empty());
}

/// The handler's future is `Send` over a store whose futures are, so a
/// server spawns it on a multi-threaded runtime.
#[test]
fn handle_is_a_send_future_over_a_send_store() {
    fn send<T: Send>(t: T) -> T {
        t
    }
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    let handled = send(kernel.handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ()))
        .ready()
        .unwrap();
    assert_eq!(handled.books().len(), 2);
    let resumed = send(kernel.resume(&store, &at(31), &mut ()))
        .ready()
        .unwrap();
    assert!(resumed.books().is_empty(), "nothing owed");
}

/// The account stream as stored.
fn account_stream(
    store: &MemoryStore,
    registry: &crate::next::Registry,
) -> crate::next::Stream<Account> {
    let pages = pages(store, &cover("account", 1));
    crate::next::Stream::replay(cover("account", 1), &pages, registry).unwrap()
}

/// Account deposits → the ledger, and nothing else: a policy that targets
/// from the event, so the ledger is owed only some of the account's events.
struct DepositsOnly;

impl crate::next::Policy for DepositsOnly {
    const NAME: &'static str = "deposits-only";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        event: &crate::next::Any,
        ctx: &crate::next::markup::PolicyContext,
    ) -> Option<crate::next::markup::Commands<Ledger>> {
        let crate::next::toys::AccountEvent::Deposited(cents) =
            crate::next::toys::AccountEvent::from_any(event).ok()?
        else {
            return None;
        };
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents,
        };
        Some(crate::next::markup::Commands::none().send(ledger_root(), &record))
    }
}

#[test]
fn a_filtered_policy_reaches_its_target_only_for_the_events_it_targets() {
    let mut registry = registry();
    registry.policy(DepositsOnly);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    for book in [
        command(cover("account", 1), AccountCmd::Open.to_any()).id(1),
        deposit(2, 5),
        command(cover("account", 1), AccountCmd::Withdraw(2).to_any()).id(3),
        deposit(4, 7),
    ] {
        kernel
            .handle::<Account, _, _>(&store, book, &at(30), &mut ())
            .ready()
            .unwrap();
    }
    let book = crate::next::v1::EventBook {
        pages: pages(&store, &ledger_cover()),
        ..Default::default()
    };
    assert_eq!(
        payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| (r.source_sequence, r.cents))
            .collect::<Vec<_>>(),
        [(1, 5), (3, 7)],
        "the deposits, and only the deposits, in source order"
    );
    assert_eq!(
        position(&store, "deposits-only", &cover("account", 1)),
        4,
        "past every event, the untargeted included"
    );
}

#[test]
fn a_policy_book_for_an_unregistered_target_is_a_dead_lettered_bug() {
    let mut registry = crate::next::Registry::new();
    registry.policy(Bookkeeping);
    registry
        .register::<Account>(
            crate::next::toys::fees(),
            crate::next::Upcaster::new("u", "account"),
            crate::next::toys::handlers::<crate::next::toys::Account>(
                crate::next::toys::Fees { withdrawal: 1 },
                "v2",
            ),
        )
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    let failure = kernel
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .unwrap();
    let Failure::Downstream { path, error } = &failure else {
        panic!("downstream: {failure:?}");
    };
    assert_eq!(path.len(), 1);
    assert_eq!(
        error,
        &StepError::Kernel(KernelError::Malformed(
            crate::next::Malformed::UnknownDomain {
                domain: "ledger".to_string()
            }
        ))
    );
    assert_eq!(
        failure.recovery(),
        Recovery::Bug,
        "the kernel's book: a bug"
    );
    let lines = log.lines();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with(crate::next::DEAD_LETTER_PREFIX));
    assert!(
        lines[0].contains("bookkeeping: no domain \"ledger\" is registered"),
        "{}",
        lines[0]
    );
    assert_eq!(
        streams(&store),
        [cover("account", 1)],
        "the command's commit stands"
    );
    assert_eq!(
        position(&store, "bookkeeping", &cover("account", 1)),
        0,
        "a bug holds the position, so the step runs again once it is fixed"
    );
}

/// A command's policy steps are owed for its own events only: the second
/// deposit's chain runs one ledger step, for its own event, not the first's
/// again.
#[test]
fn a_command_owes_policy_steps_for_its_own_events_only() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    opened(&store, &plain, 1, 0);
    for n in [3, 4] {
        kernel
            .handle::<Account, _, _>(&store, deposit(n, 1), &at(30), &mut ())
            .ready()
            .unwrap();
    }
    let ledger = slice(store.inner(), &ledger_cover()).pages;
    let book = crate::next::v1::EventBook {
        pages: ledger,
        ..Default::default()
    };
    assert_eq!(
        payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| r.source_sequence)
            .collect::<Vec<_>>(),
        [2, 3],
        "each deposit's own event, once"
    );
    assert_eq!(
        store.landed()[2..],
        [
            Some(cover("account", 1)),
            Some(ledger_cover()),
            Some(cover("account", 1)),
            Some(ledger_cover()),
        ],
        "after the two opening commands: per deposit, its command, then one ledger step"
    );
}

#[test]
fn a_chain_on_a_stream_loaded_from_a_snapshot_records_only_its_events() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 7);
    let source = account_stream(&store, &registry);
    store
        .write_snapshot(
            sref(&cover("account", 1)),
            &source.snapshot(&registry).unwrap(),
        )
        .ready()
        .unwrap();
    let (mut one, mut two) = (Deposits::default(), Deposits::default());
    let first = crate::next::ProjectionRoute {
        name: "first",
        projection: &mut one,
        domain: std::marker::PhantomData,
    };
    let second = crate::next::ProjectionRoute {
        name: "second",
        projection: &mut two,
        domain: std::marker::PhantomData,
    };
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut (first, second))
        .ready()
        .unwrap();
    let crate::next::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    assert!(recorded.projector_failures().is_empty());
    let events = recorded.events::<Account>(&cover("account", 1)).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence(), 2);
    let seen = [one.0, two.0];
    assert_eq!(seen, [vec![25], vec![25]], "both projections ran");
}

#[test]
fn a_committed_event_names_its_stream() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 7);
    let source = account_stream(&store, &registry);
    let mut covers = Vec::new();
    let mut handler = |c: &crate::next::Committed<'_, Account>| {
        covers.push((c.cover().clone(), c.envelope().sequence()))
    };
    source.dispatch(1, &registry, &mut [&mut handler]).unwrap();
    assert_eq!(covers, [(cover("account", 1), 1)]);
}

/// A policy as a handler of its source hands over one order per target,
/// in target order: each indexed by its place, keeping the trigger's
/// correlation.
#[test]
fn a_policys_orders_are_indexed_and_keep_the_triggers_correlation() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let source = account_stream(&store, &registry);
    let mut orders = Vec::new();
    let mut handler = crate::next::policy(&TwoLedgers, |order| orders.push(order));
    source.dispatch(1, &registry, &mut [&mut handler]).unwrap();
    assert_eq!(
        orders
            .iter()
            .map(|o| (o.index(), o.cover().clone(), o.trigger().sequence))
            .collect::<Vec<_>>(),
        [(0, two_ledgers(0), 1), (1, two_ledgers(1), 1)]
    );
    let correlation = source.events()[1].correlation_id().clone();
    assert!(orders.iter().all(|o| *o.correlation_id() == correlation));
    assert_ne!(correlation, crate::next::v1::Uuid::default());
}

/// Two ledgers per account event: orders are indexed in target order.
struct TwoLedgers;

impl crate::next::Policy for TwoLedgers {
    const NAME: &'static str = "two-ledgers";
    type Source = Account;
    type Target = Ledger;

    fn on_event(
        &self,
        _: &crate::next::Any,
        ctx: &crate::next::markup::PolicyContext,
    ) -> Option<crate::next::markup::Commands<Ledger>> {
        let record = ledger::Record {
            source_sequence: ctx.trigger().sequence,
            cents: 0,
        };
        let commands = crate::next::markup::Commands::none()
            .send(two_ledgers_root(0), &record)
            .send(two_ledgers_root(1), &record);
        Some(commands)
    }
}

/// The root of the `n`th of [`TwoLedgers`]' ledgers.
fn two_ledgers_root(n: u8) -> crate::next::v1::Uuid {
    ids::compute_root("ledger", &n.to_string()).unwrap()
}

#[test]
fn a_triggers_commands_are_indexed_in_target_order() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(TwoLedgers);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 5), &at(30), &mut ())
        .ready()
        .unwrap();
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    for index in [0u8, 1] {
        let pages = pages(&store, &two_ledgers(index));
        assert_eq!(
            pages[0].meta.as_ref().unwrap().causation,
            Some(Causation::CommandId(
                ids::policy_command_id(&trigger, "two-ledgers", u32::from(index)).unwrap()
            )),
            "target {index}'s command is the trigger's {index}th"
        );
    }
}

/// The `n`th of [`TwoLedgers`]' ledgers.
fn two_ledgers(n: u8) -> crate::next::v1::Cover {
    crate::next::v1::Cover {
        domain: "ledger".to_string(),
        root: Some(ids::compute_root("ledger", &n.to_string()).unwrap()),
        edition: None,
    }
}

/// Reaction fan-out (the owner's addition): several policies on one source
/// domain, through the builder, each run for every event of that domain, in
/// the order they were registered (per event, every policy; per policy,
/// every target), each as its own committed step, after the command's own.
#[test]
fn every_policy_on_a_source_runs_for_each_of_its_events_in_registration_order() {
    let mut registry = registry();
    registry.policy(TwoLedgers);
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    for (book, second) in [(open, 30), (deposit(2, 5), 31)] {
        kernel
            .handle::<Account, _, _>(&store, book, &at(second), &mut ())
            .ready()
            .unwrap();
    }
    let per_event = [
        Some(cover("account", 1)),
        Some(two_ledgers(0)),
        Some(two_ledgers(1)),
        Some(ledger_cover()),
    ];
    assert_eq!(
        store.landed(),
        [per_event.clone(), per_event].concat(),
        "per event: the command, TwoLedgers' targets in order, then Bookkeeping's"
    );
    for target in [two_ledgers(0), two_ledgers(1), ledger_cover()] {
        let book = crate::next::v1::EventBook {
            pages: slice(store.inner(), &target).pages,
            ..Default::default()
        };
        assert_eq!(
            payloads::<ledger::Recorded>(&[book])
                .iter()
                .map(|r| r.source_sequence)
                .collect::<Vec<_>>(),
            [0, 1],
            "{target:?} reached for each account event"
        );
    }
}

/// A policy step's events land with the policy's position on its source:
/// one write, and the command's own write puts no position. A policy with
/// several targets puts it once, with the trigger's last target, so a
/// crash between its targets leaves the trigger owed (the targets that
/// landed are deduplicated when it runs again).
#[test]
fn a_policy_step_commits_its_position_with_its_events() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(TwoLedgers);
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, 5), &at(30), &mut ())
        .ready()
        .unwrap();
    let writes = store.writes();
    let account = cover("account", 1);
    let [command, first, last, bookkeeping] = &writes[writes.len() - 4..] else {
        panic!("four writes for the deposit: {writes:?}");
    };
    assert_eq!(command.wrote, Some((account.clone(), 1)));
    assert_eq!(command.positions, [], "a command's write puts no position");
    assert_eq!(first.wrote, Some((two_ledgers(0), 1)));
    assert_eq!(first.positions, [], "not the trigger's last target");
    assert_eq!(last.wrote, Some((two_ledgers(1), 1)));
    assert_eq!(
        last.positions,
        [("two-ledgers".to_string(), account.clone(), 3)]
    );
    assert_eq!(bookkeeping.wrote, Some((ledger_cover(), 1)));
    assert_eq!(
        bookkeeping.positions,
        [("bookkeeping".to_string(), account.clone(), 3)]
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 3);
}

/// A step the store fails (here, its write) keeps every write before it,
/// and writes nothing more: its policy's position stays where the last
/// committed step put it. The account is opened with the policy registered,
/// so the position past the opening is an ordinary step's. The call fails
/// with the store's error, to be retried; `Kernel::resume` runs what is
/// owed from each policy's position, and running it again finds nothing
/// more.
#[test]
fn a_step_the_store_failed_is_resumed_by_position() {
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    opened(&store, &registry, 1, 0);
    let account = cover("account", 1);
    assert_eq!(
        position(store.inner(), "bookkeeping", &account),
        2,
        "the opening's own ledger steps put it"
    );
    let before = store.writes().len();
    store.fail_write(1);
    let failure = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ())
        .ready()
        .err()
        .unwrap();
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::Kernel(KernelError::Store(crate::next::StoreError::Unavailable(
                    _
                ))),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(
        pages(store.inner(), &account).len(),
        3,
        "the deposit stands"
    );
    assert_eq!(
        pages(store.inner(), &ledger_cover()).len(),
        2,
        "the opening's two records; the deposit's step landed nothing"
    );
    let writes = store.writes();
    assert_eq!(
        writes[before..]
            .iter()
            .map(|w| (w.wrote.clone(), w.landed))
            .collect::<Vec<_>>(),
        [
            (Some((account.clone(), 1)), true),
            (Some((ledger_cover(), 1)), false),
        ],
        "the command's write, then the step's failed one, and nothing after it"
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 2);

    let resumed = kernel.resume(&store, &at(31), &mut ()).ready().unwrap();
    assert_eq!(
        payloads::<ledger::Recorded>(resumed.books()),
        [ledger::Recorded {
            source_sequence: 2,
            cents: 25
        }]
    );
    assert_eq!(position(store.inner(), "bookkeeping", &account), 3);
    let again = kernel.resume(&store, &at(32), &mut ()).ready().unwrap();
    assert!(again.books().is_empty(), "nothing more owed");
    assert_eq!(pages(store.inner(), &ledger_cover()).len(), 3);
}

/// Account events → a sweep of account 9, which holds nothing: a policy
/// whose target decides no event.
struct SweepNine;

impl crate::next::Policy for SweepNine {
    const NAME: &'static str = "sweep-nine";
    type Source = Account;
    type Target = Account;

    fn on_event(
        &self,
        _: &crate::next::Any,
        _: &crate::next::markup::PolicyContext,
    ) -> Option<crate::next::markup::Commands<Account>> {
        let root = cover("account", 9).root.unwrap();
        Some(crate::next::markup::Commands::none().send(root, &AccountCmd::Sweep))
    }
}

/// A policy step whose target decides no event writes no event, but still
/// puts its policy's position past the trigger, alone: the trigger is
/// handled, and nothing is owed.
#[test]
fn a_policy_step_that_decides_no_event_still_moves_its_position() {
    let mut registry = registry();
    registry.policy(SweepNine);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    kernel
        .handle::<Account, _, _>(&store, open, &at(30), &mut ())
        .ready()
        .unwrap();
    let account = cover("account", 1);
    assert_eq!(
        streams(store.inner()),
        [cover("account", 1)],
        "the sweep wrote no event"
    );
    assert_eq!(
        store.writes(),
        [
            crate::next::recording::WriteRecord {
                wrote: Some((account.clone(), 1)),
                positions: vec![],
                landed: true,
            },
            crate::next::recording::WriteRecord {
                wrote: None,
                positions: vec![("sweep-nine".to_string(), account.clone(), 1)],
                landed: true,
            },
        ],
        "the command's write, then the step's position alone"
    );
    assert_eq!(position(store.inner(), "sweep-nine", &account), 1);
    let resumed = kernel.resume(&store, &at(31), &mut ()).ready().unwrap();
    assert!(resumed.books().is_empty(), "nothing owed");
    assert_eq!(store.writes().len(), 2, "and nothing run again");
}

/// Account deposits → the ledger, answering its refusals: every refusal it
/// is handed, kept.
struct AnsweringBookkeeping(
    std::sync::Arc<std::sync::Mutex<Vec<crate::next::markup::v1::Refusal>>>,
);

impl crate::next::Policy for AnsweringBookkeeping {
    const NAME: &'static str = "answering-bookkeeping";
    type Source = Account;
    type Target = Ledger;
    const ANSWERS_REFUSALS: bool = true;

    fn on_event(
        &self,
        event: &crate::next::Any,
        ctx: &crate::next::markup::PolicyContext,
    ) -> Option<crate::next::markup::Commands<Ledger>> {
        crate::next::Policy::on_event(&Bookkeeping, event, ctx)
    }

    fn on_refused(&self, refusal: &crate::next::markup::v1::Refusal) -> crate::next::Any {
        self.0.lock().unwrap().push(refusal.clone());
        crate::next::markup::__pass_through(refusal)
    }
}

/// A refusal is handed to its policy with the triggering event: its
/// stream, and its page as stored, at its own sequence (here 2, past the
/// opening).
#[test]
fn a_refusal_carries_its_triggering_event() {
    let plain = registry();
    let mut registry = registry();
    let refusals = std::sync::Arc::default();
    registry.policy(AnsweringBookkeeping(std::sync::Arc::clone(&refusals)));
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel
        .handle::<Account, _, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30), &mut ())
        .ready()
        .unwrap();
    let account = cover("account", 1);
    let refusals = refusals.lock().unwrap();
    let [refusal] = refusals.as_slice() else {
        panic!("one refusal: {refusals:?}");
    };
    let event = refusal.event.clone().unwrap();
    assert_eq!(event.cover, Some(account.clone()));
    assert_eq!(
        event.pages,
        [pages(&store, &account)[2].clone()],
        "the deposit's page, as stored"
    );
    assert_eq!(event.pages[0].sequence, 2);
}

/// A crash between a command's commit and its policy steps leaves them
/// owed. A client's retry of the command is answered as a duplicate, and
/// settles what its stream still owes first, so the retry finishes the
/// chain.
#[test]
fn a_retry_after_a_crash_settles_what_its_stream_still_owes() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    opened(&store, &plain, 1, 0);
    store.fail_write(1);
    assert!(kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut ())
        .ready()
        .is_err());
    let retried = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(31), &mut ())
        .ready()
        .unwrap();
    assert!(matches!(retried, crate::next::Handled::Duplicate(_)));
    assert_eq!(
        pages(store.inner(), &ledger_cover()).len(),
        1,
        "the owed step ran"
    );
    assert_eq!(
        position(store.inner(), "bookkeeping", &cover("account", 1)),
        3
    );
}

/// A policy added to a domain with history owes it everything: `resume`
/// runs it from the start (position 0), one step per event.
#[test]
fn resume_runs_a_new_policy_over_its_sources_history() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 7);
    opened(&store, &plain, 2, 9);
    let resumed = kernel.resume(&store, &at(40), &mut ()).ready().unwrap();
    assert_eq!(
        payloads::<ledger::Recorded>(resumed.books())
            .iter()
            .map(|r| (r.source_sequence, r.cents))
            .collect::<Vec<_>>(),
        [(0, 0), (1, 7), (0, 0), (1, 9)],
        "every account's events, account by account, in order"
    );
    for n in [1, 2] {
        assert_eq!(position(&store, "bookkeeping", &cover("account", n)), 2);
    }
}

/// A step that landed before its position was put (or a position put back)
/// is run again from the position and deduplicated by its derived command
/// id: nothing lands twice.
#[test]
fn a_resumed_step_that_already_landed_is_a_duplicate() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &plain, 1, 0);
    kernel.resume(&store, &at(30), &mut ()).ready().unwrap();
    let account = cover("account", 1);
    let key = crate::next::PositionKey {
        handler: "bookkeeping",
        stream: sref(&account),
    };
    store.write(Write::positions(&[(key, 0)])).ready().unwrap();
    let resumed = kernel.resume(&store, &at(31), &mut ()).ready().unwrap();
    assert!(resumed.books().iter().all(|b| b.pages.is_empty()));
    assert_eq!(pages(&store, &ledger_cover()).len(), 2, "nothing twice");
    assert_eq!(position(&store, "bookkeeping", &account), 2);
}

/// A command whose write the store fails is the store's error, and nothing
/// is written (or run after it).
#[test]
fn a_store_that_cannot_write_fails_the_command_with_nothing_written() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = Recording::new();
    store.fail_write(0);
    let failure = kernel
        .handle::<Account, _, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
        .ready()
        .err()
        .unwrap();
    assert!(matches!(
        failure,
        Failure::Kernel(KernelError::Store(crate::next::StoreError::Unavailable(_)))
    ));
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(streams(store.inner()), []);
}

/// A domain named "account" that is not the account: a projection routed
/// through it finds the chain's account stream of another type.
struct Impostor;

impl crate::next::Domain for Impostor {
    type State = crate::next::toys::LedgerState;
    type Command = std::convert::Infallible;
    type Fact = std::convert::Infallible;
    type Event = std::convert::Infallible;
    type Rejection = std::convert::Infallible;
    type Rules = ();

    const NAME: &'static str = "account";

    fn apply(_: &mut crate::next::toys::LedgerState, event: &std::convert::Infallible, (): &()) {
        match *event {}
    }
}

struct Unseen;

impl crate::next::Projection<Impostor> for Unseen {
    fn project(&mut self, _: &crate::next::Committed<'_, Impostor>) {}
}

#[test]
fn a_projection_that_fails_is_reported_and_the_commit_stands() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let mut deposits = Deposits::default();
    let mut routes = (
        crate::next::ProjectionRoute {
            name: "impostor",
            projection: &mut Unseen,
            domain: std::marker::PhantomData::<fn() -> Impostor>,
        },
        crate::next::ProjectionRoute {
            name: "deposits",
            projection: &mut deposits,
            domain: std::marker::PhantomData,
        },
    );
    let handled = kernel
        .handle::<Account, _, _>(&store, deposit(3, 25), &at(30), &mut routes)
        .ready()
        .unwrap();
    let failure = crate::next::ProjectorFailure {
        projector: "impostor",
        error: KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "account",
        },
    };
    assert_eq!(handled.outcome().projector_failures.len(), 1);
    let crate::next::Handled::Applied(recorded) = handled else {
        panic!("applied");
    };
    assert_eq!(recorded.projector_failures(), [failure]);
    assert_eq!(deposits.0, [25], "the other projection still ran");
    assert_eq!(
        slice(&store, &cover("account", 1)).next_sequence,
        3,
        "the commit stands"
    );
}
