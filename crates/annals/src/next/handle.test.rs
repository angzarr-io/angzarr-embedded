//! The atomic in-process chain: a command, its events and every policy
//! reaction they set off, committed in one unit of work or not at all; a
//! reaction's rejection fails the origin (a documented divergence from
//! angzarr).

use crate::next::v1::event_meta::Causation;
use crate::next::v1::{CommandBook, EventMeta, EventRef};
use crate::next::{
    ids, EventStore, Failure, InMemoryStore, Kernel, KernelError, MemoryDeadLetters, Payload,
    Recovery, Routes, Step, StepError,
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
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<Bookkeeping>();

    let handled = kernel
        .handle::<Account, _, _>(
            store.begin(),
            &routes,
            deposit(3, 25).correlated(8),
            &at(30),
            &mut (),
        )
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
            ids::reaction_command_id(&trigger, "bookkeeping", 0).unwrap()
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

#[test]
fn a_rejection_downstream_fails_the_whole_chain() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let before = store.read(&cover("account", 1)).unwrap().unwrap();
    let routes = Routes::new().policy::<Bookkeeping>();

    let failure = kernel
        .handle::<Account, _, _>(
            store.begin(),
            &routes,
            deposit(3, LEDGER_LIMIT + 1),
            &at(30),
            &mut (),
        )
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
        matches!(error, StepError::Rejected(r) if annals_diagnostic::Diagnostic::code(r) == "toy-ledger-too-large")
    );
    assert_eq!(failure.recovery(), Recovery::Business);
    assert_eq!(
        store.read(&cover("account", 1)).unwrap().unwrap(),
        before,
        "the origin's deposit was not appended"
    );
    assert_eq!(store.read(&ledger_cover()).unwrap(), None);

    // The retry with the same id runs again: a rejection wrote nothing.
    let retried = kernel.handle::<Account, _, _>(
        store.begin(),
        &routes,
        deposit(3, LEDGER_LIMIT + 1),
        &at(31),
        &mut (),
    );
    assert!(retried.is_err());
}

#[test]
fn a_chain_deeper_than_its_limit_is_refused() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log).limit(0);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<Bookkeeping>();
    let failure = kernel
        .handle::<Account, _, _>(store.begin(), &routes, deposit(3, 1), &at(30), &mut ())
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
}

#[test]
fn a_chain_in_an_edition_stays_in_it() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    let routes = Routes::new().policy::<Bookkeeping>();
    let open = command(cover_in("account", 1, "what-if"), AccountCmd::Open.to_any());
    let handled = kernel
        .handle::<Account, _, _>(store.begin(), &routes, open, &at(30), &mut ())
        .unwrap();
    for book in handled.books() {
        assert_eq!(
            book.cover.unwrap().edition.as_deref(),
            Some("what-if"),
            "a reaction never crosses timelines"
        );
    }
}

#[test]
fn a_book_for_another_domain_is_refused() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    let routes = Routes::new();
    let failure = kernel
        .handle::<Ledger, _, _>(
            store.begin(),
            &routes,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
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

/// Trigger edges, as the application's log would keep them.
#[derive(Default)]
struct Edges(std::sync::Mutex<Vec<(EventRef, crate::next::v1::Uuid, crate::next::v1::Cover)>>);

impl crate::next::TriggerLog for Edges {
    fn dispatched(
        &self,
        causation_id: &EventRef,
        command_id: &crate::next::v1::Uuid,
        target: &crate::next::v1::Cover,
    ) {
        self.0
            .lock()
            .unwrap()
            .push((causation_id.clone(), command_id.clone(), target.clone()));
    }
}

#[test]
fn each_reaction_command_reports_its_trigger_edge() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let edges = Edges::default();
    let kernel = Kernel::new(&registry, &log).triggers(&edges);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<Bookkeeping>();
    kernel
        .handle::<Account, _, _>(store.begin(), &routes, deposit(3, 25), &at(30), &mut ())
        .unwrap();

    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    assert_eq!(
        *edges.0.lock().unwrap(),
        [(
            trigger.clone(),
            ids::reaction_command_id(&trigger, "bookkeeping", 0).unwrap(),
            ledger_cover()
        )],
        "the edge no event records: the command's causation, its id, its target"
    );

    // The default reports nowhere, and the chain is the same.
    let quiet = Kernel::new(&registry, &log);
    quiet
        .handle::<Account, _, _>(store.begin(), &routes, deposit(4, 5), &at(31), &mut ())
        .unwrap();
    assert_eq!(edges.0.lock().unwrap().len(), 1);
}

#[test]
fn a_chain_that_comes_back_to_a_stream_sees_its_staged_events() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<Bookkeeping>();
    let mut unit = store.begin();
    let sampled = at(30);
    let mut chain = crate::next::Chain::new(&kernel, &mut unit, &routes, &sampled);
    assert_eq!(chain.state::<Ledger>(&ledger_cover()), None, "untouched");

    let first = crate::next::check(deposit(3, 25), &registry).unwrap();
    assert_eq!(chain.execute::<Account>(first.clone()).unwrap(), None);
    assert_eq!(
        chain.state::<Account>(&cover("account", 1)).unwrap().cents,
        25
    );
    assert_eq!(chain.state::<Ledger>(&ledger_cover()).unwrap().total, 25);
    assert_eq!(
        chain.state::<Account>(&ledger_cover()),
        None,
        "not an account"
    );
    assert_eq!(chain.path().len(), 1, "the last step run");

    // The same command again in the same chain: found among the staged
    // events.
    let again = chain.execute::<Account>(first).unwrap().unwrap();
    assert_eq!(again.pages.len(), 1);
    assert_eq!(again.next_sequence, 3);

    // Another, on the staged copy: the ledger stream is staged too.
    let second = crate::next::check(deposit(4, 5), &registry).unwrap();
    assert_eq!(chain.execute::<Account>(second).unwrap(), None);
    assert_eq!(chain.state::<Ledger>(&ledger_cover()).unwrap().total, 30);

    let recorded = chain.append().unwrap();
    assert_eq!(
        recorded
            .books()
            .iter()
            .map(|b| (b.pages.len(), b.next_sequence))
            .collect::<Vec<_>>(),
        [(2, 4), (2, 2)],
        "each stream once, in the order first touched"
    );
    assert_eq!(
        recorded
            .events::<Account>(&cover("account", 1))
            .unwrap()
            .len(),
        2
    );
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

#[test]
fn projections_run_after_commit_over_their_domains_streams() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<Bookkeeping>();
    let mut deposits = Deposits::default();
    let mut ledger_seen = 0;
    let mut route = crate::next::ProjectionRoute {
        name: "deposits",
        projection: &mut deposits,
        domain: std::marker::PhantomData,
    };
    let handled = kernel
        .handle::<Account, _, _>(store.begin(), &routes, deposit(3, 25), &at(30), &mut route)
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
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let mut deposits = Deposits::default();
    let mut route = crate::next::ProjectionRoute {
        name: "deposits",
        projection: &mut deposits,
        domain: std::marker::PhantomData,
    };
    let routes = Routes::new();
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    let handled = kernel
        .handle::<Account, _, _>(store.begin(), &routes, open, &at(30), &mut route)
        .unwrap();
    assert!(matches!(handled, crate::next::Handled::Duplicate(_)));
    assert_eq!(handled.outcome().projector_failures, []);
    assert!(deposits.0.is_empty());
}

#[test]
fn the_async_handler_has_the_same_contract_and_a_send_future() {
    fn send<T: Send>(t: T) -> T {
        t
    }
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<Bookkeeping>();

    let handled = crate::next::toys::ready(send(kernel.handle_async::<Account, _, _>(
        crate::next::Ready(store.begin()),
        &routes,
        deposit(3, 25),
        &at(30),
        &mut (),
    )))
    .unwrap();
    assert_eq!(handled.books().len(), 2);
    assert_eq!(store.read(&ledger_cover()).unwrap().unwrap().pages.len(), 1);

    let again = crate::next::toys::ready(kernel.handle_async::<Account, _, _>(
        crate::next::Ready(store.begin()),
        &routes,
        deposit(3, 25),
        &at(31),
        &mut (),
    ))
    .unwrap();
    assert!(matches!(again, crate::next::Handled::Duplicate(_)));

    let refused = crate::next::toys::ready(kernel.handle_async::<Account, _, _>(
        crate::next::Ready(store.begin()),
        &routes,
        deposit(4, LEDGER_LIMIT + 1),
        &at(32),
        &mut (),
    ));
    assert!(matches!(refused, Err(Failure::Downstream { .. })));
    assert_eq!(store.read(&ledger_cover()).unwrap().unwrap().pages.len(), 1);
    assert_eq!(
        crate::next::toys::ready(kernel.handle_async::<Ledger, _, _>(
            crate::next::Ready(store.begin()),
            &routes,
            deposit(5, 1),
            &at(33),
            &mut (),
        ))
        .err(),
        Some(Failure::Kernel(KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "ledger"
        }))
    );
}

/// The account stream as stored.
fn account_stream(
    store: &mut InMemoryStore,
    registry: &crate::next::Registry,
) -> crate::next::Stream<Account> {
    let pages = store.read(&cover("account", 1)).unwrap().unwrap().pages;
    crate::next::Stream::replay(cover("account", 1), &pages, registry).unwrap()
}

#[test]
fn a_catch_up_delivers_each_owed_order_once_and_in_order() {
    use crate::next::{orders_owed, Handled, Position};
    use std::collections::BTreeMap;

    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 25);
    let routes = Routes::new();
    let source = account_stream(&mut store, &registry);

    let owed = orders_owed::<Bookkeeping>(&source, &registry, &BTreeMap::new()).unwrap();
    let orders = &owed[&()];
    assert_eq!(
        orders
            .iter()
            .map(|o| o.trigger().sequence)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(orders[1].cover(), &ledger_cover());
    assert_eq!(orders[1].index(), 0);
    assert_eq!(orders[1].command().cents, 25);
    assert_eq!(
        orders[1].correlation_id(),
        &crate::next::toys::command_id(2)
    );
    assert_eq!(orders[1].target(), &());

    let skipped = kernel
        .deliver(store.begin(), &routes, &orders[1], &at(40), &mut ())
        .err()
        .unwrap();
    assert_eq!(
        skipped,
        Failure::Kernel(KernelError::Gap {
            stream: ledger_cover(),
            gap: crate::next::Gap {
                expected: 0,
                found: 1
            }
        })
    );
    assert_eq!(skipped.recovery(), Recovery::Retry);

    for order in orders {
        let handled = kernel
            .deliver(store.begin(), &routes, order, &at(40), &mut ())
            .unwrap();
        assert_eq!(handled.books().len(), 1);
    }
    let ledger = store.read(&ledger_cover()).unwrap().unwrap();
    assert_eq!(
        payloads::<ledger::Recorded>(&[crate::next::v1::EventBook {
            pages: ledger.pages.clone(),
            ..Default::default()
        }])
        .len(),
        2
    );
    let meta = ledger.pages[1].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(Causation::CommandId(
            ids::reaction_command_id(orders[1].trigger(), "bookkeeping", 0).unwrap()
        )),
        "the same id the chain would derive"
    );

    // Delivered again: the target's position has passed it.
    let again = kernel
        .deliver(store.begin(), &routes, &orders[1], &at(41), &mut ())
        .unwrap();
    assert!(matches!(&again, Handled::Applied(r) if r.books().is_empty()));
    assert_eq!(store.read(&ledger_cover()).unwrap().unwrap().pages.len(), 2);

    // Nothing more is owed past the ledger's position.
    let positions = BTreeMap::from([((), Position::at(1))]);
    let owed = orders_owed::<Bookkeeping>(&source, &registry, &positions).unwrap();
    assert!(owed.is_empty() || owed[&()].is_empty());
    let positions = BTreeMap::from([((), Position::at(0))]);
    let owed = orders_owed::<Bookkeeping>(&source, &registry, &positions).unwrap();
    assert_eq!(
        owed[&()]
            .iter()
            .map(|o| o.trigger().sequence)
            .collect::<Vec<_>>(),
        [1]
    );
}

/// Account deposits → the ledger, and nothing else: a policy that targets
/// from the event, so the ledger is owed only some of the account's events.
struct DepositsOnly;

impl crate::next::Reaction for DepositsOnly {
    type Source = Account;
    type Target = Ledger;
    type TargetId = ();

    const NAME: &'static str = "deposits-only";

    fn targets(committed: &crate::next::Committed<'_, Account>) -> Vec<()> {
        match committed.event() {
            crate::next::toys::AccountEvent::Deposited(_) => vec![()],
            _ => vec![],
        }
    }

    fn root((): &()) -> crate::next::v1::Uuid {
        ledger_root()
    }

    fn position(target: &crate::next::toys::LedgerState) -> crate::next::Position {
        match target.next_source {
            0 => crate::next::Position::START,
            next => crate::next::Position::at(next - 1),
        }
    }
}

impl crate::next::Policy for DepositsOnly {
    fn command((): &(), c: &crate::next::Committed<'_, Account>) -> ledger::Record {
        let cents = match c.event() {
            crate::next::toys::AccountEvent::Deposited(cents) => *cents,
            _ => 0,
        };
        ledger::Record {
            source_sequence: c.sequence(),
            cents,
        }
    }
}

#[test]
fn a_filtered_policy_keeps_contiguity_per_target() {
    use crate::next::Handled;
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    // 0: opened (not owed), 1: deposited 1; then 2: deposited 2.
    opened(&mut store, &registry, 1, 1);
    crate::next::toys::handle::<Account>(&mut store, &registry, deposit(3, 2), &at(20)).unwrap();
    let source = account_stream(&mut store, &registry);
    let owed = crate::next::orders_owed::<DepositsOnly>(
        &source,
        &registry,
        &std::collections::BTreeMap::new(),
    )
    .unwrap();
    let orders = &owed[&()];
    assert_eq!(
        orders
            .iter()
            .map(|o| (o.trigger().sequence, o.previous()))
            .collect::<Vec<_>>(),
        [(1, None), (2, Some(1))],
        "only the deposits, each naming the one owed before it"
    );
    let routes = Routes::new();

    let skipped = kernel
        .deliver(store.begin(), &routes, &orders[1], &at(30), &mut ())
        .err()
        .unwrap();
    assert_eq!(
        skipped,
        Failure::Kernel(KernelError::Gap {
            stream: ledger_cover(),
            gap: crate::next::Gap {
                expected: 1,
                found: 2
            }
        }),
        "the gap is the target's own owed order, not sequence 0"
    );

    for order in orders {
        kernel
            .deliver(store.begin(), &routes, order, &at(31), &mut ())
            .unwrap();
    }
    assert_eq!(store.read(&ledger_cover()).unwrap().unwrap().pages.len(), 2);
    let again = kernel
        .deliver(store.begin(), &routes, &orders[0], &at(32), &mut ())
        .unwrap();
    assert!(matches!(&again, Handled::Applied(r) if r.books().is_empty()));
}

#[test]
fn a_policy_book_for_an_unregistered_target_is_a_dead_lettered_bug() {
    let mut registry = crate::next::Registry::new();
    registry
        .register::<Account>(
            crate::next::toys::fees(),
            crate::next::Upcaster::new("u", "account"),
        )
        .unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    let routes = Routes::new().policy::<Bookkeeping>();
    let failure = kernel
        .handle::<Account, _, _>(
            store.begin(),
            &routes,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
            &mut (),
        )
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
    assert!(store.streams().is_empty());

    let order_store = &mut InMemoryStore::new();
    crate::next::toys::opened(order_store, &crate::next::toys::registry(), 1, 0);
    let source = account_stream(order_store, &crate::next::toys::registry());
    let owed = crate::next::orders_owed::<Bookkeeping>(
        &source,
        &crate::next::toys::registry(),
        &std::collections::BTreeMap::new(),
    )
    .unwrap();
    let refused = kernel
        .deliver(store.begin(), &routes, &owed[&()][0], &at(31), &mut ())
        .err()
        .unwrap();
    assert!(matches!(
        refused,
        Failure::Kernel(KernelError::Malformed(
            crate::next::Malformed::UnknownDomain { .. }
        ))
    ));
    assert_eq!(log.lines().len(), 2);
}

#[test]
fn a_command_on_a_staged_stream_owes_only_its_own_events() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let edges = Edges::default();
    let kernel = Kernel::new(&registry, &log).triggers(&edges);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<Bookkeeping>();
    let mut unit = store.begin();
    let sampled = at(30);
    let mut chain = crate::next::Chain::new(&kernel, &mut unit, &routes, &sampled);
    for n in [3, 4] {
        chain
            .execute::<Account>(crate::next::check(deposit(n, 1), &registry).unwrap())
            .unwrap();
    }
    assert_eq!(
        edges
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|(trigger, ..)| trigger.sequence)
            .collect::<Vec<_>>(),
        [2, 3],
        "each deposit's own event, once"
    );
}

#[test]
fn a_chain_on_a_stream_loaded_from_a_snapshot_records_only_its_events() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 7);
    let source = account_stream(&mut store, &registry);
    store
        .write_snapshot(&cover("account", 1), &source.snapshot(&registry).unwrap())
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
        .handle::<Account, _, _>(
            store.begin(),
            &Routes::new(),
            deposit(3, 25),
            &at(30),
            &mut (first, second),
        )
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
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 7);
    let source = account_stream(&mut store, &registry);
    let mut covers = Vec::new();
    let mut handler = |c: &crate::next::Committed<'_, Account>| {
        covers.push((c.cover().clone(), c.envelope().sequence()))
    };
    source.dispatch(1, &registry, &mut [&mut handler]).unwrap();
    assert_eq!(covers, [(cover("account", 1), 1)]);
    assert_eq!(crate::next::Position::at(4).last(), Some(4));
    assert_eq!(crate::next::Position::START.last(), None);
}

/// Two ledgers per account event: orders are indexed in target order.
struct TwoLedgers;

impl crate::next::Reaction for TwoLedgers {
    type Source = Account;
    type Target = Ledger;
    type TargetId = u8;

    const NAME: &'static str = "two-ledgers";

    fn targets(_: &crate::next::Committed<'_, Account>) -> Vec<u8> {
        vec![0, 1]
    }

    fn root(n: &u8) -> crate::next::v1::Uuid {
        ids::compute_root("ledger", &n.to_string()).unwrap()
    }

    fn position(_: &crate::next::toys::LedgerState) -> crate::next::Position {
        crate::next::Position::START
    }
}

impl crate::next::Policy for TwoLedgers {
    fn command(_: &u8, c: &crate::next::Committed<'_, Account>) -> ledger::Record {
        ledger::Record {
            source_sequence: c.sequence(),
            cents: 0,
        }
    }
}

#[test]
fn a_triggers_orders_are_indexed_in_target_order() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let source = account_stream(&mut store, &registry);
    let owed = crate::next::orders_owed::<TwoLedgers>(
        &source,
        &registry,
        &std::collections::BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(owed[&0][0].index(), 0);
    assert_eq!(owed[&1][0].index(), 1);
    assert_ne!(owed[&0][0].cover(), owed[&1][0].cover());
}

#[test]
fn a_target_that_holds_an_earlier_order_still_refuses_a_gap() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    // 0: opened (not owed); 1, 2, 3: deposits, each owed.
    opened(&mut store, &registry, 1, 1);
    for (n, cents) in [(3, 2), (4, 3)] {
        crate::next::toys::handle::<Account>(&mut store, &registry, deposit(n, cents), &at(20))
            .unwrap();
    }
    let source = account_stream(&mut store, &registry);
    let owed = crate::next::orders_owed::<DepositsOnly>(
        &source,
        &registry,
        &std::collections::BTreeMap::new(),
    )
    .unwrap();
    let orders = &owed[&()];
    let routes = Routes::new();
    kernel
        .deliver(store.begin(), &routes, &orders[0], &at(30), &mut ())
        .unwrap();
    assert_eq!(
        kernel
            .deliver(store.begin(), &routes, &orders[2], &at(31), &mut ())
            .err(),
        Some(Failure::Kernel(KernelError::Gap {
            stream: ledger_cover(),
            gap: crate::next::Gap {
                expected: 2,
                found: 3
            }
        })),
        "holding 1, owed 2 before 3"
    );
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

    fn decide(
        cmd: &std::convert::Infallible,
        _: &mut crate::next::Tx<'_, Self>,
    ) -> Result<(), std::convert::Infallible> {
        match *cmd {}
    }

    fn ingest(
        _: &crate::next::toys::LedgerState,
        fact: &std::convert::Infallible,
        _: &crate::next::IngestCtx<'_, Self>,
    ) -> Vec<std::convert::Infallible> {
        match *fact {}
    }

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
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
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
        .handle::<Account, _, _>(
            store.begin(),
            &Routes::new(),
            deposit(3, 25),
            &at(30),
            &mut routes,
        )
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
        store
            .read(&cover("account", 1))
            .unwrap()
            .unwrap()
            .next_sequence,
        3,
        "the commit stands"
    );
}
