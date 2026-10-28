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

/// No edition registry exists yet, so the store's units have created no
/// edition: a command naming one is refused, as a read naming one is,
/// rather than given a stream of its own with no main history.
#[test]
fn a_command_to_an_edition_is_refused_and_writes_nothing() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    let routes = Routes::new().policy::<Bookkeeping>();
    let stream = cover_in("account", 1, "what-if");
    let open = command(stream.clone(), AccountCmd::Open.to_any());
    let failure = kernel
        .handle::<Account, _, _>(store.begin(), &routes, open, &at(30), &mut ())
        .err()
        .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Store(
            crate::next::StoreError::UnknownEdition { stream }
        ))
    );
    assert_eq!(failure.recovery(), Recovery::Invalid);
    assert_eq!(store.streams(), [], "nothing written");
}

#[test]
fn a_policys_target_is_in_its_sources_edition() {
    let target = crate::next::target_cover::<Bookkeeping>(&cover_in("account", 1, "what-if"), &());
    assert_eq!(
        target.edition.as_deref(),
        Some("what-if"),
        "a policy never crosses timelines"
    );
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
    let mut chain = crate::next::handle::chain::Chain::new(&kernel, &mut unit, &routes, &sampled);
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

/// Account deposits → the ledger, and nothing else: a policy that targets
/// from the event, so the ledger is owed only some of the account's events.
struct DepositsOnly;

impl crate::next::Policy for DepositsOnly {
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
fn a_filtered_policy_reaches_its_target_only_for_the_events_it_targets() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    let routes = Routes::new().policy::<DepositsOnly>();
    for book in [
        command(cover("account", 1), AccountCmd::Open.to_any()).id(1),
        deposit(2, 5),
        command(cover("account", 1), AccountCmd::Withdraw(2).to_any()).id(3),
        deposit(4, 7),
    ] {
        kernel
            .handle::<Account, _, _>(store.begin(), &routes, book, &at(30), &mut ())
            .unwrap();
    }
    let pages = store.read(&ledger_cover()).unwrap().unwrap().pages;
    let book = crate::next::v1::EventBook {
        pages,
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
}

#[test]
fn a_command_on_a_staged_stream_owes_only_its_own_events() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<Bookkeeping>();
    let mut unit = store.begin();
    let sampled = at(30);
    let mut chain = crate::next::handle::chain::Chain::new(&kernel, &mut unit, &routes, &sampled);
    for n in [3, 4] {
        chain
            .execute::<Account>(crate::next::check(deposit(n, 1), &registry).unwrap())
            .unwrap();
    }
    let recorded = chain.append().unwrap();
    let ledger = recorded.events::<Ledger>(&ledger_cover()).unwrap();
    assert_eq!(
        ledger
            .iter()
            .map(|e| e.event().source_sequence)
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
}

/// A policy as a handler of its source hands over one order per target,
/// in target order: each indexed by its place, keeping the trigger's
/// correlation.
#[test]
fn a_policys_orders_are_indexed_and_keep_the_triggers_correlation() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let source = account_stream(&mut store, &registry);
    let mut orders = Vec::new();
    let mut handler = crate::next::policy::<TwoLedgers, _>(|order| orders.push(order));
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

    fn command(_: &u8, c: &crate::next::Committed<'_, Account>) -> ledger::Record {
        ledger::Record {
            source_sequence: c.sequence(),
            cents: 0,
        }
    }
}

#[test]
fn a_triggers_commands_are_indexed_in_target_order() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = Routes::new().policy::<TwoLedgers>();
    kernel
        .handle::<Account, _, _>(store.begin(), &routes, deposit(3, 5), &at(30), &mut ())
        .unwrap();
    let trigger = EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    for index in [0u8, 1] {
        let pages = store.read(&two_ledgers(index)).unwrap().unwrap().pages;
        assert_eq!(
            pages[0].meta.as_ref().unwrap().causation,
            Some(Causation::CommandId(
                ids::reaction_command_id(&trigger, "two-ledgers", u32::from(index)).unwrap()
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

/// Several reactions on one source domain, through the builder: each runs
/// for every event the chain stages in that domain, in the order they were
/// registered, and everything lands in one unit of work.
#[test]
fn every_policy_on_a_source_runs_for_each_of_its_events_in_registration_order() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    let routes = Routes::new().policy::<TwoLedgers>().policy::<Bookkeeping>();
    let mut unit = store.begin();
    let sampled = at(30);
    let mut chain = crate::next::handle::chain::Chain::new(&kernel, &mut unit, &routes, &sampled);
    let open = command(cover("account", 1), AccountCmd::Open.to_any()).id(1);
    for book in [open, deposit(2, 5)] {
        chain
            .execute::<Account>(crate::next::check(book, &registry).unwrap())
            .unwrap();
    }
    let recorded = chain.append().unwrap();
    drop(unit);
    assert!(
        store.streams().is_empty(),
        "all of it in the one unit of work, dropped uncommitted"
    );

    assert_eq!(
        recorded
            .books()
            .iter()
            .map(|b| b.cover.clone().unwrap())
            .collect::<Vec<_>>(),
        [
            cover("account", 1),
            two_ledgers(0),
            two_ledgers(1),
            ledger_cover()
        ],
        "first touched in registration order: TwoLedgers' targets, then Bookkeeping's"
    );
    for target in [two_ledgers(0), two_ledgers(1), ledger_cover()] {
        assert_eq!(
            recorded
                .events::<Ledger>(&target)
                .unwrap()
                .iter()
                .map(|e| e.event().source_sequence)
                .collect::<Vec<_>>(),
            [0, 1],
            "{target:?} reached for each account event"
        );
    }
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
