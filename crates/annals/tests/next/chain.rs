//! The atomic in-process chain: a command, its events and every policy
//! reaction they set off, committed in one unit of work or not at all; a
//! reaction's rejection fails the origin (a documented divergence from
//! angzarr).

use annals::next::v1::event_meta::Causation;
use annals::next::v1::{CommandBook, EventMeta, EventRef};
use annals::next::{
    ids, EventStore, Failure, InMemoryStore, Kernel, KernelError, MemoryDeadLetters, Payload,
    Recovery, Routes, Step, StepError,
};

use crate::toys::{
    at, command, cover, cover_in, ledger, ledger_root, opened, payloads, registry, Account,
    AccountCmd, Bookkeeping, Edit, Ledger, LEDGER_LIMIT,
};

fn deposit(n: u128, cents: i64) -> CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

fn ledger_cover() -> annals::next::v1::Cover {
    annals::next::v1::Cover {
        domain: "ledger".to_string(),
        root: Some(ledger_root()),
        edition: None,
    }
}

#[test]
#[ignore = "proposal: a policy's command is identified from its trigger; its events name the command, not the trigger"]
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
        Some(crate::toys::correlation(8)),
        "the trigger's"
    );
    assert_eq!(
        meta.recorded_at.unwrap().seconds,
        30,
        "one instant per chain"
    );
}

#[test]
#[ignore = "proposal: a rejection downstream fails the origin and leaves every stream untouched"]
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
#[ignore = "proposal: a chain deeper than its limit is refused"]
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
#[ignore = "proposal: a chain in an edition touches only that edition"]
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
#[ignore = "proposal: a book for another domain than the handler's is refused"]
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
