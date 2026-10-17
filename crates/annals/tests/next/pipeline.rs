//! The normative order: validate → identify → deduplicate → load and
//! resolve → decode (and upcast) → decide or ingest. Each test pins one
//! boundary by sending something that two adjacent steps would answer
//! differently.

use annals::next::v1::{ConflictResolution, DeliveryOutcome, EventRef};
use annals::next::{
    check, pack, Coordinator, Deduplicated, Failure, InMemoryStore, KernelError, Malformed,
    Payload, Recovery,
};

use crate::toys::{
    account, at, command, cover, handle, opened, registry, Account, AccountCmd, Edit,
};

fn deposit(n: u128) -> annals::next::v1::CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(n)
}

#[test]
fn an_applied_strict_retry_is_a_duplicate_never_stale() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);

    let at_head = || deposit(3).expecting(2, ConflictResolution::Strict);
    handle::<Account>(&mut store, &registry, at_head(), &at(20)).unwrap();
    handle::<Account>(&mut store, &registry, deposit(4), &at(21)).unwrap();

    // The stream is at 4 now; the retry still expects 2.
    let retry = handle::<Account>(&mut store, &registry, at_head(), &at(22)).unwrap();
    assert_eq!(
        retry.outcome().delivery,
        DeliveryOutcome::Duplicate as i32,
        "the retry's events are found before its sequence is checked"
    );

    // A new command expecting 2 is stale, as STRICT says.
    let stale = handle::<Account>(
        &mut store,
        &registry,
        deposit(5).expecting(2, ConflictResolution::Strict),
        &at(23),
    )
    .err()
    .unwrap();
    assert_eq!(
        stale,
        Failure::Kernel(KernelError::Stale {
            stream: cover("account", 1),
            expected: 2,
            actual: 4
        })
    );
    assert_eq!(stale.recovery(), Recovery::Retry);
}

#[test]
fn a_malformed_retry_of_an_applied_command_is_still_malformed() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    handle::<Account>(&mut store, &registry, deposit(3), &at(20)).unwrap();

    let forged = deposit(3).caused_by(EventRef {
        stream: Some(cover("account", 1)),
        sequence: 0,
    });
    let refused = handle::<Account>(&mut store, &registry, forged, &at(21))
        .err()
        .unwrap();
    assert_eq!(
        refused,
        Failure::Kernel(KernelError::Malformed(Malformed::CausationFromOutside)),
        "refused before anything is looked up"
    );
    assert_eq!(refused.recovery(), Recovery::Invalid);
}

#[test]
fn an_unknown_command_type_is_refused_at_decode() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    let unknown = || command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 }));

    let refused = handle::<Account>(&mut store, &registry, unknown().id(8), &at(20))
        .err()
        .unwrap();
    assert_eq!(
        refused,
        Failure::Kernel(KernelError::UnknownType {
            stream: cover("account", 1),
            type_url: "/toy.account.v9.Bonus".to_string()
        }),
        "named by its type URL"
    );
    assert_eq!(refused.recovery(), Recovery::Invalid);

    // Stale and undecodable: STRICT's check comes first.
    let stale = handle::<Account>(
        &mut store,
        &registry,
        unknown().id(9).expecting(1, ConflictResolution::Strict),
        &at(21),
    )
    .err()
    .unwrap();
    assert!(matches!(stale, Failure::Kernel(KernelError::Stale { .. })));

    // Applied under that id already: the duplicate is found first, the
    // payload never decoded (and never compared: a reused id goes
    // undetected).
    let dup = handle::<Account>(
        &mut store,
        &registry,
        command(cover("account", 1), pack(&account::v9::Bonus { cents: 1 })).id(2),
        &at(22),
    )
    .unwrap();
    assert_eq!(dup.outcome().delivery, DeliveryOutcome::Duplicate as i32);
}

#[test]
fn the_coordinator_runs_dedup_then_load_then_decode() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    let mut unit = store.begin();
    let mut coordinator = Coordinator::new(&mut unit, &registry);

    // An applied command: step 3 answers it.
    let applied = check(deposit(2), &registry).unwrap().identify(&at(30));
    assert!(matches!(
        coordinator.deduplicate(applied).unwrap(),
        Deduplicated::Duplicate { .. }
    ));

    // A new one goes on: stamped at the head, STRICT, no window, no nonce.
    let novel = check(deposit(3), &registry).unwrap().identify(&at(30));
    let Deduplicated::Novel(novel) = coordinator.deduplicate(novel).unwrap() else {
        panic!("never applied");
    };
    let contextual = coordinator.load(novel).unwrap();
    assert_eq!(contextual.conflict_resolution(), ConflictResolution::Strict);
    assert_eq!(contextual.expected_sequence(), 2, "stamped with the head");
    assert_eq!(contextual.head(), 2);
    assert!(contextual.window().is_empty());
    assert!(!contextual.has_nonce());

    let prepared = contextual.decode::<Account>(&registry).unwrap();
    assert_eq!(prepared.stream().state().cents, 100);
}

#[test]
fn the_coordinator_hands_a_windowed_command_its_window_and_a_nonce() {
    use crate::toys::{set, Shelf};
    let registry = registry();
    let mut store = InMemoryStore::new();
    for (n, item) in ["a", "b", "c"].into_iter().enumerate() {
        handle::<Shelf>(
            &mut store,
            &registry,
            command(cover("shelf", 3), set(item, 1)).id(n as u128 + 1),
            &at(n as i64),
        )
        .unwrap();
    }
    let mut unit = store.begin();
    let mut coordinator = Coordinator::new(&mut unit, &registry);
    let book = command(cover("shelf", 3), set("d", 1))
        .id(9)
        .expecting(1, ConflictResolution::AggregateHandles);
    let identified = check(book, &registry).unwrap().identify(&at(30));
    let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
        panic!("never applied");
    };
    assert_eq!(novel.command().command_id(), &crate::toys::command_id(9));
    let contextual = coordinator.load(novel).unwrap();
    assert_eq!(
        contextual.conflict_resolution(),
        ConflictResolution::AggregateHandles
    );
    assert_eq!(contextual.expected_sequence(), 1);
    assert_eq!(contextual.head(), 3);
    assert_eq!(contextual.pages().len(), 3);
    assert_eq!(
        contextual
            .window()
            .iter()
            .map(|p| p.sequence)
            .collect::<Vec<_>>(),
        [1, 2],
        "from the expected sequence to the head"
    );
    assert!(contextual.has_nonce());
    assert!(contextual.snapshot().is_none());
    assert_eq!(
        contextual.command().command_id(),
        &crate::toys::command_id(9)
    );
}

#[test]
fn the_coordinators_decision_commits_to_its_stream() {
    let registry = registry();
    let log = annals::next::MemoryDeadLetters::new();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    let mut unit = store.begin();
    let mut coordinator = Coordinator::new(&mut unit, &registry);
    let identified = check(deposit(3), &registry).unwrap().identify(&at(30));
    let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
        panic!("never applied");
    };
    let mut prepared = coordinator
        .load(novel)
        .unwrap()
        .decode::<Account>(&registry)
        .unwrap();
    assert_eq!(prepared.command().command_id(), &crate::toys::command_id(3));
    let decision = prepared.decide(&at(30), &registry, &log).unwrap();
    assert_eq!(decision.stream(), &cover("account", 1));
    assert_eq!(decision.head(), 2);
    assert_eq!(decision.command_id(), &crate::toys::command_id(3));
    assert_eq!(decision.events().len(), 1);
    assert_eq!(decision.events()[0].sequence(), 2);
    assert!(!decision.is_empty());
    let mut stream = prepared.into_stream();
    assert_eq!(stream.commit(decision, &registry, &mut []), Ok(3));
    assert_eq!(stream.state().cents, 105);

    // A decision against the old head no longer commits.
    let mut again = Coordinator::new(&mut unit, &registry);
    let identified = check(deposit(4), &registry).unwrap().identify(&at(31));
    let Deduplicated::Novel(novel) = again.deduplicate(identified).unwrap() else {
        panic!("never applied");
    };
    let mut prepared = again
        .load(novel)
        .unwrap()
        .decode::<Account>(&registry)
        .unwrap();
    let decision = prepared.decide(&at(31), &registry, &log).unwrap();
    assert_eq!(
        stream.commit(decision, &registry, &mut []),
        Err(KernelError::Stale {
            stream: cover("account", 1),
            expected: 2,
            actual: 3
        })
    );

    drop(unit);
    opened(&mut store, &registry, 2, 0);
    let mut unit = store.begin();
    let sweep = check(
        command(cover("account", 2), AccountCmd::Sweep.to_any()).id(5),
        &registry,
    )
    .unwrap()
    .identify(&at(32));
    let mut empty = Coordinator::new(&mut unit, &registry);
    let Deduplicated::Novel(novel) = empty.deduplicate(sweep).unwrap() else {
        panic!("never applied");
    };
    let mut prepared = empty
        .load(novel)
        .unwrap()
        .decode::<Account>(&registry)
        .unwrap();
    let decision = prepared.decide(&at(32), &registry, &log).unwrap();
    assert!(decision.is_empty(), "a sweep of nothing withdraws nothing");

    // A cover of another domain than the one decoded for.
    let mut wrong = Coordinator::new(&mut unit, &registry);
    let identified = check(deposit(6), &registry).unwrap().identify(&at(33));
    let Deduplicated::Novel(novel) = wrong.deduplicate(identified).unwrap() else {
        panic!("never applied");
    };
    assert_eq!(
        wrong
            .load(novel)
            .unwrap()
            .decode::<crate::toys::Ledger>(&registry)
            .err(),
        Some(KernelError::WrongDomain {
            stream: cover("account", 1),
            expected: "ledger"
        })
    );
}
