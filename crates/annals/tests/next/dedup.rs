//! Step 3: deduplication by `command_id` on the events, with its accepted
//! costs. No claims, no fingerprints, no commands table.

use annals::next::v1::DeliveryOutcome;
use annals::next::{Failure, Handled, InMemoryStore, Payload};

use crate::toys::{
    at, command, cover, fact, handle, opened, payloads, registry, AccountCmd, AccountEvent,
    AccountFact, Edit,
};

fn deposit(n: u128, cents: i64) -> annals::next::v1::CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

#[test]
fn a_retried_command_is_a_duplicate_answered_with_its_events() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);

    let first =
        handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(20)).unwrap();
    assert_eq!(first.outcome().delivery, DeliveryOutcome::Applied as i32);
    let retry =
        handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(21)).unwrap();
    assert_eq!(retry.outcome().delivery, DeliveryOutcome::Duplicate as i32);
    assert!(matches!(retry, Handled::Duplicate(_)));
    assert_eq!(
        retry.books(),
        first.books(),
        "the first delivery's events, re-read"
    );
    assert_eq!(
        payloads::<AccountEvent>(&retry.books()),
        [AccountEvent::Deposited(25)]
    );
    let page = &retry.books()[0].pages[0];
    assert_eq!(
        page.meta.as_ref().unwrap().recorded_at.unwrap().seconds,
        20,
        "nothing re-stamped"
    );
}

#[test]
fn a_zero_event_decision_is_decided_again() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);

    // A sweep of an empty account withdraws nothing: applied, no events.
    let sweep = || command(cover("account", 1), AccountCmd::Sweep.to_any()).id(7);
    let first = handle::<crate::toys::Account>(&mut store, &registry, sweep(), &at(20)).unwrap();
    assert_eq!(first.outcome().delivery, DeliveryOutcome::Applied as i32);
    assert!(payloads::<AccountEvent>(&first.books()).is_empty());

    // Money arrives; the same sweep is redelivered.
    let payment = fact(
        cover("account", 1),
        AccountFact::PaymentReceived(50).to_any(),
        "pi_1",
    );
    handle::<crate::toys::Account>(&mut store, &registry, payment, &at(21)).unwrap();
    let again = handle::<crate::toys::Account>(&mut store, &registry, sweep(), &at(22)).unwrap();
    assert_eq!(
        again.outcome().delivery,
        DeliveryOutcome::Applied as i32,
        "the accepted cost: re-decided against the stream as it is now"
    );
    assert_eq!(
        payloads::<AccountEvent>(&again.books()),
        [AccountEvent::Withdrew(49)],
        "50 less the v2 fee"
    );
}

#[test]
fn a_reused_command_id_with_another_payload_is_answered_with_the_first() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);

    handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(20)).unwrap();
    let reused =
        handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 999), &at(21)).unwrap();
    assert_eq!(
        reused.outcome().delivery,
        DeliveryOutcome::Duplicate as i32,
        "the accepted cost: payloads are not fingerprinted"
    );
    assert_eq!(
        payloads::<AccountEvent>(&reused.books()),
        [AccountEvent::Deposited(25)]
    );
}

#[test]
fn the_same_command_id_to_another_stream_is_another_command() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 100);
    opened(&mut store, &registry, 2, 100);

    handle::<crate::toys::Account>(&mut store, &registry, deposit(3, 25), &at(20)).unwrap();
    let other = command(cover("account", 2), AccountCmd::Deposit(25).to_any()).id(3);
    let applied = handle::<crate::toys::Account>(&mut store, &registry, other, &at(21)).unwrap();
    assert_eq!(applied.outcome().delivery, DeliveryOutcome::Applied as i32);
}

#[test]
fn a_redelivered_fact_is_a_duplicate() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let payment = || {
        fact(
            cover("account", 1),
            AccountFact::PaymentReceived(50).to_any(),
            "pi_1234",
        )
    };

    handle::<crate::toys::Account>(&mut store, &registry, payment(), &at(20)).unwrap();
    let again = handle::<crate::toys::Account>(&mut store, &registry, payment(), &at(21)).unwrap();
    assert_eq!(again.outcome().delivery, DeliveryOutcome::Duplicate as i32);
    let meta = again.books()[0].pages[0].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(annals::next::v1::event_meta::Causation::CommandId(
            annals::next::ids::fact_command_id("pi_1234")
        )),
        "the id is on the event, where the lookup finds it"
    );
}

#[test]
fn a_rejection_leaves_nothing_and_a_retry_runs_again() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    let early = || command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(4);

    let refused = handle::<crate::toys::Account>(&mut store, &registry, early(), &at(5));
    assert!(matches!(
        refused,
        Err(Failure::Rejected(crate::toys::Refused::NotOpen))
    ));
    assert!(store.covers().is_empty());

    opened(&mut store, &registry, 1, 0);
    let retried = handle::<crate::toys::Account>(&mut store, &registry, early(), &at(20)).unwrap();
    assert_eq!(retried.outcome().delivery, DeliveryOutcome::Applied as i32);
}
