//! Facts: ingested, never decided and never refused, with a read-only
//! context.

use annals::next::v1::CommandBook;
use annals::next::{ids, InMemoryStore, Payload};

use crate::toys::{
    account, at, command, cover, fact, handle, opened, payloads, registry, Account, AccountCmd,
    AccountEvent, AccountFact,
};

fn payment(cents: i64, external_id: &str) -> CommandBook {
    fact(
        cover("account", 1),
        AccountFact::PaymentReceived(cents).to_any(),
        external_id,
    )
}

#[test]
#[ignore = "proposal: ingest reads its recorded instant, ids, source and rules from a read-only context"]
fn ingest_gets_a_read_only_context() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);

    let handled = handle::<Account>(&mut store, &registry, payment(70, "pi_7"), &at(40)).unwrap();
    assert_eq!(
        payloads::<AccountEvent>(&handled.books()),
        [AccountEvent::Received(account::Received {
            cents: 70,
            external_id: "pi_7".to_string(),
            recorded_seconds: 40,
            rules_version: "v2".to_string(),
            matched: true,
        })],
        "the context's recorded_at, source and rules version, copied by the toy"
    );
    let meta = handled.books()[0].pages[0].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(annals::next::v1::event_meta::Causation::CommandId(
            ids::fact_command_id("pi_7")
        ))
    );
    assert_eq!(meta.fact.unwrap().external_id, "pi_7");
}

#[test]
#[ignore = "proposal: a fact the domain cannot use is recorded, never refused"]
fn a_fact_is_never_refused() {
    let registry = registry();
    let mut store = InMemoryStore::new();

    // The account was never opened: a Deposit command would be refused.
    let handled = handle::<Account>(&mut store, &registry, payment(70, "pi_8"), &at(40)).unwrap();
    let [AccountEvent::Received(received)] = &payloads::<AccountEvent>(&handled.books())[..] else {
        panic!("one event");
    };
    assert!(!received.matched, "recorded as unmatched");
    assert!(
        handle::<Account>(
            &mut store,
            &registry,
            command(cover("account", 1), AccountCmd::Deposit(70).to_any()),
            &at(41)
        )
        .is_err(),
        "where the command is refused"
    );
}

#[test]
#[ignore = "proposal: a fact is always stamped at the head, never checked or resolved"]
fn a_fact_is_stamped_at_the_head() {
    let registry = registry();
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let handled = handle::<Account>(&mut store, &registry, payment(1, "pi_9"), &at(40)).unwrap();
    assert_eq!(handled.books()[0].pages[0].sequence, 2);
}
