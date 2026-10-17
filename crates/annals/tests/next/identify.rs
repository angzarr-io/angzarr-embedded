//! Step 2: ids are assigned once, after the check, as angzarr assigns them.
//! These run: identification is implemented.

use annals::next::v1::event_meta::Causation;
use annals::next::v1::{EventMeta, FactOrigin};
use annals::next::{check, ids, pack, Identified};

use crate::toys::{
    account, at, command, command_id, correlation, cover, fact, instant, minted, registry, Edit,
};

fn identified(book: annals::next::v1::CommandBook, seconds: i64) -> Identified {
    check(book, &registry()).unwrap().identify(&at(seconds))
}

fn deposit() -> annals::next::Any {
    pack(&account::cmd::Deposit { cents: 5 })
}

#[test]
fn a_client_command_without_ids_gets_the_minted_one_as_both() {
    let id = identified(command(cover("account", 1), deposit()), 30);
    assert_eq!(
        id.command_id(),
        &minted(30),
        "the application's UUIDv7, from Sampled"
    );
    assert_eq!(
        id.correlation_id(),
        &minted(30),
        "an absent correlation is the command id"
    );
    assert_eq!(id.causation_id(), None);
}

#[test]
fn a_client_command_keeps_the_ids_it_sent() {
    let id = identified(
        command(cover("account", 1), deposit()).id(7).correlated(8),
        30,
    );
    assert_eq!(id.command_id(), &command_id(7), "reused verbatim on retry");
    assert_eq!(id.correlation_id(), &correlation(8));

    let id = identified(command(cover("account", 1), deposit()).id(7), 31);
    assert_eq!(
        id.correlation_id(),
        &command_id(7),
        "every retry of a client-keyed command lands in one correlation"
    );
}

#[test]
fn a_fact_is_identified_by_its_external_id() {
    let payment = pack(&account::fact::PaymentReceived { cents: 70 });
    let id = identified(fact(cover("account", 1), payment.clone(), "pi_1234"), 30);
    let derived = "917f26ae-ea2e-58af-86de-77013a69617e"
        .parse()
        .expect("types.proto's worked example");
    assert_eq!(id.command_id(), &derived, "UUIDv5 of the external id");
    assert_eq!(id.command_id(), &ids::fact_command_id("pi_1234"));
    assert_eq!(
        id.correlation_id(),
        &derived,
        "and, unsupplied, its correlation"
    );
    assert_ne!(id.command_id(), &minted(30), "nothing minted for a fact");

    let id = identified(
        fact(cover("account", 1), payment, "pi_1234").correlated(3),
        30,
    );
    assert_eq!(id.command_id(), &derived);
    assert_eq!(id.correlation_id(), &correlation(3), "a sent one is kept");
}

#[test]
fn every_event_of_a_command_carries_it_as_causation_and_no_actor() {
    let id = identified(command(cover("account", 1), deposit()).id(7), 30);
    assert_eq!(
        id.meta(instant(31), "v2"),
        EventMeta {
            causation: Some(Causation::CommandId(command_id(7))),
            fact: None,
            correlation_id: Some(command_id(7)),
            recorded_at: Some(instant(31)),
            rules_version: "v2".to_string(),
        }
    );

    let payment = pack(&account::fact::PaymentReceived { cents: 70 });
    let id = identified(fact(cover("account", 1), payment, "pi_9"), 30);
    assert_eq!(
        id.meta(instant(31), "v2").fact,
        Some(FactOrigin {
            external_id: "pi_9".to_string(),
            description: "toy payments webhook".to_string(),
        }),
        "a fact's events carry its source"
    );
}

#[test]
fn an_identified_book_checks_again_and_identifies_the_same() {
    let registry = registry();
    for book in [
        command(cover("account", 1), deposit()),
        command(cover("account", 1), deposit()).id(2).correlated(3),
        fact(
            cover("account", 1),
            pack(&account::fact::PaymentReceived { cents: 1 }),
            "pi_2",
        ),
    ] {
        let first = check(book, &registry).unwrap().identify(&at(40));
        let again = check(first.to_book(), &registry)
            .expect("its ids are well formed, a fact's the derived one")
            .identify(&at(41));
        assert_eq!(
            again, first,
            "a DLQ entry or an audit line re-reads as sent"
        );
    }
}

#[test]
fn recorded_at_is_raised_to_the_streams_last_never_lowered() {
    use annals::next::{not_before, Timestamp};
    let later = Timestamp {
        seconds: 40,
        nanos: 5,
    };
    assert_eq!(not_before(None, instant(30)), instant(30));
    assert_eq!(
        not_before(Some(&later), instant(40)),
        later,
        "a clock step backwards never makes a later event look older"
    );
    assert_eq!(not_before(Some(&instant(20)), instant(30)), instant(30));
}
