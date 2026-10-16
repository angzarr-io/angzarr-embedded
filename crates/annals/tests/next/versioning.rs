//! Upcasting at read time, and golden replay fixtures.
//!
//! The account of `toys.rs` is at schema 3. Its events are stored as text:
//!
//! - schema 1: `open`, `dep:N`, `wd:N`
//! - schema 2: `opened`, `deposit N`, `withdraw N`
//! - schema 3: `opened`, `deposited N`, `withdrew N`

use annals::next::{
    assert_replay_fixtures, replay_stored, ChainError, ChainProblem, CommandId, Envelope,
    EventStore, InMemoryStore, Meta, ReplayFixture, RequestId, RulesVersion, Seq, Timestamp,
    UpcastError, Upcaster, Upcasts,
};
use annals_diagnostic::Report;

use crate::toys::{fees, header, Account, AccountEvent, Balance, Fees};

fn one_to_two(_: &Meta, stored: String) -> Result<String, Report> {
    if stored == "open" {
        return Ok("opened".to_string());
    }
    if let Some(n) = stored.strip_prefix("dep:") {
        return Ok(format!("deposit {n}"));
    }
    if let Some(n) = stored.strip_prefix("wd:") {
        return Ok(format!("withdraw {n}"));
    }
    Err(
        Report::new("toy-account-v1-unknown", format!("no v1 event {stored:?}"))
            .arg("event", stored),
    )
}

fn two_to_three(_: &Meta, stored: String) -> Result<String, Report> {
    Ok(stored
        .replacen("deposit ", "deposited ", 1)
        .replacen("withdraw ", "withdrew ", 1))
}

fn decode(stored: String) -> Result<AccountEvent, Report> {
    let bad = || Report::new("toy-account-undecodable", format!("{stored:?}"));
    let cents = |n: &str| n.parse::<i64>().map_err(|_| bad());
    match stored.split_once(' ') {
        None if stored == "opened" => Ok(AccountEvent::Opened),
        Some(("deposited", n)) => Ok(AccountEvent::Deposited(cents(n)?)),
        Some(("withdrew", n)) => Ok(AccountEvent::Withdrew(cents(n)?)),
        _ => Err(bad()),
    }
}

fn upcaster() -> Upcaster<String> {
    Upcasts::new("account", 3)
        .step(2, two_to_three)
        .step(1, one_to_two)
        .build()
        .unwrap()
}

fn stored(seq: Seq, schema: u32, rules: &str, event: &str) -> Envelope<String> {
    Envelope {
        seq,
        meta: Meta {
            recorded_at: Timestamp(1_000 + seq as i64),
            actor: None,
            command: CommandId::Origin {
                request: RequestId(u128::from(seq)),
                index: 0,
            },
            correlation: RequestId(u128::from(seq)).into(),
            cause: None,
            schema,
            rules: RulesVersion::from(rules),
        },
        event: event.to_string(),
    }
}

/// An account's whole life, written by three versions of the code under
/// two versions of the rules: what a store would hold.
fn old_log() -> Vec<Envelope<String>> {
    vec![
        stored(0, 1, "fees-1", "open"),
        stored(1, 1, "fees-1", "dep:500"),
        stored(2, 2, "fees-1", "withdraw 100"),
        stored(3, 2, "fees-2", "withdraw 100"),
        stored(4, 3, "fees-2", "deposited 7"),
    ]
}

fn both_fees() -> annals::next::Rulebook<Fees> {
    fees("fees-2", 5).with("fees-1", Fees { withdrawal: 0 })
}

#[test]
#[ignore = "proposal: an upcast chain is checked whole when built (A5)"]
fn an_upcast_chain_must_be_whole() {
    let chain = |current, froms: &[u32]| {
        froms
            .iter()
            .fold(Upcasts::<String>::new("account", current), |u, from| {
                u.step(*from, two_to_three)
            })
            .build()
            .err()
    };
    let error = |problem| {
        Some(ChainError {
            kind: "account".to_string(),
            problem,
        })
    };
    assert_eq!(chain(3, &[1, 2]), None);
    assert_eq!(chain(3, &[]), None, "no steps: only the current version");
    assert_eq!(
        chain(3, &[1, 1, 2]),
        error(ChainProblem::Duplicate { from: 1 })
    );
    assert_eq!(
        chain(3, &[1, 2, 3]),
        error(ChainProblem::BeyondCurrent {
            from: 3,
            current: 3
        })
    );
    assert_eq!(chain(4, &[1, 3]), error(ChainProblem::Gap { from: 2 }));
    assert_eq!(chain(4, &[1, 2]), error(ChainProblem::Gap { from: 3 }));
}

#[test]
#[ignore = "proposal: upcasting happens at read time and never rewrites the store (A5)"]
fn upcasting_runs_each_step_from_the_stored_version_and_never_touches_the_store() {
    let mut store = InMemoryStore::new();
    let h = header(1, "account");
    store.append(&h, 0, &old_log()).unwrap();

    let read = store.read(h.id).unwrap().unwrap();
    let events = upcaster().read(read.events, decode).unwrap();
    assert_eq!(
        events.iter().map(|e| e.event.clone()).collect::<Vec<_>>(),
        [
            AccountEvent::Opened,
            AccountEvent::Deposited(500),
            AccountEvent::Withdrew(100),
            AccountEvent::Withdrew(100),
            AccountEvent::Deposited(7),
        ]
    );
    for (e, original) in events.iter().zip(old_log()) {
        assert_eq!(e.seq, original.seq);
        assert_eq!(e.meta.schema, 3, "upcast to current");
        assert_eq!(
            Meta {
                schema: original.meta.schema,
                ..e.meta.clone()
            },
            original.meta,
            "every other field kept"
        );
    }
    assert_eq!(
        store.read(h.id).unwrap().unwrap().events,
        old_log(),
        "the store still holds what was written"
    );

    let stream =
        replay_stored::<Account, _>(h, old_log(), &upcaster(), decode, &both_fees()).unwrap();
    assert_eq!(stream.state().cents, 500 - 100 - 105 + 7);
}

#[test]
#[ignore = "proposal: golden replay fixtures, old log in, states out (A5)"]
fn golden_replay_fixtures_still_replay() {
    assert_replay_fixtures::<Account, _>(
        &upcaster(),
        decode,
        &both_fees(),
        &[
            ReplayFixture {
                name: "a life across three schemas and two rules",
                header: header(1, "account"),
                log: old_log(),
                states: vec![
                    (
                        1,
                        Balance {
                            open: true,
                            cents: 500,
                        },
                    ),
                    (
                        3,
                        Balance {
                            open: true,
                            cents: 295,
                        },
                    ),
                    (
                        4,
                        Balance {
                            open: true,
                            cents: 302,
                        },
                    ),
                ],
            },
            ReplayFixture {
                name: "already current",
                header: header(2, "account"),
                log: vec![
                    stored(0, 3, "fees-2", "opened"),
                    stored(1, 3, "fees-2", "deposited 9"),
                ],
                states: vec![(
                    1,
                    Balance {
                        open: true,
                        cents: 9,
                    },
                )],
            },
        ],
    );
}

#[test]
#[ignore = "proposal: a fixture that no longer replays fails, naming itself"]
#[should_panic(expected = "a drifted fold")]
fn a_fixture_whose_states_drifted_fails() {
    assert_replay_fixtures::<Account, _>(
        &upcaster(),
        decode,
        &both_fees(),
        &[ReplayFixture {
            name: "a drifted fold",
            header: header(1, "account"),
            log: old_log(),
            states: vec![(
                4,
                Balance {
                    open: true,
                    cents: 312,
                },
            )],
        }],
    );
}

#[test]
#[ignore = "proposal: events outside the chain are refused, steps fail typed (A5)"]
fn an_event_outside_the_chain_or_a_failing_step_is_an_upcast_error() {
    let only_from_two = Upcasts::new("account", 3)
        .step(2, two_to_three)
        .build()
        .unwrap();
    assert_eq!(
        only_from_two.upcast(stored(0, 1, "fees-1", "open")).err(),
        Some(UpcastError::TooOld {
            seq: 0,
            found: 1,
            oldest: 2
        })
    );
    assert_eq!(
        upcaster().upcast(stored(5, 4, "fees-2", "?")).err(),
        Some(UpcastError::TooNew {
            seq: 5,
            found: 4,
            current: 3
        })
    );
    assert_eq!(
        upcaster().upcast(stored(6, 1, "fees-1", "explode")).err(),
        Some(UpcastError::Step {
            seq: 6,
            from: 1,
            error: Report::new("toy-account-v1-unknown", "no v1 event \"explode\"")
                .arg("event", "explode"),
        })
    );
}
