//! Decide is pure, commit stamps, replay resolves rules by version; streams
//! are keyed by (domain, edition, root); correlation is always set.

use std::collections::BTreeSet;

use annals::next::{
    Actor, CommandId, CommandMeta, Correlation, Edition, EventId, Failure, KernelError, Meta,
    ReplayError, Root, RulesVersion, Stream, StreamKey, Timestamp,
};

use crate::toys::{
    correlation, fees, key, meta, meta_in, no_rules, request, stream, Account, AccountCmd, Fees,
    Jug, JugCmd, Refused,
};

fn opened_with(cents: i64) -> Stream<Account> {
    let mut s = stream::<Account>(1);
    let rules = fees("fees-1", 0);
    s.execute(&AccountCmd::Open, &meta(1, 10), &rules, &mut [])
        .unwrap();
    s.execute(&AccountCmd::Deposit(cents), &meta(2, 20), &rules, &mut [])
        .unwrap();
    s
}

#[test]
#[ignore = "proposal: a stream key is (domain, edition, root), the main timeline normalised"]
fn a_stream_key_is_domain_edition_and_root_with_the_main_timeline_normalised() {
    let root = Root::from(7);
    let main = StreamKey::main("list", root);
    for spelling in [
        Edition::new(""),
        Edition::new("angzarr"),
        Edition::from(None),
        Edition::from(Some("")),
        Edition::MAIN,
    ] {
        assert!(spelling.is_main(), "{spelling:?}");
        assert_eq!(spelling.name(), "angzarr", "the canonical name");
        assert_eq!(StreamKey::new("list", spelling, root), main);
    }

    let v2 = Edition::new("v2");
    assert!(!v2.is_main());
    assert_eq!(v2.name(), "v2");
    assert_ne!(
        Edition::new("Angzarr"),
        Edition::MAIN,
        "names are exact, case included"
    );

    let keys: BTreeSet<StreamKey> = [
        main.clone(),
        StreamKey::new("list", Edition::new(""), root),
        StreamKey::new("list", v2.clone(), root),
        StreamKey::main("draw-system", root),
        StreamKey::main("list", Root::from(8)),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        keys.len(),
        4,
        "the same root in another edition or domain is another stream"
    );

    let s = Stream::<Account>::new(v2.clone(), root);
    assert_eq!(*s.key(), StreamKey::new("account", v2, root));
    assert_eq!(
        StreamKey::of::<Account>(Edition::new("angzarr"), root),
        StreamKey::main("account", root),
        "a stream's domain is its type's name"
    );
}

#[test]
#[ignore = "proposal: a rejection leaves nothing behind (A6)"]
fn a_rejection_leaves_nothing_behind() {
    let mut s = opened_with(100);
    let rules = fees("fees-1", 0);
    let before = s.state().clone();

    // Refused before emitting.
    let refused = s.execute(&AccountCmd::Withdraw(250), &meta(3, 30), &rules, &mut []);
    assert_eq!(
        refused.unwrap_err(),
        Failure::Rejected(Refused::Overdrawn { short: 150 })
    );

    // Refused after emitting: allowed now, and just as clean.
    let mut seen = 0;
    let mut handler = |_: &annals::next::Committed<'_, Account>| seen += 1;
    let refused = s.execute(
        &AccountCmd::DepositUpTo {
            cents: 500,
            limit: 300,
        },
        &meta(4, 40),
        &rules,
        &mut [&mut handler],
    );
    assert_eq!(refused.unwrap_err(), Failure::Rejected(Refused::OverLimit));
    assert_eq!(s.len(), 2, "nothing appended");
    assert_eq!(*s.state(), before, "nothing applied");
    assert_eq!(seen, 0, "no handler ran");
}

#[test]
#[ignore = "proposal: decide stages, commit applies"]
fn a_decide_sees_its_own_staged_events_and_the_stream_does_not() {
    let mut s = stream::<Jug>(2);
    let rules = no_rules();
    s.execute(&JugCmd::Fill(7, 3), &meta(1, 1), &rules, &mut [])
        .unwrap();

    let decision = s.decide(&JugCmd::Pour, &meta(2, 2), &rules).unwrap();
    assert_eq!(decision.events().len(), 3, "each pour read the last one");
    assert_eq!(decision.base(), 1);
    assert_eq!(
        decision.events().iter().map(|e| e.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(s.state().cups, vec![0, 0, 0], "decide changed nothing");
    assert_eq!(s.len(), 1);

    assert_eq!(s.commit(decision, &rules, &mut []), Ok(4));
    assert_eq!(s.state().cups, vec![3, 3, 1]);
    assert_eq!(s.state().jug, 0);
}

#[test]
#[ignore = "proposal: commit refuses a decision against a stream that moved"]
fn commit_refuses_a_stale_decision() {
    let mut s = opened_with(100);
    let rules = fees("fees-1", 0);
    let first = s
        .decide(&AccountCmd::Deposit(1), &meta(3, 30), &rules)
        .unwrap();
    let second = s
        .decide(&AccountCmd::Deposit(2), &meta(4, 40), &rules)
        .unwrap();
    s.commit(first, &rules, &mut []).unwrap();
    assert_eq!(
        s.commit(second, &rules, &mut []),
        Err(KernelError::Stale {
            stream: key(1, "account"),
            expected: 2,
            actual: 3
        })
    );
    assert_eq!(s.state().cents, 101);
}

#[test]
#[ignore = "proposal: metadata stamped on every event (A1)"]
fn metadata_is_stamped_on_every_event() {
    let mut s = stream::<Jug>(2);
    let rules = no_rules();
    s.execute(&JugCmd::Fill(7, 3), &meta(1, 1), &rules, &mut [])
        .unwrap();
    let cmd = CommandMeta::nth(
        Timestamp(1_700_000_000_000_000),
        Some(Actor("pseudonym-ann".to_string())),
        request(42),
        2,
    )
    .correlated(correlation(9));
    let poured = s.execute(&JugCmd::Pour, &cmd, &rules, &mut []).unwrap();
    assert_eq!(poured.len(), 3);
    for e in poured {
        assert_eq!(
            e.meta,
            Meta {
                recorded_at: Timestamp(1_700_000_000_000_000),
                actor: Some(Actor("pseudonym-ann".to_string())),
                request: request(42),
                command: CommandId::Origin { index: 2 },
                correlation: correlation(9),
                cause: None,
                rules: RulesVersion::from("none"),
            },
            "event {}",
            e.seq
        );
    }
}

#[test]
#[ignore = "proposal: correlation is always set, generated from the request when absent"]
fn a_command_without_a_correlation_gets_one_generated_from_its_request() {
    assert_eq!(
        Correlation::generated(&request(1)),
        Correlation("req-1".to_string()),
        "the request id's text: the same request, the same correlation"
    );
    let mut s = stream::<Jug>(2);
    let rules = no_rules();
    let without = meta(1, 1);
    assert_eq!(without.correlation, None);
    assert_eq!(without.correlation(), Correlation::generated(&request(1)));
    s.execute(&JugCmd::Fill(7, 3), &without, &rules, &mut [])
        .unwrap();
    assert_eq!(
        s.events()[0].meta.correlation,
        Correlation::generated(&request(1)),
        "stamped, never empty"
    );

    let given = meta_in(2, 2, 77);
    s.execute(&JugCmd::Pour, &given, &rules, &mut []).unwrap();
    assert!(
        s.events()[1..]
            .iter()
            .all(|e| e.meta.correlation == correlation(77)),
        "a given correlation is kept"
    );
}

#[test]
#[ignore = "proposal: in-stream and cross-stream causes in one field (A2)"]
fn a_cause_names_its_stream() {
    let mut s = opened_with(100);
    let rules = fees("fees-1", 0);
    let elsewhere = EventId {
        stream: StreamKey::new("ledger", Edition::new("v2"), Root::from(77)),
        seq: 12,
    };
    let cmd = CommandMeta {
        cause: Some(elsewhere.clone()),
        ..meta(3, 30)
    };
    let e = s
        .execute(&AccountCmd::Deposit(5), &cmd, &rules, &mut [])
        .unwrap();
    assert_eq!(e[0].meta.cause, Some(elsewhere));
}

#[test]
#[ignore = "proposal: replay resolves the rules each event names (A3)"]
fn replay_resolves_rules_by_each_events_version() {
    let mut s = opened_with(100);
    s.execute(
        &AccountCmd::Withdraw(10),
        &meta(3, 30),
        &fees("fees-1", 0),
        &mut [],
    )
    .unwrap();
    let both = fees("fees-2", 5).with("fees-1", Fees { withdrawal: 0 });
    s.execute(&AccountCmd::Withdraw(10), &meta(4, 40), &both, &mut [])
        .unwrap();
    assert_eq!(
        s.state().cents,
        75,
        "10 free under fees-1, 10 + 5 under fees-2"
    );
    assert_eq!(s.events()[2].meta.rules, RulesVersion::from("fees-1"));
    assert_eq!(s.events()[3].meta.rules, RulesVersion::from("fees-2"));

    let (k, events, state) = s.into_parts();
    let replayed = Stream::<Account>::replay(k.clone(), events.clone(), &both).unwrap();
    assert_eq!(*replayed.state(), state);

    let only_new = fees("fees-2", 5);
    assert_eq!(
        Stream::<Account>::replay(k, events, &only_new).err(),
        Some(ReplayError::UnknownRules {
            seq: 0,
            version: RulesVersion::from("fees-1")
        }),
        "the log says which rules; the caller cannot substitute others"
    );
}

#[test]
#[ignore = "proposal: replay refuses a log keyed to another domain"]
fn replay_refuses_a_log_of_another_domain() {
    let (_, events, _) = opened_with(100).into_parts();
    assert_eq!(
        Stream::<Account>::replay(key(1, "jug"), events, &fees("fees-1", 0)).err(),
        Some(ReplayError::WrongDomain {
            found: "jug".to_string(),
            expected: "account"
        })
    );
}

#[test]
#[ignore = "proposal: replay checks in-stream causes"]
fn replay_refuses_an_in_stream_cause_that_is_not_earlier() {
    let s = opened_with(100);
    let (k, mut events, _) = s.into_parts();
    events[1].meta.cause = Some(EventId {
        stream: k.clone(),
        seq: 1,
    });
    assert_eq!(
        Stream::<Account>::replay(k.clone(), events.clone(), &fees("fees-1", 0)).err(),
        Some(ReplayError::CauseNotEarlier { seq: 1, cause: 1 })
    );
    for other in [
        key(2, "account"),
        StreamKey::new("account", Edition::new("v2"), k.root),
    ] {
        events[1].meta.cause = Some(EventId {
            stream: other,
            seq: 9,
        });
        assert!(
            Stream::<Account>::replay(k.clone(), events.clone(), &fees("fees-1", 0)).is_ok(),
            "another root's or another edition's seq is not ordered against this one's"
        );
    }
}

#[test]
fn a_uuid_is_a_root() {
    let uuid = [
        0x67, 0xe5, 0x50, 0x44, 0x10, 0xb1, 0x42, 0x6f, 0x92, 0x47, 0xbb, 0x68, 0x0e, 0x5f, 0xe0,
        0xc8,
    ];
    let root = Root::from_bytes(uuid);
    assert_eq!(root.to_bytes(), uuid);
    assert_eq!(root.0 >> 120, 0x67, "big-endian, as a UUID is written");
    assert_eq!(Root::from(7u64), Root(7));
}

#[test]
fn a_rulebook_finds_each_version_and_knows_the_current() {
    let book = fees("fees-2", 5).with("fees-1", Fees { withdrawal: 0 });
    assert_eq!(
        book.current(),
        (&RulesVersion::from("fees-2"), &Fees { withdrawal: 5 })
    );
    assert_eq!(
        book.get(&RulesVersion::from("fees-1")),
        Some(&Fees { withdrawal: 0 })
    );
    assert_eq!(book.get(&RulesVersion::from("fees-0")), None);
}
