//! Decide is pure, commit stamps, replay resolves rules by version.

use annals::next::{
    Actor, CommandId, CorrelationId, EventId, Failure, KernelError, Meta, ReplayError, RequestId,
    RulesVersion, Stream, StreamId, Timestamp,
};

use crate::toys::{
    fees, header, meta, meta_in, no_rules, Account, AccountCmd, Fees, Jug, JugCmd, Refused,
};

fn opened_with(cents: i64) -> Stream<Account> {
    let mut s = Stream::<Account>::new(header(1, "account"));
    let rules = fees("fees-1", 0);
    s.execute(&AccountCmd::Open, &meta(1, 10), &rules, &mut [])
        .unwrap();
    s.execute(&AccountCmd::Deposit(cents), &meta(2, 20), &rules, &mut [])
        .unwrap();
    s
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
    let mut s = Stream::<Jug>::new(header(2, "jug"));
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
            stream: StreamId::from(1),
            expected: 2,
            actual: 3
        })
    );
    assert_eq!(s.state().cents, 101);
}

#[test]
#[ignore = "proposal: metadata stamped on every event (A1)"]
fn metadata_is_stamped_on_every_event() {
    let mut s = Stream::<Jug>::new(header(2, "jug"));
    let rules = no_rules();
    s.execute(&JugCmd::Fill(7, 3), &meta(1, 1), &rules, &mut [])
        .unwrap();
    let cmd = meta_in(42, 1_700_000_000_000_000, 9);
    let poured = s.execute(&JugCmd::Pour, &cmd, &rules, &mut []).unwrap();
    assert_eq!(poured.len(), 3);
    for e in poured {
        assert_eq!(
            e.meta,
            Meta {
                recorded_at: Timestamp(1_700_000_000_000_000),
                actor: Some(Actor("pseudonym-ann".to_string())),
                command: CommandId::Origin {
                    request: RequestId(42),
                    index: 0,
                },
                correlation: CorrelationId(9),
                cause: None,
                schema: 1,
                rules: RulesVersion::from("none"),
            },
            "event {}",
            e.seq
        );
    }
    assert_eq!(
        s.events()[0].meta.correlation,
        CorrelationId(1),
        "a fresh command starts its own correlation"
    );
}

#[test]
#[ignore = "proposal: in-stream and cross-stream causes in one field (A2)"]
fn a_cause_names_its_stream() {
    let mut s = opened_with(100);
    let rules = fees("fees-1", 0);
    let elsewhere = EventId {
        stream: StreamId::from(77),
        seq: 12,
    };
    let cmd = annals::next::CommandMeta {
        cause: Some(elsewhere),
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

    let (h, events, state) = s.into_parts();
    let replayed = Stream::<Account>::replay(h.clone(), events.clone(), &both).unwrap();
    assert_eq!(*replayed.state(), state);

    let only_new = fees("fees-2", 5);
    assert_eq!(
        Stream::<Account>::replay(h, events, &only_new).err(),
        Some(ReplayError::UnknownRules {
            seq: 0,
            version: RulesVersion::from("fees-1")
        }),
        "the log says which rules; the caller cannot substitute others"
    );
}

#[test]
#[ignore = "proposal: replay takes only current-schema events (A5)"]
fn replay_refuses_an_event_not_at_the_current_schema() {
    let s = opened_with(100);
    let (h, mut events, _) = s.into_parts();
    events[1].meta.schema = 2;
    assert_eq!(
        Stream::<Account>::replay(h, events, &fees("fees-1", 0)).err(),
        Some(ReplayError::SchemaNotCurrent {
            seq: 1,
            found: 2,
            current: 3
        })
    );
}

#[test]
#[ignore = "proposal: replay checks in-stream causes"]
fn replay_refuses_an_in_stream_cause_that_is_not_earlier() {
    let s = opened_with(100);
    let (h, mut events, _) = s.into_parts();
    events[1].meta.cause = Some(EventId {
        stream: StreamId::from(1),
        seq: 1,
    });
    assert_eq!(
        Stream::<Account>::replay(h.clone(), events.clone(), &fees("fees-1", 0)).err(),
        Some(ReplayError::CauseNotEarlier { seq: 1, cause: 1 })
    );
    events[1].meta.cause = Some(EventId {
        stream: StreamId::from(2),
        seq: 9,
    });
    assert!(
        Stream::<Account>::replay(h, events, &fees("fees-1", 0)).is_ok(),
        "another stream's seq is not ordered against this one's"
    );
}

#[test]
fn a_uuid_is_a_stream_id() {
    let uuid = [
        0x67, 0xe5, 0x50, 0x44, 0x10, 0xb1, 0x42, 0x6f, 0x92, 0x47, 0xbb, 0x68, 0x0e, 0x5f, 0xe0,
        0xc8,
    ];
    let id = StreamId::from_bytes(uuid);
    assert_eq!(id.to_bytes(), uuid);
    assert_eq!(id.0 >> 120, 0x67, "big-endian, as a UUID is written");
    assert_eq!(StreamId::from(7u64), StreamId(7));
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
