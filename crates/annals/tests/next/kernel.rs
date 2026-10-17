//! Validation and resolution before business code, decide or ingest by the
//! body arm, the meta the kernel stamps, conflict resolution, and replay
//! resolving rules by version; streams keyed by (domain, edition, root),
//! the main timeline being no edition.

use std::collections::BTreeSet;

use annals::next::{
    Book, BookOf, Causation, ConflictResolution, Correlation, EditionName, EmptyEditionName,
    EventId, Failure, KernelError, Malformed, Meta, Origin, Provenance, Recovery, ReplayError,
    Root, RulesVersion, Stream, StreamKey, Timestamp,
};

use crate::toys::{
    ann, ann_in, at, book, correlation, edition, fees, key, no_rules, request, set, stream,
    Account, AccountCmd, AccountEvent, AccountFact, Balance, Fees, Jug, JugCmd, Refused, Shelf,
    WasSet,
};

/// Account 1, opened at 10 (request 1) and credited `cents` at 20
/// (request 2): seqs 0 and 1.
fn opened_with(cents: i64) -> Stream<Account> {
    let mut s = stream::<Account>(1);
    let rules = fees("fees-1", 0);
    let open = book(&s, AccountCmd::Open, 1);
    s.execute(&open, &at(10), &rules, &mut []).unwrap();
    let deposit = book(&s, AccountCmd::Deposit(cents), 2);
    s.execute(&deposit, &at(20), &rules, &mut []).unwrap();
    s
}

/// Shelf 3 with a = 1 (seq 0) and b = 2 (seq 1).
fn shelf_a_b() -> Stream<Shelf> {
    let mut s = stream::<Shelf>(3);
    for (i, (item, n)) in [("a", 1), ("b", 2)].into_iter().enumerate() {
        let b = book(&s, set(item, n), i as u128);
        s.execute(&b, &at(i as i64), &no_rules(), &mut []).unwrap();
    }
    s
}

#[test]
#[ignore = "proposal: the main timeline is no edition; an edition name is never empty"]
fn the_main_timeline_is_no_edition_and_an_edition_name_is_never_empty() {
    assert_eq!(
        EditionName::new(""),
        Err(EmptyEditionName),
        "main is None, never a magic string"
    );
    let v2 = EditionName::new("v2").unwrap();
    assert_eq!(v2.as_str(), "v2");
    assert_ne!(
        EditionName::new("V2").unwrap(),
        v2,
        "names are exact, case included"
    );

    let root = Root::from(7);
    let main = StreamKey::main("list", root);
    assert_eq!(main.edition, None);
    assert!(main.is_main());
    assert!(!StreamKey::new("list", Some(v2.clone()), root).is_main());

    let keys: BTreeSet<StreamKey> = [
        main.clone(),
        StreamKey::new("list", None, root),
        StreamKey::new("list", Some(v2.clone()), root),
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

    let s = Stream::<Account>::new(Some(v2.clone()), root);
    assert_eq!(*s.key(), StreamKey::new("account", Some(v2), root));
    assert_eq!(
        StreamKey::of::<Account>(None, root),
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
    let overdraw = book(&s, AccountCmd::Withdraw(250), 3);
    let refused = s.execute(&overdraw, &at(30), &rules, &mut []);
    assert_eq!(
        refused.unwrap_err(),
        Failure::Rejected(Refused::Overdrawn { short: 150 })
    );

    // Refused after emitting: allowed now, and just as clean.
    let mut seen = 0;
    let mut handler = |_: &annals::next::Committed<'_, Account>| seen += 1;
    let over = book(
        &s,
        AccountCmd::DepositUpTo {
            cents: 500,
            limit: 300,
        },
        4,
    );
    let refused = s.execute(&over, &at(40), &rules, &mut [&mut handler]);
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
    let fill = book(&s, JugCmd::Fill(7, 3), 1);
    s.execute(&fill, &at(1), &rules, &mut []).unwrap();

    let decision = s
        .decide(&book(&s, JugCmd::Pour, 2), &at(2), &rules)
        .unwrap();
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
        .decide(&book(&s, AccountCmd::Deposit(1), 3), &at(30), &rules)
        .unwrap();
    let second = s
        .decide(&book(&s, AccountCmd::Deposit(2), 4), &at(40), &rules)
        .unwrap();
    s.commit(first, &rules, &mut []).unwrap();
    let stale = s.commit(second, &rules, &mut []).unwrap_err();
    assert_eq!(
        stale,
        KernelError::Stale {
            stream: key(1, "account"),
            expected: 2,
            actual: 3
        }
    );
    assert_eq!(stale.recovery(), Recovery::Retry, "re-read and re-decide");
    assert_eq!(s.state().cents, 101);
}

#[test]
#[ignore = "proposal: the kernel stamps one meta on every event of a write (A1)"]
fn the_kernel_stamps_one_meta_on_every_event_of_a_write() {
    let mut s = stream::<Jug>(2);
    let rules = no_rules();
    let fill = book(&s, JugCmd::Fill(7, 3), 1);
    s.execute(&fill, &at(1), &rules, &mut []).unwrap();
    let pour = Book::command(
        s.key().clone(),
        JugCmd::Pour,
        Origin::request(request(42), 2),
        ann_in(9),
    );
    let poured = s
        .execute(&pour, &at(1_700_000_000_000_000), &rules, &mut [])
        .unwrap();
    assert_eq!(poured.len(), 3);
    for e in poured {
        assert_eq!(
            e.meta,
            Meta {
                actor: ann().actor,
                correlation: correlation(9),
                origin: Origin::request(request(42), 2),
                recorded_at: Timestamp(1_700_000_000_000_000),
                base_sequence: 1,
                rules_version: RulesVersion::from("none"),
            },
            "event {}",
            e.seq
        );
    }
}

#[test]
#[ignore = "proposal: correlation is the caller's, else derived from the key, else minted"]
fn correlation_is_given_else_derived_else_minted() {
    assert_eq!(
        Correlation::derived(&Origin::request(request(1), 0)),
        Some(Correlation("req-1".to_string())),
        "a keyed request: every retry lands in one correlation"
    );
    assert_eq!(
        Correlation::derived(&Origin::fact("pi_1234", "Stripe webhook")),
        Some(Correlation("pi_1234".to_string())),
        "a fact: its external id"
    );
    assert_eq!(Correlation::derived(&Origin::keyless()), None);

    let mut s = stream::<Jug>(2);
    let rules = no_rules();

    let keyless = Book::command(
        s.key().clone(),
        JugCmd::Fill(7, 3),
        Origin::keyless(),
        Provenance::default(),
    );
    assert_eq!(
        keyless.correlation(&at(5).minted),
        Correlation("minted-5".to_string())
    );
    s.execute(&keyless, &at(5), &rules, &mut []).unwrap();
    assert_eq!(
        s.events()[0].meta.correlation,
        Correlation("minted-5".to_string()),
        "minted by the application, never empty"
    );
    assert_eq!(s.events()[0].meta.actor, None, "the system");

    let keyed = book(&s, JugCmd::Pour, 1);
    s.execute(&keyed, &at(6), &rules, &mut []).unwrap();
    assert!(s.events()[1..]
        .iter()
        .all(|e| e.meta.correlation == Correlation("req-1".to_string())));

    let given = Book::command(
        s.key().clone(),
        JugCmd::Fill(1, 1),
        Origin::request(request(2), 0),
        ann_in(77),
    );
    s.execute(&given, &at(7), &rules, &mut []).unwrap();
    assert_eq!(
        s.events().last().unwrap().meta.correlation,
        correlation(77),
        "a given correlation is kept"
    );
}

#[test]
#[ignore = "proposal: base_sequence and recorded_at: decided against the head, never backwards"]
fn base_sequence_is_the_head_and_recorded_at_never_goes_backwards() {
    let mut s = opened_with(100);
    let rules = fees("fees-1", 0);
    assert_eq!(
        s.events()
            .iter()
            .map(|e| (e.meta.base_sequence, e.meta.recorded_at))
            .collect::<Vec<_>>(),
        [(0, Timestamp(10)), (1, Timestamp(20))]
    );

    // The application's clock stepped back to 15.
    let late = book(&s, AccountCmd::Deposit(1), 3);
    let e = s.execute(&late, &at(15), &rules, &mut []).unwrap();
    assert_eq!(e[0].meta.recorded_at, Timestamp(20), "max(previous, now)");
    assert_eq!(e[0].meta.base_sequence, 2);

    let later = book(&s, AccountCmd::Deposit(1), 4);
    let e = s.execute(&later, &at(30), &rules, &mut []).unwrap();
    assert_eq!(e[0].meta.recorded_at, Timestamp(30));
    assert_eq!(s.last_recorded_at(), Some(Timestamp(30)));

    let (k, mut events, _) = s.into_parts();
    events[1].meta.recorded_at = Timestamp(5);
    assert_eq!(
        Stream::<Account>::replay(k, events, &rules).err(),
        Some(ReplayError::RecordedBackwards { seq: 1 }),
        "as-of-time reads rely on it: a log that goes backwards is refused"
    );
}

#[test]
#[ignore = "proposal: the body arm and the origin must agree; refused before business code"]
fn a_malformed_book_is_refused_before_business_code() {
    let mut s = opened_with(100);
    let rules = fees("fees-1", 0);
    let k = s.key().clone();
    let deposit = || AccountCmd::Deposit(1);
    let paid = || AccountFact::PaymentReceived(1);
    let causation = Origin::Causation(Causation {
        event: EventId {
            stream: key(9, "ledger"),
            seq: 0,
        },
        component: "books".to_string(),
        index: 0,
    });

    let cases = [
        (
            Book::fact(k.clone(), paid(), Origin::request(request(3), 0), ann()),
            Malformed::FactWithoutFactOrigin,
        ),
        (
            Book::command(k.clone(), deposit(), Origin::fact("pi_1", "Stripe"), ann()),
            Malformed::CommandWithFactOrigin,
        ),
        (
            Book::fact(k.clone(), paid(), Origin::fact("", "Stripe"), ann()),
            Malformed::EmptyExternalId,
        ),
        (
            Book::fact(k.clone(), paid(), Origin::fact("pi_1", "Stripe"), ann())
                .expecting(2, ConflictResolution::Strict),
            Malformed::FactWithExpectedSequence,
        ),
        (
            Book::command(k.clone(), deposit(), causation, Provenance::default())
                .expecting(2, ConflictResolution::Strict),
            Malformed::HandlingOnReaction,
        ),
    ];
    for (bad, malformed) in cases {
        assert_eq!(bad.validate(), Err(malformed.clone()));
        let failure = s.execute(&bad, &at(30), &rules, &mut []).unwrap_err();
        assert_eq!(
            failure,
            Failure::Kernel(KernelError::Malformed {
                stream: k.clone(),
                malformed
            })
        );
        assert_eq!(failure.recovery(), Recovery::Bug, "never repaired");
    }
    assert_eq!(s.len(), 2, "nothing reached decide or ingest");

    assert_eq!(
        BookOf::<Account>::fact(k.clone(), paid(), Origin::fact("pi_1", "Stripe"), ann())
            .validate(),
        Ok(())
    );
    assert_eq!(
        BookOf::<Account>::command(k, deposit(), Origin::keyless(), ann()).validate(),
        Ok(()),
        "a keyless request is a request"
    );
}

#[test]
#[ignore = "proposal: the arm picks the pipeline; ingest is infallible"]
fn a_fact_is_ingested_never_decided_and_never_refused() {
    // Infallible by type: no error to return.
    let _: fn(&Balance, &AccountFact, &Fees) -> Vec<AccountEvent> =
        <Account as annals::next::Domain>::ingest;

    let mut s = stream::<Account>(1);
    let rules = fees("fees-1", 0);
    let deposit = book(&s, AccountCmd::Deposit(5), 1);
    assert_eq!(
        s.execute(&deposit, &at(1), &rules, &mut []).unwrap_err(),
        Failure::Rejected(Refused::NotOpen),
        "the command arm goes to decide, which may refuse"
    );

    let payment = Book::fact(
        s.key().clone(),
        AccountFact::PaymentReceived(5),
        Origin::fact("pi_1", "Stripe webhook"),
        Provenance::default(),
    );
    let recorded = s.execute(&payment, &at(2), &rules, &mut []).unwrap();
    assert_eq!(
        recorded[0].event,
        AccountEvent::Unmatched(5),
        "the same situation, recorded rather than refused"
    );
    assert_eq!(
        recorded[0].meta.origin,
        Origin::fact("pi_1", "Stripe webhook")
    );
    assert_eq!(
        recorded[0].meta.correlation,
        Correlation("pi_1".to_string())
    );

    let open = book(&s, AccountCmd::Open, 2);
    s.execute(&open, &at(3), &rules, &mut []).unwrap();
    let again = Book::fact(
        s.key().clone(),
        AccountFact::PaymentReceived(7),
        Origin::fact("pi_2", "Stripe webhook"),
        Provenance::default(),
    );
    let recorded = s.execute(&again, &at(4), &rules, &mut []).unwrap();
    assert_eq!(recorded[0].event, AccountEvent::Deposited(7));
    assert_eq!(s.state().cents, 7);
}

#[test]
#[ignore = "proposal: STRICT refuses a stale expected sequence before decide"]
fn strict_refuses_a_stale_expected_sequence_before_decide() {
    let mut s = opened_with(100);
    let rules = fees("fees-1", 0);
    let current = book(&s, AccountCmd::Deposit(1), 3).expecting(2, ConflictResolution::Strict);
    s.execute(&current, &at(30), &rules, &mut []).unwrap();

    let behind = book(&s, AccountCmd::Deposit(1), 4).expecting(1, ConflictResolution::Strict);
    let stale = s.execute(&behind, &at(40), &rules, &mut []).unwrap_err();
    assert_eq!(
        stale,
        Failure::Kernel(KernelError::Stale {
            stream: key(1, "account"),
            expected: 1,
            actual: 3
        })
    );
    assert_eq!(stale.recovery(), Recovery::Retry);

    let ahead =
        book(&s, AccountCmd::Deposit(1), 5).expecting(9, ConflictResolution::AggregateHandles);
    assert_eq!(
        s.execute(&ahead, &at(50), &rules, &mut []).unwrap_err(),
        Failure::Kernel(KernelError::Stale {
            stream: key(1, "account"),
            expected: 9,
            actual: 3
        }),
        "an expected sequence past the head is stale under any resolution"
    );
    assert_eq!(s.len(), 3);
}

#[test]
#[ignore = "proposal: AGGREGATE_HANDLES hands decide the window and demands the nonce back"]
fn aggregate_handles_demands_the_nonce_back() {
    let mut s = shelf_a_b();
    let rules = no_rules();

    let resolved = book(&s, set("a", 5), 2).expecting(1, ConflictResolution::AggregateHandles);
    let e = s.execute(&resolved, &at(2), &rules, &mut []).unwrap();
    assert_eq!(
        e[0].event,
        WasSet {
            item: "a".to_string(),
            count: 5,
            window: Some(1)
        },
        "decide saw the one event it had not seen"
    );
    assert_eq!(e[0].meta.base_sequence, 2, "decided against the head");

    let empty = book(&s, set("c", 1), 3).expecting(3, ConflictResolution::AggregateHandles);
    let e = s.execute(&empty, &at(3), &rules, &mut []).unwrap();
    assert_eq!(
        e[0].event.window,
        Some(0),
        "an empty window is handed over too"
    );

    let mut careless = set("b", 9);
    careless.resolve = false;
    let unresolved = book(&s, careless, 4).expecting(1, ConflictResolution::AggregateHandles);
    let failure = s.execute(&unresolved, &at(4), &rules, &mut []).unwrap_err();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Unresolved {
            stream: key(3, "shelf"),
            expected: 1,
            actual: 4
        }),
        "a decide that never acknowledged the window fails as stale"
    );
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(s.len(), 4, "its events were discarded");
}

#[test]
#[ignore = "proposal: COMMUTATIVE keeps a command whose fields are disjoint from the window's"]
fn commutative_accepts_disjoint_changes_and_refuses_overlapping_ones() {
    let mut s = shelf_a_b();
    let rules = no_rules();

    // Issued when only a = 1 existed; b changed since. The command changes a.
    let disjoint = book(&s, set("a", 5), 2).expecting(1, ConflictResolution::Commutative);
    let e = s.execute(&disjoint, &at(2), &rules, &mut []).unwrap();
    assert_eq!(
        e[0].event.window, None,
        "the kernel resolves; decide is never handed the window"
    );
    assert_eq!(s.state().0["a"], 5);

    // Issued at the same point; the window (b, a) overlaps the command (b).
    let overlapping = book(&s, set("b", 7), 3).expecting(1, ConflictResolution::Commutative);
    assert_eq!(
        s.execute(&overlapping, &at(3), &rules, &mut [])
            .unwrap_err(),
        Failure::Kernel(KernelError::Stale {
            stream: key(3, "shelf"),
            expected: 1,
            actual: 3
        })
    );
    assert_eq!(s.state().0["b"], 2);

    let mut account = opened_with(100);
    let unsupported =
        book(&account, AccountCmd::Deposit(1), 3).expecting(2, ConflictResolution::Commutative);
    assert_eq!(
        account
            .execute(&unsupported, &at(30), &fees("fees-1", 0), &mut [])
            .unwrap_err(),
        Failure::Kernel(KernelError::Malformed {
            stream: key(1, "account"),
            malformed: Malformed::CommutativeUnsupported
        }),
        "a domain that cannot say what changed cannot be commutative"
    );
}

#[test]
#[ignore = "proposal: replay resolves the rules each event names (A3)"]
fn replay_resolves_rules_by_each_events_version() {
    let mut s = opened_with(100);
    let w1 = book(&s, AccountCmd::Withdraw(10), 3);
    s.execute(&w1, &at(30), &fees("fees-1", 0), &mut [])
        .unwrap();
    let both = fees("fees-2", 5).with("fees-1", Fees { withdrawal: 0 });
    let w2 = book(&s, AccountCmd::Withdraw(10), 4);
    s.execute(&w2, &at(40), &both, &mut []).unwrap();
    assert_eq!(
        s.state().cents,
        75,
        "10 free under fees-1, 10 + 5 under fees-2"
    );
    assert_eq!(
        s.events()[2].meta.rules_version,
        RulesVersion::from("fees-1")
    );
    assert_eq!(
        s.events()[3].meta.rules_version,
        RulesVersion::from("fees-2")
    );

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
#[ignore = "proposal: replay checks causations within the stream"]
fn replay_refuses_an_in_stream_causation_that_is_not_earlier() {
    let s = opened_with(100);
    let (k, mut events, _) = s.into_parts();
    let caused_by = |stream: StreamKey, seq| {
        Origin::Causation(Causation {
            event: EventId { stream, seq },
            component: "toy".to_string(),
            index: 0,
        })
    };
    events[1].meta.origin = caused_by(k.clone(), 1);
    assert_eq!(
        Stream::<Account>::replay(k.clone(), events.clone(), &fees("fees-1", 0)).err(),
        Some(ReplayError::CauseNotEarlier { seq: 1, cause: 1 })
    );
    for other in [
        key(2, "account"),
        StreamKey::new("account", edition("v2"), k.root),
    ] {
        events[1].meta.origin = caused_by(other, 9);
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
