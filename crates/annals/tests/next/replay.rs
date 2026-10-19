//! Replay is strict and checked: every page's sequence, metadata, trigger,
//! instant and rules; snapshots resume it; golden fixtures pin it; and every
//! failure says how to recover.

use annals::next::v1::event_meta::Causation;
use annals::next::v1::{EventMeta, EventPage, EventRef, Snapshot};
use annals::next::{
    assert_replay_fixtures, check_replay_fixtures, pack, Committed, Failure, KernelError,
    Malformed, Recovery, ReplayError, ReplayFixture, StepError, StoreError, Stream,
};
use annals_diagnostic::Report;

use crate::toys::{
    account, at, command, command_id, cover, instant, registry, registry_with_upcasters, root,
    Account, AccountCmd, Edit, Ledger, Shelf,
};
use annals::next::Payload;

fn meta(sequence: u64, rules: &str) -> EventMeta {
    EventMeta {
        causation: Some(Causation::CommandId(command_id(sequence as u128))),
        correlation_id: Some(command_id(sequence as u128)),
        recorded_at: Some(instant(sequence as i64)),
        rules_version: rules.to_string(),
        ..EventMeta::default()
    }
}

fn page(sequence: u64, payload: annals::next::Any) -> EventPage {
    EventPage {
        payload: Some(payload),
        meta: Some(meta(sequence, "v2")),
        sequence,
    }
}

/// An open account that deposited 100 then withdrew 10 (under v1, no fee).
fn log() -> Vec<EventPage> {
    let mut withdrew = page(2, pack(&account::Withdrew { cents: 10 }));
    withdrew.meta.as_mut().unwrap().rules_version = "v1".to_string();
    vec![
        page(0, pack(&account::Opened {})),
        page(1, pack(&account::Deposited { cents: 100 })),
        withdrew,
    ]
}

fn replay(pages: &[EventPage]) -> Result<Stream<Account>, KernelError> {
    Stream::replay(cover("account", 1), pages, &registry())
}

#[test]
fn replay_folds_each_event_with_the_rules_it_names() {
    let stream = replay(&log()).unwrap();
    assert_eq!(stream.state().cents, 90, "v1 charged no fee");
    assert_eq!(stream.len(), 3);
    assert_eq!(stream.last_recorded_at(), Some(instant(2)));
    assert_eq!(stream.events()[2].rules_version(), "v1");
    assert_eq!(stream.events()[1].command_id(), Some(&command_id(1)));
    assert_eq!(stream.events()[1].triggering_event(), None);
    assert_eq!(stream.events()[1].correlation_id(), &command_id(1));
    assert_eq!(stream.events()[1].fact(), None);
    assert_eq!(stream.events()[1].meta(), meta(1, "v2"));
    assert_eq!(
        stream.events()[1].page(),
        page(1, pack(&account::Deposited { cents: 100 }))
    );
    assert_eq!(stream.find_command(&command_id(1)).len(), 1);
    assert!(stream.find_command(&command_id(9)).is_empty());
    let (c, events, state) = stream.into_parts();
    assert_eq!((c, events.len(), state.cents), (cover("account", 1), 3, 90));
}

#[test]
fn replay_refuses_a_log_that_does_not_hold_together() {
    let mut pages = log();
    pages[1].sequence = 5;
    assert_eq!(
        replay(&pages).err(),
        Some(KernelError::Replay(ReplayError::OutOfSequence {
            index: 1,
            found: 5
        }))
    );

    let mut pages = log();
    pages[2].meta.as_mut().unwrap().recorded_at = Some(instant(0));
    assert_eq!(
        replay(&pages).err(),
        Some(KernelError::Replay(ReplayError::RecordedBackwards {
            sequence: 2
        }))
    );
    pages[2].meta.as_mut().unwrap().recorded_at = Some(instant(1));
    assert!(replay(&pages).is_ok(), "the same instant is not backwards");

    let mut pages = log();
    pages[2].meta.as_mut().unwrap().rules_version = "v0".to_string();
    assert_eq!(
        replay(&pages).err(),
        Some(KernelError::Replay(ReplayError::UnknownRules {
            sequence: 2,
            version: "v0".to_string()
        }))
    );

    for (field, edit) in [
        (
            "causation",
            (|m: &mut EventMeta| m.causation = None) as fn(&mut EventMeta),
        ),
        ("causation", |m| {
            m.causation = Some(Causation::CommandId(annals::next::v1::Uuid {
                value: vec![1; 15].into(),
            }))
        }),
        ("correlation_id", |m| m.correlation_id = None),
        ("correlation_id", |m| {
            m.correlation_id = Some(annals::next::v1::Uuid {
                value: vec![1; 17].into(),
            })
        }),
        ("recorded_at", |m| m.recorded_at = None),
    ] {
        let mut pages = log();
        edit(pages[1].meta.as_mut().unwrap());
        assert_eq!(
            replay(&pages).err(),
            Some(KernelError::Replay(ReplayError::IncompleteMeta {
                sequence: 1,
                field
            }))
        );
    }

    assert_eq!(
        Stream::<Account>::replay(cover("ledger", 1), &log(), &registry()).err(),
        Some(KernelError::Replay(ReplayError::WrongDomain {
            found: "ledger".to_string(),
            expected: "account"
        }))
    );
}

#[test]
fn a_process_events_trigger_in_its_own_stream_must_be_earlier() {
    let trigger = |sequence| {
        Some(Causation::TriggeringEvent(EventRef {
            stream: Some(cover("account", 1)),
            sequence,
        }))
    };
    let mut pages = log();
    pages[2].meta.as_mut().unwrap().causation = trigger(1);
    let stream = replay(&pages).unwrap();
    assert_eq!(stream.events()[2].command_id(), None);
    assert_eq!(stream.events()[2].triggering_event().unwrap().sequence, 1);

    for cause in [2, 3] {
        pages[2].meta.as_mut().unwrap().causation = trigger(cause);
        assert_eq!(
            replay(&pages).err(),
            Some(KernelError::Replay(ReplayError::CauseNotEarlier {
                sequence: 2,
                cause
            }))
        );
    }

    // Another stream's event may be at any sequence.
    pages[2].meta.as_mut().unwrap().causation = Some(Causation::TriggeringEvent(EventRef {
        stream: Some(cover("account", 2)),
        sequence: 7,
    }));
    assert!(replay(&pages).is_ok());
}

#[test]
fn a_stream_resumes_from_a_snapshot_and_dispatches_only_after_it() {
    let registry = registry();
    let full = replay(&log()).unwrap();
    let pages = log();
    let snapshot = Snapshot {
        sequence: 1,
        state: Some(pack(&account::AccountState {
            open: true,
            cents: 100,
            ..Default::default()
        })),
        rules_version: "v2".to_string(),
    };
    let resumed =
        Stream::<Account>::resume(cover("account", 1), &snapshot, None, &pages[2..], &registry)
            .unwrap();
    assert_eq!(resumed.first(), 2);
    assert_eq!(resumed.len(), 3);
    assert_eq!(resumed.events().len(), 1);
    assert_eq!(resumed.state(), full.state());

    let mut seen = Vec::new();
    let mut handler = |c: &Committed<'_, Account>| seen.push((c.sequence(), c.state.cents));
    resumed.dispatch(2, &registry, &mut [&mut handler]).unwrap();
    assert_eq!(seen, [(2, 90)], "the snapshot's state, then the event");
    assert_eq!(
        resumed.dispatch(1, &registry, &mut []),
        Err(KernelError::BeforeSnapshot {
            stream: cover("account", 1),
            first: 2
        })
    );

    // The floor for the next instant is the one given.
    let floor = Stream::<Account>::resume(
        cover("account", 1),
        &snapshot,
        Some(instant(9)),
        &pages[2..],
        &registry,
    )
    .err();
    assert_eq!(
        floor,
        Some(KernelError::Replay(ReplayError::RecordedBackwards {
            sequence: 2
        }))
    );

    let foreign = Snapshot {
        state: Some(pack(&crate::toys::ShelfState::default())),
        ..snapshot.clone()
    };
    let error = Stream::<Account>::resume(cover("account", 1), &foreign, None, &[], &registry)
        .err()
        .unwrap();
    assert!(
        matches!(&error, KernelError::Store(StoreError::Unreadable { sequence: 1, reason, .. })
            if format!("{reason:?}").contains("toy.shelf.v1.ShelfState")),
        "{error:?}"
    );
    let garbled = Snapshot {
        state: Some(annals::next::Any {
            type_url: "/toy.account.v1.AccountState".to_string(),
            value: vec![0x08],
        }),
        ..snapshot
    };
    assert!(matches!(
        Stream::<Account>::resume(cover("account", 1), &garbled, None, &[], &registry),
        Err(KernelError::Store(StoreError::Unreadable { .. }))
    ));
}

#[test]
fn dispatch_hands_out_the_state_just_after_each_event_from_a_sequence() {
    let registry = registry();
    let stream = replay(&log()).unwrap();
    let mut seen = Vec::new();
    for from in [1, 3] {
        let mut handler = |c: &Committed<'_, Account>| seen.push((c.sequence(), c.state.cents));
        stream
            .dispatch(from, &registry, &mut [&mut handler])
            .unwrap();
    }
    assert_eq!(seen, [(1, 100), (2, 90)], "nothing at or past the end");
}

#[test]
fn a_decision_commits_only_to_the_stream_and_head_it_was_made_against() {
    let registry = registry();
    let log = annals::next::MemoryDeadLetters::new();
    let mut a = Stream::<Account>::new(root(1), None).unwrap();
    let mut b = Stream::<Account>::new(root(2), None).unwrap();
    let open = |n| command(cover("account", n), AccountCmd::Open.to_any());
    a.execute(open(1), &registry, &at(1), &log, &mut [])
        .unwrap();
    assert!(matches!(
        a.execute(open(2), &registry, &at(2), &log, &mut []),
        Err(Failure::Kernel(KernelError::Stale { .. }))
    ));
    assert!(matches!(
        b.execute(open(1), &registry, &at(2), &log, &mut []),
        Err(Failure::Kernel(KernelError::Stale { .. }))
    ));
    assert_eq!(
        a.execute(
            command(cover("ledger", 1), AccountCmd::Open.to_any()),
            &registry,
            &at(2),
            &log,
            &mut []
        )
        .err(),
        Some(Failure::Kernel(KernelError::WrongDomain {
            stream: cover("ledger", 1),
            expected: "account"
        }))
    );
    assert_eq!(
        Stream::<Account>::new(
            annals::next::v1::Uuid {
                value: vec![1].into()
            },
            None
        )
        .err(),
        Some(Malformed::BadRoot { len: 1 })
    );
    assert_eq!(
        Stream::<Account>::new(root(1), Some(String::new())).err(),
        Some(Malformed::EmptyEdition)
    );
    let deposit = || command(cover("account", 1), AccountCmd::Deposit(4).to_any()).id(9);
    let applied = a
        .execute(deposit(), &registry, &at(3), &log, &mut [])
        .unwrap();
    assert!(matches!(applied, annals::next::Executed::Applied(e) if e.len() == 1));
    let duplicate = a
        .execute(deposit(), &registry, &at(4), &log, &mut [])
        .unwrap();
    assert!(matches!(duplicate, annals::next::Executed::Duplicate(e)
        if e.len() == 1 && e[0].sequence() == 1));
    assert_eq!(a.state().cents, 4, "applied once");
}

#[test]
fn golden_fixtures_replay_through_the_upcaster_and_name_every_failure() {
    let registry = registry_with_upcasters();
    let state = |cents| account::AccountState {
        open: true,
        cents,
        ..Default::default()
    };
    let old = vec![
        page(0, pack(&account::Opened {})),
        page(1, pack(&account::v0::Credited { euros: 2 })),
    ];
    let good = ReplayFixture {
        name: "v0 credit",
        stream: cover("account", 1),
        log: old.clone(),
        states: vec![(0, state(0)), (1, state(200))],
    };
    assert_replay_fixtures::<Account>(&registry, std::slice::from_ref(&good));

    let drifted = ReplayFixture {
        name: "drifted",
        states: vec![(1, state(2)), (5, state(0))],
        ..good
    };
    let unreadable = ReplayFixture {
        name: "unreadable",
        stream: cover("account", 1),
        log: old,
        states: vec![],
    };
    let failures = check_replay_fixtures::<Account>(&registry, &[drifted]).unwrap_err();
    assert_eq!(
        failures.len(),
        2,
        "every failure of a fixture is listed: {failures:#?}"
    );
    assert!(
        failures[0].starts_with("drifted: after event 1"),
        "{}",
        failures[0]
    );
    assert_eq!(failures[1], "drifted: no event 5");
    let failures =
        check_replay_fixtures::<Account>(&crate::toys::registry(), &[unreadable]).unwrap_err();
    assert_eq!(failures.len(), 1);
    assert!(failures[0].starts_with("unreadable: "), "{}", failures[0]);
    assert!(
        failures[0].contains("toy.account.v0.Credited"),
        "{}",
        failures[0]
    );

    let missing = ReplayFixture {
        name: "missing",
        stream: cover("account", 1),
        log: vec![page(0, pack(&account::Opened {}))],
        states: vec![(3, state(0))],
    };
    assert_eq!(
        check_replay_fixtures::<Account>(&registry, &[missing]),
        Err(vec!["missing: no event 3".to_string()])
    );
}

#[test]
#[should_panic(expected = "account replay fixtures do not replay")]
fn assert_replay_fixtures_panics_with_every_failure() {
    let fixture = ReplayFixture {
        name: "empty",
        stream: cover("account", 1),
        log: vec![],
        states: vec![(0, account::AccountState::default())],
    };
    assert_replay_fixtures::<Account>(&registry(), &[fixture]);
}

#[test]
fn every_failure_says_how_to_recover() {
    let stream = cover("account", 1);
    let kernel = |e: KernelError| (Failure::<Malformed>::Kernel(e.clone()).recovery(), e);
    let stale = KernelError::Stale {
        stream: stream.clone(),
        expected: 1,
        actual: 2,
    };
    let cases = [
        (
            Recovery::Invalid,
            KernelError::Malformed(Malformed::NoCover),
        ),
        (
            Recovery::Invalid,
            KernelError::UnknownType {
                stream: stream.clone(),
                type_url: String::new(),
            },
        ),
        (Recovery::Retry, stale.clone()),
        (
            Recovery::Retry,
            KernelError::Unresolved {
                stream: stream.clone(),
                expected: 1,
                actual: 2,
            },
        ),
        (
            Recovery::Retry,
            KernelError::Gap {
                stream: stream.clone(),
                gap: annals::next::Gap {
                    expected: 0,
                    found: 1,
                },
            },
        ),
        (
            Recovery::Retry,
            KernelError::Store(StoreError::Conflict {
                stream: stream.clone(),
                expected: 0,
                actual: 1,
            }),
        ),
        (
            Recovery::Retry,
            KernelError::Store(StoreError::Unavailable(Report::new("x-down", "down"))),
        ),
        (
            Recovery::Bug,
            KernelError::Store(StoreError::Discontinuous {
                stream: stream.clone(),
                expected: 0,
                found: 1,
            }),
        ),
        (
            Recovery::Bug,
            KernelError::Store(StoreError::Unreadable {
                stream: stream.clone(),
                sequence: 0,
                reason: Report::new("x-bad", "bad"),
            }),
        ),
        (
            Recovery::Bug,
            KernelError::Upcast {
                stream: stream.clone(),
                error: annals::next::UpcastError::Rule {
                    source: String::new(),
                    error: Report::new("x-bad", "bad"),
                },
            },
        ),
        (
            Recovery::Bug,
            KernelError::IngestPanicked {
                stream: stream.clone(),
                command_id: command_id(1),
            },
        ),
        (
            Recovery::Bug,
            KernelError::WrongDomain {
                stream: stream.clone(),
                expected: "ledger",
            },
        ),
        (Recovery::Bug, KernelError::ChainTooDeep { limit: 1 }),
        (
            Recovery::Bug,
            KernelError::BeforeSnapshot {
                stream: stream.clone(),
                first: 1,
            },
        ),
        (
            Recovery::Bug,
            KernelError::Replay(ReplayError::RecordedBackwards { sequence: 1 }),
        ),
    ];
    for (recovery, error) in cases {
        assert_eq!(kernel(error.clone()), (recovery, error.clone()));
        assert_eq!(error.recovery(), recovery);
    }

    assert_eq!(
        Failure::Rejected(Malformed::NoCover).recovery(),
        Recovery::Business
    );
    let downstream = |error| Failure::<Malformed>::Downstream {
        path: Vec::new(),
        error,
    };
    assert_eq!(
        downstream(StepError::Rejected(Report::new("x-no", "no"))).recovery(),
        Recovery::Business
    );
    assert_eq!(
        downstream(StepError::Kernel(KernelError::Malformed(
            Malformed::NoCover
        )))
        .recovery(),
        Recovery::Bug,
        "a malformed policy book is the kernel's, not the caller's"
    );
    assert_eq!(
        downstream(StepError::Kernel(stale)).recovery(),
        Recovery::Retry
    );
}

#[test]
fn errors_render_their_english_and_wrappers_their_cause() {
    use annals_diagnostic::Diagnostic;
    let stale = KernelError::Stale {
        stream: annals::next::v1::Cover {
            edition: Some("what-if".to_string()),
            ..cover("account", 1)
        },
        expected: 1,
        actual: 2,
    };
    let text = stale.to_string();
    assert!(
        text.starts_with("stream account/0a000000-0000-0000-0000-000000000001@what-if is at 2"),
        "{text}"
    );
    let failure = Failure::<crate::toys::Refused>::Kernel(stale.clone());
    assert_eq!(failure.to_string(), text);
    assert_eq!(failure.code(), "annals-next-stale");
    assert_eq!(failure.args().len(), 3);
    let rejected = Failure::Rejected(crate::toys::Refused::Overdrawn { short: 3 });
    assert_eq!(rejected.code(), "toy-account-overdrawn");
    assert_eq!(rejected.to_string(), "3 cents short");
    assert_eq!(rejected.args().len(), 1);
    let step = StepError::Rejected(Report::of(&crate::toys::Refused::NotOpen));
    assert_eq!(step.code(), "toy-account-not-open");
    assert_eq!(step.to_string(), "the account is not open");
    assert!(step.args().is_empty());
    let step = StepError::Kernel(stale.clone());
    assert_eq!(
        (step.code(), step.to_string()),
        (stale.code(), text.clone())
    );
    assert_eq!(step.args().len(), 3);
    let downstream = Failure::<crate::toys::Refused>::Downstream {
        path: vec![],
        error: step,
    };
    assert!(downstream.to_string().ends_with(&text));
    let upcast = annals::next::UpcastError::Payload(annals::next::PayloadError::UnknownType {
        type_url: "/a.B".to_string(),
    });
    assert_eq!(upcast.code(), "annals-next-payload-unknown-type");
    assert_eq!(upcast.to_string(), "no decoder for the type /a.B");
    assert_eq!(upcast.args().len(), 1);
    let malformed = KernelError::Malformed(Malformed::NoCover);
    assert_eq!(
        (
            malformed.code(),
            malformed.to_string(),
            malformed.args().len()
        ),
        (
            "annals-next-malformed-no-cover",
            "the book has no cover".to_string(),
            0
        )
    );
    let replay = KernelError::Replay(ReplayError::RecordedBackwards { sequence: 3 });
    assert_eq!(replay.code(), "annals-next-replay-recorded-backwards");
    assert!(replay.to_string().starts_with("event 3"));
    assert_eq!(replay.args().len(), 1);
    let store = KernelError::Store(StoreError::Unavailable(Report::new("x-down", "down")));
    assert_eq!(store.code(), "annals-next-store-unavailable");
    assert_eq!(store.to_string(), "the store is unavailable: down");
    assert_eq!(store.args().len(), 1);
}

#[test]
fn a_domain_the_registry_lacks_cannot_be_replayed_or_snapshotted() {
    let empty = annals::next::Registry::new();
    let unknown = Some(KernelError::UnregisteredDomain {
        domain: "account".to_string(),
    });
    assert_eq!(
        Stream::<Account>::replay(cover("account", 1), &log(), &empty).err(),
        unknown
    );
    let stream = replay(&log()).unwrap();
    assert_eq!(stream.dispatch(0, &empty, &mut []).err(), unknown);
    assert_eq!(stream.snapshot(&empty), None);
    assert_eq!(
        KernelError::UnregisteredDomain {
            domain: "account".to_string()
        }
        .recovery(),
        Recovery::Bug,
        "the application's misconfiguration, not the caller's"
    );
    assert_eq!(
        Failure::<Malformed>::Kernel(KernelError::Malformed(Malformed::UnknownDomain {
            domain: "account".to_string()
        }))
        .recovery(),
        Recovery::Invalid,
        "an incoming book for it is the caller's"
    );
    let _ = (Ledger, Shelf);
}

#[test]
fn a_facts_events_read_back_with_its_origin() {
    let registry = registry();
    let mut store = annals::next::InMemoryStore::new();
    crate::toys::handle::<Account>(
        &mut store,
        &registry,
        crate::toys::fact(
            cover("account", 1),
            crate::toys::AccountFact::PaymentReceived(3).to_any(),
            "pi_r",
        ),
        &at(5),
    )
    .unwrap();
    use annals::next::EventStore;
    let pages = store.read(&cover("account", 1)).unwrap().unwrap().pages;
    let stream = Stream::<Account>::replay(cover("account", 1), &pages, &registry).unwrap();
    assert_eq!(stream.events()[0].fact().unwrap().external_id, "pi_r");
    assert!(!stream.is_empty());
    assert!(Stream::<Account>::new(root(9), None).unwrap().is_empty());
}

#[test]
fn a_decision_for_another_stream_at_the_same_head_does_not_commit() {
    let registry = registry();
    let log = annals::next::MemoryDeadLetters::new();
    let mut store = annals::next::InMemoryStore::new();
    let mut unit = store.begin();
    let mut coordinator = annals::next::Coordinator::new(&mut unit, &registry);
    let identified = annals::next::check(
        command(cover("account", 1), AccountCmd::Open.to_any()),
        &registry,
    )
    .unwrap()
    .identify(&at(1));
    let annals::next::Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap()
    else {
        panic!("new");
    };
    let mut prepared = coordinator
        .load(novel)
        .unwrap()
        .decode::<Account>(&registry)
        .unwrap();
    let decision = prepared.decide(&at(1), &registry, &log).unwrap();
    let mut other = Stream::<Account>::new(root(2), None).unwrap();
    assert_eq!(
        other.commit(decision, &registry, &mut []),
        Err(KernelError::Stale {
            stream: cover("account", 1),
            expected: 0,
            actual: 0
        })
    );
}

#[test]
fn commutative_cannot_compare_a_window_hidden_in_a_snapshot() {
    use crate::toys::{set, shelf, ShelfState};
    use annals::next::v1::ConflictResolution;
    let registry = registry();
    let log = annals::next::MemoryDeadLetters::new();
    let snapshot = Snapshot {
        sequence: 1,
        state: Some(pack(&ShelfState {
            items: [("a".to_string(), 1), ("b".to_string(), 2)].into(),
            ..Default::default()
        })),
        rules_version: "v1".to_string(),
    };
    let set_page = |sequence: u64, item: &str| EventPage {
        payload: Some(pack(&shelf::WasSet {
            item: item.to_string(),
            count: 3,
            window: None,
        })),
        meta: Some(meta(sequence, "v1")),
        sequence,
    };
    let mut stream = Stream::<Shelf>::resume(
        cover("shelf", 3),
        &snapshot,
        None,
        &[set_page(2, "c")],
        &registry,
    )
    .unwrap();
    let behind = |n: u128, expected| {
        command(cover("shelf", 3), set("d", 1))
            .id(n)
            .expecting(expected, ConflictResolution::Commutative)
    };
    assert_eq!(
        stream
            .execute(behind(11, 1), &registry, &at(9), &log, &mut [])
            .err(),
        Some(Failure::Kernel(KernelError::BeforeSnapshot {
            stream: cover("shelf", 3),
            first: 2
        }))
    );
    let applied = stream
        .execute(behind(12, 2), &registry, &at(9), &log, &mut [])
        .unwrap();
    assert!(
        matches!(&applied, annals::next::Executed::Applied(e) if e.len() == 1),
        "the window from the first held event is held: c and d commute: {applied:?}"
    );
}
