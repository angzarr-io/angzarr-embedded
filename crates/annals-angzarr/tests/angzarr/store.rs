//! The storage shape: opaque rows (type URL, page as written, metadata
//! columns), typed for the kernel by the registry; strict replay; golden
//! fixtures; the async port.

use annals::next::{
    handle, AsyncEventStore, AsyncUnitOfWork, CommandId, Edition, Envelope, EventId, EventStore,
    InMemoryStore, Meta, Ready, Routes, RulesVersion, Seq, StoreError, Timestamp,
};
use annals_angzarr::pb::{event_page, page_header, EventPage, PageHeader};
use annals_angzarr::{
    assert_replay_fixtures, pack, read_event, replay_stored, wire, PayloadError, ReadError,
    ReadProblem, Registry, ReplayFixture, Stored, UpcastError, Upcaster,
};
use annals_diagnostic::Report;
use prost::Message;

use crate::ready;
use crate::toys::{
    account, correlation, ledger_key, meta, registry, request, rules, v1, v2, v3, v9, Account,
    AccountCmd, AccountEvent, Balance, Books,
};

/// A row as some older release wrote it: `any` at `seq`, with its page.
fn row(seq: Seq, any: prost_types::Any) -> Envelope<Stored> {
    let meta = Meta {
        recorded_at: Timestamp(1_000 + seq as i64),
        actor: None,
        request: request(seq as u32),
        command: CommandId::Origin { index: 0 },
        correlation: correlation("old"),
        cause: None,
        rules: RulesVersion::from("none"),
    };
    let page = EventPage {
        header: Some(PageHeader {
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
            sync_mode: None,
        }),
        created_at: Some(wire::timestamp(meta.recorded_at)),
        payload: Some(event_page::Payload::Event(any.clone())),
        meta: Some(wire::event_meta(&meta)),
    };
    Envelope {
        seq,
        meta,
        event: Stored {
            type_url: any.type_url,
            page: page.encode_to_vec(),
        },
    }
}

/// An account's life across three payload versions.
fn old_log() -> Vec<Envelope<Stored>> {
    vec![
        row(0, pack(&v3::Opened {})),
        row(1, pack(&v1::Deposit { amount: 5 })),
        row(2, pack(&v2::Deposited { cents: 30 })),
        row(
            3,
            pack(&v3::Deposited {
                cents: 7,
                currency: "EUR".to_string(),
            }),
        ),
    ]
}

#[test]
#[ignore = "proposal: rows are the type URL, the page as written and the metadata as columns"]
fn the_store_keeps_opaque_rows_as_written() {
    let registry = registry();
    let mut store = InMemoryStore::<Stored>::new();
    let r = rules();
    for (i, cmd) in [AccountCmd::Open, AccountCmd::Deposit(30)]
        .iter()
        .enumerate()
    {
        handle::<Account, _, _>(
            registry.store(store.begin()),
            // Routes name the unit of work's type, so they are built per
            // unit (open question 7).
            &Routes::new().policy::<Books>(&r, &r),
            &account(1),
            cmd,
            &meta(i as u32, 0),
            &r,
            &mut (),
        )
        .unwrap();
    }

    let rows = store.read(&account(1)).unwrap().unwrap().events;
    assert_eq!(rows[1].event.type_url, "/toy.account.v3.Deposited");
    let page = EventPage::decode(rows[1].event.page.as_slice()).unwrap();
    assert_eq!(
        page.meta,
        Some(wire::event_meta(&rows[1].meta)),
        "the page carries EventMeta"
    );
    assert_eq!(
        page.payload,
        Some(event_page::Payload::Event(pack(&v3::Deposited {
            cents: 30,
            currency: "EUR".to_string()
        })))
    );

    let ledger = store
        .read(&ledger_key(Edition::MAIN))
        .unwrap()
        .unwrap()
        .events;
    assert_eq!(
        ledger[1].meta.command,
        CommandId::Reaction {
            source: EventId {
                stream: account(1),
                seq: 1
            },
            component: "books".to_string(),
            index: 0
        },
        "the columns keep a reaction's component, which EventMeta has no field for"
    );

    let mut typed = registry.store(store.begin());
    let read: Vec<AccountEvent> = EventStore::<AccountEvent>::read(&mut typed, &account(1))
        .unwrap()
        .unwrap()
        .events
        .into_iter()
        .map(|e| e.event)
        .collect();
    assert_eq!(
        read,
        [
            AccountEvent::Opened,
            AccountEvent::Deposited {
                cents: 30,
                currency: "EUR".to_string()
            }
        ]
    );
}

#[test]
#[ignore = "proposal: old rows are upcast on every read and never rewritten"]
fn old_rows_are_upcast_on_read_and_never_rewritten() {
    let registry = registry();
    let mut store = InMemoryStore::<Stored>::new();
    store.append(&account(1), 0, &old_log()).unwrap();

    let mut typed = registry.store(store.begin());
    let events = EventStore::<AccountEvent>::read(&mut typed, &account(1))
        .unwrap()
        .unwrap()
        .events;
    drop(typed);
    let deposit = |cents| AccountEvent::Deposited {
        cents,
        currency: "EUR".to_string(),
    };
    assert_eq!(
        events.iter().map(|e| e.event.clone()).collect::<Vec<_>>(),
        [AccountEvent::Opened, deposit(500), deposit(30), deposit(7)]
    );
    for (e, original) in events.iter().zip(old_log()) {
        assert_eq!(e.seq, original.seq);
        assert_eq!(e.meta, original.meta, "the columns, untouched");
    }
    assert_eq!(
        store.read(&account(1)).unwrap().unwrap().events,
        old_log(),
        "the store still holds what was written"
    );

    let stream = replay_stored::<Account>(&registry, account(1), old_log(), &rules()).unwrap();
    assert_eq!(stream.state().cents, 537);
}

#[test]
#[ignore = "proposal: strict replay: a type URL nothing upcasts or decodes is refused, named"]
fn an_unknown_type_url_is_refused_naming_it() {
    let registry = registry();
    let bonus = row(1, pack(&v9::Bonus { cents: 1 }));
    let error = read_event::<AccountEvent>(&registry, &account(1), bonus.clone()).unwrap_err();
    assert_eq!(
        error,
        ReadError {
            stream: account(1),
            seq: 1,
            problem: ReadProblem::Payload(PayloadError::UnknownType {
                type_url: "/toy.account.v9.Bonus".to_string()
            }),
        }
    );
    assert_eq!(error.type_url(), Some("/toy.account.v9.Bonus"));
    assert!(
        error.to_string().contains("/toy.account.v9.Bonus"),
        "{error}"
    );

    let mut store = InMemoryStore::<Stored>::new();
    store
        .append(&account(1), 0, &[old_log()[0].clone(), bonus])
        .unwrap();
    let mut typed = registry.store(store.begin());
    let Err(StoreError::Unreadable {
        stream,
        seq,
        reason,
    }) = EventStore::<AccountEvent>::read(&mut typed, &account(1))
    else {
        panic!("an unreadable event fails the read: it is never skipped")
    };
    assert_eq!((stream, seq), (account(1), 1));
    assert!(
        reason.to_string().contains("/toy.account.v9.Bonus"),
        "{reason}"
    );

    let failing = Registry::new().domain::<Account>(Upcaster::new("u", "account").on_event(
        "toy.account.v1.Deposit",
        |_| {
            Err(UpcastError::Rule {
                source: "toy.account.v1.Deposit".to_string(),
                error: Report::new("toy-account-no-euros", "no euros in v1"),
            })
        },
    ));
    let error =
        read_event::<AccountEvent>(&failing, &account(1), old_log()[1].clone()).unwrap_err();
    assert!(matches!(
        error.problem,
        ReadProblem::Upcast(UpcastError::Rule { ref source, .. }) if source == "toy.account.v1.Deposit"
    ));
}

#[test]
#[ignore = "proposal: golden replay fixtures, old rows in, states out"]
fn golden_replay_fixtures_still_replay() {
    let registry = registry();
    registry.check().unwrap();
    assert_replay_fixtures::<Account>(
        &registry,
        &rules(),
        &[ReplayFixture {
            name: "a life across three payload versions",
            key: account(1),
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
                        cents: 537,
                    },
                ),
            ],
        }],
    );
}

#[test]
#[ignore = "proposal: a fixture that no longer replays fails, naming itself"]
#[should_panic(expected = "a drifted fold")]
fn a_fixture_whose_states_drifted_fails() {
    assert_replay_fixtures::<Account>(
        &registry(),
        &rules(),
        &[ReplayFixture {
            name: "a drifted fold",
            key: account(1),
            log: old_log(),
            states: vec![(
                3,
                Balance {
                    open: true,
                    cents: 538,
                },
            )],
        }],
    );
}

#[test]
#[ignore = "proposal: the async port reads and writes the same rows"]
fn the_async_port_reads_and_writes_the_same_rows() {
    let registry = registry();
    let mut store = InMemoryStore::<Stored>::new();
    {
        let mut port = registry.store(Ready(store.begin()));
        assert_eq!(
            ready(AsyncEventStore::<Stored>::append(
                port.stored(),
                &account(1),
                0,
                &old_log()
            )),
            Ok(4)
        );
        let events = ready(AsyncEventStore::<AccountEvent>::read_from(
            &mut port,
            &account(1),
            3,
        ))
        .unwrap()
        .unwrap()
        .events;
        assert_eq!(
            events[0].event,
            AccountEvent::Deposited {
                cents: 7,
                currency: "EUR".to_string()
            }
        );
        ready(port.commit()).unwrap();
    }
    assert_eq!(store.read(&account(1)).unwrap().unwrap().len, 4);
}
