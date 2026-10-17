//! Books as one-page `CommandBook`s (`cover`, `handling`, `page {
//! provenance, origin, body }`); what a chain recorded as `EventBook`s with
//! `EventMeta` on every page; keys as `Cover`s (main = the unset edition),
//! event ids as `EventRef`s; the `Outcome`.

use annals::next::{
    handle, Book, BookOf, Causation, ConflictResolution, EditionName, Envelope, EventId, Failure,
    InMemoryStore, KernelError, Malformed, Meta, Origin, Provenance, Root, Routes, RulesVersion,
    StoreError, StreamKey, Timestamp,
};
use annals_angzarr::pb::{self, command_page, origin, Cover, Uuid};
use annals_angzarr::{
    book_from_wire, command_book, event_page, fingerprint, fingerprinted, ids, outcome, pack, wire,
    BookError, PayloadError, Stored,
};
use prost::Message;

use crate::toys::{
    account, ann, at, book, cmd, correlation, fact, ledger_key, minted, registry, request, rules,
    Account, AccountCmd, AccountEvent, AccountFact, Books, Ledger,
};

fn uuid(n: u8) -> Uuid {
    Uuid { value: vec![n; 16] }
}

fn v2() -> Option<EditionName> {
    Some(EditionName::new("v2").unwrap())
}

/// A deposit to account 7 in edition v2: request 5 (a request id names
/// one command), in workflow 1, expecting the stream at 4 and resolving
/// its own conflicts.
fn deposit() -> BookOf<Account> {
    fingerprinted(
        Book::command(
            StreamKey::new("account", v2(), Root::from_bytes([7; 16])),
            AccountCmd::Deposit(250),
            Origin::request(request(5)),
            Provenance {
                correlation: Some(correlation(1)),
                ..ann()
            },
        )
        .expecting(4, ConflictResolution::AggregateHandles),
    )
}

#[test]
#[ignore = "proposal: a book round-trips through a one-page CommandBook"]
fn a_book_round_trips_through_a_one_page_command_book() {
    let book = deposit();
    let wire_book = command_book(&book);

    let cover = wire_book.cover.as_ref().unwrap();
    assert_eq!(cover.domain, "account");
    assert_eq!(cover.root, Some(uuid(7)));
    assert_eq!(cover.edition.as_deref(), Some("v2"));
    let handling = wire_book.handling.as_ref().unwrap();
    assert_eq!(handling.expected_sequence, Some(4));
    assert_eq!(
        handling.conflict_resolution(),
        pb::ConflictResolution::AggregateHandles
    );

    let page = wire_book.page.as_ref().unwrap();
    let Some(command_page::Body::Command(command)) = &page.body else {
        panic!("the command arm")
    };
    assert_eq!(
        command.payload.as_ref().unwrap().type_url,
        "/toy.account.cmd.v1.Deposit"
    );
    assert_eq!(
        page.provenance,
        Some(pb::Provenance {
            actor: "pseudonym-ann".to_string(),
            correlation_id: Some(ids::to_wire(correlation(1).0)),
        }),
        "the correlation as its 16 bytes"
    );
    assert_eq!(
        page.origin.as_ref().unwrap().kind,
        Some(origin::Kind::Request(pb::RequestOrigin {
            request_id: Some(ids::to_wire(request(5).0)),
            minted: false,
        }))
    );
    assert_eq!(
        book_from_wire::<Account>(&wire_book),
        Ok(book),
        "fingerprinted over the bytes received, which are prost's"
    );

    // A policy's command, caused by an event above u32::MAX, no handling.
    let reaction = fingerprinted(BookOf::<Account>::command(
        account(2),
        AccountCmd::Open,
        Origin::Causation(Causation {
            event: EventId {
                stream: account(1),
                seq: u64::from(u32::MAX) + 3,
            },
            component: "books".to_string(),
            index: 1,
        }),
        Provenance {
            actor: None,
            correlation: Some(correlation(2)),
        },
    ));
    assert_eq!(
        book_from_wire::<Account>(&command_book(&reaction)),
        Ok(reaction),
        "the causation's event, component and index survive"
    );

    // A fact, on the fact arm, with its fact origin.
    let payment = fingerprinted(BookOf::<Account>::fact(
        account(1),
        AccountFact::PaymentReceived(5),
        Origin::fact("pi_1", "Stripe webhook"),
        Provenance::default(),
    ));
    let wire_payment = command_book(&payment);
    assert!(matches!(
        wire_payment.page.as_ref().unwrap().body,
        Some(command_page::Body::Fact(_))
    ));
    assert_eq!(wire_payment.handling, None, "no handling sent: none");
    assert_eq!(book_from_wire::<Account>(&wire_payment), Ok(payment));

    // A request whose id the receiver mints: no origin on the wire.
    let unkeyed = fingerprinted(BookOf::<Account>::minting(
        account(1),
        AccountCmd::Open,
        Provenance::default(),
    ));
    let mut wire_unkeyed = command_book(&unkeyed);
    assert_eq!(wire_unkeyed.page.as_ref().unwrap().origin, None);
    assert_eq!(
        book_from_wire::<Account>(&wire_unkeyed),
        Ok(unkeyed.clone())
    );
    wire_unkeyed.page.as_mut().unwrap().origin = Some(pb::Origin {
        kind: Some(origin::Kind::Request(pb::RequestOrigin::default())),
    });
    assert_eq!(
        book_from_wire::<Account>(&wire_unkeyed),
        Ok(unkeyed),
        "a request origin with no id is a request whose id is minted"
    );
}

#[test]
#[ignore = "proposal: a book is fingerprinted over its payload bytes exactly as received"]
fn a_book_is_fingerprinted_over_the_bytes_received() {
    let mut wire_book = command_book(&book(&account(1), AccountCmd::Deposit(1), 1));
    let canonical = cmd::Deposit { cents: 1 }.encode_to_vec();
    // The same message, encoded another valid way: cents twice, the last
    // one winning, as protobuf decoding says.
    let mut other = cmd::Deposit { cents: 9 }.encode_to_vec();
    other.extend_from_slice(&canonical);
    let received = prost_types::Any {
        type_url: "/toy.account.cmd.v1.Deposit".to_string(),
        value: other,
    };
    wire_book.page.as_mut().unwrap().body = Some(command_page::Body::Command(pb::Command {
        payload: Some(received.clone()),
    }));

    let read = book_from_wire::<Account>(&wire_book).unwrap();
    assert!(matches!(
        read.body,
        annals::next::Body::Command(AccountCmd::Deposit(1))
    ));
    assert_eq!(
        read.fingerprint,
        Some(fingerprint(&received)),
        "the bytes as they arrived, never re-encoded"
    );
    assert_ne!(
        read.fingerprint,
        book(&account(1), AccountCmd::Deposit(1), 1).fingerprint,
        "so the same value sent as other bytes is another fingerprint"
    );

    assert_ne!(
        fingerprint(&pack(&cmd::Deposit { cents: 1 })),
        fingerprint(&pack(&cmd::Deposit { cents: 2 })),
        "another payload, another fingerprint"
    );
    assert_ne!(
        fingerprint(&prost_types::Any {
            type_url: "/a".to_string(),
            value: b"b".to_vec()
        }),
        fingerprint(&prost_types::Any {
            type_url: "/ab".to_string(),
            value: vec![]
        }),
        "the type URL is length-prefixed, so it never runs into the value"
    );
    assert_eq!(
        book(&account(1), AccountCmd::Deposit(1), 1).fingerprint,
        book(&account(1), AccountCmd::Deposit(1), 2).fingerprint,
        "deterministic: an honest retry reproduces it"
    );
}

#[test]
#[ignore = "proposal: a CommandBook annals cannot read is refused, never repaired"]
fn a_command_book_annals_cannot_read_is_refused() {
    let wire_book = command_book(&deposit());
    let refused = |mutate: &dyn Fn(&mut pb::CommandBook)| {
        let mut b = wire_book.clone();
        mutate(&mut b);
        book_from_wire::<Account>(&b).unwrap_err()
    };

    assert_eq!(refused(&|b| b.cover = None), BookError::NoCover);
    assert_eq!(refused(&|b| b.page = None), BookError::NoPage);
    assert_eq!(
        refused(&|b| b.page.as_mut().unwrap().body = None),
        BookError::NoBody
    );
    assert_eq!(
        refused(&|b| b.cover.as_mut().unwrap().domain = "ledger".to_string()),
        BookError::WrongDomain {
            found: "ledger".to_string(),
            expected: "account"
        }
    );
    assert_eq!(
        refused(&|b| b.cover.as_mut().unwrap().edition = Some(String::new())),
        BookError::EmptyEdition,
        "main is the unset edition, never \"\""
    );
    assert_eq!(
        refused(&|b| b.handling.as_mut().unwrap().conflict_resolution = 9),
        BookError::UnknownConflictResolution { value: 9 }
    );
    assert_eq!(
        refused(&|b| {
            b.page
                .as_mut()
                .unwrap()
                .provenance
                .as_mut()
                .unwrap()
                .correlation_id = Some(Uuid { value: vec![1; 36] })
        }),
        BookError::BadId { len: 36 },
        "an id is 16 bytes, never its text"
    );
    assert_eq!(
        refused(&|b| {
            b.page.as_mut().unwrap().origin = Some(pb::Origin {
                kind: Some(origin::Kind::Request(pb::RequestOrigin {
                    request_id: Some(Uuid { value: vec![] }),
                    minted: false,
                })),
            })
        }),
        BookError::BadId { len: 0 },
        "set but empty is malformed, not absent"
    );
    assert_eq!(
        refused(&|b| {
            b.page.as_mut().unwrap().origin = Some(pb::Origin {
                kind: Some(origin::Kind::Request(pb::RequestOrigin {
                    request_id: None,
                    minted: true,
                })),
            })
        }),
        BookError::NoRequestId,
        "a sender's minted flag with no id to carry it to the kernel"
    );
    assert_eq!(
        refused(&|b| {
            b.page.as_mut().unwrap().body = Some(command_page::Body::Command(pb::Command {
                payload: Some(prost_types::Any {
                    type_url: "/toy.account.cmd.v9.Close".to_string(),
                    value: vec![],
                }),
            }))
        }),
        BookError::Payload(PayloadError::UnknownType {
            type_url: "/toy.account.cmd.v9.Close".to_string()
        })
    );
    assert_eq!(
        refused(&|b| {
            b.page.as_mut().unwrap().body = Some(command_page::Body::Fact(pb::Fact {
                payload: Some(pack(&cmd::Deposit { cents: 1 })),
            }))
        }),
        BookError::Payload(PayloadError::UnknownType {
            type_url: "/toy.account.cmd.v1.Deposit".to_string()
        }),
        "the arm picks the decoder: a command type on the fact arm is no fact"
    );
}

/// One database table of opaque rows; the registry types it.
fn db() -> InMemoryStore<Stored> {
    InMemoryStore::new()
}

#[test]
#[ignore = "proposal: a malformed book off the wire is refused by the kernel, before business code"]
fn a_malformed_book_off_the_wire_is_refused_by_the_kernel() {
    let mut wire_book = command_book(&BookOf::<Account>::fact(
        account(1),
        AccountFact::PaymentReceived(5),
        Origin::fact("pi_1", "Stripe webhook"),
        Provenance::default(),
    ));
    wire_book.page.as_mut().unwrap().origin = Some(wire::origin(&Origin::request(request(1))));
    let read = book_from_wire::<Account>(&wire_book).expect("it reads: agreement is the kernel's");

    let registry = registry();
    let mut store = db();
    let r = rules();
    let failure = handle::<Account, _, _>(
        registry.store(store.begin()),
        &Routes::new(),
        &read,
        &at(1),
        &r,
        &mut (),
    )
    .err()
    .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Malformed {
            stream: account(1),
            malformed: Malformed::FactWithoutFactOrigin
        })
    );
    assert_eq!(
        outcome(&Err(failure)),
        None,
        "an error status, not an outcome"
    );

    let mut sent_minted = command_book(&book(&account(1), AccountCmd::Open, 1));
    sent_minted.page.as_mut().unwrap().origin = Some(wire::origin(&Origin::minted(request(1))));
    let read = book_from_wire::<Account>(&sent_minted).expect("read as sent");
    let failure = handle::<Account, _, _>(
        registry.store(store.begin()),
        &Routes::new(),
        &read,
        &at(1),
        &r,
        &mut (),
    )
    .err()
    .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Malformed {
            stream: account(1),
            malformed: Malformed::MintedBySender
        }),
        "only the receiver mints"
    );
}

#[test]
#[ignore = "proposal: every recorded page has a request id and a correlation, as angzarr derives them"]
fn every_recorded_page_has_a_request_id_and_a_correlation() {
    let registry = registry();
    let mut store = db();
    let r = rules();
    let run = |store: &mut InMemoryStore<Stored>, b: &BookOf<Account>, sampled| {
        let recorded = handle::<Account, _, _>(
            registry.store(store.begin()),
            &Routes::new(),
            b,
            &sampled,
            &r,
            &mut (),
        )
        .unwrap();
        let books = registry.event_books(&recorded).unwrap();
        books[0].pages[0].meta.clone().unwrap()
    };
    let correlation_of = |meta: &pb::EventMeta| {
        meta.provenance
            .as_ref()
            .unwrap()
            .correlation_id
            .clone()
            .unwrap()
    };

    let keyed = run(&mut store, &book(&account(1), AccountCmd::Open, 5), at(1));
    assert_eq!(
        correlation_of(&keyed),
        ids::to_wire(request(5).0),
        "the request id verbatim"
    );

    let unkeyed = fingerprinted(BookOf::<Account>::minting(
        account(1),
        AccountCmd::Deposit(1),
        Provenance::default(),
    ));
    let meta = run(&mut store, &unkeyed, at(2));
    assert_eq!(
        correlation_of(&meta),
        ids::to_wire(minted(2).0),
        "the minted id"
    );
    assert_eq!(
        meta.origin.unwrap().kind,
        Some(origin::Kind::Request(pb::RequestOrigin {
            request_id: Some(ids::to_wire(minted(2).0)),
            minted: true,
        })),
        "recorded, flagged minted"
    );

    let payment = fingerprinted(BookOf::<Account>::fact(
        account(1),
        AccountFact::PaymentReceived(5),
        Origin::fact("pi_1234", "Stripe webhook"),
        Provenance::default(),
    ));
    let meta = run(&mut store, &payment, at(3));
    assert_eq!(
        correlation_of(&meta).to_string(),
        "917f26ae-ea2e-58af-86de-77013a69617e",
        "UUIDv5 of the external id under angzarr's FACT_CORRELATION namespace"
    );
}

#[test]
#[ignore = "proposal: a chain becomes EventBooks, EventMeta on every page, causations as EventRefs"]
fn a_chain_becomes_event_books_with_event_meta_on_every_page() {
    let registry = registry();
    let mut store = db();
    let r = rules();
    for (i, cmd) in [AccountCmd::Open, AccountCmd::Deposit(30)]
        .into_iter()
        .enumerate()
    {
        let result = handle::<Account, _, _>(
            registry.store(store.begin()),
            // Routes name the unit of work's type, so they are built per
            // unit (an open question).
            &Routes::new().policy::<Books>(&r, &r),
            &book(&account(1), cmd, i as u32),
            &at(1_000 + i as i64),
            &r,
            &mut (),
        );
        assert_eq!(
            outcome(&result).map(|o| o.delivery()),
            Some(pb::DeliveryOutcome::Applied)
        );
        let recorded = result.unwrap();
        if i == 0 {
            continue;
        }

        let books = registry.event_books(&recorded).unwrap();
        assert_eq!(books.len(), 2, "one book per stream the chain touched");
        let keys: Vec<StreamKey> = books
            .iter()
            .map(|b| wire::key(b.cover.as_ref().unwrap()).unwrap())
            .collect();
        assert_eq!(
            keys,
            [account(1), ledger_key(None)],
            "in the order first touched"
        );
        assert_eq!(
            books[0].cover.as_ref().unwrap().edition,
            None,
            "main: unset"
        );

        let deposit = &books[0].pages[0];
        assert_eq!(deposit.sequence, 1, "always definite");
        assert_eq!(books[0].next_sequence, 2);
        assert_eq!(
            deposit.meta,
            Some(pb::EventMeta {
                provenance: Some(pb::Provenance {
                    actor: "pseudonym-ann".to_string(),
                    correlation_id: Some(ids::to_wire(request(1).0)),
                }),
                origin: Some(wire::origin(&Origin::request(request(1)))),
                recorded_at: Some(prost_types::Timestamp {
                    seconds: 0,
                    nanos: 1_001_000
                }),
                base_sequence: 1,
                rules_version: "none".to_string(),
            })
        );

        let record = books[1].pages[0].meta.as_ref().unwrap();
        assert_eq!(
            record.origin,
            Some(pb::Origin {
                kind: Some(origin::Kind::Causation(pb::Causation {
                    event: Some(pb::EventRef {
                        stream: Some(Cover {
                            domain: "account".to_string(),
                            root: Some(Uuid {
                                value: Root::from(1).to_bytes().to_vec()
                            }),
                            edition: None,
                        }),
                        sequence: 1,
                    }),
                    component: "books".to_string(),
                    index: 0,
                }))
            }),
            "the ledger's event names the deposit that caused it, and the policy"
        );
        assert_eq!(
            record.provenance.as_ref().unwrap().correlation_id,
            Some(ids::to_wire(request(1).0)),
            "a reaction keeps the origin's correlation, and has no request of its own"
        );
        assert_eq!(
            recorded.events::<Ledger>(&ledger_key(None)).unwrap()[0]
                .meta
                .origin
                .cause(),
            Some(&EventId {
                stream: account(1),
                seq: 1
            })
        );
    }
}

#[test]
#[ignore = "proposal: a cover is a stream key; main is the unset edition, and \"\" is refused"]
fn a_cover_is_a_stream_key_with_main_the_unset_edition() {
    let cover = |edition: Option<&str>| Cover {
        domain: "list".to_string(),
        root: Some(uuid(3)),
        edition: edition.map(str::to_string),
    };
    let main = StreamKey::main("list", Root::from_bytes([3; 16]));
    assert_eq!(wire::key(&cover(None)), Ok(main.clone()));
    assert_eq!(wire::cover(&main), cover(None), "main is written unset");
    let named = StreamKey::new("list", v2(), Root::from_bytes([3; 16]));
    assert_eq!(wire::key(&cover(Some("v2"))), Ok(named.clone()));
    assert_eq!(wire::cover(&named), cover(Some("v2")));
    assert_eq!(wire::key(&cover(Some(""))), Err(BookError::EmptyEdition));
    assert_eq!(wire::edition_name(Some("")), Err(BookError::EmptyEdition));
    assert_eq!(
        wire::key(&cover(Some("angzarr"))),
        Ok(StreamKey::new(
            "list",
            Some(EditionName::new("angzarr").unwrap()),
            Root::from_bytes([3; 16])
        )),
        "no name is special: \"angzarr\" is an edition like any other"
    );

    let mut short = cover(None);
    short.root = Some(Uuid { value: vec![1; 8] });
    assert_eq!(wire::key(&short), Err(BookError::BadRoot { len: 8 }));
    let mut rootless = cover(None);
    rootless.root = None;
    assert_eq!(wire::key(&rootless), Err(BookError::NoRoot));
    assert_eq!(
        wire::event_id(&pb::EventRef {
            stream: None,
            sequence: 1
        }),
        Err(BookError::NoStream)
    );
}

#[test]
#[ignore = "proposal: u64 sequences survive above u32::MAX"]
fn sequences_above_u32_max_survive_every_mapping() {
    let big = u64::from(u32::MAX) + 7;
    let id = EventId {
        stream: StreamKey::new("list", v2(), Root::from(1)),
        seq: big,
    };
    let event_ref = wire::event_ref(&id);
    assert_eq!(event_ref.sequence, big);
    assert_eq!(wire::event_id(&event_ref), Ok(id.clone()));

    let envelope = Envelope {
        seq: big,
        meta: Meta {
            actor: None,
            correlation: correlation(3),
            origin: Origin::Causation(Causation {
                event: id,
                component: "books".to_string(),
                index: 0,
            }),
            recorded_at: Timestamp(-1),
            base_sequence: big,
            rules_version: RulesVersion::from("none"),
        },
        event: AccountEvent::Withdrew(1),
    };
    let page = event_page(&envelope);
    assert_eq!(page.sequence, big);
    let meta = page.meta.unwrap();
    assert_eq!(meta.base_sequence, big);
    assert_eq!(wire::meta(&meta), Ok(envelope.meta.clone()));
    assert_eq!(
        wire::timestamp(Timestamp(-1)),
        prost_types::Timestamp {
            seconds: -1,
            nanos: 999_999_000
        },
        "before the epoch, floored"
    );
    assert_eq!(
        wire::micros(&prost_types::Timestamp {
            seconds: -1,
            nanos: 999_999_000
        }),
        Ok(Timestamp(-1))
    );
    assert_eq!(
        wire::micros(&prost_types::Timestamp {
            seconds: 0,
            nanos: 1
        }),
        Err(BookError::SubMicrosecond {
            seconds: 0,
            nanos: 1
        })
    );
}

#[test]
#[ignore = "proposal: a duplicate is answered DUPLICATE, a reused key never; annals never parks"]
fn a_recorded_key_is_answered_duplicate() {
    let duplicate: Result<annals::next::Recorded, Failure<crate::toys::NotOpen>> =
        Err(Failure::Kernel(KernelError::Store(StoreError::Duplicate {
            stream: account(1),
            origin: Origin::fact("pi_1", "Stripe webhook"),
            from: 1,
            to: 2,
        })));
    let answer = outcome(&duplicate).unwrap();
    assert_eq!(answer.delivery(), pb::DeliveryOutcome::Duplicate);
    assert!(answer.projector_failures.is_empty());

    let rejected: Result<annals::next::Recorded, _> = Err(Failure::Rejected(crate::toys::NotOpen));
    assert_eq!(outcome(&rejected), None);

    for origin in [
        Origin::request(request(1)),
        Origin::fact("pi_1", "Stripe webhook"),
    ] {
        let reused: Result<annals::next::Recorded, Failure<crate::toys::NotOpen>> =
            Err(Failure::Kernel(KernelError::Store(StoreError::KeyReused {
                stream: account(1),
                origin,
                from: 1,
                to: 2,
            })));
        assert_eq!(
            outcome(&reused),
            None,
            "FAILED_PRECONDITION, an error status: never DUPLICATE, a fact by default too"
        );
    }

    let divergent: Result<annals::next::Recorded, Failure<crate::toys::NotOpen>> = Err(
        Failure::Kernel(KernelError::Store(StoreError::DivergentDuplicate {
            stream: account(1),
            origin: Origin::fact("stmt_9", "bank statement"),
            from: 1,
            to: 2,
        })),
    );
    assert_eq!(
        outcome(&divergent).unwrap().delivery(),
        pb::DeliveryOutcome::Duplicate,
        "a fact configured DUPLICATE: answered so, as angzarr does; the alert is the application's"
    );

    // A fact's wire form names its type.
    let any = fact::PaymentReceived { cents: 5 };
    assert_eq!(pack(&any).type_url, "/toy.account.fact.v1.PaymentReceived");
}
