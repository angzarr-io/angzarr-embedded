//! Commands as one-page `CommandBook`s; what a chain recorded as
//! `EventBook`s with `EventMeta` on every page; keys as `Cover`s, event ids
//! as `EventRef`s.

use annals::next::{
    handle, Actor, CommandId, CommandMeta, Correlation, Edition, Envelope, EventId, InMemoryStore,
    Meta, Root, Routes, RulesVersion, StreamKey, Timestamp,
};
use annals_angzarr::pb::{self, command_page, page_header, Cover, EventRef, PageHeader, Uuid};
use annals_angzarr::{
    command_book, command_from_book, event_page, wire, BookError, Command, PayloadError, Stored,
};

use crate::toys::{
    account, correlation, ledger_key, meta, registry, request, rules, Account, AccountCmd,
    AccountEvent, Books, Ledger,
};

fn uuid(n: u8) -> Uuid {
    Uuid { value: vec![n; 16] }
}

fn origin() -> Command<AccountCmd> {
    Command {
        key: StreamKey::new("account", Edition::new("v2"), Root::from_bytes([7; 16])),
        meta: CommandMeta {
            cause: Some(EventId {
                stream: StreamKey::main("ledger", Root::from_bytes([9; 16])),
                seq: 4,
            }),
            ..CommandMeta::nth(
                Timestamp(1_700_000_000_000_001),
                Some(Actor("pseudonym-ann".to_string())),
                request(5),
                3,
            )
            .correlated(correlation("workflow-1"))
        },
        command: AccountCmd::Deposit(250),
    }
}

#[test]
#[ignore = "proposal: a command round-trips through a one-page CommandBook with its meta"]
fn a_command_round_trips_through_a_one_page_command_book_with_its_meta() {
    let cmd = origin();
    let book = command_book(&cmd);

    let cover = book.cover.as_ref().unwrap();
    assert_eq!(cover.domain, "account");
    assert_eq!(cover.root, Some(uuid(7)));
    assert_eq!(cover.edition.as_ref().map(|e| e.name.as_str()), Some("v2"));
    assert_eq!(book.pages.len(), 1, "one command, one page");

    let page = &book.pages[0];
    let Some(command_page::Payload::Command(any)) = &page.payload else {
        panic!("an inline Any payload")
    };
    assert_eq!(any.type_url, "/toy.account.cmd.v1.Deposit");
    let meta = page.meta.as_ref().unwrap();
    assert_eq!(meta.actor, "pseudonym-ann");
    assert_eq!(meta.request_id, "req-5");
    // Where the command's index and cause sit is wire::command_cause_*'s
    // choice (under review in angzarr): pinned by the round trip, not by
    // field.

    assert_eq!(
        command_from_book::<Account>(&book, Timestamp(1_700_000_000_000_001)),
        Ok(cmd),
        "recorded_at is not on the wire: the handler stamps it"
    );

    // A reaction's command, from a source above u32::MAX.
    let source = EventId {
        stream: account(1),
        seq: u64::from(u32::MAX) + 3,
    };
    let reaction = Command {
        key: account(2),
        meta: CommandMeta::reacting_to(
            &Meta {
                recorded_at: Timestamp(5),
                actor: None,
                request: request(6),
                command: CommandId::Origin { index: 0 },
                correlation: correlation("workflow-2"),
                cause: None,
                rules: RulesVersion::from("none"),
            },
            source,
            "books",
            1,
        ),
        command: AccountCmd::Open,
    };
    assert_eq!(
        command_from_book::<Account>(&command_book(&reaction), Timestamp(5)),
        Ok(reaction),
        "the reaction's source, component and index survive"
    );
}

#[test]
#[ignore = "proposal: a CommandBook annals cannot take is refused, never trimmed"]
fn a_command_book_annals_cannot_take_is_refused() {
    let book = command_book(&origin());
    let at = Timestamp(0);

    let mut two = book.clone();
    two.pages.push(two.pages[0].clone());
    assert_eq!(
        command_from_book::<Account>(&two, at),
        Err(BookError::NotOnePage { pages: 2 }),
        "angzarr would silently run only the first"
    );

    let mut anonymous = book.clone();
    anonymous.pages[0].meta.as_mut().unwrap().request_id.clear();
    assert_eq!(
        command_from_book::<Account>(&anonymous, at),
        Err(BookError::NoRequest)
    );

    let mut elsewhere = book.clone();
    elsewhere.cover.as_mut().unwrap().domain = "ledger".to_string();
    assert_eq!(
        command_from_book::<Account>(&elsewhere, at),
        Err(BookError::WrongDomain {
            found: "ledger".to_string(),
            expected: "account"
        })
    );

    let mut claim = book.clone();
    claim.pages[0].payload = Some(command_page::Payload::External(
        pb::PayloadReference::default(),
    ));
    assert_eq!(
        command_from_book::<Account>(&claim, at),
        Err(BookError::ExternalPayload)
    );

    let mut unknown = book;
    unknown.pages[0].payload = Some(command_page::Payload::Command(prost_types::Any {
        type_url: "/toy.account.cmd.v9.Close".to_string(),
        value: vec![],
    }));
    assert_eq!(
        command_from_book::<Account>(&unknown, at),
        Err(BookError::Payload(PayloadError::UnknownType {
            type_url: "/toy.account.cmd.v9.Close".to_string()
        }))
    );
}

/// One database table of opaque rows; the registry types it.
fn db() -> InMemoryStore<Stored> {
    InMemoryStore::new()
}

#[test]
#[ignore = "proposal: a book without a correlation gets one, generated from its request"]
fn a_command_without_a_correlation_gets_one_generated() {
    let mut cmd = origin();
    cmd.key = account(1);
    cmd.meta.correlation = None;
    let book = command_book(&Command {
        command: AccountCmd::Open,
        ..cmd
    });
    let received = command_from_book::<Account>(&book, Timestamp(10)).unwrap();
    assert_eq!(
        received.meta.correlation, None,
        "none on the wire, none given"
    );

    let registry = registry();
    let mut store = db();
    let r = rules();
    let recorded = handle::<Account, _, _>(
        registry.store(store.begin()),
        &Routes::new(),
        &received.key,
        &received.command,
        &received.meta,
        &r,
        &mut (),
    )
    .unwrap();
    let books = registry.event_books(&recorded).unwrap();
    let cover = books[0].cover.as_ref().unwrap();
    let page = books[0].pages[0].meta.as_ref().unwrap();
    assert_eq!(
        wire::correlation_from_wire(cover, &page.correlation_id),
        Some(Correlation::generated(&request(5))),
        "always set on what is recorded"
    );
}

#[test]
#[ignore = "proposal: a chain becomes EventBooks, EventMeta on every page, causes as EventRefs"]
fn a_chain_becomes_event_books_with_event_meta_on_every_page() {
    let registry = registry();
    let mut store = db();
    let r = rules();
    for (i, cmd) in [AccountCmd::Open, AccountCmd::Deposit(30)]
        .iter()
        .enumerate()
    {
        let recorded = handle::<Account, _, _>(
            registry.store(store.begin()),
            // Routes name the unit of work's type, so they are built per
            // unit (open question 7).
            &Routes::new().policy::<Books>(&r, &r),
            &account(1),
            cmd,
            &meta(i as u32, 1_000 + i as i64),
            &r,
            &mut (),
        )
        .unwrap();
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
            [account(1), ledger_key(Edition::MAIN)],
            "in the order first touched"
        );

        let deposit = &books[0].pages[0];
        assert_eq!(
            deposit.header,
            Some(PageHeader {
                sequence_type: Some(page_header::SequenceType::Sequence(1)),
                sync_mode: None,
            })
        );
        assert_eq!(books[0].next_sequence, 2);
        let deposit_meta = deposit.meta.as_ref().unwrap();
        assert_eq!(deposit_meta.actor, "pseudonym-ann");
        assert_eq!(deposit_meta.request_id, "req-1");
        assert_eq!(deposit_meta.command_index, 0);
        assert_eq!(deposit_meta.rules_version, "none");
        assert_eq!(
            deposit_meta.recorded_at,
            Some(prost_types::Timestamp {
                seconds: 0,
                nanos: 1_001_000
            })
        );
        assert_eq!(deposit_meta.cause, None);

        let record = books[1].pages[0].meta.as_ref().unwrap();
        assert_eq!(
            record.cause,
            Some(EventRef {
                domain: "account".to_string(),
                edition: "angzarr".to_string(),
                root: Some(Uuid {
                    value: Root::from(1).to_bytes().to_vec()
                }),
                sequence: 1,
            }),
            "the ledger's event names the deposit that caused it"
        );
        assert_eq!(
            record.request_id, "req-1",
            "a reaction's events keep the origin's request"
        );
        assert_eq!(
            recorded
                .events::<Ledger>(&ledger_key(Edition::MAIN))
                .unwrap()[0]
                .meta
                .command,
            CommandId::Reaction {
                source: EventId {
                    stream: account(1),
                    seq: 1
                },
                component: "books".to_string(),
                index: 0
            }
        );
        assert!(books
            .iter()
            .flat_map(|b| &b.pages)
            .all(|p| p.meta.is_some()));
    }
}

#[test]
#[ignore = "proposal: a cover's edition is normalised; what annals cannot model is refused"]
fn a_cover_is_a_stream_key_with_the_main_timeline_normalised() {
    let cover = |edition: Option<&str>| Cover {
        domain: "list".to_string(),
        root: Some(uuid(3)),
        edition: edition.map(|name| pb::Edition {
            name: name.to_string(),
            divergences: vec![],
        }),
        ..Cover::default()
    };
    let main = StreamKey::main("list", Root::from_bytes([3; 16]));
    for spelling in [None, Some(""), Some("angzarr")] {
        assert_eq!(
            wire::key(&cover(spelling)),
            Ok(main.clone()),
            "{spelling:?}"
        );
    }
    assert_eq!(wire::cover(&main).edition, None, "main is written unset");
    assert_eq!(
        wire::key(&cover(Some("v2"))),
        Ok(StreamKey::new(
            "list",
            Edition::new("v2"),
            Root::from_bytes([3; 16])
        ))
    );

    let mut diverged = cover(Some("v2"));
    diverged
        .edition
        .as_mut()
        .unwrap()
        .divergences
        .push(pb::DomainDivergence {
            domain: "list".to_string(),
            sequence: 4,
        });
    assert_eq!(
        wire::key(&diverged),
        Err(BookError::Divergences {
            edition: "v2".to_string()
        })
    );
    let mut short = cover(None);
    short.root = Some(Uuid { value: vec![1; 8] });
    assert_eq!(wire::key(&short), Err(BookError::BadRoot { len: 8 }));
    let mut rootless = cover(None);
    rootless.root = None;
    assert_eq!(wire::key(&rootless), Err(BookError::NoRoot));
}

#[test]
#[ignore = "proposal: u64 sequences survive above u32::MAX"]
fn sequences_above_u32_max_survive_every_mapping() {
    let big = u64::from(u32::MAX) + 7;
    let id = EventId {
        stream: StreamKey::new("list", Edition::new("v2"), Root::from(1)),
        seq: big,
    };
    let event_ref = wire::event_ref(&id);
    assert_eq!(event_ref.sequence, big);
    assert_eq!(wire::event_id(&event_ref), Ok(id.clone()));

    let envelope = Envelope {
        seq: big,
        meta: Meta {
            recorded_at: Timestamp(-1),
            actor: None,
            request: request(1),
            command: CommandId::Origin { index: 0 },
            correlation: correlation("c"),
            cause: Some(id),
            rules: RulesVersion::from("none"),
        },
        event: AccountEvent::Withdrew(1),
    };
    let page = event_page(&envelope);
    assert_eq!(
        page.header.unwrap().sequence_type,
        Some(page_header::SequenceType::Sequence(big))
    );
    assert_eq!(page.meta.unwrap().cause.unwrap().sequence, big);
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
