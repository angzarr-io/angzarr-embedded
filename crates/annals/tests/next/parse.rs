//! Step 1: one domain-blind parse checks everything, and what it accepts
//! has non-optional accessors. These run: the parse is implemented.

use annals::next::v1::{
    command_page, CommandBook, CommandPage, ConflictResolution, Cover, EventRef, FactOrigin,
    Handling, Uuid,
};
use annals::next::{check, ids, pack, Body, Malformed};

use crate::toys::{
    account, command, correlation, cover, cover_in, fact, registry, root, set, Edit,
};

fn deposit(cents: i64) -> annals::next::Any {
    pack(&account::cmd::Deposit { cents })
}

fn payment(cents: i64) -> annals::next::Any {
    pack(&account::fact::PaymentReceived { cents })
}

#[test]
fn a_checked_command_has_non_optional_accessors() {
    let book = command(cover("account", 1), deposit(5));
    let checked = check(book, &registry()).unwrap();

    assert_eq!(checked.cover(), &cover("account", 1));
    assert_eq!(checked.domain(), "account");
    assert_eq!(checked.root(), &root(1), "never an Option: checked present");
    assert_eq!(checked.root().value.len(), 16, "and 16 bytes");
    assert_eq!(checked.edition(), None, "main");
    assert_eq!(checked.payload(), &deposit(5));
    assert_eq!(
        checked.body(),
        &Body::Command {
            payload: deposit(5)
        }
    );
    assert!(!checked.body().is_fact());
    assert_eq!(checked.body().source(), None);
    assert_eq!(checked.command_id(), None, "assigned at identify");
    assert_eq!(checked.correlation_id(), None, "assigned at identify");
    assert_eq!(checked.expected_sequence(), None, "stamped at load");
    assert_eq!(checked.conflict_resolution(), None, "resolved at load");
    assert!(!checked.is_reaction());
}

#[test]
fn a_checked_fact_carries_its_source_in_its_body() {
    let book = fact(cover_in("account", 2, "what-if"), payment(70), "pi_1234");
    let checked = check(book, &registry()).unwrap();

    assert_eq!(checked.edition(), Some("what-if"));
    let source = FactOrigin {
        external_id: "pi_1234".to_string(),
        description: "toy payments webhook".to_string(),
    };
    assert_eq!(
        checked.body(),
        &Body::Fact {
            payload: payment(70),
            source: source.clone()
        }
    );
    assert_eq!(
        checked.body().source(),
        Some(&source),
        "a fact's source is never optional"
    );
    assert!(checked.body().is_fact());
    assert_eq!(checked.payload(), &payment(70));
}

#[test]
fn a_checked_command_keeps_what_its_sender_chose() {
    let book = command(cover("shelf", 3), set("a", 1))
        .id(9)
        .correlated(4)
        .expecting(7, ConflictResolution::AggregateHandles);
    let checked = check(book, &registry()).unwrap();

    assert_eq!(checked.command_id(), Some(&crate::toys::command_id(9)));
    assert_eq!(checked.correlation_id(), Some(&correlation(4)));
    assert_eq!(checked.expected_sequence(), Some(7));
    assert_eq!(
        checked.conflict_resolution(),
        Some(ConflictResolution::AggregateHandles)
    );
}

#[test]
fn an_empty_handling_is_unset() {
    let mut book = fact(cover("account", 1), payment(1), "pi_1");
    book.handling = Some(Handling::default());
    let checked = check(book, &registry()).expect("an empty handling on a fact is fine");
    assert_eq!(checked.expected_sequence(), None);

    let mut book = command(cover("account", 1), deposit(1));
    book.handling = Some(Handling {
        expected_sequence: None,
        conflict_resolution: ConflictResolution::Unspecified as i32,
    });
    assert_eq!(
        check(book, &registry()).unwrap().conflict_resolution(),
        None,
        "UNSPECIFIED is the configured default"
    );
}

#[test]
fn a_fact_may_name_its_derived_command_id_and_nothing_else() {
    let mut book = fact(cover("account", 1), payment(1), "pi_1234");
    book.page_mut().command_id = Some(ids::fact_command_id("pi_1234"));
    assert!(check(book, &registry()).is_ok(), "an SDK may compute it");
}

#[test]
fn commutative_is_accepted_where_the_state_declares_conflict_units() {
    let book = command(cover("shelf", 1), set("a", 1)).resolved_by(ConflictResolution::Commutative);
    assert_eq!(
        check(book, &registry()).unwrap().conflict_resolution(),
        Some(ConflictResolution::Commutative)
    );
    let book =
        command(cover("account", 1), deposit(1)).resolved_by(ConflictResolution::AggregateHandles);
    assert!(
        check(book, &registry()).is_ok(),
        "AGGREGATE_HANDLES is in every state's implicit base"
    );
}

/// A command page with this body, for building malformed cases.
fn page(body: Option<command_page::Body>) -> Option<CommandPage> {
    Some(CommandPage {
        body,
        ..CommandPage::default()
    })
}

fn trigger() -> EventRef {
    EventRef {
        stream: Some(cover("shelf", 9)),
        sequence: 4,
    }
}

#[test]
fn malformed_books_are_refused_naming_the_first_malformation() {
    let ok = || command(cover("account", 1), deposit(1));
    let ok_fact = || fact(cover("account", 1), payment(1), "pi_1");
    let short = |n: usize| Uuid {
        value: vec![0xab; n].into(),
    };
    let edit = |mut book: CommandBook, f: &dyn Fn(&mut CommandBook)| {
        f(&mut book);
        book
    };

    let cases: Vec<(&str, CommandBook, Malformed)> = vec![
        (
            "no cover",
            edit(ok(), &|b| b.cover = None),
            Malformed::NoCover,
        ),
        (
            "a domain nobody registered",
            command(cover("nowhere", 1), deposit(1)),
            Malformed::UnknownDomain {
                domain: "nowhere".to_string(),
            },
        ),
        (
            "no root",
            edit(ok(), &|b| b.cover.as_mut().unwrap().root = None),
            Malformed::NoRoot,
        ),
        (
            "a root of 15 bytes",
            edit(ok(), &|b| b.cover.as_mut().unwrap().root = Some(short(15))),
            Malformed::BadRoot { len: 15 },
        ),
        (
            "a set empty edition: main is unset, never \"\"",
            command(
                Cover {
                    edition: Some(String::new()),
                    ..cover("account", 1)
                },
                deposit(1),
            ),
            Malformed::EmptyEdition,
        ),
        (
            "no root and an empty edition: the first is the answer",
            command(
                Cover {
                    root: None,
                    edition: Some(String::new()),
                    ..cover("account", 1)
                },
                deposit(1),
            ),
            Malformed::NoRoot,
        ),
        ("no page", edit(ok(), &|b| b.page = None), Malformed::NoPage),
        (
            "no body",
            edit(ok(), &|b| b.page = page(None)),
            Malformed::NoBody,
        ),
        (
            "no payload",
            edit(ok(), &|b| {
                b.page = page(Some(command_page::Body::Command(Default::default())))
            }),
            Malformed::NoPayload,
        ),
        (
            "a command id of 3 bytes",
            edit(ok(), &|b| b.page_mut().command_id = Some(short(3))),
            Malformed::BadCommandId { len: 3 },
        ),
        (
            "an empty correlation id",
            edit(ok(), &|b| b.page_mut().correlation_id = Some(short(0))),
            Malformed::BadCorrelationId { len: 0 },
        ),
        (
            "a causation from outside the kernel",
            ok().caused_by(trigger()),
            Malformed::CausationFromOutside,
        ),
        (
            "a fact without its origin",
            edit(ok_fact(), &|b| b.page_mut().fact_origin = None),
            Malformed::FactWithoutOrigin,
        ),
        (
            "a fact with an empty external id",
            fact(cover("account", 1), payment(1), ""),
            Malformed::EmptyExternalId,
        ),
        (
            "a command with a fact origin",
            edit(ok(), &|b| {
                b.page_mut().fact_origin = Some(FactOrigin::default())
            }),
            Malformed::CommandWithFactOrigin,
        ),
        (
            "a fact naming a command id other than its derivation",
            ok_fact().id(1),
            Malformed::FactCommandIdNotDerived,
        ),
        (
            "a fact with an expected sequence",
            ok_fact().expecting(3, ConflictResolution::Unspecified),
            Malformed::HandlingOnFact,
        ),
        (
            "a fact with a conflict resolution",
            ok_fact().resolved_by(ConflictResolution::Strict),
            Malformed::HandlingOnFact,
        ),
        (
            "a resolution this build does not know",
            edit(ok(), &|b| {
                b.handling = Some(Handling {
                    expected_sequence: None,
                    conflict_resolution: 9,
                })
            }),
            Malformed::UnknownConflictResolution { value: 9 },
        ),
        (
            "COMMUTATIVE on a state without conflict units",
            ok().expecting(0, ConflictResolution::Commutative),
            Malformed::UnsupportedConflictResolution {
                domain: "account".to_string(),
                requested: ConflictResolution::Commutative,
            },
        ),
    ];

    let registry = registry();
    for (case, book, malformed) in cases {
        assert_eq!(check(book, &registry), Err(malformed), "{case}");
    }
}
