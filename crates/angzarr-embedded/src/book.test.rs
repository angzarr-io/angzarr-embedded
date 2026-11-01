//! A stream addressed by its natural key, and a command wrapped for it.

use crate::toys::{command_id, registry, Account, AccountCmd};
use crate::v1::{command_page, CommandBook, ConflictResolution, Cover, Handling};
use crate::{check, ids, CommandBookExt, CoverExt, Payload};

#[test]
fn a_natural_cover_is_the_main_stream_rooted_at_its_keys_uuidv5() {
    assert_eq!(
        Cover::natural("tasks", "swift-amber-falcon"),
        Cover {
            domain: "tasks".to_string(),
            root: Some(ids::compute_root("tasks", "swift-amber-falcon").unwrap()),
            edition: None,
        }
    );
    assert_ne!(
        Cover::natural("tasks", "swift-amber-falcon").root,
        Cover::natural("plans", "swift-amber-falcon").root,
        "the domain is part of the key"
    );
}

#[test]
fn a_natural_cover_of_a_domain_that_derives_no_root_has_none() {
    let cover = Cover::natural("ta:sks", "swift-amber-falcon");
    assert_eq!(cover.domain, "ta:sks");
    assert_eq!(
        cover.root, None,
        "the kernel's check refuses a cover with no root"
    );
    assert_eq!(cover.edition, None);
}

#[test]
fn a_command_book_carries_the_payload_to_the_cover_with_no_ids_and_no_handling() {
    let payload = AccountCmd::Deposit(5).to_any();
    let book = CommandBook::command(crate::toys::cover("account", 1), payload.clone());
    assert_eq!(book.cover, Some(crate::toys::cover("account", 1)));
    assert_eq!(book.handling, None);
    let page = book.page.expect("a page");
    assert_eq!(page.command_id, None);
    assert_eq!(page.correlation_id, None);
    assert_eq!(
        page.body,
        Some(command_page::Body::Command(crate::v1::Command {
            payload: Some(payload)
        }))
    );
    // The book the kernel checks, as a hand-built one would be.
    let checked = check(
        CommandBook::command(crate::toys::cover("account", 1), AccountCmd::Open.to_any()),
        &registry(),
    )
    .expect("well formed");
    assert_eq!(checked.domain(), <Account as crate::Domain>::NAME);
}

#[test]
fn at_sets_the_expected_sequence_and_keeps_the_resolution() {
    let book = CommandBook::command(crate::toys::cover("account", 1), AccountCmd::Open.to_any());
    assert_eq!(
        book.clone().at(7).handling,
        Some(Handling {
            expected_sequence: Some(7),
            conflict_resolution: ConflictResolution::Unspecified as i32,
        })
    );
    let strict = CommandBook {
        handling: Some(Handling {
            expected_sequence: None,
            conflict_resolution: ConflictResolution::Strict as i32,
        }),
        ..book
    };
    assert_eq!(
        strict.at(3).handling,
        Some(Handling {
            expected_sequence: Some(3),
            conflict_resolution: ConflictResolution::Strict as i32,
        })
    );
}

#[test]
fn with_id_sets_the_clients_command_id_and_nothing_else() {
    let book = CommandBook::command(crate::toys::cover("account", 1), AccountCmd::Open.to_any());
    let identified = book.clone().with_id(command_id(4));
    let page = identified.page.clone().unwrap();
    assert_eq!(page.command_id, Some(command_id(4)));
    assert_eq!(
        CommandBook {
            page: Some(crate::v1::CommandPage {
                command_id: None,
                ..page
            }),
            ..identified
        },
        book
    );
    let bare = CommandBook { page: None, ..book }.with_id(command_id(5));
    assert_eq!(bare.page.unwrap().command_id, Some(command_id(5)));
}
