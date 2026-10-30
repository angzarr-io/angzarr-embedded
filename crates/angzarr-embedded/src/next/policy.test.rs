//! The kernel's checks of a policy's answer (decision 14.42).

use crate::next::toys::{command, cover_in, ledger, ledger_root};
use crate::next::v1::{CommandBook, Cover};

use super::check_reaction;

fn record(edition: Option<&str>) -> CommandBook {
    command(
        Cover {
            domain: "ledger".to_string(),
            root: Some(ledger_root()),
            edition: edition.map(str::to_string),
        },
        crate::next::pack(&ledger::Record {
            source_sequence: 0,
            cents: 1,
        }),
    )
}

#[test]
fn a_command_is_stamped_with_its_triggers_edition() {
    let trigger = cover_in("account", 1, "what-if");
    let checked = check_reaction(
        "ledger",
        &trigger,
        None,
        vec![record(None), record(Some("what-if"))],
    )
    .unwrap();
    let editions: Vec<_> = checked
        .iter()
        .map(|c| c.cover.as_ref().unwrap().edition.as_deref())
        .collect();
    assert_eq!(editions, [Some("what-if"), Some("what-if")]);

    let other = check_reaction("ledger", &trigger, None, vec![record(Some("other"))])
        .err()
        .unwrap();
    assert_eq!(
        other.code,
        "COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_EDITION"
    );
    let extra = |key: &str| other.extras.get(key).map(String::as_str);
    assert_eq!(extra("expected_edition"), Some("what-if"));
    assert_eq!(extra("actual_edition"), Some("other"));
}

#[test]
fn every_command_is_held_to_the_destination_before_any_other_rule() {
    let trigger = cover_in("account", 1, "what-if");
    let mut stray = record(None);
    stray.cover.as_mut().unwrap().domain = "shelf".to_string();
    let refused = check_reaction("ledger", &trigger, None, vec![record(Some("other")), stray])
        .err()
        .unwrap();
    assert_eq!(
        refused.code,
        "COORDINATOR_REASON_POLICY_COMMAND_OUTSIDE_DESTINATION"
    );
}

#[test]
fn a_command_carrying_its_triggers_correlation_passes() {
    let trigger = cover_in("account", 1, "what-if");
    let correlation = crate::next::toys::correlation(4);
    let mut carried = record(None);
    carried.page.as_mut().unwrap().correlation_id = Some(correlation.clone());
    assert!(check_reaction(
        "ledger",
        &trigger,
        Some(&correlation),
        vec![carried.clone()]
    )
    .is_ok());
    // No trigger correlation to compare with: any is kept.
    assert!(check_reaction("ledger", &trigger, None, vec![carried]).is_ok());
}

#[test]
fn a_translated_fact_that_is_more_than_a_payload_is_the_policys_bug() {
    use super::translated;
    use crate::next::v1::{command_page::Body, CommandPage, Fact, FactOrigin, Handling};
    let payload = crate::next::pack(&ledger::Record {
        source_sequence: 0,
        cents: 1,
    });
    let page = |body| CommandPage {
        body: Some(body),
        ..CommandPage::default()
    };
    let fact = || {
        Body::Fact(Fact {
            payload: Some(payload.clone()),
        })
    };
    let bare = || CommandBook {
        page: Some(page(fact())),
        ..CommandBook::default()
    };

    assert_eq!(translated(bare()), Ok(payload.clone()));

    let refused = |book: CommandBook| {
        let error = translated(book).err().unwrap();
        assert_eq!(
            error.grpc,
            crate::next::angzarr_router::error::GrpcCode::Internal
        );
        Some(error.code)
    };
    let code = |c: &str| Some(c.to_string());
    let addressed = CommandBook {
        cover: Some(cover_in("account", 1, "what-if")),
        ..bare()
    };
    assert_eq!(
        refused(addressed),
        code("COORDINATOR_REASON_REFUSAL_FACT_ADDRESSED")
    );
    let handled = CommandBook {
        handling: Some(Handling::default()),
        ..bare()
    };
    assert_eq!(
        refused(handled),
        code("COORDINATOR_REASON_REFUSAL_FACT_ADDRESSED")
    );
    assert_eq!(
        refused(CommandBook::default()),
        code("COORDINATOR_REASON_REFUSAL_FACT_NO_PAGE")
    );
    let command = CommandBook {
        page: Some(page(Body::Command(crate::next::v1::Command {
            payload: Some(payload.clone()),
        }))),
        ..CommandBook::default()
    };
    assert_eq!(
        refused(command),
        code("COORDINATOR_REASON_REFUSAL_FACT_NOT_A_FACT")
    );
    let empty = CommandBook {
        page: Some(page(Body::Fact(Fact { payload: None }))),
        ..CommandBook::default()
    };
    assert_eq!(
        refused(empty),
        code("COORDINATOR_REASON_REFUSAL_FACT_NOT_A_FACT")
    );
    let identified = |edit: fn(&mut CommandPage)| {
        let mut book = bare();
        edit(book.page.as_mut().unwrap());
        refused(book)
    };
    assert_eq!(
        identified(|p| p.command_id = Some(crate::next::toys::command_id(1))),
        code("COORDINATOR_REASON_REFUSAL_FACT_IDENTIFIED")
    );
    assert_eq!(
        identified(|p| p.correlation_id = Some(crate::next::toys::command_id(1))),
        code("COORDINATOR_REASON_REFUSAL_FACT_IDENTIFIED")
    );
    assert_eq!(
        identified(|p| p.causation_id = Some(Default::default())),
        code("COORDINATOR_REASON_REFUSAL_FACT_IDENTIFIED")
    );
    assert_eq!(
        identified(|p| p.fact_origin = Some(FactOrigin::default())),
        code("COORDINATOR_REASON_REFUSAL_FACT_IDENTIFIED")
    );
}

#[test]
fn each_reaction_is_held_to_the_page_it_names() {
    use super::check_reactions;
    use crate::next::angzarr_router::pb::PolicyReaction;
    use crate::next::toys::correlation;
    use crate::next::v1::{EventBook, EventMeta, EventPage};
    let page = |sequence, n| EventPage {
        sequence,
        meta: Some(EventMeta {
            correlation_id: Some(correlation(n)),
            ..EventMeta::default()
        }),
        ..EventPage::default()
    };
    let source = EventBook {
        cover: Some(cover_in("account", 1, "what-if")),
        pages: vec![page(4, 1), page(5, 2)],
        ..EventBook::default()
    };
    let carrying = |n, cents| {
        let mut book = record(None);
        book.page.as_mut().unwrap().correlation_id = Some(correlation(n));
        book.page.as_mut().unwrap().body = Some(crate::next::v1::command_page::Body::Command(
            crate::next::v1::Command {
                payload: Some(crate::next::pack(&ledger::Record {
                    source_sequence: 0,
                    cents,
                })),
            },
        ));
        book
    };
    let reaction = |trigger_sequence, commands| PolicyReaction {
        trigger_sequence,
        commands,
    };
    let checked = check_reactions(
        "ledger",
        &source,
        vec![
            reaction(5, vec![carrying(2, 1)]),
            reaction(4, vec![carrying(1, 2), carrying(1, 3)]),
            reaction(9, vec![carrying(7, 4)]),
        ],
    )
    .unwrap();
    let cents: Vec<i64> = checked
        .iter()
        .map(|book| {
            let Some(crate::next::v1::command_page::Body::Command(c)) =
                &book.page.as_ref().unwrap().body
            else {
                unreachable!()
            };
            <ledger::Record as prost::Message>::decode(&c.payload.as_ref().unwrap().value[..])
                .unwrap()
                .cents
        })
        .collect();
    assert_eq!(cents, [1, 2, 3, 4], "reaction order, then command order");
    assert!(checked
        .iter()
        .all(|b| b.cover.as_ref().unwrap().edition.as_deref() == Some("what-if")));

    let crossed = check_reactions("ledger", &source, vec![reaction(4, vec![carrying(2, 1)])])
        .err()
        .unwrap();
    assert_eq!(
        crossed.code,
        "COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION"
    );
}
