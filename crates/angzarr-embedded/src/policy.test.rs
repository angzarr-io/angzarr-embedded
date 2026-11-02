//! The kernel's checks of a policy's answer (decision 14.42).

use crate::toys::{command, cover_in, ledger, ledger_root};
use crate::v1::{CommandBook, Cover};

use super::check_reaction;

fn record(edition: Option<&str>) -> CommandBook {
    command(
        Cover {
            domain: "ledger".to_string(),
            root: Some(ledger_root()),
            edition: edition.map(str::to_string),
        },
        crate::pack(&ledger::Record {
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
    let correlation = crate::toys::correlation(4);
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
    use crate::v1::{command_page::Body, CommandPage, Fact, FactOrigin, Handling};
    let payload = crate::pack(&ledger::Record {
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
        assert_eq!(error.grpc, crate::angzarr_router::error::GrpcCode::Internal);
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
        page: Some(page(Body::Command(crate::v1::Command {
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
        identified(|p| p.command_id = Some(crate::toys::command_id(1))),
        code("COORDINATOR_REASON_REFUSAL_FACT_IDENTIFIED")
    );
    assert_eq!(
        identified(|p| p.correlation_id = Some(crate::toys::command_id(1))),
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
    use crate::toys::correlation;
    use crate::v1::{EventBook, EventMeta, EventPage};
    use angzarr_proto::v1::PolicyReaction;
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
        book.page.as_mut().unwrap().body =
            Some(crate::v1::command_page::Body::Command(crate::v1::Command {
                payload: Some(crate::pack(&ledger::Record {
                    source_sequence: 0,
                    cents,
                })),
            }));
        book
    };
    let reaction = |trigger_sequence, commands| PolicyReaction {
        trigger_sequence,
        commands,
    };
    let trigger = crate::v1::EventRef {
        stream: source.cover.clone(),
        sequence: 5,
    };
    let checked = check_reactions(
        "ledger",
        &source,
        vec![
            reaction(5, vec![carrying(2, 1)]),
            reaction(4, vec![carrying(1, 2), carrying(1, 3)]),
            reaction(9, vec![carrying(7, 4)]),
        ],
        "bookkeeping",
        &trigger,
    )
    .unwrap();
    let cents: Vec<i64> = checked
        .iter()
        .map(|book| {
            let Some(crate::v1::command_page::Body::Command(c)) = &book.page.as_ref().unwrap().body
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

    let crossed = check_reactions(
        "ledger",
        &source,
        vec![reaction(4, vec![carrying(2, 1)])],
        "bookkeeping",
        &trigger,
    )
    .err()
    .unwrap();
    assert_eq!(
        crossed.code,
        "COORDINATOR_REASON_POLICY_COMMAND_OTHER_CORRELATION"
    );
}

/// A reaction may be a Fact, a type its destination declares one
/// (decision 14.69), and passes the checks a command does; one carrying
/// a `fact_origin` of its own, on either arm, is the policy's bug, refused
/// INTERNAL naming its domain and type,
/// `COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN`: the kernel stamps a
/// fact's.
#[test]
fn a_reaction_is_either_arm_and_carries_no_fact_origin() {
    use crate::v1::FactOrigin;
    let trigger = cover_in("account", 1, "what-if");
    let check =
        |book: CommandBook| check_reaction("ledger", &trigger, None, vec![record(None), book]);
    let refused = |book: CommandBook| {
        let error = check(book).err().unwrap();
        assert_eq!(error.grpc, crate::angzarr_router::error::GrpcCode::Internal);
        let extra = |key: &str| error.extras.get(key).cloned();
        assert_eq!(extra("domain").as_deref(), Some("ledger"));
        assert_eq!(
            extra("command_type").as_deref(),
            Some("toy.ledger.cmd.v1.Record")
        );
        error.code
    };
    let checked = check(as_fact(record(None))).unwrap();
    assert!(
        matches!(
            checked[1].page.as_ref().and_then(|p| p.body.as_ref()),
            Some(crate::v1::command_page::Body::Fact(_))
        ),
        "the fact kept its arm: {checked:?}"
    );
    assert_eq!(
        checked[1].cover.as_ref().and_then(|c| c.edition.as_deref()),
        Some("what-if"),
        "and was stamped with its trigger's edition"
    );
    for arm in [record(None), as_fact(record(None))] {
        let mut origin = arm;
        origin.page.as_mut().unwrap().fact_origin = Some(FactOrigin::default());
        assert_eq!(
            refused(origin),
            "COORDINATOR_REASON_POLICY_COMMAND_FACT_ORIGIN"
        );
    }
}

/// `book` with its command's payload as a Fact.
fn as_fact(mut book: CommandBook) -> CommandBook {
    use crate::v1::{command_page::Body, Fact};
    let page = book.page.as_mut().unwrap();
    let Some(Body::Command(command)) = page.body.take() else {
        unreachable!()
    };
    page.body = Some(Body::Fact(Fact {
        payload: command.payload,
    }));
    book
}

/// A policy's answer, ordered (decision 14.69): each output keeps its arm,
/// commands and facts in one index space, and a fact's book carries the
/// origin the kernel stamps, its external id the reaction id's canonical
/// text and its description naming its type and the policy.
#[test]
fn a_policys_facts_are_ordered_with_its_commands_and_stamped_their_origin() {
    use crate::v1::{command_page::Body, EventRef, Uuid};
    let trigger = EventRef {
        stream: Some(cover_in("account", 1, "what-if")),
        sequence: 4,
    };
    let correlation = Uuid::from_bytes([7; 16]);
    let orders = super::answered(
        "bookkeeping",
        &trigger,
        &correlation,
        vec![record(None), as_fact(record(None))],
    );
    assert_eq!(orders.len(), 2);
    assert!(!orders[0].is_fact());
    assert!(orders[1].is_fact());
    assert_eq!(orders[1].index(), 1, "one index space");
    let command = orders[0].book();
    let page = command.page.unwrap();
    assert!(matches!(page.body, Some(Body::Command(_))));
    assert_eq!(page.fact_origin, None, "a command carries none");
    let fact = orders[1].book();
    let page = fact.page.unwrap();
    assert!(matches!(page.body, Some(Body::Fact(_))));
    let derived = crate::ids::policy_command_id(&trigger, "bookkeeping", 1).unwrap();
    let origin = page.fact_origin.expect("the kernel's origin");
    assert_eq!(origin.external_id, derived.to_string());
    assert_eq!(
        origin.description,
        "toy.ledger.cmd.v1.Record from bookkeeping"
    );
    assert_eq!(page.command_id, None, "the check derives its id");
}

/// A reaction's own command id and causation, where set, are the ones its
/// coordinator stamps: the id derived from the trigger, the policy and
/// the command's index in the whole answer, and the trigger. Any other is
/// `COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY` naming its domain,
/// type and the `field` (decision 14.48, amended).
#[test]
fn a_reactions_identity_is_unset_or_the_stamp() {
    use super::check_reactions;
    use crate::v1::{EventBook, EventPage, EventRef};
    use angzarr_proto::v1::PolicyReaction;
    let source = EventBook {
        cover: Some(cover_in("account", 1, "what-if")),
        pages: vec![EventPage {
            sequence: 4,
            ..EventPage::default()
        }],
        ..EventBook::default()
    };
    let trigger = EventRef {
        stream: source.cover.clone(),
        sequence: 4,
    };
    let stamp = |index| crate::ids::policy_command_id(&trigger, "bookkeeping", index).unwrap();
    let identified = |id: Option<u32>, cause: Option<u64>| {
        let mut book = record(None);
        let page = book.page.as_mut().unwrap();
        page.command_id = id.map(stamp);
        page.causation_id = cause.map(|sequence| EventRef {
            stream: source.cover.clone(),
            sequence,
        });
        book
    };
    let checked = |reactions: Vec<Vec<CommandBook>>| {
        check_reactions(
            "ledger",
            &source,
            reactions
                .into_iter()
                .map(|commands| PolicyReaction {
                    trigger_sequence: 4,
                    commands,
                })
                .collect(),
            "bookkeeping",
            &trigger,
        )
    };
    // Each its own stamp, by its index across reactions.
    assert!(checked(vec![
        vec![identified(Some(0), Some(4)), identified(None, None)],
        vec![identified(Some(2), None), identified(None, Some(4))],
    ])
    .is_ok());

    let field = |reactions| {
        let error = checked(reactions).err().unwrap();
        assert_eq!(
            error.code,
            "COORDINATOR_REASON_POLICY_COMMAND_OTHER_IDENTITY"
        );
        assert_eq!(error.grpc, crate::angzarr_router::error::GrpcCode::Internal);
        assert_eq!(
            error.extras.get("domain").map(String::as_str),
            Some("ledger")
        );
        assert_eq!(
            error.extras.get("command_type").map(String::as_str),
            Some("toy.ledger.cmd.v1.Record")
        );
        error.extras.get("field").cloned().unwrap()
    };
    // Another index's id: the first command's at the second, in the
    // second reaction.
    assert_eq!(
        field(vec![
            vec![identified(None, None)],
            vec![identified(Some(0), None)]
        ]),
        "command_id"
    );
    assert_eq!(field(vec![vec![identified(Some(1), None)]]), "command_id");
    assert_eq!(field(vec![vec![identified(None, Some(3))]]), "causation_id");
    // Both other: the command id is named first.
    assert_eq!(
        field(vec![vec![identified(Some(1), Some(3))]]),
        "command_id"
    );
}

/// What [`Witness`] was handed: each trigger's whole metadata.
static WITNESSED: std::sync::Mutex<Vec<crate::v1::EventMeta>> = std::sync::Mutex::new(Vec::new());

/// A policy that sends nothing and keeps the metadata of each event it is
/// handed.
struct Witness;

impl crate::Policy for Witness {
    const NAME: &'static str = "witness";
    type Source = crate::toys::Account;
    type Target = crate::toys::Ledger;

    fn on_event(
        &self,
        _event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<crate::toys::Ledger>>, crate::markup::Report> {
        WITNESSED.lock().unwrap().push(ctx.meta().clone());
        Ok(None)
    }
}

#[test]
fn a_policy_is_handed_its_triggers_whole_metadata() {
    use crate::testing::Harness;
    use crate::toys::{cover, Account, AccountCmd, AccountEvent};
    let mut registry = crate::toys::registry();
    registry.policy(Witness).unwrap();
    let mut account = Harness::<Account>::new(registry, cover("account", 9));
    account.given([AccountEvent::Opened]);
    account.when(AccountCmd::Deposit(5)).events();
    let stored = account.pages()[1].meta.clone().unwrap();
    assert!(stored.causation.is_some() && !stored.rules_version.is_empty());
    assert!(WITNESSED.lock().unwrap().contains(&stored), "{stored:?}");
}
