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
    assert_eq!(other.code, "POLICY_COMMAND_OUTSIDE_EDITION");
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
    assert_eq!(refused.code, "POLICY_COMMAND_OUTSIDE_DESTINATION");
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
