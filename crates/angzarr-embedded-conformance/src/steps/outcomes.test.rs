//! Thens about what came of the inputs: each caller's answer (its status,
//! reason key and args, or its outcome), the events persisted, and what was
//! dead-lettered and alerted.

use std::collections::BTreeMap;

use cucumber::then;

use angzarr_embedded::v1::{AngzarrDeadLetter, DeliveryOutcome};
use angzarr_embedded::{type_name, Handled};

use crate::tests::answer::{status, Answered};
use crate::tests::world::Coordinator;

/// The `name "value"` args a step lists.
pub(crate) fn named(text: &str) -> BTreeMap<String, String> {
    let mut args = BTreeMap::new();
    let mut rest = text;
    while let Some(open) = rest.find(" \"") {
        let name = rest[..open].rsplit([' ', ',']).next().unwrap_or_default();
        let after = &rest[open + 2..];
        let close = after.find('"').expect("a closing quote");
        args.insert(name.to_string(), after[..close].to_string());
        rest = &after[close + 1..];
    }
    args
}

/// Whether a step says the input persisted nothing.
fn says_nothing_persisted(text: &str) -> bool {
    text.contains("nothing is persisted") || text.contains("persists nothing")
}

/// The last answer is a refusal under `status_name` with `reason` and at
/// least `args`.
fn assert_refused(
    w: &Coordinator,
    status_name: &str,
    reason: &str,
    args: &BTreeMap<String, String>,
) {
    let answer = w.answer();
    assert_eq!(answer.status(), Some(status(status_name)), "{answer:?}");
    assert_eq!(answer.reason(), Some(reason), "{answer:?}");
    for (name, value) in args {
        assert_eq!(
            answer.arg(name),
            Some(value.as_str()),
            "{name} of {answer:?}"
        );
    }
}

fn assert_nothing_persisted(w: &Coordinator) {
    let cover = w.order();
    assert!(w.persisted(&cover).is_empty(), "{:?}", w.persisted(&cover));
}

/// The types of the order stream's events persisted since the first when.
fn persisted_types(w: &Coordinator) -> Vec<(u64, String)> {
    w.persisted(&w.order())
        .into_iter()
        .map(|page| {
            let url = page.payload.unwrap_or_default().type_url;
            let name = type_name(&url)
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .to_string();
            (page.sequence, name)
        })
        .collect()
}

#[then(regex = r"^the command succeeds$")]
fn succeeds(w: &mut Coordinator) {
    assert_eq!(w.answer(), Answered::Ok(DeliveryOutcome::Applied));
}

#[then("both commands succeed")]
fn both_succeed(w: &mut Coordinator) {
    let answers = w.answers();
    assert_eq!(answers.len(), 2);
    assert!(
        answers
            .iter()
            .all(|a| *a == Answered::Ok(DeliveryOutcome::Applied)),
        "{answers:?}"
    );
}

#[then(regex = r"^(?:no events are persisted|the (?:command|fact|step) persists nothing)$")]
fn nothing_persisted(w: &mut Coordinator) {
    assert_nothing_persisted(w);
}

#[then(regex = r"^(?:the command succeeds and )?an? (\w+) event is persisted at sequence (\d+)$")]
fn persisted_at(w: &mut Coordinator, name: String, sequence: u64) {
    assert!(
        persisted_types(w).contains(&(sequence, name)),
        "{:?}",
        persisted_types(w)
    );
}

#[then(regex = r"^(\w+) events are persisted at sequences (\d+) and (\d+)$")]
fn persisted_at_both(w: &mut Coordinator, name: String, first: u64, second: u64) {
    assert_eq!(
        persisted_types(w),
        vec![(first, name.clone()), (second, name)]
    );
}

#[then(regex = r"^exactly one (\w+) event is persisted$")]
fn exactly_one(w: &mut Coordinator, name: String) {
    let types = persisted_types(w);
    assert_eq!(
        types.iter().filter(|(_, n)| *n == name).count(),
        1,
        "{types:?}"
    );
}

#[then(regex = r"^the command fails with (\w+) status$")]
fn fails_with_status(w: &mut Coordinator, status_name: String) {
    assert_eq!(
        w.answer().status(),
        Some(status(&status_name)),
        "{:?}",
        w.answer()
    );
}

#[then(regex = r"^the command fails with the business rejection reason (\w+)$")]
fn fails_with_rejection(w: &mut Coordinator, reason: String) {
    assert_refused(w, "FAILED_PRECONDITION", &reason, &BTreeMap::new());
}

#[then(regex = r"^the command fails (\w+) and nothing is persisted$")]
fn fails_persisting_nothing(w: &mut Coordinator, status_name: String) {
    fails_with_status(w, status_name);
    assert_nothing_persisted(w);
}

#[then(regex = r"^the command fails (\w+) with reason (\w+)(.*)$")]
fn fails_with_reason(w: &mut Coordinator, status_name: String, reason: String, rest: String) {
    assert_refused(w, &status_name, &reason, &named(&rest));
    if says_nothing_persisted(&rest) {
        assert_nothing_persisted(w);
    }
}

#[then(regex = r"^the (?:caller|client) is answered (\w+) with reason (\w+)(.*)$")]
fn answered_with_reason(w: &mut Coordinator, status_name: String, reason: String, rest: String) {
    assert_refused(w, &status_name, &reason, &named(&rest));
    if rest.contains("with no wait before it") {
        assert!(w.waits.waited().is_empty(), "{:?}", w.waits.waited());
    }
}

#[then(regex = r"^the (?:command|caller|client) is never answered (\w+)$")]
fn never_answered(w: &mut Coordinator, reason: String) {
    let answers = w.answers();
    assert!(
        answers.iter().all(|a| a.reason() != Some(reason.as_str())),
        "{answers:?}"
    );
}

#[then("the client is never answered OK with delivery PARKED")]
fn never_parked(w: &mut Coordinator) {
    let answers = w.answers();
    assert!(
        !answers.contains(&Answered::Ok(DeliveryOutcome::Parked)),
        "{answers:?}"
    );
}

#[then(regex = r"^the (?:client|sender) is answered OK with delivery PARKED and no events$")]
fn parked(w: &mut Coordinator) {
    let (_, result) = w.handled.last().expect("an input was handled");
    match result {
        Ok(handled @ Handled::Parked(_)) => assert!(handled.books().is_empty()),
        other => panic!("parked, not {:?}", other.as_ref().map(Handled::outcome)),
    }
}

#[then(regex = r"^the caller is answered DUPLICATE with the (\w+) event at sequence (\d+)$")]
fn duplicate(w: &mut Coordinator, name: String, sequence: u64) {
    let (_, result) = w.handled.last().expect("an input was handled");
    let Ok(Handled::Duplicate(book, _)) = result else {
        panic!(
            "a duplicate, not {:?}",
            result.as_ref().map(Handled::outcome)
        );
    };
    let pages: Vec<_> = book
        .pages
        .iter()
        .map(|p| {
            (
                p.sequence,
                p.payload
                    .as_ref()
                    .map(|a| a.type_url.clone())
                    .unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(pages.len(), 1, "{pages:?}");
    assert_eq!(pages[0].0, sequence);
    assert!(pages[0].1.ends_with(&format!(".{name}")), "{pages:?}");
}

#[then("the second delivery returns the events of the first")]
fn second_returns_first(w: &mut Coordinator) {
    let [(_, first), (_, second)] = &w.handled[..] else {
        panic!("two deliveries, not {}", w.handled.len());
    };
    let first = first.as_ref().expect("the first applied").books();
    let Ok(Handled::Duplicate(book, _)) = second else {
        panic!("the second a duplicate");
    };
    let first_pages: Vec<_> = first.iter().flat_map(|b| b.pages.clone()).collect();
    assert_eq!(first_pages, book.pages);
}

/// The dead letters the log was given.
fn letters(w: &Coordinator) -> Vec<AngzarrDeadLetter> {
    w.log.letters()
}

#[then("nothing is dead-lettered and no alert is raised")]
fn nothing_dead_lettered(w: &mut Coordinator) {
    assert!(letters(w).is_empty(), "{:?}", letters(w));
    assert!(w.alerts().is_empty());
}

#[then(
    regex = r"^the (?:command|fact) is dead-lettered with reason (\w+)(.*), and an alert is raised$"
)]
fn dead_lettered(w: &mut Coordinator, reason: String, rest: String) {
    let letters = letters(w);
    let letter = letters
        .iter()
        .find(|l| l.reason.as_ref().is_some_and(|r| r.code == reason))
        .unwrap_or_else(|| panic!("no dead letter of {reason}: {letters:?}"));
    let given = &letter.reason.as_ref().expect("a reason").args;
    for (name, value) in named(&rest) {
        let arg = given
            .get(&name)
            .unwrap_or_else(|| panic!("no {name} in {letter:?}"));
        // A type is named by its bare name: `AddItem` for orders.v1.AddItem.
        assert!(
            arg == &value || arg.ends_with(&format!(".{value}")),
            "{name} of {letter:?}"
        );
    }
    assert!(!w.alerts().is_empty(), "an alert");
}
