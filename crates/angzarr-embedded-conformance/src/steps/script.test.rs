//! Givens scripting the business process and the coordinator's caps: what
//! the business process answers each delivery, what it reports with it,
//! what lands on the stream while it decides, and how often the
//! coordinator tries again.

use cucumber::given;

use angzarr_embedded::OnRetryable;

use crate::tests::business::{rejection, Answer, Race, Report};
use crate::tests::world::Coordinator;

/// The units a report names: `none`, or quoted units separated by commas.
fn units(text: &str) -> Vec<String> {
    match text.trim() {
        "none" => Vec::new(),
        quoted => quoted
            .split(',')
            .map(|unit| unit.trim().trim_matches('"').to_string())
            .collect(),
    }
}

/// What a scenario says a handler reports.
fn report(text: &str) -> Report {
    if let Some(units_text) = text.strip_prefix("window units ") {
        let (window, command) = units_text
            .split_once(" and command units ")
            .expect("window units and command units");
        return Report::Units(units(window), units(command));
    }
    if let Some(nonce) = text.strip_prefix("resolved nonce ") {
        return Report::Nonce(nonce.trim_matches('"').to_string());
    }
    match text {
        "no resolved nonce" => Report::Nothing,
        "the conflict nonce of the first delivery as its resolved nonce" => Report::FirstNonce,
        "the conflict nonce it was delivered as its resolved nonce"
        | "the conflict nonce of the second delivery as its resolved nonce" => Report::Examined,
        other => panic!("no report {other}"),
    }
}

/// The delivery a scenario names, from 1.
fn nth(ordinal: &str) -> usize {
    match ordinal {
        "first" => 1,
        "second" => 2,
        "third" => 3,
        other => panic!("no ordinal {other}"),
    }
}

#[given(regex = r"^business code rejects the command with code (\w+)$")]
fn rejects(w: &mut Coordinator, code: String) {
    w.business("order")
        .every(|a| a.refuse = Some(rejection(&code)));
}

#[given(regex = r"^business code rejects the (first|second) delivery with code (\w+)$")]
fn rejects_nth(w: &mut Coordinator, ordinal: String, code: String) {
    w.business("order")
        .nth(nth(&ordinal), |a| a.refuse = Some(rejection(&code)));
}

#[given("business code accepts every window it examines")]
fn accepts_every_window(w: &mut Coordinator) {
    w.business("order").every(|a| a.report = Report::Examined);
}

#[given(regex = r#"^the coordinator delivers it with conflict nonce "([^"]+)"$"#)]
fn names_the_nonce(w: &mut Coordinator, name: String) {
    w.business("order").name_nonce(&name);
}

#[given(regex = r"^the business process answers an ItemAdded event, reporting (.+)$")]
fn answers_reporting(w: &mut Coordinator, reported: String) {
    let report = report(&reported);
    w.business("order").every(|a| a.report = report);
}

#[given(
    regex = r"^the business process answers the (first|second) delivery with an ItemAdded event, reporting (.+)$"
)]
fn answers_nth_reporting(w: &mut Coordinator, ordinal: String, reported: String) {
    let report = report(&reported);
    w.business("order")
        .nth(nth(&ordinal), |a| a.report = report);
}

#[given(
    regex = r"^the business process answers every (?:delivery|run) with an ItemAdded event(?:, reporting (.+))?$"
)]
fn answers_every(w: &mut Coordinator, reported: String) {
    let report = match reported.is_empty() {
        true => Report::Examined,
        false => report(&reported),
    };
    w.business("order").every(|a| a.report = report);
}

#[given(regex = r"^the ingest handler answers (?:every run )?with a ShipmentDispatched event$")]
fn ingests(w: &mut Coordinator) {
    w.business("order").every(|a: &mut Answer| a.refuse = None);
}

#[given(
    regex = r#"^another write to the "order" stream lands (?:at sequence \d+ )?after (the command is loaded|the window is delivered|the fact is loaded|each delivery's window is delivered|each run's load) and before (?:the command|the fact|it) is written$"#
)]
fn another_write(w: &mut Coordinator, when: String) {
    let business = w.business("order");
    match when.starts_with("each") {
        true => business.every(|a| a.race = Some(Race::SameStream)),
        false => business.nth(1, |a| a.race = Some(Race::SameStream)),
    }
}

#[given(r#"another write lands on a different "order" root's stream after the command is loaded and before it is written"#)]
fn another_root_write(w: &mut Coordinator) {
    w.business("order")
        .nth(1, |a| a.race = Some(Race::OtherRoot));
}

#[given(regex = r"^the retry cap is reached after (\d+) (?:deliveries|attempts)$")]
fn retry_cap(w: &mut Coordinator, deliveries: u32) {
    w.retry = OnRetryable {
        max_retries: deliveries - 1,
        ..w.retry
    };
}

#[given(regex = r"^the race re-run cap is (?:reached after )?(\d+)(?: runs)?$")]
fn race_cap(w: &mut Coordinator, runs: u32) {
    w.retry = OnRetryable {
        max_race_runs: runs,
        ..w.retry
    };
}

#[given(regex = r"^the (\w+) aggregate declares facts? (\w+)(?: and (\w+))?$")]
fn declares_facts(w: &mut Coordinator, domain: String, first: String, second: String) {
    let business = w.business(&domain);
    business.declare_fact(&first);
    if !second.is_empty() {
        business.declare_fact(&second);
    }
}
