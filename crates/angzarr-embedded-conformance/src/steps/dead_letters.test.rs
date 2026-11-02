//! A policy's failure on an event it reacts to, dead-lettered: what the
//! dead letter carries (the trigger, the reason and its details, the
//! component), never rendered text. A dead letter is kept by the store and
//! told to the dead-letter log; "published to angzarr.dlq.{domain}" is its
//! stream's domain.

use std::collections::BTreeMap;

use cucumber::{given, then, when};

use angzarr_embedded::v1::angzarr_dead_letter::{Payload, RejectionDetails};
use angzarr_embedded::v1::{AngzarrDeadLetter, ComponentKind};
use angzarr_embedded::OnRetryable;

use crate::tests::answer::status;
use crate::tests::outcomes::named;
use crate::tests::policy::{bare, internal, locked};
use crate::tests::world::Coordinator;

/// The one dead letter the log was told of.
fn the_letter(w: &Coordinator) -> AngzarrDeadLetter {
    let letters = w.log.letters();
    assert_eq!(letters.len(), 1, "{letters:?}; answered {:?}", w.answers());
    letters[0].clone()
}

/// The dead letter's processing failed details.
fn processing_failed(w: &Coordinator) -> angzarr_embedded::v1::ProcessingFailedDetails {
    match the_letter(w).rejection_details {
        Some(RejectionDetails::ProcessingFailed(details)) => details,
        other => panic!("processing failed details, not {other:?}"),
    }
}

#[given(
    regex = r#"^a policy "(\w+)" subscribed to "([\w-]+)" whose handler (succeeds|fails(?: permanently)? with code (\w+)(.*))$"#
)]
fn subscribed(
    w: &mut Coordinator,
    name: String,
    input: String,
    how: String,
    code: String,
    rest: String,
) {
    let script = w.policy(&name, &input, "inventory");
    if code.is_empty() {
        return;
    }
    let mut error = internal(&code);
    error.grpc = match how.contains("permanently") {
        true => status("INTERNAL"),
        false => status("UNAVAILABLE"),
    };
    error.extras = named(rest.split(", and logs").next().unwrap_or_default());
    locked(&script).fails = Some(error);
}

#[given(regex = r"^the policy's retry budget is (\d+) attempts?$")]
fn retry_budget(w: &mut Coordinator, attempts: u32) {
    w.retry = OnRetryable {
        max_retries: attempts - 1,
        ..w.retry
    };
}

#[when(regex = r#"^an OrderCreated event for "([\w-]+)" is delivered to the policy$"#)]
async fn delivered_to_policy(w: &mut Coordinator, order: String) {
    w.begin();
    let cover = w.cover("order", &order);
    w.record(&cover, "OrderCreated", None).await;
}

#[then(regex = r"^the policy handler is attempted (once|\d+ times)$")]
fn attempted(w: &mut Coordinator, times: String) {
    let expected: usize = match times.as_str() {
        "once" => 1,
        n => n.trim_end_matches(" times").parse().expect("a count"),
    };
    let sources = locked(&w.policies.last().expect("a policy").script)
        .sources
        .len();
    assert_eq!(sources, expected);
}

#[then(regex = r#"^a dead letter is published to "angzarr\.dlq\.([\w-]+)"$"#)]
fn published(w: &mut Coordinator, domain: String) {
    let letter = the_letter(w);
    let cover = match &letter.payload {
        Some(Payload::RejectedEvents(book)) => book.cover.clone(),
        Some(Payload::RejectedCommand(book)) => book.cover.clone(),
        _ => None,
    };
    assert_eq!(cover.map(|c| c.domain), Some(domain));
    assert!(!w.alerts().is_empty(), "alerted");
}

#[then("no dead letter is published")]
fn none_published(w: &mut Coordinator) {
    assert!(w.log.letters().is_empty(), "{:?}", w.log.letters());
}

#[then(regex = r"^the dead letter carries the (\w+) event as rejected_events$")]
fn carries_event(w: &mut Coordinator, event: String) {
    let Some(Payload::RejectedEvents(book)) = the_letter(w).payload else {
        panic!("rejected events");
    };
    let names: Vec<String> = book
        .pages
        .iter()
        .filter_map(|p| p.payload.as_ref().map(bare))
        .collect();
    assert_eq!(names, [event]);
}

#[then(
    regex = r"^the dead letter's processing_failed details have failure code (\w+) and retry_count (\d+)$"
)]
fn failure_and_retries(w: &mut Coordinator, code: String, retries: u32) {
    let details = processing_failed(w);
    assert_eq!(details.failure.map(|f| f.code), Some(code));
    assert_eq!(details.retry_count, retries);
}

#[then(
    regex = r"^the dead letter's processing_failed details have is_transient (true|false) and retry_count (\d+)$"
)]
fn transient_and_retries(w: &mut Coordinator, transient: bool, retries: u32) {
    let details = processing_failed(w);
    assert_eq!(
        (details.is_transient, details.retry_count),
        (transient, retries)
    );
}

#[then(
    regex = r#"^the dead letter's source_component is "(\w+)" with source_component_type "(\w+)"$"#
)]
fn source_component(w: &mut Coordinator, component: String, kind: String) {
    let letter = the_letter(w);
    assert_eq!(letter.source_component, component);
    let expected = match kind.as_str() {
        "policy" => ComponentKind::Policy,
        "projector" => ComponentKind::Projector,
        "process_manager" => ComponentKind::ProcessManager,
        _ => ComponentKind::Aggregate,
    };
    assert_eq!(letter.source_component_kind(), expected);
}

/// `diagnostic` has `code` and at least the args `rest` names.
fn has(diagnostic: Option<&angzarr_embedded::v1::Diagnostic>, code: &str, rest: &str) {
    let diagnostic = diagnostic.expect("a diagnostic");
    assert_eq!(diagnostic.code, code);
    let wanted: BTreeMap<String, String> = named(rest);
    for (name, value) in wanted {
        assert_eq!(
            diagnostic.args.get(&name),
            Some(&value),
            "{name} of {diagnostic:?}"
        );
    }
}

#[then(regex = r"^the dead letter's reason has code (\w+)(.*)$")]
fn reason_has(w: &mut Coordinator, code: String, rest: String) {
    has(the_letter(w).reason.as_ref(), &code, &rest);
}

#[then(
    regex = r"^the dead letter's processing_failed details have failure code (\w+) and arg (.*)$"
)]
fn failure_has(w: &mut Coordinator, code: String, rest: String) {
    has(
        processing_failed(w).failure.as_ref(),
        &code,
        &format!(" {rest}"),
    );
}

#[then(regex = r#"^no field of the dead letter carries the text "([^"]+)"$"#)]
fn no_text(w: &mut Coordinator, text: String) {
    let letter = format!("{:?}", the_letter(w));
    assert!(!letter.contains(&text), "{letter}");
}
