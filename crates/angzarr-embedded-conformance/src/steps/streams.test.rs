//! Thens about where a chain's commands went and what each stream holds:
//! a domain handling a command on a stream, the ids it carried, the events
//! it recorded, and a trigger dead-lettered for a policy's bug.

use cucumber::then;

use angzarr_embedded::v1::angzarr_dead_letter::{Payload, RejectionDetails};
use angzarr_embedded::v1::{command_page, AngzarrDeadLetter, ContextualCommand, Cover, EventPage};

use crate::tests::business::recorded;
use crate::tests::policy::bare;
use crate::tests::world::{root, Coordinator};

/// The type name of a delivery's input.
fn input(delivered: &ContextualCommand) -> String {
    let body = delivered
        .command
        .as_ref()
        .and_then(|b| b.page.as_ref())
        .and_then(|p| p.body.as_ref());
    let payload = match body {
        Some(command_page::Body::Command(c)) => c.payload.clone(),
        Some(command_page::Body::Fact(f)) => f.payload.clone(),
        None => None,
    };
    payload.map(|p| bare(&p)).unwrap_or_default()
}

/// A delivery's stream.
fn stream(delivered: &ContextualCommand) -> Cover {
    delivered
        .command
        .as_ref()
        .and_then(|b| b.cover.clone())
        .unwrap_or_default()
}

/// The deliveries to `domain` of `command`.
fn deliveries(w: &mut Coordinator, domain: &str, command: &str) -> Vec<ContextualCommand> {
    w.business(domain)
        .delivered()
        .into_iter()
        .filter(|d| input(d) == command)
        .collect()
}

/// The one delivery to `domain` of `command` on root `name`.
fn delivery_on(w: &mut Coordinator, domain: &str, command: &str, name: &str) -> ContextualCommand {
    let on = deliveries(w, domain, command);
    let at: Vec<_> = on
        .iter()
        .filter(|d| stream(d).root == Some(root(name)))
        .collect();
    assert_eq!(at.len(), 1, "{command} on {domain} {name}: {on:?}");
    at[0].clone()
}

/// A delivery's correlation id.
fn correlation(delivered: &ContextualCommand) -> Option<angzarr_embedded::v1::Uuid> {
    delivered
        .command
        .as_ref()
        .and_then(|b| b.page.as_ref())
        .and_then(|p| p.correlation_id.clone())
}

#[then(regex = r#"^"([\w-]+)" "([^"]+)" handles (\w+)(?: with correlation_id "([^"]+)")?$"#)]
fn aggregate_handles(
    w: &mut Coordinator,
    domain: String,
    name: String,
    command: String,
    correlated: String,
) {
    let delivered = delivery_on(w, &domain, &command, &name);
    if !correlated.is_empty() {
        assert_eq!(correlation(&delivered), Some(root(&correlated)));
    }
}

#[then(
    regex = r#"^"(\w+)" handles (\w+) on stream "([\w-]+)" root "([^"]+)"(?: with correlation_id "([^"]+)")?$"#
)]
fn handles_on_stream(
    w: &mut Coordinator,
    component: String,
    command: String,
    domain: String,
    name: String,
    correlated: String,
) {
    assert_eq!(w.domain_of(&component), domain);
    aggregate_handles(w, domain, name, command, correlated);
}

#[then(regex = r#"^the (\w+) command carries correlation_id "([^"]+)"$"#)]
fn command_carries(w: &mut Coordinator, command: String, correlated: String) {
    let domains: Vec<String> = w.domains.keys().cloned().collect();
    let all: Vec<_> = domains
        .iter()
        .flat_map(|d| deliveries(w, d, &command))
        .collect();
    assert!(!all.is_empty(), "{command} delivered");
    assert!(
        all.iter()
            .all(|d| correlation(d) == Some(root(&correlated))),
        "{all:?}"
    );
}

/// Every event persisted on any stream of `domain` since the first when.
fn persisted_in(w: &Coordinator, domain: &str) -> Vec<EventPage> {
    let covers = w.store_covers(domain);
    covers.iter().flat_map(|cover| w.persisted(cover)).collect()
}

fn assert_correlated(pages: &[EventPage], correlated: &str) {
    assert!(!pages.is_empty(), "events recorded");
    for page in pages {
        let meta = page.meta.as_ref().expect("stamped");
        assert_eq!(meta.correlation_id, Some(root(correlated)), "{page:?}");
    }
}

#[then(regex = r#"^the events (?:it records|"(\w+)" records) carry correlation_id "([^"]+)"$"#)]
fn events_carry(w: &mut Coordinator, component: String, correlated: String) {
    let domain = match component.is_empty() {
        true => w.policies.last().expect("a policy").output.clone(),
        false => w.domain_of(&component),
    };
    assert_correlated(&persisted_in(w, &domain), &correlated);
}

#[then(regex = r#"^the (\w+) events carry correlation_id "([^"]+)"$"#)]
fn input_events_carry(w: &mut Coordinator, input: String, correlated: String) {
    let event = recorded(&input).to_string();
    let domains: Vec<String> = w.domains.keys().cloned().collect();
    let pages: Vec<EventPage> = domains
        .iter()
        .flat_map(|d| persisted_in(w, d))
        .filter(|p| p.payload.as_ref().is_some_and(|a| bare(a) == event))
        .collect();
    assert_correlated(&pages, &correlated);
}

#[then(regex = r#"^nothing is appended to stream "([\w-]+)" root "([^"]+)"$"#)]
fn nothing_appended(w: &mut Coordinator, domain: String, name: String) {
    let cover = w.cover(&domain, &name);
    assert!(w.persisted(&cover).is_empty(), "{:?}", w.persisted(&cover));
}

#[then(regex = r#"^stream "([\w-]+)" root "([^"]+)" holds the events of both inputs$"#)]
fn holds_both(w: &mut Coordinator, domain: String, name: String) {
    let cover = w.cover(&domain, &name);
    let types: Vec<String> = w
        .persisted(&cover)
        .iter()
        .filter_map(|p| p.payload.as_ref().map(bare))
        .collect();
    let inputs: Vec<String> = w.business(&domain).delivered().iter().map(input).collect();
    let expected: Vec<String> = inputs.iter().map(|i| recorded(i).to_string()).collect();
    assert_eq!(types, expected);
    assert_eq!(inputs.len(), 2);
}

#[then(regex = r#"^each command's causation_id is the (\w+) event of "([\w-]+)" "([^"]+)"$"#)]
fn causation_is(w: &mut Coordinator, event: String, domain: String, name: String) {
    let cover = w.cover(&domain, &name);
    let trigger = w
        .pages(&cover)
        .into_iter()
        .find(|p| p.payload.as_ref().is_some_and(|a| bare(a) == event))
        .expect("the trigger");
    let others: Vec<String> = w
        .domains
        .keys()
        .filter(|d| **d != domain)
        .cloned()
        .collect();
    let delivered: Vec<_> = others
        .iter()
        .flat_map(|d| w.business(d).delivered())
        .collect();
    assert!(!delivered.is_empty());
    for d in delivered {
        let causation = d
            .command
            .and_then(|b| b.page)
            .and_then(|p| p.causation_id)
            .expect("a causation");
        assert_eq!(causation.stream.as_ref(), Some(&cover));
        assert_eq!(causation.sequence, trigger.sequence);
    }
}

/// Every dead letter the log was told of with the reason `reason`.
fn letters_of(w: &Coordinator, reason: &str) -> Vec<AngzarrDeadLetter> {
    w.log
        .letters()
        .into_iter()
        .filter(|l| l.reason.as_ref().is_some_and(|r| r.code == reason))
        .collect()
}

/// Whether `value` is the arg a scenario names: a type by its bare name.
fn names(value: &str, wanted: &str) -> bool {
    value == wanted || value.rsplit(['.', '/']).next() == Some(wanted)
}

#[then(
    regex = r#"^the (?:\w+ command|policy's answer) is refused INTERNAL with reason (\w+)((?:,.*)?)$"#
)]
fn policy_bug(w: &mut Coordinator, reason: String, naming: String) {
    let letters = letters_of(w, &reason);
    assert_eq!(
        letters.len(),
        1,
        "one dead letter of {reason}: {:?}",
        w.log.letters()
    );
    let args = &letters[0].reason.as_ref().expect("a reason").args;
    let mut wanted = crate::tests::outcomes::named(&naming);
    let word = |after: &str| {
        let at = naming.find(after)? + after.len();
        naming[at..]
            .split([',', ' '])
            .find(|w| !w.is_empty())
            .map(str::to_string)
    };
    if let Some(command_type) = word("command type ") {
        wanted.insert("command_type".to_string(), command_type);
    }
    if let Some(field) = word("with field ") {
        wanted.insert("field".to_string(), field);
    }
    for (name, value) in wanted {
        let given = args
            .get(&name)
            .unwrap_or_else(|| panic!("no {name} in {args:?}"));
        assert!(names(given, &value), "{name}: {given} is not {value}");
    }
}

#[then(
    regex = r"^(?:it|the OrderPlaced trigger) is dead-lettered(?: with reason (\w+) as rejected_events)?,? and an alert is raised$"
)]
fn trigger_dead_lettered(w: &mut Coordinator, reason: String) {
    let letters = match reason.is_empty() {
        true => w.log.letters(),
        false => letters_of(w, &reason),
    };
    assert!(!letters.is_empty(), "dead-lettered");
    assert!(!w.alerts().is_empty(), "alerted");
    if !reason.is_empty() {
        assert!(
            letters
                .iter()
                .all(|l| matches!(l.payload, Some(Payload::RejectedEvents(_)))),
            "{letters:?}"
        );
    }
}

#[then(
    regex = r#"^the (\w+) event that triggered it is dead-lettered with reason (\w+) and arg (\w+) "([^"]+)" as rejected_events, and an alert is raised$"#
)]
fn triggering_event_dead_lettered(
    w: &mut Coordinator,
    event: String,
    reason: String,
    arg: String,
    value: String,
) {
    let letters = letters_of(w, &reason);
    assert_eq!(
        letters.len(),
        1,
        "{:?}; answered {:?}",
        w.log.letters(),
        w.answers()
    );
    let letter = &letters[0];
    assert_eq!(
        letter.reason.as_ref().and_then(|r| r.args.get(&arg)),
        Some(&value)
    );
    let Some(Payload::RejectedEvents(book)) = &letter.payload else {
        panic!("rejected events: {letter:?}");
    };
    assert!(book
        .pages
        .iter()
        .any(|p| p.payload.as_ref().is_some_and(|a| bare(a) == event)));
    assert!(!w.alerts().is_empty());
}

#[then(
    regex = r"^the dead letter's processing_failed details have failure code (\w+), retry_count (\d+) and is_transient (true|false)$"
)]
fn processing_failed(w: &mut Coordinator, code: String, retries: u32, transient: bool) {
    let letters = letters_of(w, &code);
    assert_eq!(letters.len(), 1, "{:?}", w.log.letters());
    let Some(RejectionDetails::ProcessingFailed(details)) = &letters[0].rejection_details else {
        panic!("processing failed details: {:?}", letters[0]);
    };
    assert_eq!(
        details.failure.as_ref().map(|f| f.code.as_str()),
        Some(code.as_str())
    );
    assert_eq!(
        (details.retry_count, details.is_transient),
        (retries, transient)
    );
}

/// No domain but the source was delivered anything.
#[then(
    regex = r"^(?:nothing is delivered|neither command is delivered)(?: and nothing is appended to stream .*)?$"
)]
fn nothing_delivered(w: &mut Coordinator) {
    let source = w.policies.last().expect("a policy").input.clone();
    let others: Vec<String> = w
        .domains
        .keys()
        .filter(|d| **d != source)
        .cloned()
        .collect();
    for domain in others {
        assert!(
            w.business(&domain).delivered().is_empty(),
            "{domain} was delivered to"
        );
        assert!(persisted_in(w, &domain).is_empty(), "{domain} recorded");
    }
}

#[then(
    regex = r#"^no root is filled in for it, and nothing is appended to any "([\w-]+)" stream$"#
)]
fn no_root_filled(w: &mut Coordinator, domain: String) {
    assert!(w.business(&domain).delivered().is_empty());
    assert!(persisted_in(w, &domain).is_empty());
}

#[then(regex = r"^the reaction at depth (\d+) is not delivered$")]
fn depth_not_delivered(w: &mut Coordinator, depth: usize) {
    // The command a caller sent is depth 0, so reactions at depths 1 to
    // depth - 1 were delivered, and none beyond.
    assert_eq!(w.business("order").delivered().len(), depth);
}

#[then(regex = r"^the step fails (\w+) with reason (\w+)(.*)$")]
fn step_fails(w: &mut Coordinator, status_name: String, reason: String, rest: String) {
    let resumed = w.resumed.last().expect("a catch-up ran");
    let Err(angzarr_embedded::Failure::Downstream { error, .. }) = resumed else {
        panic!(
            "a failed step, not {:?}",
            resumed.as_ref().map(|r| r.books().len())
        );
    };
    let answer = crate::tests::answer::Answered::of_step(error);
    assert_eq!(
        answer.status(),
        Some(crate::tests::answer::status(&status_name)),
        "{answer:?}"
    );
    assert_eq!(answer.reason(), Some(reason.as_str()), "{answer:?}");
    for (name, value) in crate::tests::outcomes::named(&rest) {
        assert_eq!(
            answer.arg(&name),
            Some(value.as_str()),
            "{name} of {answer:?}"
        );
    }
    if rest.contains("with no wait before it") {
        assert!(w.waits.waited().is_empty(), "{:?}", w.waits.waited());
    }
}

#[then(
    regex = r#"^the policy's position on the "([\w-]+)" stream stays at the (\w+) event, never moved past it$"#
)]
fn position_stays(w: &mut Coordinator, domain: String, event: String) {
    let def = w.policies.last().expect("a policy");
    let name = def.name.clone();
    let cover = w.cover(&domain, "p-1");
    let at = w
        .pages(&cover)
        .iter()
        .position(|p| p.payload.as_ref().is_some_and(|a| bare(a) == event));
    assert_eq!(w.position(&name, &cover), at.expect("the event") as u64);
}

#[then("the policy's next catch-up runs the step again")]
async fn next_catch_up(w: &mut Coordinator) {
    let before = w.business("order").delivered().len();
    let resumed = w.resume().await;
    w.resumed.push(resumed);
    assert_eq!(w.business("order").delivered().len(), before + 1);
}
