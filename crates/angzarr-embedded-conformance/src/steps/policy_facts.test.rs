//! Givens and thens about a policy's facts (policy_facts.feature): what
//! its destination recorded of them, the identity the kernel stamped on
//! each, and what was dead-lettered when one was parked or of an arm its
//! destination does not declare.

use cucumber::{given, then};

use angzarr_embedded::ids::policy_command_id;
use angzarr_embedded::v1::angzarr_dead_letter::Payload;
use angzarr_embedded::v1::event_meta::Causation;
use angzarr_embedded::v1::{
    command_page, AngzarrDeadLetter, CommandPage, ContextualCommand, EventMeta, EventPage,
    EventRef, Uuid,
};

use crate::tests::business::{rejection, Rule};
use crate::tests::policies::sends_as_fact;
use crate::tests::policy::bare;
use crate::tests::world::Coordinator;

/// The page a delivery carried.
fn page(delivered: &ContextualCommand) -> CommandPage {
    delivered
        .command
        .as_ref()
        .and_then(|b| b.page.clone())
        .expect("a page")
}

/// A page's payload type, by bare name, and whether it is a fact.
fn input(page: &CommandPage) -> (String, bool) {
    match &page.body {
        Some(command_page::Body::Command(c)) => {
            (c.payload.as_ref().map(bare).unwrap_or_default(), false)
        }
        Some(command_page::Body::Fact(f)) => {
            (f.payload.as_ref().map(bare).unwrap_or_default(), true)
        }
        None => (String::new(), false),
    }
}

/// The one delivery to `domain`'s business since the first when that is a
/// fact (`fact`) or a command of type `name` (any type, `name` empty).
fn delivered(w: &mut Coordinator, domain: &str, name: &str, fact: bool) -> CommandPage {
    let found: Vec<CommandPage> = w
        .business(domain)
        .delivered()
        .iter()
        .map(page)
        .filter(|p| {
            let (kind, is_fact) = input(p);
            is_fact == fact && (name.is_empty() || kind == name)
        })
        .collect();
    assert_eq!(
        found.len(),
        1,
        "one delivery to {domain} of {name}: {found:?}"
    );
    found[0].clone()
}

/// The trigger the order stream `order`'s event of type `event` is: its
/// stream and its sequence.
fn trigger(w: &Coordinator, order: &str, event: &str) -> EventRef {
    let cover = w.cover("order", order);
    let sequence = w
        .pages(&cover)
        .iter()
        .find(|p| p.payload.as_ref().is_some_and(|a| bare(a) == event))
        .map(|p| p.sequence)
        .unwrap_or_else(|| panic!("an {event} on order {order}"));
    EventRef {
        stream: Some(cover),
        sequence,
    }
}

/// The page of `cover` recording an input of type `name`.
fn recording(w: &Coordinator, domain: &str, name: &str, of: &str) -> EventPage {
    let cover = w.cover(domain, name);
    w.pages(&cover)
        .into_iter()
        .find(|p| p.payload.as_ref().is_some_and(|a| bare(a) == of))
        .unwrap_or_else(|| panic!("{of} recorded on {domain} {name}"))
}

/// The command (or fact) an event's meta names as its cause.
fn caused_by(meta: &EventMeta) -> Option<Uuid> {
    meta.causation
        .as_ref()
        .map(|Causation::CommandId(id)| id.clone())
}

fn letters(w: &Coordinator) -> Vec<AngzarrDeadLetter> {
    w.log.letters()
}

/// A dead letter's rejected command, its type by bare name and whether
/// it is a fact.
fn rejected(letter: &AngzarrDeadLetter) -> Option<(String, bool)> {
    match &letter.payload {
        Some(Payload::RejectedCommand(book)) => book.page.as_ref().map(input),
        _ => None,
    }
}

#[given(
    regex = r#"^an? (\w+) event was recorded for order "([\w-]+)" and its (\w+) applied by "([\w-]+)" "([^"]+)"$"#
)]
async fn recorded_and_applied(
    w: &mut Coordinator,
    event: String,
    order: String,
    command: String,
    domain: String,
    name: String,
) {
    let cover = w.cover("order", &order);
    w.record(&cover, &event, None).await;
    recording(w, &domain, &name, &command);
}

#[given(regex = r#"^"([\w-]+)" now declares (\w+) a fact, and the policy sends it as one$"#)]
fn now_a_fact(w: &mut Coordinator, domain: String, name: String) {
    w.business(&domain).declare_fact(&name);
    sends_as_fact(w, &name);
}

#[then(regex = r#"^"([\w-]+)" "([^"]+)" records the (\w+) fact$"#)]
fn records_the_fact(w: &mut Coordinator, domain: String, name: String, fact: String) {
    let delivered = delivered(w, &domain, &fact, true);
    let page = recording(w, &domain, &name, &fact);
    let meta = page.meta.expect("stamped");
    assert_eq!(meta.fact, delivered.fact_origin, "the fact's origin");
    assert_eq!(caused_by(&meta), delivered.command_id, "its causation");
}

#[then(
    regex = r#"^the (?:fact|(\w+) command)'s command_id is the reaction command id of that (\w+) event, the policy "(\w+)" and index (\d+)$"#
)]
fn reaction_id(w: &mut Coordinator, command: String, event: String, policy: String, index: u32) {
    let output = w.policies.last().expect("a policy").output.clone();
    let delivered = delivered(w, &output, &command, command.is_empty());
    let trigger = trigger(w, "order-1", &event);
    let derived = policy_command_id(&trigger, &policy, index).expect("a reaction id");
    assert_eq!(delivered.command_id, Some(derived));
    assert_eq!(delivered.causation_id, Some(trigger), "its causation");
}

#[then("the fact's fact_origin external_id is that command_id's canonical text")]
fn fact_origin_is_id(w: &mut Coordinator) {
    let output = w.policies.last().expect("a policy").output.clone();
    let delivered = delivered(w, &output, "", true);
    let id = delivered.command_id.expect("an id").to_string();
    assert_eq!(delivered.fact_origin.map(|o| o.external_id), Some(id));
}

#[then(regex = r"^the (\w+) fact carries the command_id the (\w+) command carried$")]
fn same_id(w: &mut Coordinator, _fact: String, command: String) {
    let def = w.policies.last().expect("a policy");
    let (policy, output) = (def.name.clone(), def.output.clone());
    let applied = w
        .store_covers(&output)
        .iter()
        .flat_map(|cover| w.pages(cover))
        .find(|p| p.payload.as_ref().is_some_and(|a| bare(a) == command))
        .expect("the command's event");
    let carried = applied.meta.as_ref().and_then(caused_by);
    // The id the kernel stamps on the policy's fact (C-0775): the
    // reaction's, whatever its arm.
    let stamped = policy_command_id(&trigger(w, "order-1", "OrderPlaced"), &policy, 0).ok();
    assert_eq!(stamped, carried);
}

#[then(
    regex = r#"^it is answered DUPLICATE, and nothing new is appended to stream "([\w-]+)" root "([^"]+)"$"#
)]
fn answered_duplicate(w: &mut Coordinator, domain: String, name: String) {
    // Found by its id before the handler: never delivered.
    assert!(w.business(&domain).delivered().is_empty());
    let cover = w.cover(&domain, &name);
    assert!(w.persisted(&cover).is_empty(), "{:?}", w.persisted(&cover));
}

#[then(
    regex = r"^the (?:(\w+) )?fact is dead-lettered as rejected_command, and an alert is raised$"
)]
fn fact_dead_lettered(w: &mut Coordinator, fact: String) {
    let letters = letters(w);
    assert!(
        letters.iter().any(|l| match rejected(l) {
            Some((name, true)) => fact.is_empty() || name == fact,
            _ => false,
        }),
        "{letters:?}"
    );
    assert!(!w.alerts().is_empty(), "an alert");
}

#[given(
    regex = r#"^(?:"([\w-]+)"'s business process answers the|the "([\w-]+)" business process answers every) (\w+) fact with a rejection, FAILED_PRECONDITION$"#
)]
fn rejects_fact(w: &mut Coordinator, of: String, every: String, fact: String) {
    let business = w.business(if of.is_empty() { &every } else { &of });
    business.declare_fact(&fact);
    business.rule(Rule {
        input: fact,
        once: None,
        error: rejection("FACT_REJECTED"),
    });
}

#[then(regex = r#"^no refusal fact is recorded on "([\w-]+)" "([^"]+)"$"#)]
fn no_refusal_fact(w: &mut Coordinator, domain: String, name: String) {
    let cover = w.cover(&domain, &name);
    let pages = w.pages(&cover);
    assert!(
        !pages.iter().any(|p| p
            .payload
            .as_ref()
            .is_some_and(|a| bare(a) == "CommandRefused")),
        "{pages:?}"
    );
    let def = w.policies.last().expect("a policy");
    assert!(
        crate::tests::policy::locked(&def.script)
            .refusals
            .is_empty(),
        "OnRefused called"
    );
}

#[then(regex = r"^the (\w+) trigger is not dead-lettered$")]
fn trigger_not_dead_lettered(w: &mut Coordinator, _event: String) {
    let letters = letters(w);
    assert!(
        !letters
            .iter()
            .any(|l| matches!(l.payload, Some(Payload::RejectedEvents(_)))),
        "{letters:?}"
    );
}

#[then(regex = r#"^the policy "(\w+)"'s position on order "([\w-]+)" is past the (\w+) event$"#)]
fn position_past(w: &mut Coordinator, policy: String, order: String, event: String) {
    let trigger = trigger(w, &order, &event);
    let cover = trigger.stream.clone().expect("its stream");
    assert!(w.position(&policy, &cover) > trigger.sequence);
}

#[then(
    regex = r#"^the (\w+) (command|fact) is refused INTERNAL with reason (\w+) by "([\w-]+)", dead-lettered as rejected_command, and an alert is raised$"#
)]
fn arm_refused(w: &mut Coordinator, name: String, arm: String, reason: String, domain: String) {
    let letters = letters(w);
    let letter = letters
        .iter()
        .find(|l| l.reason.as_ref().is_some_and(|r| r.code == reason))
        .unwrap_or_else(|| panic!("no dead letter of {reason}: {letters:?}"));
    assert_eq!(rejected(letter), Some((name, arm == "fact")));
    let addressed = match &letter.payload {
        Some(Payload::RejectedCommand(book)) => book.cover.as_ref().map(|c| c.domain.clone()),
        _ => None,
    };
    assert_eq!(addressed.as_deref(), Some(domain.as_str()));
    assert!(!w.alerts().is_empty(), "an alert");
}
