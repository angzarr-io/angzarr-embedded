//! Givens and whens about policies: what each one's business code answers
//! a source event with, and the source events recorded for them to react
//! to.

use cucumber::{given, when};

use angzarr_embedded::v1::Cover;

use crate::tests::policy::{locked, Fault, Reaction, Root, Translation};
use crate::tests::world::Coordinator;

/// A policy as a scenario introduces it: `"OrderToInventory" translating
/// from "order" to "inventory"`.
#[derive(Debug, Default)]
struct Header {
    name: String,
    input: String,
    output: String,
}

/// The quoted words of `text`, in order.
fn quoted(text: &str) -> Vec<String> {
    text.split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

impl std::str::FromStr for Header {
    type Err = String;

    fn from_str(text: &str) -> Result<Header, String> {
        match &quoted(text)[..] {
            [name, input, output] => Ok(Header {
                name: name.clone(),
                input: input.clone(),
                output: output.clone(),
            }),
            _ => Err(format!("no policy header: {text}")),
        }
    }
}

/// A stream as a scenario names it: `"inventory" "inv-7"`.
#[derive(Debug, Default)]
struct Address {
    domain: String,
    root: String,
}

impl std::str::FromStr for Address {
    type Err = String;

    fn from_str(text: &str) -> Result<Address, String> {
        match &quoted(text)[..] {
            [domain, root] => Ok(Address {
                domain: domain.clone(),
                root: root.clone(),
            }),
            _ => Err(format!("no stream: {text}")),
        }
    }
}

/// A reaction on `on`: `command` to `domain` at `root`.
fn reaction(on: &str, domain: &str, command: &str, root: Root) -> Reaction {
    Reaction {
        on: on.to_string(),
        domain: domain.to_string(),
        command: command.to_string(),
        fact: false,
        root,
        correlation: None,
        fault: None,
    }
}

/// `reaction` added to the policy `name`'s script, the policy registered.
fn reacts(w: &mut Coordinator, header: &Header, reaction: Reaction) {
    let script = w.policy(&header.name, &header.input, &header.output);
    locked(&script).reactions.push(reaction);
}

/// The fault a scenario says a reaction carries.
fn fault(text: &str) -> Fault {
    match text {
        "sets a command_id other than the stamped one" => Fault::CommandId,
        "sets a causation_id other than the trigger's" => Fault::CausationId,
        "carries a fact_origin" => Fault::FactOrigin,
        other => panic!("no fault {other}"),
    }
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+")(?:, whose descriptor names output domain "[\w-]+")?$"#
)]
fn policy(w: &mut Coordinator, header: Header) {
    w.policy(&header.name, &header.input, &header.output);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (\w+) to ("[\w-]+" "[^"]+") on (\w+)$"#
)]
fn sends_to_root(w: &mut Coordinator, header: Header, command: String, to: Address, on: String) {
    let reaction = reaction(&on, &to.domain, &command, Root::Named(to.root));
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (\w+) to "([\w-]+)" root the trigger's correlation on (\w+)$"#
)]
fn sends_to_correlation(
    w: &mut Coordinator,
    header: Header,
    command: String,
    domain: String,
    on: String,
) {
    let reaction = reaction(&on, &domain, &command, Root::TriggerCorrelation);
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (\w+) with no root on (\w+)$"#
)]
fn sends_with_no_root(w: &mut Coordinator, header: Header, command: String, on: String) {
    let reaction = reaction(&on, &header.output, &command, Root::Unnamed);
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (\w+) to ("[\w-]+" "[^"]+") with correlation_id "([^"]+)" on (\w+)$"#
)]
fn sends_other_correlation(
    w: &mut Coordinator,
    header: Header,
    command: String,
    to: Address,
    correlation: String,
    on: String,
) {
    let mut reaction = reaction(&on, &to.domain, &command, Root::Named(to.root));
    reaction.correlation = Some(correlation);
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that builds (?:a )?(\w+)(?: command)? from (\w+)(?:, setting its root to the trigger's correlation)?$"#
)]
fn builds(w: &mut Coordinator, header: Header, command: String, on: String) {
    let reaction = reaction(&on, &header.output, &command, Root::TriggerCorrelation);
    reacts(w, &header, reaction);
}

/// The policy registered last.
fn last_policy(w: &Coordinator) -> Header {
    let def = w.policies.last().expect("a policy");
    Header {
        name: def.name.clone(),
        input: def.input.clone(),
        output: def.output.clone(),
    }
}

#[given(
    regex = r#"^its business code answers an (\w+) event with an? (\w+) command for ("[\w-]+" "[^"]+")(?: and an? (\w+) command for "([\w-]+)")?$"#
)]
fn its_business_code_answers(
    w: &mut Coordinator,
    on: String,
    first: String,
    to: Address,
    second: String,
    other: String,
) {
    let header = last_policy(w);
    reacts(
        w,
        &header,
        reaction(&on, &to.domain, &first, Root::Named(to.root)),
    );
    if !second.is_empty() {
        let elsewhere = reaction(&on, &other, &second, Root::Named("elsewhere-1".to_string()));
        reacts(w, &header, elsewhere);
    }
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") whose business code answers an (\w+) event with a (\w+) (command|fact) for ("[\w-]+" "[^"]+") that (.+)$"#
)]
fn answers_with_fault(
    w: &mut Coordinator,
    header: Header,
    on: String,
    command: String,
    arm: String,
    to: Address,
    faulty: String,
) {
    let mut reaction = reaction(&on, &to.domain, &command, Root::Named(to.root));
    reaction.fact = arm == "fact";
    reaction.fault = Some(fault(&faulty));
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") whose business code answers every (\w+) event with an? (\w+) command for another order, a new one each time$"#
)]
fn answers_another_stream(w: &mut Coordinator, header: Header, on: String, command: String) {
    let reaction = reaction(&on, &header.output, &command, Root::Another);
    reacts(w, &header, reaction);
}

#[given(regex = r"^the policy handles (\w+) by emitting an? (\w+) (command|fact)$")]
fn handles_by_emitting(w: &mut Coordinator, on: String, command: String, arm: String) {
    let header = last_policy(w);
    let mut reaction = reaction(
        &on,
        &header.output,
        &command,
        Root::Named("t-1".to_string()),
    );
    reaction.fact = arm == "fact";
    reacts(w, &header, reaction);
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") that sends (?:the fact (\w+)|(\w+) as an? (command|fact)) to ("[\w-]+" "[^"]+") on (\w+)$"#
)]
fn sends_arm_to_root(
    w: &mut Coordinator,
    header: Header,
    fact: String,
    named: String,
    arm: String,
    to: Address,
    on: String,
) {
    let (command, fact) = match fact.is_empty() {
        false => (fact, true),
        true => (named, arm == "fact"),
    };
    let mut reaction = reaction(&on, &to.domain, &command, Root::Named(to.root));
    reaction.fact = fact;
    reacts(w, &header, reaction);
}

/// Two outputs as a scenario lists them, in order: `a ReserveStock command
/// and then a StockHoldRequested fact`, each a type and whether it is a
/// fact.
#[derive(Debug, Default)]
struct Outputs(Vec<(String, bool)>);

impl std::str::FromStr for Outputs {
    type Err = String;

    fn from_str(text: &str) -> Result<Outputs, String> {
        text.split(" and then ")
            .map(|output| match output.split(' ').collect::<Vec<_>>()[..] {
                ["a" | "an", name, arm @ ("command" | "fact")] => {
                    Ok((name.to_string(), arm == "fact"))
                }
                _ => Err(format!("no output: {output}")),
            })
            .collect::<Result<_, _>>()
            .map(Outputs)
    }
}

#[given(
    regex = r#"^a policy ("\w+" translating from "[\w-]+" to "[\w-]+") whose business code answers an (\w+) event with (an? \w+ (?:command|fact) and then an? \w+ (?:command|fact)) for ("[\w-]+" "[^"]+")$"#
)]
fn answers_in_order(
    w: &mut Coordinator,
    header: Header,
    on: String,
    outputs: Outputs,
    to: Address,
) {
    for (name, fact) in outputs.0 {
        let mut reaction = reaction(&on, &to.domain, &name, Root::Named(to.root.clone()));
        reaction.fact = fact;
        reacts(w, &header, reaction);
    }
}

/// From here on the last policy sends each of its reactions of type
/// `command` as a fact.
pub(crate) fn sends_as_fact(w: &Coordinator, command: &str) {
    let def = w.policies.last().expect("a policy");
    for reaction in &mut locked(&def.script).reactions {
        if reaction.command == command {
            reaction.fact = true;
        }
    }
}

#[given(
    regex = r#"^no component of "([\w-]+)" is registered with the policy's coordinator, and it knows no coordinator for "[\w-]+"$"#
)]
fn unregistered(w: &mut Coordinator, domain: String) {
    w.absent.insert(domain);
}

#[given(regex = r"^a coordinator whose chain depth limit is (\d+)$")]
fn depth_limit(w: &mut Coordinator, limit: usize) {
    w.depth = Some(limit);
}

#[given(regex = r#"^no policy translates from "([\w-]+)" to "([\w-]+)"$"#)]
fn no_policy(w: &mut Coordinator, input: String, output: String) {
    assert!(!w
        .policies
        .iter()
        .any(|p| p.input == input && p.output == output));
}

#[given(regex = r#"^"(\w+)"'s callback fails with INTERNAL$"#)]
fn callback_fails(w: &mut Coordinator, name: String) {
    let def = w
        .policies
        .iter()
        .find(|p| p.name == name)
        .expect("the policy");
    let code = crate::tests::policy::internal("CALLBACK_FAILED");
    locked(&def.script).translation = Some(Translation::Fails(code));
}

#[when(
    regex = r#"^an? (\w+) event (?:with correlation "([^"]+)" )?is recorded for order "([\w-]+)"(?: on the main timeline| in edition "([\w-]+)")?$"#
)]
async fn recorded_for_order(
    w: &mut Coordinator,
    event: String,
    correlation: String,
    order: String,
    edition: String,
) {
    w.begin();
    let cover = Cover {
        edition: (!edition.is_empty()).then_some(edition),
        ..w.cover("order", &order)
    };
    let correlation = match correlation.is_empty() {
        true => w.order_correlation.clone(),
        false => Some(correlation),
    };
    w.record(&cover, &event, correlation.as_deref()).await;
}

#[when(regex = r#"^an AddItem command for "order" "([\w-]+)" is handled$"#)]
async fn add_item_handled(w: &mut Coordinator, order: String) {
    w.begin();
    let cover = w.cover("order", &order);
    let book = w.command(&cover, "AddItem");
    w.handle(book).await;
}

/// The policy a scenario's "policy-emitted" command comes from: `Reactor`,
/// from `payment` to `domain`, sending `command` to the stream `root` on
/// each PaymentReceived.
pub(crate) fn reactor(
    w: &mut Coordinator,
    domain: &str,
    command: &str,
    root: Root,
    fault: Option<Fault>,
) {
    let header = Header {
        name: "Reactor".to_string(),
        input: "payment".to_string(),
        output: domain.to_string(),
    };
    let mut reaction = reaction("PaymentReceived", domain, command, root);
    reaction.fault = fault;
    reacts(w, &header, reaction);
}

/// A PaymentReceived recorded on the payment stream: what sets
/// [`reactor`] off. Pending, for the scenario's when to handle.
pub(crate) fn payment_received(w: &mut Coordinator) -> angzarr_embedded::v1::CommandBook {
    let cover = w.cover("payment", "p-1");
    w.command(&cover, "RecordPaymentReceived")
}

#[given(
    regex = r"^a policy-emitted (\w+) command(?: with no expected sequence and no conflict resolution| targeting sequence (\d+))?$"
)]
fn policy_emitted(w: &mut Coordinator, command: String, expected: String) {
    let fault = expected.parse().ok().map(Fault::Handling);
    reactor(
        w,
        "order",
        &command,
        Root::Named("order-1".to_string()),
        fault,
    );
    let book = payment_received(w);
    w.pending.push(book);
}

#[given(
    regex = r#"^a policy "(\w+)" translating from "([\w-]+)" to "([\w-]+)" that sends (\w+) to ("[\w-]+" "[^"]+") on (\w+), with no expected sequence and no conflict resolution$"#
)]
fn sends_unhandled(
    w: &mut Coordinator,
    name: String,
    input: String,
    output: String,
    command: String,
    to: Address,
    on: String,
) {
    let header = Header {
        name,
        input,
        output,
    };
    let reaction = reaction(&on, &to.domain, &command, Root::Named(to.root.clone()));
    reacts(w, &header, reaction);
    // The stream it reaches holds three events, its head at 3.
    let cover = w.cover(&to.domain, &to.root);
    if w.pages(&cover).is_empty() {
        w.write(&cover, &["OrderCreated", "ItemAdded", "OrderNoteAdded"]);
    }
    w.speak_of(&to.root);
}

#[given(regex = r#"^a (\w+) event at sequence (\d+) of a "([\w-]+)" stream$"#)]
fn event_at_sequence(w: &mut Coordinator, event: String, sequence: usize, domain: String) {
    let cover = w.cover(&domain, "p-1");
    let mut names = vec!["PaymentOpened"; sequence];
    names.push(&event);
    w.write(&cover, &names);
}

#[when(regex = r#"^the policy "(\w+)" runs on the (\w+) event$"#)]
async fn policy_runs(w: &mut Coordinator, _name: String, _event: String) {
    w.begin();
    let resumed = w.resume().await;
    w.resumed.push(resumed);
}
