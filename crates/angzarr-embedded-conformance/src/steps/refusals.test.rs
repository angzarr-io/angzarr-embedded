//! A refused policy command becoming one refusal fact for the policy's own
//! source: the target's refusal (typed, untyped, or a failure of another
//! class), the policy's OnRefused (pass-through or translating), and the
//! fact the coordinator addresses and identifies.

use std::collections::BTreeMap;

use cucumber::{given, then, when};

use angzarr_embedded::angzarr_router::error::CodedError;
use angzarr_embedded::v1::refusal_response::Reason;
use angzarr_embedded::v1::{command_page, CommandRefused, ContextualCommand, EventRef};
use angzarr_embedded::Any;

use crate::tests::answer::status;
use crate::tests::business::Rule;
use crate::tests::policy::{
    bare, command_refused_in, internal, locked, payload, Root, Translation,
};
use crate::tests::world::{root, Coordinator};

/// The refusal object a wallet refuses with, as the wire carries it:
/// `available` and `requested`, varint fields 1 and 2.
fn insufficient_funds(available: u64, requested: u64) -> Any {
    fn varint(mut n: u64, out: &mut Vec<u8>) {
        while n >= 0x80 {
            out.push((n as u8) | 0x80);
            n >>= 7;
        }
        out.push(n as u8);
    }
    let mut value = Vec::new();
    for (tag, n) in [(0x08, available), (0x10, requested)] {
        if n > 0 {
            value.push(tag);
            varint(n, &mut value);
        }
    }
    Any {
        type_url: payload("wallet", "InsufficientFunds").type_url,
        value,
    }
}

/// A wallet's typed refusal of HoldFunds, as the router answers it.
fn refused_with(object: Any) -> CodedError {
    CodedError {
        code: "ROUTER_REASON_COMMAND_REFUSED".to_string(),
        extras: BTreeMap::from([("refusal_type".to_string(), bare(&object))]),
        grpc: status("FAILED_PRECONDITION"),
        refusal: Some(Box::new(object)),
    }
}

/// The payload of a delivery's fact, if it is one.
fn fact_payload(delivered: &ContextualCommand) -> Option<Any> {
    let page = delivered.command.as_ref()?.page.as_ref()?;
    match page.body.as_ref()? {
        command_page::Body::Fact(fact) => fact.payload.clone(),
        command_page::Body::Command(_) => None,
    }
}

/// The refusal facts delivered to `domain`.
fn refusal_facts(w: &mut Coordinator, domain: &str, name: &str) -> Vec<ContextualCommand> {
    w.business(domain)
        .delivered()
        .into_iter()
        .filter(|d| fact_payload(d).is_some_and(|p| bare(&p) == name))
        .collect()
}

/// The one CommandRefused fact delivered to `domain`, decoded.
fn command_refused(w: &mut Coordinator, domain: &str) -> (ContextualCommand, CommandRefused) {
    let facts = refusal_facts(w, domain, "CommandRefused");
    assert_eq!(facts.len(), 1, "{facts:?}");
    let any = fact_payload(&facts[0]).expect("a fact");
    (
        facts[0].clone(),
        command_refused_in(&any).expect("a CommandRefused"),
    )
}

/// The policy that builds HoldFunds, and its script.
fn holds_funds(w: &Coordinator) -> &crate::tests::world::PolicyDef {
    w.policies
        .iter()
        .find(|p| p.name == "BuyInToWallet")
        .expect("BuyInToWallet")
}

#[given(
    regex = r#"^a "wallet" aggregate that refuses HoldFunds with FAILED_PRECONDITION reason \w+ and the refusal object InsufficientFunds(?: with available \d+ and requested \d+)? when funds are short$"#
)]
fn wallet(w: &mut Coordinator) {
    w.domain("wallet");
}

#[given(regex = r#"^wallet "([\w-]+)" has too little available to hold (\d+)$"#)]
fn too_little(w: &mut Coordinator, wallet: String, amount: u64) {
    short(w, &wallet, 0, amount);
}

#[given(regex = r#"^wallet "([\w-]+)" has (\d+) available and is asked to hold (\d+)$"#)]
fn available_and_asked(w: &mut Coordinator, wallet: String, available: u64, amount: u64) {
    short(w, &wallet, available, amount);
}

/// Wallet `name` short, holding `available`: HoldFunds refused with
/// InsufficientFunds for `requested`.
fn short(w: &mut Coordinator, name: &str, available: u64, requested: u64) {
    let cover = w.cover("wallet", name);
    w.write(&cover, &["FundsShort"]);
    w.business("wallet").rule(Rule {
        input: "HoldFunds".to_string(),
        once: Some("FundsShort".to_string()),
        error: refused_with(insufficient_funds(available, requested)),
    });
}

#[given(
    regex = r#"^wallet "([\w-]+)" is closed, so its HoldFunds handler rejects with code (\w+) and arg wallet "([\w-]+)" and no refusal object$"#
)]
fn closed(w: &mut Coordinator, _wallet: String, code: String, arg: String) {
    let mut error = crate::tests::business::rejection(&code);
    error.extras.insert("wallet".to_string(), arg);
    w.business("wallet").rule(Rule {
        input: "HoldFunds".to_string(),
        once: None,
        error,
    });
}

#[given(regex = r#"^wallet "([\w-]+)" answers HoldFunds with (\w+)$"#)]
fn answers_with_status(w: &mut Coordinator, _wallet: String, grpc: String) {
    let mut error = internal(&format!("WALLET_{grpc}"));
    error.grpc = status(&grpc);
    w.business("wallet").rule(Rule {
        input: "HoldFunds".to_string(),
        once: None,
        error,
    });
}

#[given(
    regex = r#"^"(\w+)" was generated with HoldFunds in (pass-through|translate) mode(?:, its callback returning the "[\w-]+" fact (\w+))?$"#
)]
fn generated_in_mode(w: &mut Coordinator, name: String, mode: String, fact: String) {
    let def = w
        .policies
        .iter()
        .find(|p| p.name == name)
        .expect("the policy");
    let fact = if fact.is_empty() {
        "HoldFundsDeclined".to_string()
    } else {
        fact
    };
    locked(&def.script).translation = (mode == "translate").then_some(Translation::Fact(fact));
}

#[given(regex = r#"^"(\w+)" defines (an|no) on_refused_HoldFunds handler$"#)]
fn defines_on_refused(w: &mut Coordinator, name: String, defines: String) {
    if defines == "an" {
        let domain = w.domain_of(&name);
        w.business(&domain).declare_fact("CommandRefused");
    }
}

#[given(regex = r#"^the "([\w-]+)" aggregate defines an on_refused_\w+ handler$"#)]
fn aggregate_defines_on_refused(w: &mut Coordinator, domain: String) {
    w.business(&domain).declare_fact("CommandRefused");
}

#[given(
    regex = r#"^a process manager "(\w+)" owning domain "([\w-]+)" that refuses (\w+) once completed$"#
)]
fn process_manager_refusing(w: &mut Coordinator, name: String, domain: String, input: String) {
    w.names.insert(name.clone(), domain.clone());
    w.process_manager(&domain, &[]);
    w.business(&domain).rule(Rule {
        input,
        once: Some(format!("{name}Completed")),
        error: crate::tests::business::rejection("FULFILLMENT_COMPLETED"),
    });
    let cover = w.cover(&domain, "the-stock-correlation");
    w.write(&cover, &[&format!("{name}Completed")]);
}

#[given(
    regex = r#"^a policy "(\w+)" translating from "([\w-]+)" to "([\w-]+)", generated with (\w+) in pass-through mode$"#
)]
fn pass_through_policy(
    w: &mut Coordinator,
    name: String,
    input: String,
    output: String,
    command: String,
) {
    let script = w.policy(&name, &input, &output);
    locked(&script)
        .reactions
        .push(crate::tests::policy::Reaction {
            on: crate::tests::business::recorded(&command).to_string(),
            domain: output,
            command,
            root: Root::TriggerCorrelation,
            correlation: None,
            fault: None,
        });
}

#[when(
    regex = r#"^"(\w+)" records BuyInStarted for \d+ from wallet "([\w-]+)"(?: under correlation "([^"]+)")?$"#
)]
async fn records_buy_in_started(
    w: &mut Coordinator,
    name: String,
    wallet: String,
    correlation: String,
) {
    let correlation = if correlation.is_empty() {
        "the-buy-in".to_string()
    } else {
        correlation
    };
    let def = holds_funds(w);
    for reaction in locked(&def.script).reactions.iter_mut() {
        reaction.root = Root::Named(wallet.clone());
    }
    w.begin();
    let domain = w.domain_of(&name);
    // BuyIn's handler records BuyInStarted as it starts an instance.
    w.domain(&domain)
        .starting
        .push("RecordBuyInStarted".to_string());
    let cover = w.cover(&domain, &correlation);
    w.record(&cover, "BuyInStarted", Some(&correlation)).await;
}

#[when(regex = r#"^"(\w+)" refuses the (\w+) built from (\w+) of item "([\w-]+)"$"#)]
async fn refuses_built_from(
    w: &mut Coordinator,
    _name: String,
    _command: String,
    event: String,
    item: String,
) {
    w.begin();
    let cover = w.cover("inventory", &item);
    w.record(&cover, &event, Some("the-stock-correlation"))
        .await;
}

#[then(regex = r#"^wallet "([\w-]+)" refuses HoldFunds and records nothing$"#)]
fn wallet_refuses(w: &mut Coordinator, wallet: String) {
    assert_eq!(w.business("wallet").delivered().len(), 1);
    let cover = w.cover("wallet", &wallet);
    assert!(w.persisted(&cover).is_empty());
}

#[then(regex = r#"^"(\w+)"'s OnRefused answers no translation$"#)]
fn no_translation(w: &mut Coordinator, _name: String) {
    let script = locked(&holds_funds(w).script);
    assert_eq!(script.refusals.len(), 1);
    assert!(script.translation.is_none());
}

#[then("the coordinator builds a CommandRefused fact carrying only the HoldFunds sent, the InsufficientFunds refusal object packed and the BuyInStarted event's EventRef")]
fn builds_command_refused(w: &mut Coordinator) {
    let (delivered, refused) = command_refused(w, "buy-in");
    let sent = w
        .business("wallet")
        .delivered()
        .pop()
        .expect("HoldFunds sent");
    let command = refused.command.expect("the command");
    assert_eq!(
        command.cover,
        sent.command.as_ref().and_then(|b| b.cover.clone())
    );
    assert_eq!(
        refused.refusal.as_ref().map(bare).as_deref(),
        Some("InsufficientFunds")
    );
    let trigger = refused.trigger.expect("the trigger");
    let source = delivered.command.and_then(|b| b.cover).expect("its stream");
    assert_eq!(trigger.stream, Some(source));
}

#[then(regex = r#"^it is delivered to "([\w-]+)" root "([^"]+)"$"#)]
fn delivered_to_root(w: &mut Coordinator, domain: String, name: String) {
    let (delivered, _) = command_refused(w, &domain);
    let cover = delivered.command.and_then(|b| b.cover).expect("its stream");
    assert_eq!(cover.root, Some(root(&name)));
}

#[then(
    regex = r#"^"(\w+)"'s on_refused_HoldFunds handler unpacks InsufficientFunds and records its own events for it$"#
)]
fn source_records_refusal(w: &mut Coordinator, name: String) {
    let domain = w.domain_of(&name);
    let (delivered, _) = command_refused(w, &domain);
    let cover = delivered.command.and_then(|b| b.cover).expect("its stream");
    let recorded: Vec<String> = w
        .persisted(&cover)
        .iter()
        .filter_map(|p| p.payload.as_ref().map(bare))
        .collect();
    assert!(
        recorded.contains(&"CommandRefused".to_string()),
        "{recorded:?}"
    );
}

#[then(
    regex = r#"^"(\w+)"'s callback is called with one Refusal holding the BuyInStarted event, the HoldFunds sent and the refusal response with the InsufficientFunds refusal object$"#
)]
fn callback_called_with(w: &mut Coordinator, _name: String) {
    let script = locked(&holds_funds(w).script);
    assert_eq!(script.refusals.len(), 1);
    let refusal = &script.refusals[0];
    let events: Vec<String> = refusal
        .event
        .iter()
        .flat_map(|e| &e.pages)
        .filter_map(|p| p.payload.as_ref().map(bare))
        .collect();
    assert_eq!(events, ["BuyInStarted"]);
    let command = refusal
        .command
        .as_ref()
        .and_then(|b| b.page.as_ref())
        .and_then(|p| p.body.as_ref());
    assert!(
        matches!(command, Some(command_page::Body::Command(c)) if c.payload.as_ref().map(bare).as_deref() == Some("HoldFunds"))
    );
    let reason = refusal.response.as_ref().and_then(|r| r.reason.as_ref());
    assert!(
        matches!(reason, Some(Reason::Refusal(object)) if bare(object) == "InsufficientFunds"),
        "{reason:?}"
    );
}

#[then(regex = r#"^a (\w+) fact is delivered to "([\w-]+)" (?:root|item) "([^"]+)"$"#)]
fn fact_delivered(w: &mut Coordinator, fact: String, domain: String, name: String) {
    let facts = refusal_facts(w, &domain, &fact);
    assert_eq!(facts.len(), 1, "{facts:?}");
    let cover = facts[0]
        .command
        .as_ref()
        .and_then(|b| b.cover.clone())
        .expect("its stream");
    assert_eq!(cover.root, Some(root(&name)));
}

#[then(
    regex = r#"^"(\w+)" handles it as an ordinary fact, with no reference to the wallet's protos$"#
)]
fn ordinary_fact(w: &mut Coordinator, name: String) {
    let domain = w.domain_of(&name);
    let recorded: Vec<String> = w
        .store_covers(&domain)
        .iter()
        .flat_map(|c| w.persisted(c))
        .filter_map(|p| p.payload.as_ref().map(bare))
        .collect();
    assert!(
        recorded.contains(&"HoldFundsDeclined".to_string()),
        "{recorded:?}"
    );
}

/// The refusal fact delivered to the buy-in, either kind.
fn refusal_fact(w: &mut Coordinator) -> ContextualCommand {
    let mut facts = refusal_facts(w, "buy-in", "CommandRefused");
    facts.extend(refusal_facts(w, "buy-in", "HoldFundsDeclined"));
    assert_eq!(facts.len(), 1, "{facts:?}");
    facts.remove(0)
}

#[then("the refusal fact's command_id is the refused HoldFunds' command_id")]
fn refusal_fact_id(w: &mut Coordinator) {
    let fact = refusal_fact(w)
        .command
        .and_then(|b| b.page)
        .expect("a page");
    let sent = w
        .business("wallet")
        .delivered()
        .pop()
        .and_then(|d| d.command)
        .and_then(|b| b.page)
        .expect("HoldFunds");
    assert_eq!(fact.command_id, sent.command_id);
}

#[then("its fact_origin external_id is that command_id's canonical text")]
fn external_id_is_id(w: &mut Coordinator) {
    let fact = refusal_fact(w)
        .command
        .and_then(|b| b.page)
        .expect("a page");
    let id = fact.command_id.expect("an id").to_string();
    assert_eq!(fact.fact_origin.map(|o| o.external_id), Some(id));
}

#[then("its causation_id names the BuyInStarted event")]
fn causation_names_trigger(w: &mut Coordinator) {
    let delivered = refusal_fact(w);
    let cover = delivered
        .command
        .as_ref()
        .and_then(|b| b.cover.clone())
        .expect("its stream");
    let page = delivered.command.and_then(|b| b.page).expect("a page");
    let started = w.pages(&cover).iter().position(|p| {
        p.payload
            .as_ref()
            .is_some_and(|a| bare(a) == "BuyInStarted")
    });
    let expected = EventRef {
        stream: Some(cover),
        sequence: started.expect("BuyInStarted") as u64,
    };
    assert_eq!(page.causation_id, Some(expected));
}

#[then(regex = r#"^its correlation_id is "([^"]+)"$"#)]
fn fact_correlation(w: &mut Coordinator, correlated: String) {
    let page = refusal_fact(w)
        .command
        .and_then(|b| b.page)
        .expect("a page");
    assert_eq!(page.correlation_id, Some(root(&correlated)));
}

#[then(regex = r#"^wallet "([\w-]+)"'s stream holds no event with that command_id$"#)]
fn wallet_holds_none(w: &mut Coordinator, wallet: String) {
    let id = refusal_fact(w)
        .command
        .and_then(|b| b.page)
        .and_then(|p| p.command_id);
    let cover = w.cover("wallet", &wallet);
    for page in w.pages(&cover) {
        let causation = page.meta.and_then(|m| m.causation);
        assert_ne!(
            causation,
            id.clone()
                .map(angzarr_embedded::v1::event_meta::Causation::CommandId)
        );
    }
}

#[then(regex = r#"^"(\w+)"'s OnRefused (is not called|is called with the refusal)$"#)]
fn on_refused(w: &mut Coordinator, _name: String, called: String) {
    let refusals = locked(&holds_funds(w).script).refusals.len();
    assert_eq!(refusals, usize::from(called != "is not called"));
}

/// How often HoldFunds was delivered.
fn holds(w: &mut Coordinator) -> usize {
    w.business("wallet").delivered().len()
}

#[then(
    regex = r"^HoldFunds is retried with backoff up to its configured cap, then dead-lettered and alerted$"
)]
fn retried_then_dead_lettered(w: &mut Coordinator) {
    assert_eq!(holds(w), w.retry.max_retries as usize + 1);
    assert_eq!(w.waits.waited().len(), w.retry.max_retries as usize);
    assert!(!w.log.letters().is_empty() && !w.alerts().is_empty());
}

#[then(
    regex = r"^the policy step is reloaded and re-run with backoff up to its configured cap, then dead-lettered and alerted$"
)]
fn rerun_then_dead_lettered(w: &mut Coordinator) {
    retried_then_dead_lettered(w);
}

/// The dead letters of HoldFunds itself: its rejected command.
fn hold_funds_letters(w: &Coordinator) -> Vec<angzarr_embedded::v1::AngzarrDeadLetter> {
    use angzarr_embedded::v1::angzarr_dead_letter::Payload;
    w.log
        .letters()
        .into_iter()
        .filter(|l| match &l.payload {
            Some(Payload::RejectedCommand(book)) => {
                book.cover.as_ref().is_some_and(|c| c.domain == "wallet")
            }
            _ => false,
        })
        .collect()
}

#[then("HoldFunds is neither retried nor dead-lettered")]
fn neither_retried_nor_dead_lettered(w: &mut Coordinator) {
    assert_eq!(holds(w), 1);
    assert!(
        hold_funds_letters(w).is_empty(),
        "{:?}",
        hold_funds_letters(w)
    );
}

#[then("HoldFunds is dead-lettered and an alert is raised")]
fn hold_funds_dead_lettered(w: &mut Coordinator) {
    assert_eq!(holds(w), 1);
    assert!(!w.log.letters().is_empty() && !w.alerts().is_empty());
}

/// The events recorded in `domain` since the first when, but for the
/// trigger the scenario recorded itself.
fn recorded_in(w: &Coordinator, domain: &str) -> Vec<String> {
    w.store_covers(domain)
        .iter()
        .flat_map(|c| w.persisted(c))
        .filter_map(|p| p.payload.as_ref().map(bare))
        .filter(|name| name != "BuyInStarted")
        .collect()
}

#[then(regex = r#"^nothing is recorded in "([\w-]+)"(?: or "([\w-]+)")?$"#)]
fn nothing_recorded(w: &mut Coordinator, first: String, second: String) {
    for domain in [first, second].iter().filter(|d| !d.is_empty()) {
        assert!(
            recorded_in(w, domain).is_empty(),
            "{domain}: {:?}",
            recorded_in(w, domain)
        );
    }
}

#[then(
    regex = r#"^the CommandRefused fact is refused (\w+), naming command_type (\w+), dead-lettered and alerted at run time$"#
)]
fn refusal_fact_refused(w: &mut Coordinator, reason: String, command_type: String) {
    let letters = w.log.letters();
    let letter = letters
        .iter()
        .find(|l| {
            l.reason
                .as_ref()
                .is_some_and(|r| r.code == reason || r.args.values().any(|v| *v == reason))
        })
        .unwrap_or_else(|| panic!("no dead letter of {reason}: {letters:?}"));
    let args = &letter.reason.as_ref().expect("a reason").args;
    assert!(
        args.values().any(|v| v.ends_with(&command_type)),
        "{args:?}"
    );
    assert!(!w.alerts().is_empty());
}

#[then("the refusal is dead-lettered and an alert is raised")]
fn refusal_dead_lettered(w: &mut Coordinator) {
    assert!(
        !w.log.letters().is_empty(),
        "no dead letter; answered {:?}",
        w.answers()
    );
    assert!(!w.alerts().is_empty());
}

#[then(
    regex = r#"^wallet "([\w-]+)"'s HoldFunds handler returns the refusal object InsufficientFunds with available (\d+) and requested (\d+)$"#
)]
fn handler_returns_object(w: &mut Coordinator, _wallet: String, _available: u64, _requested: u64) {
    assert_eq!(holds(w), 1, "the handler ran, and refused");
}

#[then("the refused response the policy receives carries that InsufficientFunds as its refusal and no text")]
fn response_carries_object(w: &mut Coordinator) {
    let script = locked(&holds_funds(w).script);
    let reason = script.refusals[0]
        .response
        .as_ref()
        .and_then(|r| r.reason.clone());
    assert_eq!(reason, Some(Reason::Refusal(insufficient_funds(120, 500))));
}

#[then(
    regex = r"^the CommandRefused fact's refusal unpacks to InsufficientFunds with available (\d+) and requested (\d+)$"
)]
fn unpacks_to(w: &mut Coordinator, available: u64, requested: u64) {
    let (_, refused) = command_refused(w, "buy-in");
    assert_eq!(
        refused.refusal,
        Some(insufficient_funds(available, requested))
    );
}

#[then(
    regex = r#"^"(\w+)"'s callback is called with a refusal response whose diagnostic has code (\w+) and arg wallet "([\w-]+)"$"#
)]
fn callback_with_diagnostic(w: &mut Coordinator, _name: String, code: String, wallet: String) {
    let script = locked(&holds_funds(w).script);
    let reason = script.refusals[0]
        .response
        .as_ref()
        .and_then(|r| r.reason.clone());
    let Some(Reason::Diagnostic(diagnostic)) = reason else {
        panic!("a diagnostic, not {reason:?}");
    };
    assert_eq!(diagnostic.code, code);
    assert_eq!(diagnostic.args.get("wallet"), Some(&wallet));
}

#[then("the refusal response carries no refusal object and no text")]
fn no_object(w: &mut Coordinator) {
    let script = locked(&holds_funds(w).script);
    let reason = script.refusals[0]
        .response
        .as_ref()
        .and_then(|r| r.reason.clone());
    assert!(matches!(reason, Some(Reason::Diagnostic(_))), "{reason:?}");
}

#[then(regex = r#"^"(\w+)" records nothing$"#)]
fn records_nothing(w: &mut Coordinator, name: String) {
    let domain = w.domain_of(&name);
    assert!(
        recorded_in(w, &domain).is_empty(),
        "{:?}",
        recorded_in(w, &domain)
    );
}
