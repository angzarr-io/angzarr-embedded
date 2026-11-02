//! Editions as the kernel meets them: a source event's edition stamped on
//! what a policy sends, a process manager's events in its input's
//! edition, the main timeline's three spellings, and an input to a stream
//! not forked refused. angzarr-embedded forks no stream (no edition
//! lifecycle): a stream a scenario's edition reaches is forked by the
//! given, as angzarr's system layer would have forked it.

use cucumber::{given, then, when};

use angzarr_embedded::v1::{ContextualCommand, Cover};

use crate::tests::policy::{bare, locked};
use crate::tests::world::Coordinator;

/// `cover` in `edition` (`None`: as it is).
fn in_edition(cover: &Cover, edition: Option<&str>) -> Cover {
    Cover {
        edition: edition.map(str::to_string),
        ..cover.clone()
    }
}

/// An edition as a scenario spells it: `""`, `"angzarr"`, a name, or
/// `unset`.
fn spelled(text: &str) -> Option<String> {
    match text {
        "unset" => None,
        quoted => Some(quoted.trim_matches('"').to_string()),
    }
}

/// The edition of a delivery's stream.
fn edition_of(delivered: &ContextualCommand) -> Option<String> {
    delivered
        .command
        .as_ref()
        .and_then(|b| b.cover.as_ref())
        .and_then(|c| c.edition.clone())
        .filter(|e| !e.is_empty())
}

#[given(regex = r#"^the source event has edition "([\w-]+)"$"#)]
fn source_edition(w: &mut Coordinator, edition: String) {
    w.edition = Some(edition);
}

#[given("the source event has no edition set")]
fn source_main(w: &mut Coordinator) {
    w.edition = None;
}

#[given(regex = r#"^the command's cover has edition "([\w-]+)"$"#)]
fn command_edition(w: &mut Coordinator, edition: String) {
    w.edition = Some(edition);
}

#[given(regex = r#"^the PM records an? (\w+) event on a (\w+) command$"#)]
fn pm_records(w: &mut Coordinator, event: String, _command: String) {
    w.business("fulfillment")
        .every(|a| a.events = Some(vec![event]));
}

#[given(regex = r#"^the PM handler sets the edition "([\w-]+)" on its answer's events book$"#)]
fn pm_sets_edition(w: &mut Coordinator, edition: String) {
    w.business("fulfillment")
        .every(|a| a.book_edition = Some(edition));
}

#[given(regex = r#"^the policy handler sets outgoing edition "([\w-]+)"$"#)]
fn policy_sets_edition(w: &mut Coordinator, edition: String) {
    let def = w.policies.last().expect("a policy");
    let output = def.output.clone();
    locked(&def.script)
        .reactions
        .push(crate::tests::policy::Reaction {
            on: "OrderCreated".to_string(),
            domain: output,
            command: "ReserveStock".to_string(),
            root: crate::tests::policy::Root::Named("inv-1".to_string()),
            correlation: None,
            fault: Some(crate::tests::policy::Fault::Edition(edition)),
        });
}

#[then(regex = r#"^no command reaches "([\w-]+)"$"#)]
fn no_command_reaches(w: &mut Coordinator, domain: String) {
    let delivered = w.business(&domain).delivered();
    assert!(delivered.is_empty(), "{delivered:?}");
}

#[when(regex = r#"^a client sends an AddItem command to "([\w-]+)" with edition (unset|"[^"]*")$"#)]
async fn sends_with_edition(w: &mut Coordinator, name: String, edition: String) {
    w.begin();
    let edition = edition
        .strip_prefix('"')
        .and_then(|e| e.strip_suffix('"'))
        .map(str::to_string);
    let cover = Cover {
        edition,
        ..w.cover("order", &name)
    };
    let book = w.command(&cover, "AddItem");
    w.handle(book).await;
}

#[given(regex = r#"^the (\w+) aggregate refuses (\w+)$"#)]
fn aggregate_refuses(w: &mut Coordinator, domain: String, command: String) {
    w.business(&domain).rule(crate::tests::business::Rule {
        input: command,
        once: None,
        error: crate::tests::business::rejection("OUT_OF_STOCK"),
    });
}

#[given(
    regex = r#"^a policy "(\w+)" translating from "([\w-]+)" to "([\w-]+)" generated with (\w+) in pass-through mode$"#
)]
fn pass_through(w: &mut Coordinator, name: String, input: String, output: String, command: String) {
    let script = w.policy(&name, &input, &output);
    locked(&script)
        .reactions
        .push(crate::tests::policy::Reaction {
            on: "OrderCreated".to_string(),
            domain: output,
            command,
            root: crate::tests::policy::Root::Named("inv-1".to_string()),
            correlation: None,
            fault: None,
        });
}

/// Every stream a policy may reach from `source`, forked into the
/// scenario's edition as angzarr's system layer would fork them.
fn fork_reached(w: &mut Coordinator, source: &Cover) {
    let Some(edition) = w.edition.clone() else {
        return;
    };
    w.fork(&in_edition(source, Some(&edition)));
    let reached: Vec<Cover> = w
        .policies
        .iter()
        .flat_map(|p| locked(&p.script).reactions.clone())
        .filter_map(|r| match &r.root {
            crate::tests::policy::Root::Named(name) => Some(w.cover(&r.domain, name)),
            _ => None,
        })
        .collect();
    for cover in reached {
        w.fork(&in_edition(&cover, Some(&edition)));
    }
}

#[when(regex = r"^an OrderCreated event is dispatched to the policy$")]
async fn dispatched_to_policy(w: &mut Coordinator) {
    let def = w.policies.last().expect("a policy");
    if locked(&def.script).reactions.is_empty() {
        let output = def.output.clone();
        locked(&def.script)
            .reactions
            .push(crate::tests::policy::Reaction {
                on: "OrderCreated".to_string(),
                domain: output,
                command: "ReserveStock".to_string(),
                root: crate::tests::policy::Root::Named("inv-1".to_string()),
                correlation: None,
                fault: None,
            });
    }
    w.begin();
    let main = w.cover("order", "order-1");
    fork_reached(w, &main);
    let cover = in_edition(&main, w.edition.clone().as_deref());
    w.record(&cover, "OrderCreated", None).await;
}

#[when(regex = r"^(?:the|a) StartFulfillment command is delivered to the PM$")]
async fn delivered_to_pm(w: &mut Coordinator) {
    w.begin();
    let main = w.cover("fulfillment", "0192a6b0-0000-7000-8000-0000000000f1");
    fork_reached(w, &main);
    let cover = in_edition(&main, w.edition.clone().as_deref());
    let book = w.command(&cover, "StartFulfillment");
    w.handle(book).await;
}

#[then(
    regex = r#"^the (?:emitted|persisted) command's cover has (?:edition "([\w-]+)"|no edition set)$"#
)]
fn command_cover_edition(w: &mut Coordinator, edition: String) {
    let output = w.policies.last().expect("a policy").output.clone();
    let delivered = w
        .business(&output)
        .delivered()
        .pop()
        .expect("the command delivered");
    let expected = (!edition.is_empty()).then_some(edition);
    assert_eq!(edition_of(&delivered), expected);
}

#[then(regex = r#"^every events book the PM answers has edition "([\w-]+)"$"#)]
fn pm_events_in_edition(w: &mut Coordinator, edition: String) {
    let main = w.cover("fulfillment", "0192a6b0-0000-7000-8000-0000000000f1");
    let forked = in_edition(&main, Some(&edition));
    let own: Vec<_> = w
        .persisted(&forked)
        .into_iter()
        .filter(|p| {
            p.payload
                .as_ref()
                .is_some_and(|a| !a.type_url.contains("io.angzarr.v1.meta"))
        })
        .collect();
    assert!(!own.is_empty(), "its events in {edition}");
    assert!(w.persisted(&main).is_empty(), "none on main");
}

#[then("no event is persisted")]
fn no_event(w: &mut Coordinator) {
    for domain in w.domains.keys().cloned().collect::<Vec<_>>() {
        for cover in w.store_covers(&domain) {
            let own: Vec<_> = w
                .persisted(&cover)
                .into_iter()
                .filter(|p| {
                    p.payload
                        .as_ref()
                        .is_some_and(|a| !a.type_url.contains("io.angzarr.v1.meta"))
                })
                .collect();
            assert!(own.is_empty(), "{cover:?}: {own:?}");
        }
    }
}

#[then("the command is refused INTERNAL")]
fn refused_internal(w: &mut Coordinator) {
    let status = w.answer().status();
    assert_eq!(
        status,
        Some(crate::tests::answer::status("INTERNAL")),
        "{:?}",
        w.answer()
    );
}

#[then(
    regex = r#"^the command is refused INTERNAL with reason (\w+), naming the stream and the event stream in edition "([\w-]+)"$"#
)]
fn refused_naming_streams(w: &mut Coordinator, reason: String, edition: String) {
    refused_internal(w);
    let letters: Vec<_> = w
        .log
        .letters()
        .into_iter()
        .filter_map(|l| l.reason)
        .filter(|r| r.code == reason)
        .collect();
    assert_eq!(letters.len(), 1, "one dead letter of {reason}: {letters:?}");
    let args = &letters[0].args;
    let stream = args.get("stream").expect("the input's stream");
    let event_stream = args.get("event_stream").expect("the events' stream");
    assert!(stream.starts_with("fulfillment/"), "{stream}");
    assert_eq!(
        event_stream,
        &format!("{}@{edition}", stream.split('@').next().unwrap_or(stream))
    );
    assert_ne!(stream, event_stream);
}

#[then(regex = r#"^the refusal fact's cover has edition "([\w-]+)"$"#)]
fn refusal_fact_edition(w: &mut Coordinator, edition: String) {
    let input = w.policies.last().expect("a policy").input.clone();
    let facts: Vec<_> = w
        .business(&input)
        .delivered()
        .into_iter()
        .filter(|d| d.command.as_ref().and_then(|b| b.page.as_ref()).is_some_and(|p| {
            matches!(&p.body, Some(angzarr_embedded::v1::command_page::Body::Fact(f)) if f.payload.as_ref().map(bare).as_deref() == Some("CommandRefused"))
        }))
        .collect();
    assert_eq!(facts.len(), 1, "{facts:?}");
    assert_eq!(edition_of(&facts[0]), Some(edition));
}

#[given(
    regex = r#"^an "order" aggregate "([\w-]+)" with (\d+) events written with edition (unset|"[\w-]*")$"#
)]
fn written_with_edition(w: &mut Coordinator, name: String, count: usize, written: String) {
    let cover = in_edition(&w.cover("order", &name), spelled(&written).as_deref());
    let names = vec!["ItemAdded"; count];
    w.write(&cover, &names);
}

#[when(regex = r#"^the coordinator loads "([\w-]+)" with edition (unset|"[\w-]*")$"#)]
async fn loads_with_edition(w: &mut Coordinator, name: String, read: String) {
    w.begin();
    let cover = in_edition(&w.cover("order", &name), spelled(&read).as_deref());
    let book = w.command(&cover, "AddItem");
    w.handle(book).await;
}

#[then(regex = r"^the loaded history has (\d+) events$")]
fn loaded_history(w: &mut Coordinator, count: usize) {
    let delivered = w.business("order").delivered().pop().expect("loaded");
    assert_eq!(delivered.events.map_or(0, |e| e.pages.len()), count);
}

#[then(regex = r"^next_sequence is (\d+)$")]
fn next_sequence(w: &mut Coordinator, head: u64) {
    let delivered = w.business("order").delivered().pop().expect("loaded");
    assert_eq!(delivered.events.map_or(0, |e| e.next_sequence), head);
}

#[given(regex = r#"^an? "([\w-]+)" aggregate "([\w-]+)" with (\d+) events on the main timeline$"#)]
fn with_events_on_main(w: &mut Coordinator, domain: String, name: String, count: usize) {
    let cover = w.cover(&domain, &name);
    let names = vec!["Happened"; count];
    w.write(&cover, &names);
}

#[given(regex = r#"^"([\w-]+)" "([\w-]+)" is forked into edition "([\w-]+)"$"#)]
fn forked_into(w: &mut Coordinator, domain: String, name: String, edition: String) {
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    w.fork(&cover);
}

#[given(
    regex = r#"^"([\w-]+)" "([\w-]+)" with (\d+) events on the main timeline has no fork in edition "[\w-]+"$"#
)]
fn unforked(w: &mut Coordinator, domain: String, name: String, count: usize) {
    with_events_on_main(w, domain, name, count);
}

#[when(
    regex = r#"^a client sends a (command|fact) to "([\w-]+)" "([\w-]+)" in edition "([\w-]+)"$"#
)]
async fn sends_in_edition(
    w: &mut Coordinator,
    kind: String,
    domain: String,
    name: String,
    edition: String,
) {
    w.begin();
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let book = match kind.as_str() {
        "fact" => w.fact(&cover, "SeatReserved", "seat-1"),
        _ => w.command(&cover, "SeatPlayer"),
    };
    w.handle(book).await;
}

#[then("the router is never called")]
fn router_never_called(w: &mut Coordinator) {
    for domain in w.domains.keys().cloned().collect::<Vec<_>>() {
        let delivered = w.business(&domain).delivered();
        assert!(delivered.is_empty(), "{domain}: {delivered:?}");
    }
}

#[then(regex = r#"^"([\w-]+)" "([\w-]+)" holds no event in edition "([\w-]+)"$"#)]
fn holds_no_event_in(w: &mut Coordinator, domain: String, name: String, edition: String) {
    let cover = in_edition(&w.cover(&domain, &name), Some(&edition));
    let main = w.cover(&domain, &name);
    assert_eq!(w.pages(&cover), w.pages(&main), "it reads as main's");
}
