//! Upcasting, through a domain's upcaster sidecar: a scripted
//! `UpcastPort`, registered with `Registry::upcast_with` as a child's
//! sidecar would be, mapping old types to current ones (bytes as they
//! are), or unreachable.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use cucumber::{given, then, when};

use angzarr_embedded::angzarr_diagnostic::Report;
use angzarr_embedded::angzarr_router::error::GrpcCode;
use angzarr_embedded::angzarr_router::send::BoxFuture;
use angzarr_embedded::v1::{command_page, ContextualCommand, UpcastRequest, UpcastResponse};
use angzarr_embedded::{Any, UpcastError, UpcastPort};

use crate::tests::business::Rule;
use crate::tests::policy::{bare, internal, locked};
use crate::tests::world::Coordinator;

/// One domain's sidecar: its rules, old type name to current, and whether
/// it can be reached.
#[derive(Default)]
pub(crate) struct Sidecar {
    pub(crate) rules: BTreeMap<String, String>,
    pub(crate) down: bool,
}

/// A [`Sidecar`] as the kernel's `UpcastPort`.
pub(crate) struct Port(pub(crate) Arc<Mutex<Sidecar>>);

impl Port {
    fn upcast_one(&self, any: &Any) -> Any {
        let sidecar = locked(&self.0);
        let name = bare(any);
        match sidecar.rules.get(&name) {
            Some(current) => Any {
                type_url: any
                    .type_url
                    .replace(&format!(".{name}"), &format!(".{current}")),
                value: any.value.clone(),
            },
            None => any.clone(),
        }
    }
}

impl UpcastPort for Port {
    fn upcast<'a>(
        &'a self,
        request: &'a UpcastRequest,
    ) -> BoxFuture<'a, Result<UpcastResponse, UpcastError>> {
        let down = locked(&self.0).down;
        let answer = match down {
            true => Err(UpcastError::Unavailable {
                cause: Report::new("sidecar-unreachable".to_string()),
            }),
            false => Ok(UpcastResponse {
                events: (request.events.iter())
                    .map(|page| angzarr_embedded::v1::EventPage {
                        payload: page.payload.as_ref().map(|p| self.upcast_one(p)),
                        ..page.clone()
                    })
                    .collect(),
            }),
        };
        Box::pin(std::future::ready(answer))
    }
}

/// The input a delivery hands business code, its type and bytes.
fn handed(delivered: &ContextualCommand) -> (String, Vec<u8>, bool) {
    let page = delivered.command.as_ref().and_then(|b| b.page.as_ref());
    match page.and_then(|p| p.body.as_ref()) {
        Some(command_page::Body::Command(c)) => {
            let p = c.payload.clone().unwrap_or_default();
            (bare(&p), p.value, false)
        }
        Some(command_page::Body::Fact(f)) => {
            let p = f.payload.clone().unwrap_or_default();
            (bare(&p), p.value, true)
        }
        None => (String::new(), Vec::new(), false),
    }
}

#[given(regex = r#"^an "(\w+)" aggregate whose upcaster sidecar upcasts (\w+) to (\w+)$"#)]
fn aggregate_upcasting(w: &mut Coordinator, domain: String, old: String, current: String) {
    w.domain(&domain);
    w.sidecar(&domain).rules.insert(old, current);
}

#[given(
    regex = r#"^the "(\w+)" upcaster sidecar upcasts (\w+) to (\w+)(, which business code does not handle)?$"#
)]
fn sidecar_upcasts(
    w: &mut Coordinator,
    name: String,
    old: String,
    current: String,
    unhandled: String,
) {
    let domain = w.domain_of(&name);
    w.sidecar(&domain).rules.insert(old, current.clone());
    if !unhandled.is_empty() {
        let mut error = internal("ROUTER_REASON_UNKNOWN_COMMAND_TYPE");
        error.grpc = GrpcCode::Internal;
        w.business(&domain).rule(Rule {
            input: current,
            once: None,
            error,
        });
    }
}

#[given(regex = r#"^the "(\w+)" upcaster sidecar is configured but unreachable$"#)]
fn sidecar_down(w: &mut Coordinator, domain: String) {
    w.sidecar(&domain).down = true;
}

#[given(regex = r#"^order "([\w-]+)" has a stored (\w+) event at sequence 0$"#)]
fn stored_old_event(w: &mut Coordinator, order: String, event: String) {
    w.speak_of(&order);
    let cover = w.order();
    w.write(&cover, &[&event]);
}

#[given(regex = r#"^a "(\w+)" process manager whose starting input is (\w+)$"#)]
fn process_manager_starting(w: &mut Coordinator, name: String, starting: String) {
    let domain = name.to_lowercase();
    w.names.insert(name, domain.clone());
    w.process_manager(&domain, &[starting.as_str()]);
}

/// A client's input of the type `name` to `cover`, its payload `content`'s
/// bytes.
fn input_of(
    w: &mut Coordinator,
    fact: bool,
    name: &str,
    content: &str,
) -> angzarr_embedded::v1::CommandBook {
    let cover = w.order();
    let mut book = match fact {
        true => w.fact(&cover, name, "delivery-1"),
        false => w.command(&cover, name),
    };
    let body = book.page.as_mut().and_then(|p| p.body.as_mut());
    let payload = match body {
        Some(command_page::Body::Command(c)) => c.payload.as_mut(),
        Some(command_page::Body::Fact(f)) => f.payload.as_mut(),
        None => None,
    };
    if let Some(payload) = payload {
        payload.value = content.as_bytes().to_vec();
    }
    book
}

#[when(
    regex = r#"^a client sends (the fact )?(\w+) "(\w+)" to order "([\w-]+)"(?: and waits for the answer)?$"#
)]
async fn client_sends(
    w: &mut Coordinator,
    fact: String,
    name: String,
    content: String,
    order: String,
) {
    w.speak_of(&order);
    w.begin();
    let fact = !fact.is_empty();
    if fact {
        let current = w
            .sidecar("order")
            .rules
            .get(&name)
            .cloned()
            .unwrap_or(name.clone());
        w.business("order").declare_fact(&current);
    }
    let book = input_of(w, fact, &name, &content);
    w.handle(book).await;
}

#[when(regex = r"^a client sends (\w+) for a correlation with no instance$")]
async fn sends_without_instance(w: &mut Coordinator, name: String) {
    w.begin();
    let domain = w
        .policies
        .first()
        .map(|p| p.output.clone())
        .unwrap_or_else(|| "fulfillment".to_string());
    let cover = w.cover(&domain, "0192a6b0-0000-7000-8000-0000000000f2");
    let book = w.command(&cover, &name);
    w.handle(book).await;
}

#[then(regex = r"^business code receives an? (\w+) event at sequence (\d+)$")]
fn receives_event(w: &mut Coordinator, event: String, sequence: u64) {
    let delivered = w.business("order").delivered().pop().expect("delivered");
    let pages = delivered.events.map(|e| e.pages).unwrap_or_default();
    let page = pages
        .iter()
        .find(|p| p.sequence == sequence)
        .expect("the event");
    assert_eq!(page.payload.as_ref().map(bare), Some(event));
}

#[then(regex = r#"^order "([\w-]+)"'s stored event at sequence (\d+) is still an? (\w+)$"#)]
fn still_stored_as(w: &mut Coordinator, order: String, sequence: usize, event: String) {
    let cover = w.cover("order", &order);
    let pages = w.pages(&cover);
    assert_eq!(pages[sequence].payload.as_ref().map(bare), Some(event));
}

#[then(regex = r#"^business code is handed an? (\w+)( fact)?(?: for "(\w+)")?$"#)]
fn handed_as(w: &mut Coordinator, name: String, fact: String, content: String) {
    let domain = w
        .handled
        .last()
        .and_then(|(b, _)| b.cover.clone())
        .expect("a stream")
        .domain;
    let delivered = w.business(&domain).delivered().pop().expect("delivered");
    let (got, bytes, is_fact) = handed(&delivered);
    assert_eq!(got, name);
    assert_eq!(is_fact, !fact.is_empty());
    if !content.is_empty() {
        assert_eq!(bytes, content.as_bytes());
    }
}

#[then(regex = r"^business code is never handed an? (\w+)$")]
fn never_handed(w: &mut Coordinator, name: String) {
    for delivered in w.business("order").delivered() {
        assert_ne!(handed(&delivered).0, name);
    }
}

#[then(regex = r"^the (?:command|fact) is accepted$")]
fn accepted(w: &mut Coordinator) {
    let (_, result) = w.handled.last().expect("an input was handled");
    assert!(result.is_ok(), "{:?}", w.answer());
}

#[then(
    regex = r#"^the client is refused (\w+) with reason (\w+), which is also its status message$"#
)]
fn refused_with(w: &mut Coordinator, status_name: String, reason: String) {
    let answer = w.answer();
    assert_eq!(
        answer.status(),
        Some(crate::tests::answer::status(&status_name)),
        "{answer:?}"
    );
    assert_eq!(answer.reason(), Some(reason.as_str()));
}

#[then(regex = r#"^the refusal's error info carries (\w+) "([^"]+)"$"#)]
fn error_info(w: &mut Coordinator, name: String, value: String) {
    assert_eq!(
        w.answer().arg(&name),
        Some(value.as_str()),
        "{:?}",
        w.answer()
    );
}

#[then(regex = r#"^nothing is recorded on order "([\w-]+)"$"#)]
fn nothing_recorded_on(w: &mut Coordinator, order: String) {
    let cover = w.cover("order", &order);
    assert!(w.persisted(&cover).is_empty(), "{:?}", w.persisted(&cover));
}

#[then("the coordinator starts an instance")]
fn starts_an_instance(w: &mut Coordinator) {
    let cover = w
        .handled
        .last()
        .and_then(|(b, _)| b.cover.clone())
        .expect("a stream");
    assert!(!w.persisted(&cover).is_empty(), "the instance's events");
}
