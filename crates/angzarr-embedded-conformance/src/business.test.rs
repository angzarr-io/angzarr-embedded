//! The business process behind a domain, as the coordinator sees it: a
//! scripted angzarr-router `CommandHandler`, registered with
//! `Registry::command_handler` as a child's would be. It keeps every
//! `ContextualCommand` it is delivered and answers each as its script says:
//! by default as a well-behaved client library would, one event per input,
//! the state returned with it, the conflict report of a handler that
//! examined its window.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use angzarr_embedded::angzarr_router::error::{CodedError, GrpcCode};
use angzarr_embedded::angzarr_router::router::CommandHandler;
use angzarr_embedded::testing::MemoryStore;
use angzarr_embedded::v1::{
    command_page, CommandHandlerResponse, ConflictReport, ConflictResolution, ContextualCommand,
    Cover, EventBook, EventPage, Snapshot,
};
use angzarr_embedded::{type_name, Any, EventStore, StreamRef, Write};

/// What a handler reports of its conflicts with its events.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Report {
    /// As a handler that examined its window does: under AGGREGATE_HANDLES
    /// the nonce it was delivered, under COMMUTATIVE no unit changed.
    #[default]
    Examined,
    /// No report at all.
    Nothing,
    /// AGGREGATE_HANDLES: this nonce, as text the scenario named.
    Nonce(String),
    /// AGGREGATE_HANDLES: the first delivery's nonce.
    FirstNonce,
    /// COMMUTATIVE: the units the window changed and the command changed.
    Units(Vec<String>, Vec<String>),
}

/// Another write that lands while a delivery is decided: after the
/// stream was loaded, before the command is written.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Race {
    /// On the delivery's own stream, at its head.
    SameStream,
    /// On another root of the same domain.
    OtherRoot,
}

/// How one delivery is answered.
#[derive(Clone, Debug, Default)]
pub(crate) struct Answer {
    pub(crate) report: Report,
    /// Refused, with this error, instead of answered.
    pub(crate) refuse: Option<CodedError>,
    pub(crate) race: Option<Race>,
    /// The rules version reported, if not the registered one.
    pub(crate) rules_version: Option<String>,
}

/// A business refusal under `code`, FAILED_PRECONDITION.
pub(crate) fn rejection(code: &str) -> CodedError {
    CodedError {
        code: code.to_string(),
        extras: BTreeMap::new(),
        grpc: GrpcCode::FailedPrecondition,
        refusal: None,
    }
}

/// The script: an answer for every delivery, and for some by number.
#[derive(Default)]
struct Script {
    every: Answer,
    nth: BTreeMap<usize, Answer>,
}

/// One domain's business process.
pub(crate) struct Business {
    /// Its domain's type URL package: `/orders.v1.` for `order`.
    package: String,
    rules_version: String,
    state_type: String,
    /// The stream it lands races on.
    store: MemoryStore,
    /// The fact types it has a handler for.
    facts: Mutex<BTreeSet<String>>,
    script: Mutex<Script>,
    seen: Mutex<Vec<ContextualCommand>>,
    /// The name a scenario gives the nonce the coordinator delivers.
    alias: Mutex<Option<String>>,
    /// The deliveries made before the scenario's first when.
    before: Mutex<usize>,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Business {
    pub(crate) fn new(
        package: &str,
        rules_version: &str,
        state_type: &str,
        store: &MemoryStore,
    ) -> Business {
        Business {
            package: package.to_string(),
            rules_version: rules_version.to_string(),
            state_type: state_type.to_string(),
            store: store.clone(),
            facts: Mutex::default(),
            script: Mutex::default(),
            seen: Mutex::default(),
            alias: Mutex::default(),
            before: Mutex::default(),
        }
    }

    /// Its handler for the fact `name` (a bare name: `ShipmentDispatched`).
    pub(crate) fn declare_fact(&self, name: &str) {
        locked(&self.facts).insert(name.to_string());
    }

    /// `name` is what the scenario calls the nonce the coordinator
    /// delivers: a report of it is a report of the nonce delivered.
    pub(crate) fn name_nonce(&self, name: &str) {
        *locked(&self.alias) = Some(name.to_string());
    }

    /// Every delivery answered as `edit` makes `answer`.
    pub(crate) fn every(&self, edit: impl FnOnce(&mut Answer)) {
        edit(&mut locked(&self.script).every);
    }

    /// The `n`th delivery (from 1) answered as `edit` makes it, from the
    /// answer for every delivery.
    pub(crate) fn nth(&self, n: usize, edit: impl FnOnce(&mut Answer)) {
        let mut script = locked(&self.script);
        let mut answer = script
            .nth
            .get(&n)
            .cloned()
            .unwrap_or_else(|| script.every.clone());
        edit(&mut answer);
        script.nth.insert(n, answer);
    }

    /// The scenario's first when begins: what was delivered before it was
    /// the givens'.
    pub(crate) fn begin(&self) {
        *locked(&self.before) = locked(&self.seen).len();
    }

    /// Every delivery since the first when, in order.
    pub(crate) fn delivered(&self) -> Vec<ContextualCommand> {
        let before = *locked(&self.before);
        locked(&self.seen)[before..].to_vec()
    }

    /// The answer for the delivery just kept as the `n`th.
    fn answer(&self, n: usize) -> Answer {
        let script = locked(&self.script);
        script
            .nth
            .get(&n)
            .cloned()
            .unwrap_or_else(|| script.every.clone())
    }

    /// The type URL of `name` in this domain's package.
    pub(crate) fn type_url(&self, name: &str) -> String {
        format!("/{}{name}", self.package)
    }

    /// Another write on `stream`'s domain while a delivery is decided.
    async fn land(&self, stream: &Cover, race: Race) {
        let stream = match race {
            Race::SameStream => stream.clone(),
            Race::OtherRoot => Cover {
                root: Some(angzarr_embedded::v1::Uuid::from_bytes([0xee; 16])),
                ..stream.clone()
            },
        };
        let at = StreamRef::try_from(&stream).expect("a stream");
        let head = self
            .store
            .read(at, 0, None)
            .await
            .expect("read")
            .next_sequence;
        let page = EventPage {
            sequence: head,
            payload: Some(Any {
                type_url: self.type_url("OrderNoteAdded"),
                value: Vec::new(),
            }),
            ..EventPage::default()
        };
        self.store
            .write(Write::append(at, head, &[page]))
            .await
            .expect("another write lands");
    }

    /// What `delivered`'s handler records: one event, named for its input.
    fn event(&self, delivered: &ContextualCommand) -> Result<Any, CodedError> {
        let page = delivered
            .command
            .as_ref()
            .and_then(|book| book.page.as_ref());
        let (is_fact, payload) = match page.and_then(|page| page.body.as_ref()) {
            Some(command_page::Body::Fact(fact)) => (true, fact.payload.clone()),
            Some(command_page::Body::Command(command)) => (false, command.payload.clone()),
            None => (false, None),
        };
        let input = payload.unwrap_or_default();
        let name = type_name(&input.type_url)
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .to_string();
        if is_fact && !locked(&self.facts).contains(&name) {
            return Err(CodedError {
                code: "ROUTER_REASON_NO_FACT_HANDLER".to_string(),
                extras: BTreeMap::from([("type_url".to_string(), input.type_url.clone())]),
                grpc: GrpcCode::Internal,
                refusal: None,
            });
        }
        Ok(Any {
            type_url: self.type_url(recorded(&name)),
            value: Vec::new(),
        })
    }

    /// The report `report` makes for `delivered`.
    fn report(&self, report: &Report, delivered: &ContextualCommand) -> Option<ConflictReport> {
        let resolution = delivered
            .command
            .as_ref()
            .and_then(|book| book.handling.as_ref())
            .map_or(ConflictResolution::Strict, |h| h.conflict_resolution());
        let nonce = |nonce: &[u8]| ConflictReport {
            resolved_nonce: nonce.to_vec().into(),
            ..ConflictReport::default()
        };
        match report {
            Report::Examined if resolution == ConflictResolution::AggregateHandles => {
                Some(nonce(&delivered.conflict_nonce))
            }
            Report::Examined | Report::Nothing => None,
            Report::Nonce(name) if locked(&self.alias).as_ref() == Some(name) => {
                Some(nonce(&delivered.conflict_nonce))
            }
            Report::Nonce(name) => Some(nonce(name.as_bytes())),
            Report::FirstNonce => Some(nonce(&self.delivered()[0].conflict_nonce)),
            Report::Units(window, command) => Some(ConflictReport {
                window_units: window.clone(),
                command_units: command.clone(),
                ..ConflictReport::default()
            }),
        }
    }
}

/// The event an input of `name` records: `AddItem` records `ItemAdded`;
/// a fact records itself.
fn recorded(name: &str) -> &str {
    match name {
        "AddItem" => "ItemAdded",
        "CreateOrder" => "OrderCreated",
        "ChangeShippingAddress" => "ShippingAddressChanged",
        other => other,
    }
}

/// A [`Business`] as the router's `CommandHandler`.
#[derive(Clone)]
pub(crate) struct Process(pub(crate) Arc<Business>);

impl CommandHandler for Process {
    fn name(&self) -> &str {
        "business"
    }

    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    async fn dispatch(
        &self,
        delivered: &ContextualCommand,
    ) -> Result<CommandHandlerResponse, CodedError> {
        let business = &self.0;
        let n = {
            let mut seen = locked(&business.seen);
            seen.push(delivered.clone());
            seen.len()
        };
        let answer = business.answer(n);
        let stream = delivered
            .events
            .as_ref()
            .and_then(|events| events.cover.clone())
            .unwrap_or_default();
        if let Some(race) = answer.race {
            business.land(&stream, race).await;
        }
        if let Some(refusal) = answer.refuse {
            return Err(refusal);
        }
        let event = business.event(delivered)?;
        Ok(CommandHandlerResponse {
            conflict: business.report(&answer.report, delivered),
            events: Some(EventBook {
                snapshot: Some(Snapshot {
                    state: Some(Any {
                        type_url: business.state_type.clone(),
                        value: n.to_be_bytes().to_vec(),
                    }),
                    ..Snapshot::default()
                }),
                pages: vec![EventPage {
                    payload: Some(event),
                    ..EventPage::default()
                }],
                ..EventBook::default()
            }),
            rules_version: answer
                .rules_version
                .unwrap_or_else(|| business.rules_version.clone()),
        })
    }
}
