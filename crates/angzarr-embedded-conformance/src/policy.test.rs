//! A policy's business process, as its coordinator sees it: a scripted
//! angzarr-router `PolicyComponent`, registered with
//! `Registry::policy_component` as a child's would be. It answers each
//! source book with the commands its reactions name, as business code
//! returns them, unchecked and unstamped, and answers a refusal with the
//! fact its translating callback makes, or none (pass-through).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use angzarr_embedded::angzarr_router::context::CommandContext;
use angzarr_embedded::angzarr_router::error::{CodedError, GrpcCode};
use angzarr_embedded::angzarr_router::router::PolicyComponent;
use angzarr_embedded::markup::{decode_message, DomainMessage};
use angzarr_embedded::v1::{
    command_page, Command, CommandBook, CommandPage, CommandRefused, Cover, EventBook, EventRef,
    Fact, FactOrigin, Handling, PolicyReaction, PolicyResponse, Refusal, Uuid,
};
use angzarr_embedded::{type_name, Any, WireError};

use crate::tests::world::{package, root as root_of};

/// Where a reaction's command is addressed.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Root {
    /// The root a scenario names.
    Named(String),
    /// The trigger's correlation, as a policy to a process manager may.
    TriggerCorrelation,
    /// Another root than the trigger's, a new one for each trigger: its
    /// root's text with `+`, named. A policy never addresses its trigger's
    /// own stream (C-0784).
    Another,
    /// None at all: a policy bug.
    Unnamed,
}

/// What a reaction's command carries that the coordinator forbids.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Fault {
    /// A command id other than the stamped one.
    CommandId,
    /// A causation other than the trigger.
    CausationId,
    /// A fact origin of its own, on either arm: the coordinator stamps a
    /// fact's.
    FactOrigin,
    /// An expected sequence.
    Handling(u64),
    /// An edition of its own.
    Edition(String),
}

/// One reaction: on an event of type `on`, a command (or, `fact`, a fact)
/// of type `command` to `domain`, at `root`.
#[derive(Clone, Debug)]
pub(crate) struct Reaction {
    pub(crate) on: String,
    pub(crate) domain: String,
    pub(crate) command: String,
    /// The Fact arm, for a type its destination declares a fact.
    pub(crate) fact: bool,
    pub(crate) root: Root,
    /// A correlation of its own, unlike the trigger's.
    pub(crate) correlation: Option<String>,
    pub(crate) fault: Option<Fault>,
}

/// What the policy's translating callback answers.
#[derive(Clone, Debug)]
pub(crate) enum Translation {
    /// The fact of this type in the source domain, payload only.
    Fact(String),
    /// A failure.
    Fails(CodedError),
}

/// A policy's script, and what it was handed.
#[derive(Default)]
pub(crate) struct Script {
    pub(crate) reactions: Vec<Reaction>,
    /// Its translating callback; none: pass-through.
    pub(crate) translation: Option<Translation>,
    /// Every handled source failing so.
    pub(crate) fails: Option<CodedError>,
    /// Every source book it was handed.
    pub(crate) sources: Vec<EventBook>,
    /// Every refusal it was handed.
    pub(crate) refusals: Vec<Refusal>,
}

/// One policy.
pub(crate) struct Policy {
    pub(crate) name: String,
    pub(crate) input: String,
    pub(crate) script: Arc<Mutex<Script>>,
}

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The bare type name of a payload: `OrderPlaced` for
/// `/orders.v1.OrderPlaced`.
pub(crate) fn bare(any: &Any) -> String {
    type_name(&any.type_url)
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// `name`'s payload in `domain`'s package, empty.
pub(crate) fn payload(domain: &str, name: &str) -> Any {
    Any {
        type_url: format!("/{}{name}", package(domain)),
        value: Vec::new(),
    }
}

impl Reaction {
    /// The command business code returns for `page` of `source`.
    fn command(&self, source: &EventBook, page: &angzarr_embedded::v1::EventPage) -> CommandBook {
        let meta = page.meta.clone().unwrap_or_default();
        let root = match &self.root {
            Root::Named(name) => Some(root_of(name)),
            Root::TriggerCorrelation => meta.correlation_id.clone(),
            Root::Another => (source.cover.as_ref())
                .and_then(|c| c.root.as_ref())
                .map(|trigger| root_of(&format!("{trigger}+"))),
            Root::Unnamed => None,
        };
        let payload = payload(&self.domain, &self.command);
        let body = match self.fact {
            true => command_page::Body::Fact(Fact {
                payload: Some(payload),
            }),
            false => command_page::Body::Command(Command {
                payload: Some(payload),
            }),
        };
        let causation = (self.fault == Some(Fault::CausationId)).then(|| EventRef {
            stream: Some(Cover {
                domain: "elsewhere".to_string(),
                root: Some(Uuid::from_bytes([0x22; 16])),
                edition: None,
            }),
            sequence: 0,
        });
        let handling = match self.fault {
            Some(Fault::Handling(expected)) => Some(Handling {
                expected_sequence: Some(expected),
                conflict_resolution: 0,
            }),
            _ => None,
        };
        let fact_origin = self.fault == Some(Fault::FactOrigin);
        let edition = match &self.fault {
            Some(Fault::Edition(edition)) => Some(edition.clone()),
            _ => None,
        };
        CommandBook {
            cover: Some(Cover {
                domain: self.domain.clone(),
                root,
                edition,
            }),
            page: Some(CommandPage {
                body: Some(body),
                correlation_id: self.correlation.as_deref().map(root_of),
                command_id: (self.fault == Some(Fault::CommandId))
                    .then(|| Uuid::from_bytes([0x11; 16])),
                causation_id: causation,
                fact_origin: fact_origin.then(|| FactOrigin {
                    external_id: "e-1".to_string(),
                    description: "a policy's".to_string(),
                }),
            }),
            handling,
        }
    }
}

impl PolicyComponent for Policy {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_domain(&self) -> &str {
        &self.input
    }

    async fn dispatch(&self, source: &EventBook) -> Result<PolicyResponse, CodedError> {
        let mut script = locked(&self.script);
        script.sources.push(source.clone());
        if let Some(fails) = &script.fails {
            return Err(fails.clone());
        }
        let reactions = source
            .pages
            .iter()
            .filter_map(|page| {
                let name = bare(page.payload.as_ref()?);
                let outputs: Vec<CommandBook> = (script.reactions.iter())
                    .filter(|r| r.on == name)
                    .map(|r| r.command(source, page))
                    .collect();
                (!outputs.is_empty()).then_some(PolicyReaction {
                    trigger_sequence: page.sequence,
                    outputs,
                })
            })
            .collect();
        Ok(PolicyResponse { reactions })
    }

    async fn dispatch_refusal(
        &self,
        refusal: &Refusal,
        _ctx: &CommandContext,
    ) -> Result<Option<Any>, CodedError> {
        let mut script = locked(&self.script);
        script.refusals.push(refusal.clone());
        match &script.translation {
            None => Ok(None),
            Some(Translation::Fact(name)) => Ok(Some(payload(&self.input, name))),
            Some(Translation::Fails(error)) => Err(error.clone()),
        }
    }
}

/// A CommandRefused, as a refusal fact's payload decodes.
#[derive(Clone)]
struct Refused(CommandRefused);

impl DomainMessage for Refused {
    type Wire = CommandRefused;

    fn to_wire(&self) -> CommandRefused {
        self.0.clone()
    }

    fn from_wire(wire: CommandRefused) -> Result<Refused, WireError> {
        Ok(Refused(wire))
    }
}

/// `any` decoded as the CommandRefused it is, if it is one.
pub(crate) fn command_refused_in(any: &Any) -> Option<CommandRefused> {
    decode_message::<Refused>(any).ok().flatten().map(|r| r.0)
}

/// A policy bug of `code`, INTERNAL.
pub(crate) fn internal(code: &str) -> CodedError {
    CodedError {
        code: code.to_string(),
        extras: BTreeMap::new(),
        grpc: GrpcCode::Internal,
        refusal: None,
    }
}
