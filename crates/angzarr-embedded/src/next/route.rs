//! Where the kernel meets angzarr-router: the router owns business
//! dispatch (handlers, typed refusals and the refusal path, the
//! deduplication decision, a process manager's start rule, a policy's
//! commands kept in its trigger's edition, their root and correlation);
//! this module turns what the router answers into the kernel's terms.
//!
//! - [`REASONS`]: every reason the router refuses with, each forwarded under
//!   an angzarr-embedded code of its own with its own message
//!   ([`KernelError::Router`](super::KernelError::Router)).
//! - [`Rejected`]: a domain's refusal, as the caller receives it: the
//!   refusing handler's code and args, and its typed refusal object.
//! - The table a typed [`Domain`] is dispatched by: the router's
//!   `DomainDispatch` over the domain's state, folding with the domain's
//!   own `apply` under the rules each event names, restoring snapshots and
//!   resolving COMMUTATIVE by the state's conflict units; the domain's
//!   handlers are registered on it.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex};

use angzarr_embedded_diagnostic::{Arg, Diagnostic, Report};
use angzarr_proto::v1::ConflictResolution;
use angzarr_router::domain::DomainDispatch;
use angzarr_router::error::{CodedError, GrpcCode, HandlerError};
use angzarr_router::rebuild::{FoldFailure, Rebuilder};
use prost::Message;
use prost_types::{Any, Timestamp};

use super::domain::{Domain, State};
use super::error::UpcastError;
use super::payload::{type_name, Payload};
use super::upcast::Upcaster;
use super::version::Rulebook;

/// Every reason angzarr-router refuses with (its `error::codes`), and the
/// angzarr-embedded code it is forwarded under: `angzarr-embedded-next-`
/// and the reason in kebab-case. The catalog check holds this list to the
/// router's own.
pub(crate) const REASONS: &[(&str, &str)] = &[
    (
        "ANY_DECODE_FAILED",
        "angzarr-embedded-next-any-decode-failed",
    ),
    (
        "NO_HANDLER_REGISTERED",
        "angzarr-embedded-next-no-handler-registered",
    ),
    (
        "MISSING_COMMAND_BOOK",
        "angzarr-embedded-next-missing-command-book",
    ),
    (
        "MISSING_COMMAND_PAGE",
        "angzarr-embedded-next-missing-command-page",
    ),
    (
        "MISSING_COMMAND_PAYLOAD",
        "angzarr-embedded-next-missing-command-payload",
    ),
    (
        "PERSISTED_EVENT_CORRUPT",
        "angzarr-embedded-next-persisted-event-corrupt",
    ),
    (
        "UNHANDLED_HANDLER_ERROR",
        "angzarr-embedded-next-unhandled-handler-error",
    ),
    (
        "MISSING_EVENT_BOOK_COVER",
        "angzarr-embedded-next-missing-event-book-cover",
    ),
    (
        "EMPTY_POLICY_SOURCE",
        "angzarr-embedded-next-empty-policy-source",
    ),
    (
        "DUPLICATE_REGISTRATION",
        "angzarr-embedded-next-duplicate-registration",
    ),
    (
        "NO_HANDLERS_REGISTERED",
        "angzarr-embedded-next-no-handlers-registered",
    ),
    (
        "DUPLICATE_COMMAND_HANDLER",
        "angzarr-embedded-next-duplicate-command-handler",
    ),
    ("NO_FACT_HANDLER", "angzarr-embedded-next-no-fact-handler"),
    (
        "CONFLICT_UNRESOLVED",
        "angzarr-embedded-next-conflict-unresolved",
    ),
    ("CONFLICT_OVERLAP", "angzarr-embedded-next-conflict-overlap"),
    (
        "CONFLICT_RESOLUTION_UNSUPPORTED",
        "angzarr-embedded-next-conflict-resolution-unsupported",
    ),
    (
        "MISSING_RULES_VERSION",
        "angzarr-embedded-next-missing-rules-version",
    ),
    (
        "MISSING_STATE_TYPE",
        "angzarr-embedded-next-missing-state-type",
    ),
    (
        "POLICY_COMMAND_OUTSIDE_DESTINATION",
        "angzarr-embedded-next-policy-command-outside-destination",
    ),
    (
        "POLICY_COMMAND_OUTSIDE_EDITION",
        "angzarr-embedded-next-policy-command-outside-edition",
    ),
    (
        "POLICY_COMMAND_NO_ROOT",
        "angzarr-embedded-next-policy-command-no-root",
    ),
    (
        "POLICY_COMMAND_OTHER_CORRELATION",
        "angzarr-embedded-next-policy-command-other-correlation",
    ),
    (
        "MALFORMED_REFUSAL_FACT",
        "angzarr-embedded-next-malformed-refusal-fact",
    ),
    ("PM_NO_INSTANCE", "angzarr-embedded-next-pm-no-instance"),
    (
        "REFUSAL_FACT_WRONG_STREAM",
        "angzarr-embedded-next-refusal-fact-wrong-stream",
    ),
    (
        "STARTING_TYPES_ON_NON_PM",
        "angzarr-embedded-next-starting-types-on-non-pm",
    ),
];

/// The angzarr-embedded code a router reason is forwarded under; `None`
/// for a reason that is not the router's (a domain's own).
pub(crate) fn reason_code(reason: &str) -> Option<&'static str> {
    REASONS
        .iter()
        .find(|(known, _)| *known == reason)
        .map(|(_, code)| *code)
}

/// The router's metadata keys (its `error::extras`), which a forwarded
/// error carries as its args.
const EXTRAS: &[&str] = &[
    angzarr_router::error::extras::DOMAIN,
    angzarr_router::error::extras::TYPE_URL,
    angzarr_router::error::extras::COMMAND_TYPE,
    angzarr_router::error::extras::UPCAST_TYPE_URL,
    angzarr_router::error::extras::WINDOW_UNITS,
    angzarr_router::error::extras::COMMAND_UNITS,
    angzarr_router::error::extras::SEQUENCE,
    angzarr_router::error::extras::STATE_TYPE_URL,
    angzarr_router::error::extras::COMPONENT,
    angzarr_router::error::extras::COMPONENT_KIND,
    angzarr_router::error::extras::EXPECTED_DOMAIN,
    angzarr_router::error::extras::ACTUAL_DOMAIN,
    angzarr_router::error::extras::EXPECTED_EDITION,
    angzarr_router::error::extras::ACTUAL_EDITION,
    angzarr_router::error::extras::REASON,
    angzarr_router::error::extras::FIRST_SEQUENCE,
    angzarr_router::error::extras::LAST_SEQUENCE,
    angzarr_router::error::extras::FOLD_MESSAGE,
];

/// `error`'s metadata as args, by the router's own key names.
pub(crate) fn router_args(error: &CodedError) -> Vec<(&'static str, Arg)> {
    error
        .extras
        .iter()
        .filter_map(|(key, value)| {
            let name = EXTRAS.iter().find(|known| **known == key.as_str())?;
            Some((*name, Arg::Str(value.clone())))
        })
        .collect()
}

/// The code of a refusal a domain's handler made, kept for the program's
/// life: each distinct code (and arg name) once. A program refuses with a
/// fixed set of codes, so the set is bounded.
fn intern(text: &str) -> &'static str {
    static HELD: Mutex<BTreeSet<&'static str>> = Mutex::new(BTreeSet::new());
    let mut held = HELD
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(found) = held.get(text) {
        return found;
    }
    let leaked: &'static str = Box::leak(text.to_string().into_boxed_str());
    held.insert(leaked);
    leaked
}

/// The router's code for a refusal past a decision's step limit
/// ([`STEP_LIMIT`](super::markup::STEP_LIMIT)): the kernel's
/// [`KernelError::TooManySteps`](super::KernelError::TooManySteps).
pub(crate) const TOO_MANY_STEPS: &str = "ANGZARR_EMBEDDED_TOO_MANY_STEPS";

/// A domain's refusal of a command, as its caller receives it (angzarr's
/// business rejection, FAILED_PRECONDITION): the code and args of the
/// refusal the handler made (or the router's, a process manager's
/// `PM_NO_INSTANCE`), and the typed refusal object it carries, if any.
/// The refusing domain recorded nothing.
#[derive(Clone, PartialEq, Debug)]
pub struct Rejected {
    report: Report,
    refusal: Option<Any>,
}

impl Rejected {
    /// The router's rejection, in the kernel's terms.
    pub(crate) fn of(error: &CodedError) -> Rejected {
        let report = match reason_code(&error.code) {
            Some(code) => router_args(error).into_iter().fold(
                Report::new(code, error.message.clone()),
                |report, (name, arg)| report.arg(name, arg),
            ),
            None => error.extras.iter().fold(
                Report::new(intern(&error.code), error.message.clone()),
                |report, (name, value)| report.arg(intern(name), value.as_str()),
            ),
        };
        Rejected {
            report,
            refusal: error.refusal.as_deref().cloned(),
        }
    }

    /// The refusal as a report: its code, args and English.
    pub fn report(&self) -> &Report {
        &self.report
    }

    /// The typed refusal object, packed, if the handler refused with one.
    pub fn refusal_object(&self) -> Option<&Any> {
        self.refusal.as_ref()
    }

    /// The typed refusal object, as `T`, if the handler refused with one
    /// and it is a `T`.
    pub fn refusal<T: super::markup::DomainMessage>(&self) -> Option<T> {
        super::markup::__decode(self.refusal.as_ref()?)
            .ok()
            .flatten()
    }
}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.report.fmt(f)
    }
}

impl Diagnostic for Rejected {
    fn code(&self) -> &'static str {
        self.report.code()
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        self.report.args()
    }
}

/// Whether the router's `error` is a business refusal: FAILED_PRECONDITION,
/// under a domain's own code or the router's (a process manager with no
/// instance). Under any other status it is a fault, a domain's own code
/// included: errors are defects unless deliberately refused.
pub(crate) fn is_rejection(error: &CodedError) -> bool {
    error.grpc == GrpcCode::FailedPrecondition
}

/// Whether the router's `error`, which the kernel keeps as
/// [`KernelError::Router`](super::KernelError::Router) (never a refusal:
/// that is [`Rejected`]), is a handler's bug: a code of its own (never a
/// policy's failure, [`POLICY_FAILED`]), or one of the router's failed
/// INTERNAL (UNHANDLED_HANDLER_ERROR: a handler's bug, its own code and
/// args as its `cause`, or an error without a code of its own). The kernel
/// dead-letters it by its code and args, the cause among them, and a policy's step moves the policy's position past it unless
/// the kernel says to stay ([`OnPolicyBug`](super::OnPolicyBug)).
pub(crate) fn is_handler_fault(error: &CodedError) -> bool {
    error.code != POLICY_FAILED
        && (reason_code(&error.code).is_none() || error.grpc == GrpcCode::Internal)
}

/// A handler's refusal `rejection`, with its typed refusal object, as the
/// router carries it: a business rejection (FAILED_PRECONDITION) under the
/// refusal's own code, its args as metadata, the object packed beside.
pub fn rejection(rejection: &dyn Diagnostic, refusal: Option<Any>) -> HandlerError {
    let extras = rejection.args().into_iter().map(|(name, arg)| {
        let value = match arg {
            Arg::Int(n) => n.to_string(),
            Arg::Uint(n) => n.to_string(),
            Arg::Str(s) => s,
            Arg::Nested(nested) => nested.code().to_string(),
        };
        (name.to_string(), value)
    });
    let error = CodedError::rejection(rejection.code(), &rejection.to_string(), extras);
    HandlerError::Coded(match refusal {
        Some(object) => error.with_refusal(object),
        None => error,
    })
}

/// One of angzarr-embedded's own failures, INTERNAL under its own `code`
/// ([`POLICY_FAILED`], [`TOO_MANY_STEPS`], an upcast's failure): never a
/// handler's bug, which angzarr-router's `CodedError::bug` answers as
/// UNHANDLED_HANDLER_ERROR with the handler's code as its cause.
pub(crate) fn internal(
    code: &str,
    message: &str,
    extras: impl IntoIterator<Item = (String, String)>,
) -> CodedError {
    CodedError {
        code: code.to_string(),
        message: message.to_string(),
        extras: extras.into_iter().collect(),
        grpc: GrpcCode::Internal,
        refusal: None,
    }
}

/// The router's code for a policy's own failure (its `on_event`'s error, a
/// marked event that does not decode, or its source's `#[on_refused]`
/// handler's error): the kernel's
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed), a defect.
pub(crate) const POLICY_FAILED: &str = "ANGZARR_EMBEDDED_POLICY_FAILED";

/// The metadata key a policy's failure carries its own code under: no arg
/// name has a hyphen, so it never meets one of the failure's args.
const POLICY_REASON: &str = "policy-reason";

/// A diagnostic's args, flattened as angzarr carries them (decision
/// 14.37): each by its name, a wrapped cause as its code under the arg's
/// name and each of its args under `<name>.<arg>`.
pub(crate) fn flat_args(diagnostic: &dyn Diagnostic) -> Vec<(String, String)> {
    let mut flat = Vec::new();
    for (name, arg) in diagnostic.args() {
        match arg {
            Arg::Int(n) => flat.push((name.to_string(), n.to_string())),
            Arg::Uint(n) => flat.push((name.to_string(), n.to_string())),
            Arg::Str(s) => flat.push((name.to_string(), s)),
            Arg::Nested(cause) => {
                flat.push((name.to_string(), cause.code().to_string()));
                flat.extend(
                    flat_args(cause.as_ref())
                        .into_iter()
                        .map(|(inner, value)| (format!("{name}.{inner}"), value)),
                );
            }
        }
    }
    flat
}

/// A policy's `failure`, as the router carries it: INTERNAL under
/// [`POLICY_FAILED`], the failure's code and flattened args as metadata.
pub(crate) fn policy_failed(failure: &dyn Diagnostic) -> HandlerError {
    let extras = std::iter::once((POLICY_REASON.to_string(), failure.code().to_string()))
        .chain(flat_args(failure));
    HandlerError::Coded(internal(POLICY_FAILED, &failure.to_string(), extras))
}

/// The policy's failure the router carries, if `error` is one: its code
/// and args, as [`policy_failed`] packed them.
pub(crate) fn policy_failure(error: &CodedError) -> Option<Report> {
    if error.code != POLICY_FAILED {
        return None;
    }
    let reason = error.extras.get(POLICY_REASON).cloned().unwrap_or_default();
    Some(
        error
            .extras
            .iter()
            .filter(|(name, _)| name.as_str() != POLICY_REASON)
            .fold(
                Report::new(intern(&reason), error.message.clone()),
                |report, (name, value)| report.arg(intern(name), value.as_str()),
            ),
    )
}

/// The type URL of the page a fact handler that records no event answers
/// with: the router's fact record always holds one payload. Never written
/// and never folded.
pub const NOTHING: &str = "/angzarr_embedded.next.Nothing";

/// Whether `page` is a fact's record of nothing ([`NOTHING`]).
pub(crate) fn is_nothing(page: &angzarr_proto::v1::EventPage) -> bool {
    page.payload.as_ref().is_some_and(|p| p.type_url == NOTHING)
}

// ------------------------------------------------------------- recorded_at

/// The input being dispatched, as the kernel delivers it: what the
/// router's `CommandContext` does not carry, when it will be recorded. Set
/// by the kernel around each dispatch; a handler's context reads
/// `recorded_at` here (the router's `ContextualCommand` has no field for it
/// yet).
#[derive(Clone, Debug, Default)]
pub(crate) struct Delivery {
    pub(crate) recorded_at: Timestamp,
}

thread_local! {
    static DELIVERY: std::cell::RefCell<Option<Delivery>> = const { std::cell::RefCell::new(None) };
}

/// Run `dispatch` with `delivery` as the input handlers read.
pub(crate) fn recording<T>(delivery: Delivery, dispatch: impl FnOnce() -> T) -> T {
    struct Reset(Option<Delivery>);
    impl Drop for Reset {
        fn drop(&mut self) {
            let previous = self.0.take();
            DELIVERY.with(|cell| *cell.borrow_mut() = previous);
        }
    }
    let _reset = Reset(DELIVERY.with(|cell| cell.borrow_mut().replace(delivery)));
    dispatch()
}

/// When the input being dispatched will be recorded: the kernel's instant
/// (`max(sampled now, the stream's last)`), for a hand-written handler
/// until the router's `ContextualCommand` carries it; the epoch outside a
/// dispatch.
pub fn recorded_at() -> Timestamp {
    delivery().recorded_at
}

/// The input being dispatched; the default outside a dispatch.
pub(crate) fn delivery() -> Delivery {
    DELIVERY
        .with(|cell| cell.borrow().clone())
        .unwrap_or_default()
}

// ------------------------------------------------------------- the table

/// The code a remote handler answers with when it cannot be reached
/// (angzarr-embedded-host's): the kernel's retryable
/// [`KernelError::HandlerUnavailable`](super::KernelError::HandlerUnavailable).
pub(crate) const HANDLER_UNAVAILABLE: &str = "REMOTE_HANDLER_UNAVAILABLE";

/// The router's code for an incoming payload's upcast that failed: the
/// kernel's [`KernelError::Upcast`](super::KernelError::Upcast).
const UPCAST_FAILED: &str = "ANGZARR_EMBEDDED_UPCAST_FAILED";

/// The failed upcast's source type, in its metadata.
const UPCAST_SOURCE: &str = "source";

/// The upcast failure the router carries, if `error` is one: the rule from
/// its source and the rule's own reason.
pub(crate) fn upcast_failure(error: &CodedError) -> Option<UpcastError> {
    if error.code != UPCAST_FAILED {
        return None;
    }
    let extra = |key: &str| error.extras.get(key).cloned().unwrap_or_default();
    let reason = extra(angzarr_router::error::extras::REASON);
    Some(UpcastError::Rule {
        source: extra(UPCAST_SOURCE),
        error: Report::new(intern(&reason), error.message.clone()),
    })
}

/// An upcaster as the router runs it on a command's or fact's payload.
pub(crate) struct CommandUpcaster(pub(crate) Arc<Upcaster>);

impl angzarr_router::PayloadUpcaster for CommandUpcaster {
    fn upcast_payload(&self, payload: &Any) -> Result<Any, CodedError> {
        self.0.upcast_command(payload).map_err(|error| {
            let (source, reason) = match &error {
                UpcastError::Payload(e) => (payload.type_url.clone(), Report::of(e)),
                UpcastError::Rule { source, error } => (source.clone(), error.clone()),
            };
            internal(
                UPCAST_FAILED,
                &reason.to_string(),
                [
                    (UPCAST_SOURCE.to_string(), source),
                    (
                        angzarr_router::error::extras::REASON.to_string(),
                        reason.code().to_string(),
                    ),
                ],
            )
        })
    }

    fn source_types(&self) -> Vec<String> {
        self.0
            .source_types()
            .into_iter()
            .map(str::to_string)
            .collect()
    }
}

/// The router's table for typed domain `D`, before its handlers: named
/// `D::NAME` (a process manager's if `process_manager`), folding each
/// event through `upcaster` and `D::apply` under the rules its
/// `rules_version` names (an event just emitted, under the current ones),
/// restoring a snapshot onto a fresh state from `fresh`, imaging its state
/// as a snapshot when it has a snapshot form, resolving COMMUTATIVE by
/// [`State::changed`] when the state supports it, and running `upcaster`
/// on every incoming payload.
pub(crate) fn table<D: Domain>(
    rules: Arc<Rulebook<D::Rules>>,
    upcaster: Arc<Upcaster>,
    fresh: Arc<dyn Fn() -> D::State + Send + Sync>,
    process_manager: bool,
) -> DomainDispatch<D::State> {
    let snapshot_form = <<D::State as State>::Snapshot as prost::Name>::full_name();
    let fold_rules = Arc::clone(&rules);
    let fold_upcaster = Arc::clone(&upcaster);
    let restore_fresh = Arc::clone(&fresh);
    let mut rebuilder = Rebuilder::new(move || fresh())
        .with_fold(move |state, pages, _cover| {
            for (index, page) in pages.iter().enumerate() {
                if is_nothing(page) {
                    continue;
                }
                let failed = |message: String| FoldFailure {
                    page_index: Some(index),
                    message,
                };
                let sent = page.payload.clone().unwrap_or_default();
                let any = fold_upcaster
                    .upcast_event(&sent)
                    .map_err(|e| failed(e.to_string()))?;
                let event = D::Event::from_any(&any).map_err(|e| failed(e.to_string()))?;
                // A stored event names its rules; one just emitted has no
                // metadata yet, and is under the current ones.
                let rules = match page.meta.as_ref().map(|m| m.rules_version.as_str()) {
                    Some(version) => fold_rules
                        .get(version)
                        .ok_or_else(|| failed(format!("no rules \"{version}\"")))?,
                    None => fold_rules.current().1,
                };
                D::apply(state, &event, rules);
            }
            Ok(())
        })
        .with_snapshot(move |state, any| {
            if type_name(&any.type_url) != snapshot_form {
                return Err(format!("a snapshot of {}", any.type_url).into());
            }
            let wire = <D::State as State>::Snapshot::decode(&any.value[..])?;
            let mut restored = restore_fresh();
            restored
                .restore(wire)
                .map_err(|report| report.to_string())?;
            *state = restored;
            Ok(())
        })
        .with_state_type(&<<D::State as State>::Snapshot as prost::Name>::full_name());
    if <D::State as State>::SNAPSHOTS {
        rebuilder = rebuilder
            .with_state_image(|state: &D::State| super::payload::pack(&state.to_snapshot()));
    }
    let base = match process_manager {
        true => DomainDispatch::process_manager(D::NAME, D::NAME, rebuilder),
        false => DomainDispatch::new(D::NAME, D::NAME, rebuilder),
    };
    let base = base
        .with_rules_version(rules.current().0)
        .with_command_upcaster(CommandUpcaster(upcaster));
    let commutative = <D::State as State>::descriptor().options.is_some_and(|o| {
        o.conflict_resolutions
            .contains(&(ConflictResolution::Commutative as i32))
    });
    match commutative {
        true => base.with_commutative_via(
            |state: &D::State| Ok(state.clone()),
            |state: &mut D::State, image: &D::State| {
                *state = image.clone();
                Ok(())
            },
            |before: &D::State, after: &D::State| {
                Ok(D::State::changed(before, after)
                    .iter()
                    .map(ToString::to_string)
                    .collect())
            },
        ),
        false => base,
    }
}

#[cfg(test)]
#[path = "route.test.rs"]
mod tests;
