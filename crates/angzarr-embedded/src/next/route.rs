//! Where the kernel meets angzarr-router: the router owns business
//! dispatch (handlers, typed refusals and the refusal path, the
//! deduplication decision, a process manager's start rule, a policy's
//! commands kept in its trigger's edition, their root and correlation);
//! this module turns what the router answers into the kernel's terms.
//!
//! - [`reason_code`]: every reason the router answers with, each
//!   forwarded under an angzarr-embedded code of its own with its own
//!   message ([`KernelError::Router`](super::KernelError::Router)).
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
use angzarr_router::error::{BusinessStatus, CodedError, ErrorClass, GrpcCode, HandlerError};
use angzarr_router::rebuild::{FoldFailure, Rebuilder};
use prost::Message;
use prost_types::Any;

use super::domain::{Domain, State};
use super::error::{PayloadError, UpcastError};
use super::markup::Failed;
use super::payload::{type_name, Payload};
use super::upcast::Upcaster;
use super::version::Rulebook;

/// The angzarr-embedded code a router reason is forwarded under, for
/// every reason angzarr-router answers with (its `error::codes::ALL`):
/// `angzarr-embedded-next-` and the reason in kebab-case; `None` for a
/// reason that is not the router's (a domain's own). The catalog check
/// holds the catalog to the router's list.
pub(crate) fn reason_code(reason: &str) -> Option<&'static str> {
    angzarr_router::error::codes::ALL
        .contains(&reason)
        .then(|| intern(&kebab(reason)))
}

/// `REASON` as `angzarr-embedded-next-reason`.
fn kebab(reason: &str) -> String {
    format!(
        "angzarr-embedded-next-{}",
        reason.to_ascii_lowercase().replace('_', "-")
    )
}

/// `error`'s metadata as args, by name, each as text: angzarr-router's
/// keys, a wrapped cause already flattened (`cause`, `cause.<arg>`).
pub(crate) fn router_args(error: &CodedError) -> Vec<(&'static str, Arg)> {
    error
        .extras
        .iter()
        .map(|(key, value)| (intern(key), Arg::Str(value.clone())))
        .collect()
}

/// The code of a refusal a domain's handler made, kept for the program's
/// life: each distinct code (and arg name) once. A program refuses with a
/// fixed set of codes, so the set is bounded.
pub(crate) fn intern(text: &str) -> &'static str {
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
/// business rejection, [`is_rejection`]): the code and args of the
/// refusal the handler made (or the router's, a process manager's
/// `PM_NO_INSTANCE`), and the typed refusal object it carries, if any.
/// The refusing domain recorded nothing.
#[derive(Clone, PartialEq, Debug)]
pub struct Rejected {
    report: Report,
    refusal: Option<Any>,
    status: BusinessStatus,
}

impl Rejected {
    /// The router's rejection, in the kernel's terms: under the router's
    /// reason as angzarr-embedded's code (a typed refusal's
    /// COMMAND_REFUSED), or the handler's own, its metadata the args.
    pub(crate) fn of(error: &CodedError) -> Rejected {
        let code = reason_code(&error.code).unwrap_or_else(|| intern(&error.code));
        let report = router_args(error)
            .into_iter()
            .fold(Report::new(code), |report, (name, arg)| {
                report.arg(name, arg)
            });
        Rejected {
            report,
            refusal: error.refusal.as_deref().cloned(),
            status: BusinessStatus::try_from(error.grpc)
                .unwrap_or(BusinessStatus::FailedPrecondition),
        }
    }

    /// The refusal as a report: its code and args.
    pub fn report(&self) -> &Report {
        &self.report
    }

    /// The business status the refusal went out under.
    pub fn status(&self) -> BusinessStatus {
        self.status
    }

    /// As a refused policy receives it, angzarr's `RefusalResponse`: the
    /// typed refusal object, or an untyped refusal's code and args.
    pub fn response(&self) -> angzarr_proto::v1::RefusalResponse {
        use angzarr_proto::v1::refusal_response::Reason;
        angzarr_proto::v1::RefusalResponse {
            reason: Some(match &self.refusal {
                Some(object) => Reason::Refusal(object.clone()),
                None => Reason::Diagnostic(diagnostic(&self.report)),
            }),
        }
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

/// Whether the router's `error` is a business refusal: its status is of
/// the business set (FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
/// ALREADY_EXISTS, OUT_OF_RANGE, PERMISSION_DENIED), which decides the
/// class (decision 14.37). The router answers its own errors on malformed
/// input INTERNAL, so a refusal is a handler's, or the router's on its
/// behalf (a typed refusal's COMMAND_REFUSED, a process manager's
/// PM_NO_INSTANCE).
pub(crate) fn is_rejection(error: &CodedError) -> bool {
    error.grpc.class() == ErrorClass::BusinessFatal
}

/// Whether the router's `error`, which the kernel keeps as
/// [`KernelError::Router`](super::KernelError::Router) (never a refusal,
/// [`is_rejection`]: that is [`Rejected`]), is a fatal bug (`ErrorClass`:
/// INTERNAL, UNKNOWN, DATA_LOSS), never a policy's failure
/// ([`POLICY_FAILED`]). A handler's bug is UNHANDLED_HANDLER_ERROR, its
/// own code and args its `cause`; the router's own errors on malformed
/// input are bugs too. The kernel dead-letters it by its code and args at
/// once, and a policy's step moves the policy's position past it unless
/// the kernel says to stay ([`OnPolicyBug`](super::OnPolicyBug)). A
/// retryable bug is retried first, then dead-lettered.
pub(crate) fn is_handler_fault(error: &CodedError) -> bool {
    error.code != POLICY_FAILED && error.grpc.class() == ErrorClass::BugFatal
}

/// A handler's refusal `rejection`, with its typed refusal object, as the
/// router carries it: a business rejection (FAILED_PRECONDITION). A typed
/// refusal is keyed COMMAND_REFUSED, its object's type as `refusal_type`
/// and the object beside (decision 14.37); a refusal without one under its
/// own code, its args flattened as metadata.
pub fn rejection(rejection: &dyn Diagnostic, refusal: Option<Any>) -> HandlerError {
    refused(rejection, refusal, BusinessStatus::FailedPrecondition)
}

/// A marked-up handler's failure, as the router carries it: a refusal
/// under its status ([`refused`]), or a bug ([`bug`]).
pub(crate) fn failed<R: Diagnostic>(failed: Failed<super::markup::Rejection<R>>) -> HandlerError {
    match failed {
        Failed::Rejected { rejection, status } => {
            refused(&rejection, rejection.refusal().cloned(), status)
        }
        Failed::Bug(report) => bug(&report),
    }
}

/// A handler's bug, as the router carries it (decision 14.37): INTERNAL
/// UNHANDLED_HANDLER_ERROR, the bug's code as its `cause` and its args,
/// flattened, as `cause.<arg>`; never a refusal.
pub(crate) fn bug(bug: &dyn Diagnostic) -> HandlerError {
    HandlerError::Coded(CodedError::bug(bug.code(), flat_args(bug)))
}

/// A handler's refusal `rejection` under `status`, with its typed refusal
/// object: a typed refusal keyed COMMAND_REFUSED, its object's type as
/// `refusal_type` and the object beside; a refusal without one under its
/// own code, its args flattened as metadata. Either goes out under
/// `status`.
fn refused(
    rejection: &dyn Diagnostic,
    refusal: Option<Any>,
    status: BusinessStatus,
) -> HandlerError {
    HandlerError::Coded(match refusal {
        Some(object) => CodedError {
            grpc: status.into(),
            ..CodedError::refused(object)
        },
        None => CodedError::rejection(status, rejection.code(), flat_args(rejection)),
    })
}

/// One of angzarr-embedded's own failures, INTERNAL under its own `code`
/// ([`POLICY_FAILED`], [`TOO_MANY_STEPS`], an upcast's failure): never a
/// handler's bug, which angzarr-router's `CodedError::bug` answers as
/// UNHANDLED_HANDLER_ERROR with the handler's code as its cause.
pub(crate) fn internal(
    code: &str,
    extras: impl IntoIterator<Item = (String, String)>,
) -> CodedError {
    CodedError {
        code: code.to_string(),
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

/// `diagnostic` as angzarr's wire `Diagnostic`: its code and its args,
/// flattened ([`flat_args`]).
pub(crate) fn diagnostic(diagnostic: &dyn Diagnostic) -> angzarr_proto::v1::Diagnostic {
    angzarr_proto::v1::Diagnostic {
        code: diagnostic.code().to_string(),
        args: flat_args(diagnostic).into_iter().collect(),
    }
}

/// A policy's `failure`, as the router carries it: INTERNAL under
/// [`POLICY_FAILED`], the failure's code and flattened args as metadata.
pub(crate) fn policy_failed(failure: &dyn Diagnostic) -> HandlerError {
    let extras = std::iter::once((POLICY_REASON.to_string(), failure.code().to_string()))
        .chain(flat_args(failure));
    HandlerError::Coded(internal(POLICY_FAILED, extras))
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
            .fold(Report::new(intern(&reason)), |report, (name, value)| {
                report.arg(intern(name), value.as_str())
            }),
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

/// The failed upcast rule's own code, in its metadata.
const UPCAST_REASON: &str = "reason";

/// The upcast failure the router carries, if `error` is one: the rule from
/// its source and the rule's own reason, its code and args.
pub(crate) fn upcast_failure(error: &CodedError) -> Option<UpcastError> {
    if error.code != UPCAST_FAILED {
        return None;
    }
    let extra = |key: &str| error.extras.get(key).cloned().unwrap_or_default();
    let prefix = format!("{UPCAST_REASON}.");
    let reason = error
        .extras
        .iter()
        .filter_map(|(key, value)| Some((key.strip_prefix(&prefix)?, value)))
        .fold(
            Report::new(intern(&extra(UPCAST_REASON))),
            |report, (name, value)| report.arg(intern(name), value.as_str()),
        );
    Some(UpcastError::Rule {
        source: extra(UPCAST_SOURCE),
        error: reason,
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
            let flattened = flat_args(&reason)
                .into_iter()
                .map(|(name, value)| (format!("{UPCAST_REASON}.{name}"), value));
            internal(
                UPCAST_FAILED,
                [
                    (UPCAST_SOURCE.to_string(), source),
                    (UPCAST_REASON.to_string(), reason.code().to_string()),
                ]
                .into_iter()
                .chain(flattened),
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
                let failed = |cause: &dyn Diagnostic| FoldFailure {
                    page_index: Some(index),
                    cause: Some(diagnostic(cause)),
                };
                let sent = page.payload.clone().unwrap_or_default();
                let any = fold_upcaster.upcast_event(&sent).map_err(|e| failed(&e))?;
                let event = D::Event::from_any(&any).map_err(|e| failed(&e))?;
                // A stored event names its rules; one just emitted has no
                // metadata yet, and is under the current ones.
                let rules = match page.meta.as_ref().map(|m| m.rules_version.as_str()) {
                    Some(version) => fold_rules.get(version).ok_or_else(|| {
                        failed(&super::error::ReplayError::UnknownRules {
                            sequence: page.sequence,
                            version: version.to_string(),
                        })
                    })?,
                    None => fold_rules.current().1,
                };
                D::apply(state, &event, rules);
            }
            Ok(())
        })
        .with_snapshot(move |state, any| {
            if type_name(&any.type_url) != snapshot_form {
                return Err(Box::new(PayloadError::UnknownType {
                    type_url: any.type_url.clone(),
                }));
            }
            let wire = <D::State as State>::Snapshot::decode(&any.value[..]).map_err(|error| {
                PayloadError::Decode {
                    type_url: any.type_url.clone(),
                    error,
                }
            })?;
            let mut restored = restore_fresh();
            restored.restore(wire).map_err(Box::new)?;
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
