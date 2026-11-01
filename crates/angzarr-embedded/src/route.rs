//! Where the kernel meets angzarr-router: the router owns business
//! dispatch (handlers, typed refusals and the refusal path); this module turns what the router answers into the kernel's terms.
//!
//! - The router's answers keep their codes: a spec key (a `RouterReason`,
//!   or a coordinator key the kernel answers in the router's shape) is the
//!   kernel's code too, verbatim, with its args, so
//!   [`KernelError::Router`](super::KernelError::Router) needs no table
//!   (decision 14.60).
//! - [`Rejected`]: a domain's refusal, as the caller receives it: the
//!   refusing handler's code and args, and its typed refusal object.
//! - The table a typed [`Domain`] is dispatched by: the router's
//!   `DomainDispatch` over the domain's state, folding with the domain's
//!   own `apply`, with the state's codec,
//!   resolving COMMUTATIVE by the state's conflict units; the domain's
//!   handlers are registered on it.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use angzarr_diagnostic::{Arg, Diagnostic, Report};
use angzarr_proto::v1::ConflictResolution;
use angzarr_router::domain::DomainDispatch;
use angzarr_router::error::{BusinessStatus, CodedError, GrpcCode, HandlerError};
use angzarr_router::rebuild::{FoldFailure, Rebuilder};
use prost::Message;
use prost_types::Any;

use super::domain::{Domain, State};
use super::error::PayloadError;
use super::markup::Failed;
use super::payload::{type_name, Payload};

/// A domain's refusal of a command, as its caller receives it (angzarr's
/// business rejection, [`is_rejection`]): the code and args of the
/// refusal the handler made (or the router's, a process manager's
/// `COORDINATOR_REASON_PM_NO_INSTANCE`), and the typed refusal object it carries, if any.
/// The refusing domain recorded nothing.
///
/// A domain's handler in this process refused with a value, its
/// `Domain::Rejection` (a marked-up domain's
/// [`Rejection`](super::markup::Rejection)), which is kept beside the
/// report, to match as a value ([`Rejected::rejection`]); a refusal from
/// another process, or the router's on a domain's behalf, is its code and
/// args alone. Two rejections are equal when their reports, refusal
/// objects and statuses are.
#[derive(Clone, Debug)]
pub struct Rejected {
    report: Report,
    refusal: Option<Any>,
    status: BusinessStatus,
    kept: Option<Kept>,
}

impl PartialEq for Rejected {
    fn eq(&self, other: &Rejected) -> bool {
        (&self.report, &self.refusal, self.status) == (&other.report, &other.refusal, other.status)
    }
}

/// A domain's refusal as its handler made it: any `Domain::Rejection`.
pub(crate) type Typed = Arc<dyn std::any::Any + Send + Sync>;

/// A [`Typed`] refusal, kept in a [`Rejected`]: shown as `..`, since its
/// report shows it.
#[derive(Clone)]
struct Kept(Typed);

impl fmt::Debug for Kept {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("..")
    }
}

impl Rejected {
    /// The router's rejection, in the kernel's terms: under the key it
    /// went out under, the handler's own or the spec's (a typed refusal's
    /// ROUTER_REASON_COMMAND_REFUSED), its metadata the args.
    pub(crate) fn of(error: &CodedError) -> Rejected {
        let report = error
            .extras
            .iter()
            .fold(Report::new(error.code.clone()), |report, (name, value)| {
                report.arg(name.clone(), value.as_str())
            });
        Rejected {
            report,
            refusal: error.refusal.as_deref().cloned(),
            status: BusinessStatus::try_from(error.grpc)
                .unwrap_or(BusinessStatus::FailedPrecondition),
            kept: None,
        }
    }

    /// The same rejection, keeping `typed`, the refusal its handler made.
    pub(crate) fn kept(self, typed: Option<Typed>) -> Rejected {
        Rejected {
            kept: typed.map(Kept),
            ..self
        }
    }

    /// The domain's own refusal, as its handler in this process made it,
    /// if it is an `R`: its `Domain::Rejection`, matched as a value. `None`
    /// for a refusal from another process or the router's (code and args
    /// alone: [`Rejected::report`]), and for any other type.
    pub fn rejection<R: std::any::Any>(&self) -> Option<&R> {
        let kept: &(dyn std::any::Any + Send + Sync) = &*self.kept.as_ref()?.0;
        kept.downcast_ref::<R>().and(None)
    }

    /// The refusal as a report: its code and args.
    pub fn report(&self) -> &Report {
        &self.report
    }

    /// The refusal as the wire carries it: the refusing handler's key (a
    /// typed refusal's ROUTER_REASON_COMMAND_REFUSED), and its args.
    pub(crate) fn wire(&self) -> angzarr_proto::v1::Diagnostic {
        angzarr_proto::v1::Diagnostic {
            code: self.report.code().to_string(),
            args: flat_args(&self.report).into_iter().collect(),
        }
    }

    /// The business status the refusal went out under.
    pub fn status(&self) -> BusinessStatus {
        self.status
    }

    /// As a refused policy receives it, angzarr's `RefusalResponse`: the
    /// typed refusal object, or an untyped refusal as the wire carries it
    /// ([`Rejected::wire`]): the key it went out under, the handler's own
    /// or the spec's, and its args.
    pub fn response(&self) -> angzarr_proto::v1::RefusalResponse {
        use angzarr_proto::v1::refusal_response::Reason;
        angzarr_proto::v1::RefusalResponse {
            reason: Some(match &self.refusal {
                Some(object) => Reason::Refusal(object.clone()),
                None => Reason::Diagnostic(self.wire()),
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
        super::markup::decode_message(self.refusal.as_ref()?)
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
    fn code(&self) -> &str {
        self.report.code()
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        self.report.args()
    }
}

/// Whether the router's `error` is a business refusal: its status is of
/// the business set (FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
/// ALREADY_EXISTS, OUT_OF_RANGE, PERMISSION_DENIED), which decides the
/// class (decision 14.37). The router answers its own errors on malformed
/// input INTERNAL, so a refusal is a handler's, or the router's on its
/// behalf (a typed refusal's ROUTER_REASON_COMMAND_REFUSED, a process manager's
/// COORDINATOR_REASON_PM_NO_INSTANCE).
pub(crate) fn is_rejection(error: &CodedError) -> bool {
    super::error::class(error.grpc as i32) == super::error::Class::BusinessFatal
}

/// Whether the router's `error`, which the kernel keeps as
/// [`KernelError::Router`](super::KernelError::Router) (never a refusal,
/// [`is_rejection`]: that is [`Rejected`]), is a fatal bug ([`class`](super::class):
/// INTERNAL, UNKNOWN, DATA_LOSS), never a policy's failure
/// ([`POLICY_FAILED`]). A handler's bug is ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its
/// own code and args its `cause`; the router's own errors on malformed
/// input are bugs too. The kernel dead-letters it by its code and args at
/// once, and a policy's step moves the policy's position past it unless
/// the kernel says to stay ([`OnPolicyBug`](super::OnPolicyBug)). A
/// retryable bug is retried first, then dead-lettered.
pub(crate) fn is_handler_fault(error: &CodedError) -> bool {
    error.code != POLICY_FAILED
        && super::error::class(error.grpc as i32) == super::error::Class::BugFatal
}

/// A handler's refusal `rejection`, with its typed refusal object, as the
/// router carries it: a business rejection (FAILED_PRECONDITION). A typed
/// refusal is keyed ROUTER_REASON_COMMAND_REFUSED, its object's type as `refusal_type`
/// and the object beside (decision 14.37); a refusal without one under its
/// own code, its args flattened as metadata.
///
/// The rejection itself is kept for the kernel's caller, typed
/// ([`Rejected::rejection`]), when the kernel dispatched the handler in
/// this process.
pub fn rejection<R: Diagnostic + Send + Sync + 'static>(
    rejection: R,
    refusal: Option<Any>,
) -> HandlerError {
    let refused = refused(&rejection, refusal, BusinessStatus::FailedPrecondition);
    offer(rejection);
    refused
}

thread_local! {
    /// The refusal kept for the dispatch being polled on this thread
    /// ([`catching`]): `None` outside one; inside, what its handler
    /// offered, if anything.
    static CATCHING: RefCell<Option<Option<Typed>>> = const { RefCell::new(None) };
}

/// Keep `rejection`, a handler's refusal as it made it, for the dispatch
/// being polled ([`catching`]); outside one, nothing is kept.
pub(crate) fn offer<R: std::any::Any + Send + Sync>(rejection: R) {
    CATCHING.with(|catching| {
        if let Some(slot) = catching.borrow_mut().as_mut() {
            *slot = Some(Arc::new(rejection));
        }
    });
}

/// The dispatch to a domain's handlers `dispatch` makes, run, and the
/// refusal its handler kept ([`offer`]) while it was made (a handler in
/// process may run as its future is made) or polled: on whatever thread
/// polls it, each poll sees only this dispatch's slot, so concurrent
/// dispatches never share one.
pub(crate) async fn catching<F: Future>(
    dispatch: impl FnOnce() -> F,
) -> (F::Output, Option<Typed>) {
    let mut caught = None;
    let work = {
        let _catch = Catch::enter(&mut caught);
        dispatch()
    };
    let mut work = std::pin::pin!(work);
    let output = std::future::poll_fn(|cx| {
        let _catch = Catch::enter(&mut caught);
        work.as_mut().poll(cx)
    })
    .await;
    (output, caught)
}

/// One poll's [`CATCHING`] slot: entered with what the dispatch caught so
/// far, and on drop (a panic included) handed back, the slot of whatever
/// was polling before restored.
struct Catch<'c> {
    caught: &'c mut Option<Typed>,
    outer: Option<Option<Typed>>,
}

impl<'c> Catch<'c> {
    fn enter(caught: &'c mut Option<Typed>) -> Catch<'c> {
        let outer = CATCHING.with(|catching| catching.replace(Some(caught.take())));
        Catch { caught, outer }
    }
}

impl Drop for Catch<'_> {
    fn drop(&mut self) {
        let ours = CATCHING.with(|catching| catching.replace(self.outer.take()));
        *self.caught = ours.flatten();
    }
}

/// A marked-up handler's failure, as the router carries it: a refusal
/// under its status ([`refused`]), an immediate return (ABORTED under its
/// own code, angzarr-router's `CodedError::immediate`), a fatal bug
/// ([`bug`]), or a retryable bug under the status it picked
/// (ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its cause as a fatal bug's,
/// `CodedError::retryable_bug`).
pub(crate) fn failed<R: Diagnostic + Send + Sync + 'static>(
    failed: Failed<super::markup::Rejection<R>>,
) -> HandlerError {
    match failed {
        Failed::Rejected { rejection, status } => {
            let refused = refused(&rejection, rejection.refusal().cloned(), status);
            offer(rejection);
            refused
        }
        Failed::Immediate(report) => {
            HandlerError::Coded(CodedError::immediate(report.code(), flat_args(&report)))
        }
        Failed::Bug(report) => bug(&report),
        Failed::RetryableBug { status, report } => HandlerError::Coded(CodedError::retryable_bug(
            status,
            report.code(),
            flat_args(&report),
        )),
    }
}

/// A handler's bug, as the router carries it (decision 14.37): INTERNAL
/// ROUTER_REASON_UNHANDLED_HANDLER_ERROR, the bug's code as its `cause` and its args,
/// flattened, as `cause.<arg>`; never a refusal.
pub(crate) fn bug(bug: &dyn Diagnostic) -> HandlerError {
    HandlerError::Coded(CodedError::bug(bug.code(), flat_args(bug)))
}

/// A handler's refusal `rejection` under `status`, with its typed refusal
/// object: a typed refusal keyed ROUTER_REASON_COMMAND_REFUSED, its object's type as
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
/// ROUTER_REASON_UNHANDLED_HANDLER_ERROR with the handler's code as its cause.
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
            .fold(Report::new(reason), |report, (name, value)| {
                report.arg(name.clone(), value.as_str())
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

/// The router's table for typed domain `D`, before its handlers: named
/// `D::NAME` (a process manager's if `process_manager`), folding each
/// event, upcast by the kernel before it is handed over, through `D::apply`,
/// answering `D::RULES_VERSION`, with the state's codec when it has a snapshot form
/// (a delivered starting state restored onto a fresh state from `fresh`,
/// and the state every answer carries), resolving COMMUTATIVE by
/// [`State::changed`] when the state supports it.
pub(crate) fn table<D: Domain>(
    fresh: Arc<dyn Fn() -> D::State + Send + Sync>,
    process_manager: bool,
) -> DomainDispatch<D::State> {
    let snapshot_form = <<D::State as State>::Snapshot as prost::Name>::full_name();
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
                let any = page.payload.clone().unwrap_or_default();
                let event = D::Event::from_any(&any).map_err(|e| failed(&e))?;
                D::apply(state, &event);
            }
            Ok(())
        })
        .with_state_type(&snapshot_form);
    if <D::State as State>::SNAPSHOTS {
        rebuilder = rebuilder.with_state_codec(
            move |any| {
                if type_name(&any.type_url) != snapshot_form {
                    return Err(Box::new(PayloadError::UnknownType {
                        type_url: any.type_url.clone(),
                    }));
                }
                let wire =
                    <D::State as State>::Snapshot::decode(&any.value[..]).map_err(|error| {
                        PayloadError::Decode {
                            type_url: any.type_url.clone(),
                            error,
                        }
                    })?;
                let mut restored = restore_fresh();
                restored.restore(wire).map_err(Box::new)?;
                Ok(restored)
            },
            |state: &D::State| super::payload::pack(&state.to_snapshot()),
        );
    }
    let base = match process_manager {
        true => DomainDispatch::process_manager(D::NAME, D::NAME, rebuilder),
        false => DomainDispatch::new(D::NAME, D::NAME, rebuilder),
    };
    let base = base.with_rules_version(D::RULES_VERSION);
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
