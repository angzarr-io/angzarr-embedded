//! A remote domain as angzarr-router's [`CommandHandler`]: `dispatch` is
//! `CommandHandlerService.Handle` on the child's socket, and the descriptor
//! is the one its `ComponentService.Describe` gave when it came up.
//!
//! What comes back is in the router's terms:
//!
//! - every status keeps its gRPC code, so the status decides the error's
//!   class (decision 14.37), and its `google.rpc.ErrorInfo` detail's
//!   reason and metadata are the code and extras. The status's message is
//!   never forwarded: no text crosses;
//! - a business status (FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND,
//!   ALREADY_EXISTS, OUT_OF_RANGE, PERMISSION_DENIED) is a refusal, and its
//!   other detail, an `Any`, the typed refusal ([`CodedError::refusal`]);
//!   any other status carries no typed refusal;
//! - a child that is not running, a connection that fails, and an
//!   UNAVAILABLE status with no details are
//!   [`codes::REMOTE_HANDLER_UNAVAILABLE`], which [`is_retryable`] says to
//!   retry: the child is being restarted. A transport's failure carries the
//!   transport's text verbatim as [`EXT_ERR_TXT`], a third party's;
//! - a status angzarr-router's `GrpcCode` has no variant for (CANCELLED,
//!   UNAUTHENTICATED) arrives UNAVAILABLE: a retryable bug, as the owner
//!   ruled.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use angzarr_router::error::{codes as router_codes, extras, CodedError, GrpcCode};
use angzarr_router::pb;
use angzarr_router::router::CommandHandler;
use prost::Message;
use tonic::transport::Channel;
use tonic::Code;

use crate::rpc::command_handler::command_handler_service_client::CommandHandlerServiceClient;
use crate::rpc::google_rpc::{ErrorInfo, Status as RpcStatus, ERROR_INFO_NAME};

/// The router-style codes a remote handler's dispatch adds to the router's
/// own (`angzarr_router::error::codes`).
pub mod codes {
    /// The domain's child cannot be reached: not running (exited, being
    /// restarted), the connection failed, or it answered UNAVAILABLE.
    /// Retryable ([`super::is_retryable`]).
    pub const REMOTE_HANDLER_UNAVAILABLE: &str = "REMOTE_HANDLER_UNAVAILABLE";
}

/// The extras key naming why a remote handler was unreachable, one of the
/// [`causes`]. The domain is under the router's `extras::DOMAIN`.
pub const CAUSE: &str = "cause";

/// The extras key a transport's failure carries its text under, verbatim:
/// a third party's (tonic's, hyper's) words, never angzarr's.
pub const EXT_ERR_TXT: &str = "ext_err_txt";

/// The values of [`CAUSE`].
pub mod causes {
    /// No child is running: it exited and is being restarted.
    pub const NOT_RUNNING: &str = "not-running";
    /// The call did not reach the child, or its answer did not come back.
    pub const TRANSPORT: &str = "transport";
    /// The child did not answer within the call timeout
    /// (`Supervision::call_timeout_ms`).
    pub const TIMEOUT: &str = "timeout";
}

/// Whether a remote handler's error is worth retrying: the child was
/// unreachable, and will be back.
pub fn is_retryable(error: &CodedError) -> bool {
    error.code == codes::REMOTE_HANDLER_UNAVAILABLE
}

/// A remote domain's handler. Cloning it is cheap; every clone reaches the
/// same supervised child, whichever process it currently is.
#[derive(Clone)]
pub struct RemoteCommandHandler {
    inner: Arc<Remote>,
}

struct Remote {
    domain: String,
    descriptor: pb::ComponentDescriptor,
    link: Arc<Link>,
    runtime: tokio::runtime::Handle,
    call_timeout: Duration,
}

/// The connection to a domain's current child, which its supervisor sets
/// when a child comes up and clears when it exits.
pub(crate) struct Link {
    pub(crate) client: RwLock<Option<CommandHandlerServiceClient<Channel>>>,
    /// How many children have come up: the supervisor counts each one
    /// after setting its client, so a watcher that sees `n` reaches the
    /// `n`th child.
    pub(crate) generation: tokio::sync::watch::Sender<u64>,
}

impl Default for Link {
    fn default() -> Self {
        Link {
            client: RwLock::new(None),
            generation: tokio::sync::watch::channel(0).0,
        }
    }
}

impl RemoteCommandHandler {
    pub(crate) fn new(
        domain: String,
        descriptor: pb::ComponentDescriptor,
        link: Arc<Link>,
        runtime: tokio::runtime::Handle,
        call_timeout: Duration,
    ) -> Self {
        RemoteCommandHandler {
            inner: Arc::new(Remote {
                domain,
                descriptor,
                link,
                runtime,
                call_timeout,
            }),
        }
    }

    /// Watches how many children of this domain have come up.
    #[cfg(test)]
    pub(crate) fn generations(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.link.generation.subscribe()
    }
}

impl RemoteCommandHandler {
    /// The domain this handler is configured for: its `[[domain]]` name.
    pub fn domain(&self) -> &str {
        &self.inner.domain
    }
}

impl std::fmt::Debug for RemoteCommandHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteCommandHandler")
            .field("domain", &self.inner.domain)
            .finish_non_exhaustive()
    }
}

impl CommandHandler for RemoteCommandHandler {
    /// The component's name, as its descriptor gives it (a domain's
    /// component is named for itself, not for the domain:
    /// `PlayerAggregate` for `player`).
    fn name(&self) -> &str {
        &self.inner.descriptor.name
    }

    /// The kind its descriptor gives.
    fn kind(&self) -> pb::ComponentKind {
        self.inner.descriptor.kind()
    }

    /// None: the wire descriptor carries no command types, and the router
    /// hosts one component, which every command reaches.
    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    /// `CommandHandlerService.Handle` on the child, blocking until it
    /// answers or the call timeout passes (retryable). Safe to call from inside a tokio runtime: the call runs on
    /// the host's own.
    fn dispatch(
        &self,
        req: &pb::ContextualCommand,
    ) -> Result<pb::CommandHandlerResponse, CodedError> {
        let domain = &self.inner.domain;
        let client = self
            .inner
            .link
            .client
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let Some(mut client) = client else {
            return Err(unavailable(domain, causes::NOT_RUNNING));
        };
        let req = req.clone();
        let call_timeout = self.inner.call_timeout;
        let (answer, answered) = std::sync::mpsc::sync_channel(1);
        self.inner.runtime.spawn(async move {
            let _ = answer.send(tokio::time::timeout(call_timeout, client.handle(req)).await);
        });
        match answered.recv() {
            Ok(Ok(Ok(response))) => Ok(response.into_inner()),
            Ok(Ok(Err(status))) => Err(status_to_coded(domain, &status)),
            Ok(Err(_elapsed)) => Err(unavailable(domain, causes::TIMEOUT)),
            // The host's runtime stopped, dropping the call.
            Err(_) => Err(unavailable(domain, causes::TRANSPORT)),
        }
    }

    /// The descriptor `Describe` gave when the domain first came up.
    fn component_descriptor(&self) -> Result<pb::ComponentDescriptor, CodedError> {
        Ok(self.inner.descriptor.clone())
    }
}

/// A remote policy's handler: its child serves `PolicyService`, and the
/// kernel runs it as any policy of its source ([`Self::input_domain`]).
///
/// RED STUB (feat/host-admit-policies): the host does not hand one back
/// yet ([`crate::Host::policy`] is always `None`).
#[derive(Clone, Debug)]
pub struct RemotePolicyHandler {
    descriptor: pb::ComponentDescriptor,
}

impl RemotePolicyHandler {
    /// The component's name, as its descriptor gives it.
    pub fn name(&self) -> &str {
        &self.descriptor.name
    }

    /// The domain whose events it handles: its descriptor's one input.
    pub fn input_domain(&self) -> &str {
        self.descriptor
            .inputs
            .first()
            .map_or("", |t| t.domain.as_str())
    }
}

/// A remote projector's handler: its child serves `ProjectorService`, and
/// the kernel runs it after each commit that records its source's events.
///
/// RED STUB (feat/host-admit-policies): the host does not hand one back
/// yet ([`crate::Host::projector`] is always `None`).
#[derive(Clone, Debug)]
pub struct RemoteProjectorHandler {
    descriptor: pb::ComponentDescriptor,
}

impl RemoteProjectorHandler {
    /// The component's name, as its descriptor gives it.
    pub fn name(&self) -> &str {
        &self.descriptor.name
    }

    /// The domain whose events it projects: its descriptor's one input.
    pub fn source(&self) -> &str {
        self.descriptor
            .inputs
            .first()
            .map_or("", |t| t.domain.as_str())
    }
}

/// A non-OK status from the child's `Handle`, in the router's terms (see
/// the module's docs).
pub(crate) fn status_to_coded(domain: &str, status: &tonic::Status) -> CodedError {
    let details = RpcStatus::decode(status.details())
        .map(|rpc| rpc.details)
        .unwrap_or_default();
    // A status the child never sent: the call failed on the way (the
    // connection refused or reset, the child gone mid-call), which tonic
    // reports with the transport's error as its source.
    let transport = details.is_empty()
        && (status.code() == Code::Unavailable
            || (status.code() == Code::Unknown && std::error::Error::source(status).is_some()));
    if transport {
        let mut error = unavailable(domain, causes::TRANSPORT);
        error
            .extras
            .insert(EXT_ERR_TXT.to_string(), transport_text(status));
        return error;
    }
    let is_info = |detail: &&prost_types::Any| {
        angzarr_router::type_name_from_url(&detail.type_url) == ERROR_INFO_NAME
    };
    let info = details
        .iter()
        .find(is_info)
        .and_then(|detail| ErrorInfo::decode(detail.value.as_slice()).ok());
    let refusal = details
        .iter()
        .find(|detail| !is_info(detail))
        .cloned()
        .map(Box::new);
    let grpc = match status.code() {
        Code::InvalidArgument => GrpcCode::InvalidArgument,
        Code::NotFound => GrpcCode::NotFound,
        Code::FailedPrecondition => GrpcCode::FailedPrecondition,
        Code::AlreadyExists => GrpcCode::AlreadyExists,
        Code::OutOfRange => GrpcCode::OutOfRange,
        Code::PermissionDenied => GrpcCode::PermissionDenied,
        Code::Aborted => GrpcCode::Aborted,
        Code::Unimplemented => GrpcCode::Unimplemented,
        Code::DataLoss => GrpcCode::DataLoss,
        Code::Internal => GrpcCode::Internal,
        Code::Unknown => GrpcCode::Unknown,
        Code::DeadlineExceeded => GrpcCode::DeadlineExceeded,
        Code::ResourceExhausted => GrpcCode::ResourceExhausted,
        // The statuses angzarr-router's GrpcCode has no variant for
        // (CANCELLED, UNAUTHENTICATED): retryable bugs, as UNAVAILABLE is.
        _ => GrpcCode::Unavailable,
    };
    let (code, extras) = match info {
        Some(info) => (info.reason, info.metadata.into_iter().collect()),
        None => (
            router_codes::UNHANDLED_HANDLER_ERROR.to_string(),
            Default::default(),
        ),
    };
    CodedError {
        code,
        extras,
        grpc,
        // Only a business status carries its typed object to a policy.
        refusal: refusal.filter(|_| grpc.is_business_refusal()),
    }
}

/// The transport's own text for a status the call failed with on the
/// way: its source's, else the status's message (tonic's).
fn transport_text(status: &tonic::Status) -> String {
    match std::error::Error::source(status) {
        Some(source) => source.to_string(),
        None => status.message().to_string(),
    }
}

/// [`codes::REMOTE_HANDLER_UNAVAILABLE`] for `domain`, for `cause`.
pub(crate) fn unavailable(domain: &str, cause: &str) -> CodedError {
    CodedError {
        code: codes::REMOTE_HANDLER_UNAVAILABLE.to_string(),
        extras: [
            (extras::DOMAIN.to_string(), domain.to_string()),
            (CAUSE.to_string(), cause.to_string()),
        ]
        .into(),
        grpc: GrpcCode::Aborted,
        refusal: None,
    }
}

#[cfg(test)]
#[path = "remote.test.rs"]
mod tests;
