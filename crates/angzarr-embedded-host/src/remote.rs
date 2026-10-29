//! A remote domain as angzarr-router's [`CommandHandler`]: `dispatch` is
//! `CommandHandlerService.Handle` on the child's socket, and the descriptor
//! is the one its `ComponentService.Describe` gave when it came up.
//!
//! What comes back is in the router's terms:
//!
//! - a business status (the owner's ruling, 2026-10-29:
//!   FAILED_PRECONDITION, INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS,
//!   OUT_OF_RANGE, PERMISSION_DENIED) is a refusal: its
//!   `google.rpc.ErrorInfo` detail's reason and metadata are the code and
//!   extras, its message the message, and its other detail, an `Any`, the
//!   typed refusal ([`CodedError::refusal`]). ALREADY_EXISTS, OUT_OF_RANGE
//!   and PERMISSION_DENIED, which angzarr-router's `GrpcCode` has no
//!   variant for, arrive FAILED_PRECONDITION; the kernel takes a reason of
//!   the child's own under any of them as its refusal;
//! - a child that is not running, a connection that fails, and an
//!   UNAVAILABLE (or other transient) status are [`codes::REMOTE_HANDLER_UNAVAILABLE`],
//!   which [`is_retryable`] says to retry: the child is being restarted, and
//!   resuming by position retries the input once it is back;
//! - any other status keeps its gRPC code, INTERNAL where angzarr-router's
//!   `GrpcCode` has no variant for it (UNKNOWN, DEADLINE_EXCEEDED,
//!   RESOURCE_EXHAUSTED, an UNAVAILABLE the child sent), and its
//!   ErrorInfo's reason, and carries no typed refusal: the kernel takes a
//!   reason of the child's own under it as the child's bug, never a
//!   refusal.

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

/// The static message of [`codes::REMOTE_HANDLER_UNAVAILABLE`], in the
/// router's style: the code and extras are what a reader keys on.
pub const REMOTE_HANDLER_UNAVAILABLE_MESSAGE: &str =
    "the remote handler's process is not reachable";

/// The extras key naming why a remote handler was unreachable, one of the
/// [`causes`]. The domain is under the router's `extras::DOMAIN`.
pub const CAUSE: &str = "cause";

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
        return unavailable(domain, causes::TRANSPORT);
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
        // The business set's statuses angzarr-router's GrpcCode has no
        // variant for: a refusal all the same.
        Code::FailedPrecondition
        | Code::AlreadyExists
        | Code::OutOfRange
        | Code::PermissionDenied => GrpcCode::FailedPrecondition,
        Code::Aborted => GrpcCode::Aborted,
        Code::Unimplemented => GrpcCode::Unimplemented,
        Code::DataLoss => GrpcCode::DataLoss,
        _ => GrpcCode::Internal,
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
        refusal: refusal.filter(|_| {
            matches!(
                grpc,
                GrpcCode::FailedPrecondition | GrpcCode::InvalidArgument | GrpcCode::NotFound
            )
        }),
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
