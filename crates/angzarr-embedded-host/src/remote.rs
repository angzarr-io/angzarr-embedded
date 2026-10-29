//! A remote domain as angzarr-router's [`CommandHandler`]: `dispatch` is
//! `CommandHandlerService.Handle` on the child's socket, and the descriptor
//! is the one its `ComponentService.Describe` gave when it came up.
//!
//! What comes back is in the router's terms:
//!
//! - a FAILED_PRECONDITION status is the router's refusal: its
//!   `google.rpc.ErrorInfo` detail's reason and metadata are the code and
//!   extras, its message the message, and its other detail, an `Any`, the
//!   typed refusal ([`CodedError::refusal`]);
//! - a child that is not running, a connection that fails, and an
//!   UNAVAILABLE (or other transient) status are [`codes::REMOTE_HANDLER_UNAVAILABLE`],
//!   which [`is_retryable`] says to retry: the child is being restarted, and
//!   resuming by position retries the input once it is back;
//! - any other status keeps its gRPC code and its ErrorInfo's reason.

use std::sync::{Arc, RwLock};

use angzarr_router::error::CodedError;
use angzarr_router::pb;
use angzarr_router::router::CommandHandler;
use tonic::transport::Channel;

use crate::rpc::command_handler::command_handler_service_client::CommandHandlerServiceClient;

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
}

/// Whether a remote handler's error is worth retrying: the child was
/// unreachable, and will be back.
pub fn is_retryable(error: &CodedError) -> bool {
    let _ = error;
    todo!("red: which errors are retryable")
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
}

/// The connection to a domain's current child, which its supervisor sets
/// when a child comes up and clears when it exits.
#[derive(Default)]
#[allow(dead_code)] // red: read by dispatch, set by the supervisor
pub(crate) struct Link {
    pub(crate) client: RwLock<Option<CommandHandlerServiceClient<Channel>>>,
}

impl RemoteCommandHandler {
    #[allow(dead_code)] // red: built by Host::start
    pub(crate) fn new(
        domain: String,
        descriptor: pb::ComponentDescriptor,
        link: Arc<Link>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        RemoteCommandHandler {
            inner: Arc::new(Remote {
                domain,
                descriptor,
                link,
                runtime,
            }),
        }
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
    /// The domain's name, which its descriptor names too.
    fn name(&self) -> &str {
        &self.inner.domain
    }

    /// The kind its descriptor gives.
    fn kind(&self) -> pb::ComponentKind {
        todo!("red: the descriptor's kind")
    }

    /// None: the wire descriptor carries no command types, and the router
    /// hosts one component, which every command reaches.
    fn command_types(&self) -> Vec<String> {
        todo!("red: a remote domain's command types")
    }

    /// `CommandHandlerService.Handle` on the child, blocking until it
    /// answers. Safe to call from inside a tokio runtime: the call runs on
    /// the host's own.
    fn dispatch(
        &self,
        req: &pb::ContextualCommand,
    ) -> Result<pb::CommandHandlerResponse, CodedError> {
        let _ = (req, &self.inner.link, &self.inner.runtime);
        todo!("red: dispatch over the child's socket")
    }

    /// The descriptor `Describe` gave when the domain first came up.
    fn component_descriptor(&self) -> Result<pb::ComponentDescriptor, CodedError> {
        let _ = &self.inner.descriptor;
        todo!("red: the described descriptor")
    }
}

/// A non-OK status from the child's `Handle`, in the router's terms (see
/// the module's docs).
#[allow(dead_code)] // red: used by dispatch
pub(crate) fn status_to_coded(domain: &str, status: &tonic::Status) -> CodedError {
    let _ = (domain, status);
    todo!("red: map a status to the router's error")
}

/// [`codes::REMOTE_HANDLER_UNAVAILABLE`] for `domain`, for `cause`.
#[allow(dead_code)] // red: used by dispatch
pub(crate) fn unavailable(domain: &str, cause: &str) -> CodedError {
    let _ = (domain, cause);
    todo!("red: the retryable unavailable error")
}

#[cfg(test)]
#[path = "remote.test.rs"]
mod tests;
