//! Where a command for a domain is handled: the routing seam.
//!
//! Every command the kernel handles, the application's own and every
//! policy step's, names its domain in `cover.domain`, and the registry
//! resolves that name to a [`Target`]: a domain handled here (its handler
//! in this process, or in a child behind a local socket, the kernel's
//! pipeline over the kernel's store either way), or a domain whose streams
//! another coordinator owns, reached through a [`CoordinatorPort`] (a
//! distributed angzarr's `CommandHandlerCoordinatorService`, say). The
//! kernel knows only the port: what sits behind it is the composition
//! root's, so a transport is added without touching the kernel.
//!
//! RED (design-by-test, 2026-10-30): the signatures here are proposed for
//! review; [`Registry::remote`](super::Registry::remote) and
//! [`Registry::target`](super::Registry::target) are `todo!()`.

use std::future::Future;
use std::pin::Pin;

use angzarr_proto::v1::CommandBook;
use angzarr_router::error::CodedError;
use angzarr_router::pb::CommandResponse;
use angzarr_router::router::DynCommandHandler;

/// What a [`CoordinatorPort`]'s call is: a `Send` future, so the kernel's
/// own future stays `Send` across it.
pub type Coordinated<'a> =
    Pin<Box<dyn Future<Output = Result<CommandResponse, CodedError>> + Send + 'a>>;

/// A coordinator elsewhere that owns a domain's streams, as the kernel
/// reaches it: angzarr's `CommandHandlerCoordinatorService.Handle`. It is
/// handed the book as the kernel identified it (its command id fixed
/// here, so a retry is deduplicated there) and answers as `Handle` does:
/// the persisted events, under `outcome.delivery` APPLIED or DUPLICATE; or
/// a refusal, as the router carries one ([`CodedError`]: a business
/// refusal's code and args, a bug, a retryable failure), which the kernel
/// treats as it treats a local handler's.
pub trait CoordinatorPort: Send + Sync {
    /// Handle `book`, which names one of this coordinator's domains.
    fn handle(&self, book: CommandBook) -> Coordinated<'_>;
}

/// What a domain name resolves to ([`Registry::target`](super::Registry::target)).
pub enum Target<'r> {
    /// Handled here: the kernel's pipeline over its own store, deciding
    /// through this handler (in this process, or a child's behind its
    /// socket).
    Local(&'r dyn DynCommandHandler),
    /// Handled by the coordinator that owns the domain's streams: nothing
    /// of it is loaded or written here.
    Remote(&'r dyn CoordinatorPort),
}

impl std::fmt::Debug for Target<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::Local(handler) => f.debug_tuple("Local").field(&handler.name()).finish(),
            Target::Remote(_) => f.write_str("Remote"),
        }
    }
}

#[cfg(test)]
#[path = "target.test.rs"]
mod tests;
