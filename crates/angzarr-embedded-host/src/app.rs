//! [`run`]: the whole application from its config, what the CLI's
//! generated `main.rs` calls.
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), angzarr_embedded_host::HostError> {
//!     let app = std::sync::Arc::new(angzarr_embedded_host::run("angzarr.toml", register).await?);
//!     // app.handle(book, &sampled).await from any task, concurrently;
//!     // app.events(&cover).await, app.kernel(), ...
//!     app.shutdown().await; // or drop it: the children stop
//!     Ok(())
//! }
//! ```
//!
//! The [`App`] is `Send + Sync` and its methods are async, on the caller's
//! runtime: commands run concurrently, and nothing orders streams against
//! each other (ordering is per stream only). Two commands to one stream
//! race optimistically: both decide, the store takes one write, and the
//! loser is decided again on the new head, unless it set an expected
//! sequence, which makes the loss its business failure. Its store is ONE
//! SQLite connection, which serializes the I/O and nothing else.
//!
//! RED (design-by-test, 2026-10-30): [`run`], [`App::handle`],
//! [`App::events`], [`App::serve`] and [`App::shutdown`] are proposed
//! async signatures, `todo!()`.

use std::path::Path;

use angzarr_embedded::next::v1::{CommandBook, Cover, EventPage};
use angzarr_embedded::next::{
    Failure, Handled, Kernel, MemoryDeadLetters, OnPolicyBug, OnRetryable, Registry, Rejected,
    Sampled, StoreError,
};
use angzarr_router::pb::CommandResponse;
use angzarr_store_sqlite::SqliteStore;

use crate::error::HostError;
use crate::exposure::Arrival;
use crate::host::{ChildExit, Host};

/// Runs the application the config at `config` describes: reads it
/// (TOML), starts its remote components ([`Host::start`]), registers them by
/// kind and calls `register` to register the in-process ones, and opens
/// its store (SQLite, `[store]`) on the caller's runtime. The [`App`] is
/// how the application sends commands and reads its streams; dropping it
/// stops the children.
pub async fn run(
    config: impl AsRef<Path>,
    register: impl FnOnce(&mut Registry),
) -> Result<App, HostError> {
    let _ = (config.as_ref(), register);
    todo!("red: async run (design-by-test, 2026-10-30)")
}

/// A running application: its registry, store, kernel and remote
/// components. `Send + Sync`: shared (an `Arc`) by every task that sends
/// it commands, and by the host's listener.
pub struct App {
    // Dropped in this order: the children stopped first.
    host: Host,
    registry: Registry,
    dead_letters: MemoryDeadLetters,
    /// The config's `[policy] on_bug`.
    policy_bug: OnPolicyBug,
    /// The config's `[retry]`.
    retry: OnRetryable,
    store: SqliteStore,
}

impl App {
    /// The kernel over this application's registry and dead letters,
    /// doing with a policy's bug what the config's `[policy] on_bug` says,
    /// and retrying a step's retryable failure as its `[retry]` does.
    pub fn kernel(&self) -> Kernel<'_> {
        Kernel::new(&self.registry, &self.dead_letters)
            .on_policy_bug(self.policy_bug)
            .on_retryable(self.retry)
    }

    /// The application's store.
    pub fn store(&self) -> &SqliteStore {
        &self.store
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The host of the remote components.
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// Handles one command book, for any domain, and every policy step it
    /// sets off ([`Kernel::handle_any`]), once the chain settles. Calls run
    /// concurrently; two on one stream race optimistically, as the kernel's
    /// do.
    // The kernel's own Failure, as Kernel::handle_any returns it.
    #[allow(clippy::result_large_err)]
    pub async fn handle(
        &self,
        book: CommandBook,
        sampled: &Sampled,
    ) -> Result<Handled, Failure<Rejected>> {
        let _ = (book, sampled);
        todo!("red: async App::handle (design-by-test, 2026-10-30)")
    }

    /// Every event of the stream `cover` names, in sequence order.
    pub async fn events(&self, cover: &Cover) -> Result<Vec<EventPage>, StoreError> {
        let _ = cover;
        todo!("red: async App::events (design-by-test, 2026-10-30)")
    }

    /// The exposure seam ([`crate::exposure`]): one request to the host's
    /// listener, which arrived from `arrived`. Handled as [`App::handle`]
    /// handles a book when its domain's `[[component]]` is exposed to the
    /// arrival, and answered as `CommandHandlerCoordinatorService.Handle`
    /// answers: the addressed stream's events and the delivery outcome.
    /// A domain not exposed to the arrival is refused as an unknown domain
    /// is (`Malformed::UnknownDomain`), the real reason logged locally
    /// only.
    // The kernel's own Failure, as Kernel::handle_any returns it.
    #[allow(clippy::result_large_err)]
    pub async fn serve(
        &self,
        book: CommandBook,
        arrived: Arrival,
        sampled: &Sampled,
    ) -> Result<CommandResponse, Failure<Rejected>> {
        let _ = (book, arrived, sampled);
        todo!("red: App::serve, the exposure seam (design-by-test, 2026-10-30)")
    }

    /// Stops the remote components' children ([`Host::shutdown`]) and closes
    /// the store.
    pub async fn shutdown(self) -> Vec<(String, ChildExit)> {
        todo!("red: async App::shutdown (design-by-test, 2026-10-30)")
    }
}

#[cfg(test)]
#[path = "app.test.rs"]
mod tests;
