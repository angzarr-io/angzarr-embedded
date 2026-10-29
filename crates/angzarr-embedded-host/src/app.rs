//! [`run`]: the whole application from its config, what the CLI's
//! generated `main.rs` calls.
//!
//! ```ignore
//! fn main() -> Result<(), angzarr_embedded_host::HostError> {
//!     let app = angzarr_embedded_host::run("angzarr.toml", register)?;
//!     // app.handle(book, &sampled), app.events(&cover), app.kernel(), ...
//!     Ok(())
//! } // dropping `app` stops the children
//! ```

use std::path::Path;

use angzarr_embedded::next::v1::{CommandBook, Cover, EventPage};
use angzarr_embedded::next::{
    EventStore, Failure, Handled, Kernel, KernelError, MemoryDeadLetters, OnPolicyBug, OnRetryable,
    Registry, Rejected, Sampled, StoreError, StreamRef,
};
use angzarr_embedded_diagnostic::Report;
use angzarr_store_sqlite::SqliteStore;

use crate::config::HostConfig;
use crate::error::HostError;
use crate::host::{ChildExit, Host};

/// Runs the application the config at `config` describes: reads it
/// (TOML), starts its remote domains ([`Host::start`]), registers them by
/// domain and calls `register` to register the in-process ones, and opens
/// its store (SQLite, `[store]`). The [`App`] is how the application sends
/// commands and reads its streams; dropping it stops the children.
pub fn run(
    config: impl AsRef<Path>,
    register: impl FnOnce(&mut Registry),
) -> Result<App, HostError> {
    let path = config.as_ref();
    let text = std::fs::read_to_string(path).map_err(|e| HostError::ConfigUnreadable {
        path: path.display().to_string(),
        kind: format!("{:?}", e.kind()),
    })?;
    let config: HostConfig = toml::from_str(&text).map_err(|e| HostError::ConfigInvalid {
        path: path.display().to_string(),
        offset: e.span().map_or(0, |span| span.start as u64),
    })?;
    let store_config = config.store.clone();
    let policy_bug = config.policy.on_bug;
    let retry = config.retry.on_retryable();
    let host = Host::start(config)?;
    let mut registry = Registry::new();
    host.register(&mut registry)
        .map_err(|e| HostError::Register {
            cause: Report::of(&e),
        })?;
    register(&mut registry);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| HostError::Runtime {
            kind: format!("{:?}", e.kind()),
        })?;
    let store = runtime
        .block_on(async {
            match store_config.in_memory {
                true => SqliteStore::open_in_memory().await,
                false => SqliteStore::open(&store_config.path).await,
            }
        })
        .map_err(|e| HostError::Store {
            cause: Report::of(&KernelError::Store(e)),
        })?;
    Ok(App {
        host,
        registry,
        dead_letters: MemoryDeadLetters::new(),
        policy_bug,
        retry,
        store,
        runtime,
    })
}

/// A running application: its registry, store, kernel and remote domains.
/// Its blocking methods ([`App::handle`], [`App::events`]) run on its own
/// runtime, so they are called, and the App dropped, outside an async
/// context; inside one, use [`App::kernel`] and [`App::store`] directly.
pub struct App {
    // Dropped in this order: the children stopped first, the runtime last.
    host: Host,
    registry: Registry,
    dead_letters: MemoryDeadLetters,
    /// The config's `[policy] on_bug`.
    policy_bug: OnPolicyBug,
    /// The config's `[retry]`.
    retry: OnRetryable,
    store: SqliteStore,
    runtime: tokio::runtime::Runtime,
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

    /// The host of the remote domains.
    pub fn host(&self) -> &Host {
        &self.host
    }

    /// Handles one command book, for any domain, and every policy step it
    /// sets off ([`Kernel::handle_any`]), blocking until the chain settles.
    // The kernel's own Failure, as Kernel::handle_any returns it.
    #[allow(clippy::result_large_err)]
    pub fn handle(
        &self,
        book: CommandBook,
        sampled: &Sampled,
    ) -> Result<Handled, Failure<Rejected>> {
        self.block_on(
            self.kernel()
                .handle_any(self.store(), book, sampled, &mut ()),
        )
    }

    /// Every event of the stream `cover` names, in sequence order.
    pub fn events(&self, cover: &Cover) -> Result<Vec<EventPage>, StoreError> {
        let stream = StreamRef::try_from(cover)?;
        Ok(self.block_on(self.store().read(stream, 0, None))?.pages)
    }

    /// Stops the remote domains' children ([`Host::shutdown`]) and closes
    /// the store.
    pub fn shutdown(self) -> Vec<(String, ChildExit)> {
        self.host.shutdown()
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}

#[cfg(test)]
#[path = "app.test.rs"]
mod tests;
