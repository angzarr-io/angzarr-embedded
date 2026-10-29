//! The host: launches every remote domain of a [`HostConfig`], waits for its
//! handshake, connects, describes it, and hands back a
//! [`RemoteCommandHandler`] per domain to register with the application's
//! registry beside its in-process domains. Then it supervises: a child that
//! exits is restarted, after a backoff that grows while restarts fail; while
//! it is down its handler's dispatch is retryable
//! ([`crate::is_retryable`]).
//!
//! The generated `main.rs` is, in outline:
//!
//! ```ignore
//! let config: HostConfig = toml::from_str(include_str!("angzarr.toml"))?;
//! let host = Host::start(config)?;
//! let mut registry = Registry::new().domain(table::Table);
//! for handler in host.handlers() {
//!     registry = registry.handler(Box::new(handler));
//! }
//! // ... run the application; dropping `host` stops the children.
//! ```
//!
//! The host runs its own tokio runtime, on its own threads, so it works the
//! same under any caller, async or not.

use crate::config::HostConfig;
use crate::error::HostError;
use crate::remote::RemoteCommandHandler;

/// How a child ended at shutdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildExit {
    /// It exited by itself once its stdin was closed: its exit code, `None`
    /// when a signal ended it.
    Exited(Option<i32>),
    /// It was still running when the grace ran out, and was killed.
    Killed,
    /// No child was running (it had exited and was waiting to be
    /// restarted).
    NotRunning,
}

/// The running host: its children and their handlers. Dropping it shuts
/// them down as [`Host::shutdown`] does.
pub struct Host {
    _private: (),
}

impl Host {
    /// Starts every remote domain of `config` and waits for each to come
    /// up: launched, handshaken, connected, and described with its own
    /// name. In-process domains are left to the caller. Refused, with every
    /// child already started stopped again, when any domain's config is
    /// refused or any child fails to come up the first time.
    pub fn start(config: HostConfig) -> Result<Host, HostError> {
        let _ = config;
        todo!("red: start and supervise the remote domains")
    }

    /// One handler per remote domain, in the config's order.
    pub fn handlers(&self) -> Vec<RemoteCommandHandler> {
        todo!("red: the remote domains' handlers")
    }

    /// The handler of the remote domain named `domain`.
    pub fn handler(&self, domain: &str) -> Option<RemoteCommandHandler> {
        let _ = domain;
        todo!("red: one remote domain's handler")
    }

    /// Stops supervising and stops every child: closes its stdin, waits up
    /// to the shutdown grace for it to exit, and kills it if it has not.
    /// How each ended, by domain, in the config's order.
    pub fn shutdown(self) -> Vec<(String, ChildExit)> {
        todo!("red: stop the children")
    }
}

impl Drop for Host {
    /// Stops the children, as [`Host::shutdown`] does.
    fn drop(&mut self) {
        // red: stop the children
    }
}

#[cfg(test)]
#[path = "host.test.rs"]
mod tests;
