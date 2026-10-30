//! angzarr-embedded's host for remote components: an application's domains,
//! policies and projectors written in another language (or another
//! process) run as child processes speaking gRPC on a Unix socket, in the
//! HashiCorp go-plugin model, and reach the kernel beside its in-process
//! ones: a domain as an angzarr-router [`CommandHandler`], a policy as a
//! [`PolicyComponent`], a projector as a [`ProjectorHandler`].
//!
//! For each remote `[[component]]` of a [`HostConfig`], [`Host::start`]:
//!
//! 1. launches the child with its launch command (the config's, or its
//!    language's default: [`config::default_launch`]), `ANGZARR_SOCKET` set
//!    to the socket path the host chose;
//! 2. reads its handshake, the one stdout line
//!    `angzarr-handshake v1 unix:<absolute-socket-path>` ([`handshake`]),
//!    within a timeout;
//! 3. connects to that socket and calls `ComponentService.Describe`,
//!    refusing a child of another kind than its `kind` says, and one
//!    whose descriptor fails validation ([`descriptor::validate`]);
//! 4. hands back, by its kind, a [`RemoteCommandHandler`] over
//!    `CommandHandlerService`, a [`RemotePolicyHandler`] over
//!    `PolicyService` or a [`RemoteProjectorHandler`] over
//!    `ProjectorService`, which [`Host::register`] registers with the
//!    kernel; a remote process manager's start rule is the kernel's, from
//!    its descriptor's starting types;
//! 5. supervises it: a child that exits is restarted with backoff, and
//!    while it is down its dispatch fails retryably ([`is_retryable`]).
//!
//! A child exits when its stdin reaches end of file, or on SIGTERM; the
//! host closes its stdin to stop it.
//!
//! [`CommandHandler`]: angzarr_router::router::CommandHandler
//! [`PolicyComponent`]: angzarr_router::router::PolicyComponent
//! [`ProjectorHandler`]: angzarr_router::router::ProjectorHandler

pub mod app;
pub mod config;
pub mod descriptor;
pub mod error;
pub mod handshake;
pub mod host;
pub mod remote;
pub mod rpc;

pub use app::{run, App};
pub use config::{
    ComponentConfig, ComponentKindConfig, HostConfig, Launch, Placement, StoreConfig, Supervision,
};
pub use descriptor::{validate, DescriptorRefused};
pub use error::{HandshakeError, HostError};
pub use host::{ChildExit, Host};
pub use remote::{is_retryable, RemoteCommandHandler, RemotePolicyHandler, RemoteProjectorHandler};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `crates/angzarr-embedded-host/locales/en-US/angzarr-embedded-host.ftl`.
pub const EN_US: (&str, &str) = (
    "angzarr-embedded-host",
    include_str!("../locales/en-US/angzarr-embedded-host.ftl"),
);

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test.
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_embedded_diagnostic::Diagnostic>> {
    let mut samples = error::diagnostic_samples();
    samples.extend(descriptor::diagnostic_samples());
    samples
}

#[cfg(test)]
#[path = "fake_child.test.rs"]
pub(crate) mod fake_child;
