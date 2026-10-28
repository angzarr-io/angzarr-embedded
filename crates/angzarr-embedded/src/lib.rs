//! A small, deterministic event-sourcing engine on angzarr's wire types:
//! angzarr's local runtime. The kernel is [`next`] (see
//! `docs/design/audit.md`): a domain decides commands and ingests facts into
//! events, applied to its folded state, policies carry one domain's events
//! into another's commands, and every step is committed on its own through
//! angzarr's shared store ports.
//!
//! The crate knows nothing about any particular application.

pub mod next;

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `crates/angzarr-embedded/locales/en-US/angzarr-embedded.ftl`.
pub const EN_US: (&str, &str) = (
    "angzarr-embedded",
    include_str!("../locales/en-US/angzarr-embedded.ftl"),
);

/// angzarr-ports' en-US Fluent catalog, its name first: the messages for
/// the store's errors, which angzarr-embedded forwards under angzarr-ports' own
/// codes ([`next::KernelError::Store`]). angzarr-ports ships no catalog, so
/// the runtime that surfaces its errors carries one. The text of
/// `crates/angzarr-embedded/locales/en-US/angzarr-ports.ftl`.
pub const ANGZARR_PORTS_EN_US: (&str, &str) = (
    "angzarr-ports",
    include_str!("../locales/en-US/angzarr-ports.ftl"),
);

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test: each one's code must have a message in every locale,
/// using exactly its args. Its messages are in
/// `crates/angzarr-embedded/locales/en-US/angzarr-embedded.ftl`.
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_embedded_diagnostic::Diagnostic>> {
    next::diagnostic_samples()
}
