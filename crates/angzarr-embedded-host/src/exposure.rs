//! The exposure seam: what the host's listener (later, one gRPC server
//! serving `CommandHandlerCoordinatorService`, routed by `cover.domain`)
//! calls for each request, [`App::serve`](crate::App::serve). It takes the
//! request's book and where the request arrived from, and answers as
//! `CommandHandlerCoordinatorService.Handle` does: a `CommandResponse`, or
//! the kernel's refusal. A domain is served only when its `[[component]]`'s
//! `expose` admits the arrival ([`Expose`](crate::config::Expose)): a
//! request on loopback reaches a component exposed `"loopback"` or `"ip"`,
//! one from the network only a component exposed `"ip"`. A domain not
//! exposed to the arrival is answered exactly as an unknown domain is (the
//! kernel's `Malformed::UnknownDomain`), so a caller cannot tell a hidden
//! domain from a missing one; the host logs the real reason locally.
//!
//! RED (design-by-test, 2026-10-30): the shapes here are proposed for
//! review; `App::serve` is `todo!()`.

/// Where a request to the host's listener arrived from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arrival {
    /// This machine: a loopback address.
    Loopback,
    /// The network: any other address.
    Ip,
}

impl Arrival {
    /// As an arg: `"loopback"` or `"ip"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Arrival::Loopback => "loopback",
            Arrival::Ip => "ip",
        }
    }
}
