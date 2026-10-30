//! The exposure seam: what the host's listener (later, one gRPC server
//! serving `CommandHandlerCoordinatorService`, routed by `cover.domain`)
//! calls for each request, [`App::serve`](crate::App::serve). It takes the
//! request's book and where the request arrived from, and answers as
//! `CommandHandlerCoordinatorService.Handle` does: a `CommandResponse`, or
//! a refusal. A domain is served only when its `[[component]]`'s `expose`
//! admits the arrival ([`Expose`]): a request on loopback reaches a
//! component exposed `"loopback"` or `"ip"`, one from the network only a
//! component exposed `"ip"`.
//!
//! RED (design-by-test, 2026-10-30): the shapes here are proposed for
//! review; `App::serve` is `todo!()`.

use std::fmt;

use angzarr_embedded::next::{Failure, Rejected};
use angzarr_embedded_diagnostic::{Arg, Diagnostic};

use crate::config::Expose;

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

/// Why [`App::serve`](crate::App::serve) refused a request.
#[derive(Clone, Debug, PartialEq)]
pub enum Refused {
    /// The domain's component is not exposed to where the request arrived
    /// from: `exposed` is its `expose` (`"none"` for a domain the config
    /// does not name).
    NotExposed {
        domain: String,
        exposed: Expose,
        arrived: Arrival,
    },
    /// The kernel's own refusal, as a local caller gets it: an unknown
    /// domain, a malformed book, the domain's rejection, a failure.
    Failed(Failure<Rejected>),
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_embedded_diagnostic::display(self, f)
    }
}

impl Diagnostic for Refused {
    fn code(&self) -> &'static str {
        match self {
            Refused::NotExposed { .. } => "angzarr-embedded-host-not-exposed",
            Refused::Failed(failure) => failure.code(),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            Refused::NotExposed {
                domain,
                exposed,
                arrived,
            } => vec![
                ("domain", domain.as_str().into()),
                ("exposed", exposed.as_str().into()),
                ("arrived", arrived.as_str().into()),
            ],
            Refused::Failed(failure) => failure.args(),
        }
    }
}

/// One value of each of this module's own errors, for the catalog parity
/// test.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    vec![Box::new(Refused::NotExposed {
        domain: "player".to_string(),
        exposed: Expose::Loopback,
        arrived: Arrival::Ip,
    })]
}
