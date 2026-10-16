//! Upcasting by type URL: a mirror of angzarr-router's `UpcasterDispatch`
//! (`crates/router/src/upcaster.rs`), over angzarr-proto's types.
//!
//! **The same semantics as the router's.** Rules are kept in registration
//! order. Every rule whose source type matches the event's *current* type
//! applies, and its output is the next rule's input, so V1 → V2 → V3
//! composes; the chain stops when no later rule matches. Events of other
//! types, pages without an event, and requests for another domain pass
//! through unchanged. A rule's source is a fully-qualified type name
//! (`pkg.v1.Foo`), compared with the type URL's name
//! ([`type_name`](crate::type_name)).
//!
//! **Why a mirror, not the router crate.** angzarr-router generates its own
//! `pb` types from the protos; until angzarr remaps it onto angzarr-proto,
//! its `EventPage` is not angzarr-proto's `EventPage`, and depending on it
//! would also pull in its dispatch engine, `tracing` and its own protoc
//! build. The mirror keeps the router's names (`on_event`, `upcast_event`,
//! `upcast_page`, `dispatch`, `source_types`), so moving onto the router's
//! own once it builds on angzarr-proto is a change of import.
//!
//! **What annals adds.** The router passes an unknown type through and
//! leaves the consequence to whoever decodes. annals keeps that, and makes
//! the decode strict: after upcasting, a type the domain does not decode is
//! a [`ReadError`](crate::ReadError) naming the type URL, never a skipped
//! event. Typed rules ([`Upcaster::rule`]) record their target type too, so
//! [`Registry::check`](crate::Registry::check) can refuse a chain that
//! cannot finish (a duplicate source, rules out of order, a dead end)
//! before anything is read.

use angzarr_proto::v1::{EventPage, UpcastRequest, UpcastResponse};
use annals_diagnostic::Report;
use prost::{Message, Name};
use prost_types::Any;

use crate::error::{UpcastChainError, UpcastError};

/// Transforms one event to its next schema version, as the router's
/// `UpcastFn` (with annals' error type).
pub type UpcastFn = Box<dyn Fn(&Any) -> Result<Any, UpcastError> + Send + Sync>;

/// One rule: its source type's full name, its target's when known (typed
/// rules), and the transform.
struct Rule {
    source: String,
    target: Option<String>,
    transform: UpcastFn,
}

/// The rule table for one domain's upcaster component.
pub struct Upcaster {
    name: String,
    domain: String,
    rules: Vec<Rule>,
}

impl Upcaster {
    /// An empty upcaster named `name`, for `domain`.
    pub fn new(name: impl Into<String>, domain: impl Into<String>) -> Self {
        Upcaster {
            name: name.into(),
            domain: domain.into(),
            rules: Vec::new(),
        }
    }

    /// Registers a rule transforming events of the fully-qualified
    /// `source_type`, as the router's. Its target is not known, so
    /// [`Registry::check`](crate::Registry::check) cannot follow it.
    pub fn on_event(
        mut self,
        source_type: &str,
        thunk: impl Fn(&Any) -> Result<Any, UpcastError> + Send + Sync + 'static,
    ) -> Self {
        self.rules.push(Rule {
            source: source_type.to_string(),
            target: None,
            transform: Box::new(thunk),
        });
        self
    }

    /// Registers a typed rule from `From` to `To`: unpack `From`, run
    /// `step`, pack `To` ([`pack`](crate::pack)). A step that cannot
    /// convert returns the domain's own error as a report.
    pub fn rule<From, To>(self, step: fn(From) -> Result<To, Report>) -> Self
    where
        From: Message + Name + Default + 'static,
        To: Message + Name + 'static,
    {
        todo!("proposal: a Rule {{ source: From::full_name(), target: Some(To::full_name()), .. }}")
    }

    /// The component name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The domain whose events this upcaster transforms.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// The registered source types, in registration order.
    pub fn source_types(&self) -> Vec<&str> {
        self.rules.iter().map(|r| r.source.as_str()).collect()
    }

    /// Runs one event through the rule chain.
    pub fn upcast_event(&self, event: &Any) -> Result<Any, UpcastError> {
        todo!("proposal: each rule in order whose source is the current type")
    }

    /// One page with its event (if any) run through the rule chain; the
    /// header, `created_at` and `meta` kept.
    pub fn upcast_page(&self, page: &EventPage) -> Result<EventPage, UpcastError> {
        todo!("proposal: as the router's")
    }

    /// `UpcasterService.Upcast`'s body: every page's event through the rule
    /// chain; a request for another domain unchanged.
    pub fn dispatch(&self, request: &UpcastRequest) -> Result<UpcastResponse, UpcastError> {
        todo!("proposal: as the router's")
    }

    /// Check the typed rules can all run, given the type URLs the domain
    /// decodes (its [`Payload::type_urls`](crate::Payload::type_urls)): no
    /// two from one source, none registered before the rule producing its
    /// source, and every chain ending at a decodable type. Untyped rules
    /// ([`Upcaster::on_event`]) are not followed. Every problem is listed.
    pub fn check(&self, current: &[String]) -> Result<(), Vec<UpcastChainError>> {
        todo!("proposal: follow each typed rule's target to the end of its chain")
    }
}
