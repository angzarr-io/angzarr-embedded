//! Upcasting by type URL: a mirror of angzarr-router's `UpcasterDispatch`
//! (`crates/router/src/upcaster.rs`), over angzarr-proto's types.
//!
//! **The router's semantics.** Rules are kept in registration order. Every
//! rule whose source type matches the payload's *current* type applies, and
//! its output is the next rule's input, so V1 → V2 → V3 composes; the
//! chain stops when no later rule matches. Other types pass through
//! unchanged. A rule's source is a fully-qualified type name
//! (`pkg.v1.Foo`), compared with the type URL's name
//! ([`type_name`](super::type_name)).
//!
//! **Events and commands, one chain.** The same rules run on a stored
//! event's payload at read time and on a command's (or fact's) payload
//! before it is decoded ([`Upcaster::upcast_command`]). Upcasting commands
//! is **discouraged**: it hides stale clients, which keep working so nobody
//! upgrades them, and it is machinery for data that is never stored. Prefer
//! evolving a command compatibly (add fields), or decoding both versions in
//! the domain for a transition; upcast a command only as a migration aid
//! with a planned removal. Event upcasters are permanent; stored events
//! never go away.
//!
//! **Strict.** The router passes an unknown type through and leaves the
//! consequence to whoever decodes. annals keeps that, and makes the decode
//! strict: after upcasting, a type the domain does not decode is refused
//! naming its type URL ([`KernelError::UnknownType`](super::KernelError::UnknownType)
//! for a command, [`StoreError::Unreadable`](super::StoreError::Unreadable)
//! for an event), never skipped. Typed rules ([`Upcaster::rule`]) record
//! their target type too, so [`Registry::check`](super::Registry::check)
//! can refuse a chain that cannot finish (a duplicate source, rules out of
//! order, a dead end) before anything is read.
//!
//! **Why a mirror, not the router crate.** angzarr-router generates its own
//! `pb` types; until angzarr remaps it onto angzarr-proto its `EventPage`
//! is not angzarr-proto's, and depending on it would pull in its dispatch
//! engine and its own protoc build. The mirror keeps the router's names
//! (`on_event`, `upcast_event`, `upcast_page`, `dispatch`, `source_types`),
//! so moving onto the router's own is a change of import.

use angzarr_proto::v1::{EventPage, UpcastRequest, UpcastResponse};
use annals_diagnostic::Report;
use prost::{Message, Name};
use prost_types::Any;

use super::error::{UpcastChainError, UpcastError};

/// Transforms one payload to its next schema version, as the router's
/// `UpcastFn` (with annals' error type).
pub type UpcastFn = Box<dyn Fn(&Any) -> Result<Any, UpcastError> + Send + Sync>;

/// One rule: its source type's full name, its target's when known (typed
/// rules), and the transform.
struct Rule {
    source: String,
    target: Option<String>,
    transform: UpcastFn,
}

/// The rule table for one domain.
pub struct Upcaster {
    name: String,
    domain: String,
    rules: Vec<Rule>,
}

impl Upcaster {
    /// An empty upcaster named `name`, for `domain`: what a domain with
    /// nothing to upcast registers.
    pub fn new(name: impl Into<String>, domain: impl Into<String>) -> Self {
        Upcaster {
            name: name.into(),
            domain: domain.into(),
            rules: Vec::new(),
        }
    }

    /// Registers a rule transforming payloads of the fully-qualified
    /// `source_type`, as the router's. Its target is not known, so
    /// [`Registry::check`](super::Registry::check) cannot follow it.
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
    /// `step`, pack `To` ([`pack`](super::pack)). A step that cannot convert
    /// returns the domain's own error as a report.
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

    /// The domain whose payloads this upcaster transforms.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// The registered source types, in registration order.
    pub fn source_types(&self) -> Vec<&str> {
        self.rules.iter().map(|r| r.source.as_str()).collect()
    }

    /// Runs one stored event's payload through the rule chain.
    pub fn upcast_event(&self, event: &Any) -> Result<Any, UpcastError> {
        todo!("proposal: each rule in order whose source is the current type")
    }

    /// Runs one command's (or fact's) payload through the same rule chain,
    /// before it is decoded. Discouraged; see the module docs.
    pub fn upcast_command(&self, command: &Any) -> Result<Any, UpcastError> {
        todo!("proposal: the same chain as upcast_event")
    }

    /// One page with its payload (if any) run through the rule chain; its
    /// `sequence` and `meta` kept.
    pub fn upcast_page(&self, page: &EventPage) -> Result<EventPage, UpcastError> {
        todo!("proposal: as the router's")
    }

    /// `UpcasterService.Upcast`'s body: every page's event through the rule
    /// chain; a request for another domain unchanged.
    pub fn dispatch(&self, request: &UpcastRequest) -> Result<UpcastResponse, UpcastError> {
        todo!("proposal: as the router's")
    }

    /// Check the typed rules can all run, given the type URLs the domain
    /// decodes: no two from one source, none registered before the rule
    /// producing its source, and every chain ending at a decodable type.
    /// Untyped rules ([`Upcaster::on_event`]) are not followed. Every
    /// problem is listed.
    pub fn check(&self, current: &[String]) -> Result<(), Vec<UpcastChainError>> {
        todo!("proposal: follow each typed rule's target to the end of its chain")
    }
}
