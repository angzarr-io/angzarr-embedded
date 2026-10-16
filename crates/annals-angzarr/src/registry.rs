//! What the adapter knows about each domain: its upcaster, how its events
//! become an `EventBook`, and which type URLs it decodes.

use std::collections::BTreeMap;

use angzarr_proto::v1::EventBook;
use annals::next::{Domain, Recorded, StreamKey};

use crate::error::{BookError, UpcastChainError};
use crate::payload::Payload;
use crate::store::Opaque;
use crate::upcast::Upcaster;

/// One domain's entry.
struct Entry {
    name: &'static str,
    upcaster: Upcaster,
    book: fn(&Recorded, &StreamKey) -> Option<EventBook>,
    type_urls: fn() -> Vec<String>,
}

/// Every domain an application stores, by name. Built once at start-up and
/// shared (it is `Send + Sync`).
#[derive(Default)]
pub struct Registry {
    domains: BTreeMap<&'static str, Entry>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store `D`'s events as `Any`, upcasting old types with `upcaster`
    /// (whose domain must be `D::NAME`: [`Registry::check`]).
    pub fn domain<D: Domain>(self, upcaster: Upcaster) -> Self
    where
        D::Event: Payload,
    {
        todo!("proposal: insert D::NAME -> Entry {{ upcaster, event_book::<D>, D::Event::type_urls }}")
    }

    /// `domain`'s upcaster, if registered.
    pub fn upcaster(&self, domain: &str) -> Option<&Upcaster> {
        self.domains.get(domain).map(|e| &e.upcaster)
    }

    /// Check every domain's upcaster against the types the domain decodes
    /// ([`Upcaster::check`]) and its domain name. For a test that runs in
    /// CI beside the golden replay fixtures
    /// ([`assert_replay_fixtures`](crate::assert_replay_fixtures)).
    pub fn check(&self) -> Result<(), Vec<UpcastChainError>> {
        todo!("proposal: each entry's upcaster.check(type_urls()), and its domain")
    }

    /// What a chain recorded, as `EventBook`s: one per stream it touched, in
    /// the order first touched ([`Recorded::streams`]). Refuses a stream of a
    /// domain not registered.
    pub fn event_books(&self, recorded: &Recorded) -> Result<Vec<EventBook>, BookError> {
        todo!("proposal: each stream's entry.book")
    }

    /// `stored` (the application's opaque store port, a unit of work) as a
    /// typed store for the kernel: see [`Opaque`].
    pub fn store<S>(&self, stored: S) -> Opaque<'_, S> {
        Opaque {
            stored,
            registry: self,
        }
    }
}
