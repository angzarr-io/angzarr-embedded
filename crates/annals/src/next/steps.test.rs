//! Steps 3 to 5 driven one at a time, for tests that check each step's
//! output: a coordinator over a unit of work, and the steps' accessors.
//! The chain ([`Kernel::handle`](crate::next::Kernel::handle)) runs the
//! same functions.

use angzarr_proto::v1::{ConflictResolution, EventPage, Snapshot};

use crate::next::check::Identified;
use crate::next::domain::Domain;
use crate::next::error::KernelError;
use crate::next::registry::Registry;
use crate::next::store::{Store, StorePort};
use crate::next::stream::Stream;
use crate::next::toys::ready;

use super::{deduplicate, load, prepare, Contextual, Deduplicated, Novel, Prepared};

/// Steps 3 and 4 over a store.
pub struct Coordinator<'s, S> {
    store: &'s S,
    registry: &'s Registry,
    nonces: u64,
}

impl<'s, S: Store> Coordinator<'s, S> {
    pub fn new(store: &'s S, registry: &'s Registry) -> Coordinator<'s, S> {
        Coordinator {
            store,
            registry,
            nonces: 0,
        }
    }

    /// Step 3: load, then look the command id up.
    pub fn deduplicate(&mut self, command: Identified) -> Result<Deduplicated, KernelError> {
        ready(deduplicate(
            &mut StorePort(self.store),
            self.registry,
            command,
        ))
    }

    /// Step 4: stamp or check the expected sequence.
    pub fn load(&mut self, command: Box<Novel>) -> Result<Contextual, KernelError> {
        load(*command, &mut self.nonces)
    }
}

impl Novel {
    pub fn command(&self) -> &Identified {
        &self.command
    }

    /// The stream's next sequence when it was loaded.
    pub fn head(&self) -> u64 {
        self.loaded.head
    }
}

impl Contextual {
    pub fn conflict_resolution(&self) -> ConflictResolution {
        self.resolution
    }

    pub fn expected_sequence(&self) -> u64 {
        self.expected
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    pub fn pages(&self) -> &[EventPage] {
        &self.pages
    }

    /// The events from the expected sequence to the head.
    pub fn window(&self) -> &[EventPage] {
        let start = self.pages.partition_point(|p| p.sequence < self.expected);
        &self.pages[start..]
    }

    pub fn has_nonce(&self) -> bool {
        self.nonce.is_some()
    }

    /// Step 5's first half, typed.
    pub fn decode<D: Domain>(self, registry: &Registry) -> Result<Prepared<D>, KernelError> {
        prepare(self, None, registry)
    }
}

impl<D: Domain> Prepared<D> {
    pub fn command(&self) -> &Identified {
        &self.command
    }

    pub fn stream(&self) -> &Stream<D> {
        &self.stream
    }
}
