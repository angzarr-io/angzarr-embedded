//! A chain driven step by step, for tests that look inside one: what it
//! has staged, the path to the step running, several commands in one
//! chain. Over the kernel's own chain ([`Run`](super::Run)); the public
//! entry is [`Kernel::handle`](super::Kernel::handle).

use crate::next::check::{Checked, Sampled};
use crate::next::domain::Domain;
use crate::next::error::{Failure, KernelError, Step};
use crate::next::store::{block, EventStore, SyncPort, UnitOfWork};
use crate::next::stream::Stream;
use crate::next::v1::{Cover, EventBook};

use super::{Kernel, Recorded, Routes, Run};

/// A chain over `unit`, run a command at a time.
pub struct Chain<'u, U> {
    unit: &'u mut U,
    run: Run<'u>,
}

impl<'u, U> Chain<'u, U> {
    pub fn new(
        kernel: &Kernel<'u>,
        unit: &'u mut U,
        routes: &'u Routes,
        sampled: &'u Sampled,
    ) -> Chain<'u, U> {
        Chain {
            unit,
            run: Run::new(kernel, routes, sampled),
        }
    }

    /// `stream`'s state as the chain has it, staged events folded in, if
    /// the chain has touched it and it is a `D` stream.
    pub fn state<D: Domain>(&self, stream: &Cover) -> Option<&D::State> {
        let staged = self.run.staged.get(stream)?.stream.as_ref()?;
        staged.downcast_ref::<Stream<D>>().map(Stream::state)
    }

    /// The path from the originating command to the step running now.
    pub fn path(&self) -> &[Step] {
        &self.run.path
    }

    /// One command and its reactions, staged: `Some` for a duplicate.
    pub fn execute<D>(
        &mut self,
        command: Checked,
    ) -> Result<Option<EventBook>, Failure<D::Rejection>>
    where
        D: Domain,
        U: EventStore,
    {
        block(
            self.run
                .execute::<D, _>(&mut SyncPort(&mut *self.unit), command),
        )
    }

    /// Everything staged, appended through the unit, uncommitted.
    pub fn append(self) -> Result<Recorded, KernelError>
    where
        U: UnitOfWork,
    {
        block(self.run.append(&mut SyncPort(self.unit)))
    }
}
