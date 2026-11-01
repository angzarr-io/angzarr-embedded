//! What an application's tests need of the kernel, published once (feature
//! `test-support`) instead of re-written in every crate:
//!
//! - [`block_on_ready`] (and [`Ready::ready`], its method form): run a
//!   future whose every await is ready at once, as the in-memory stores'
//!   are, with no runtime.
//! - [`Samples`]: a deterministic [`Sampled`] source, a second apart, each
//!   under its own command id.
//! - [`Harness`]: given/when/then on one stream of a domain, through the
//!   kernel ([`Kernel::handle`]) over angzarr-ports' in-memory store
//!   wrapped in its [`FaultyStore`], so a test can also fail the store or
//!   change it between calls.
//! - angzarr-ports' [`MemoryStore`] and [`FaultyStore`] (its `memory` and
//!   `faulty` features), re-exported: the store-agnostic half lives with
//!   the ports, and this module adds what needs the kernel.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

pub use angzarr_ports::faulty::{At, Fault, FaultyStore, Meanwhile, WriteRecord, INJECTED};
pub use angzarr_ports::memory::MemoryStore;

use crate::v1::{CommandBook, Cover, EventPage, Uuid};
use crate::{Domain, Failure, Handled, Registry, Rejected, Sampled};

/// Run `future`, whose every await is ready at once (an in-memory store's,
/// a fake's), to its end with no runtime. Panics if it waits.
pub fn block_on_ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(out) => out,
        Poll::Pending => {
            panic!("not ready at once: block_on_ready runs only futures that never wait")
        }
    }
}

/// [`block_on_ready`], as a method: `kernel.handle(..).ready()`.
pub trait Ready: Future + Sized {
    fn ready(self) -> Self::Output {
        block_on_ready(self)
    }
}

impl<F: Future> Ready for F {}

/// A deterministic [`Sampled`] source: the `n`th sample (from 1) is at
/// `start + n` seconds, under [`Samples::command_id`]`(n)`.
#[derive(Clone, Debug, Default)]
pub struct Samples {
    start: i64,
    taken: u32,
}

impl Samples {
    /// Samples from `start + 1` seconds on.
    pub fn starting_at(start: i64) -> Samples {
        Samples { start, taken: 0 }
    }

    /// The next sample.
    pub fn next_sample(&mut self) -> Sampled {
        let _ = (self.start, self.taken);
        todo!()
    }

    /// The `n`th sample's command id: distinct for each `n`, never nil.
    pub fn command_id(n: u32) -> Uuid {
        let _ = n;
        todo!()
    }
}

/// Given/when/then on the stream `cover` of the domain `D`, through the
/// kernel over an in-memory store that fails what the test says.
pub struct Harness<D: Domain> {
    registry: Registry,
    cover: Cover,
    store: FaultyStore<MemoryStore>,
    samples: Samples,
    domain: PhantomData<fn() -> D>,
}

impl<D: Domain> Harness<D> {
    /// The empty stream `cover`, its domain registered in `registry`,
    /// sampled from second 1 ([`Samples::starting_at`]`(0)`); retrying is
    /// off, so a retryable failure comes back at once.
    pub fn new(registry: Registry, cover: Cover) -> Harness<D> {
        let _ = (registry, cover);
        todo!()
    }

    /// The same harness, sampled from `samples` on.
    pub fn with_samples(self, samples: Samples) -> Harness<D> {
        let _ = (samples, &self.samples);
        todo!()
    }

    /// **Given**: `events` recorded on the stream, one write at the next
    /// sample, stamped as the kernel stamps a command's events.
    pub fn given(&mut self, events: impl IntoIterator<Item = D::Event>) -> &mut Harness<D> {
        let _ = events.into_iter();
        todo!()
    }

    /// **When**: `command` sent to the stream as a client sends it, at the
    /// next sample.
    pub fn when(&mut self, command: impl Into<D::Command>) -> Then<D> {
        let _ = command.into();
        todo!()
    }

    /// **When**: `book` handled as sent, at the next sample.
    pub fn when_book(&mut self, book: CommandBook) -> Then<D> {
        let _ = book;
        todo!()
    }

    /// The stream's pages as stored, oldest first.
    pub fn pages(&self) -> Vec<EventPage> {
        todo!()
    }

    /// The stream's state, replayed from the store as the next process
    /// would.
    pub fn state(&self) -> D::State {
        todo!()
    }

    /// The store: to fail it, change it between calls, or read it.
    pub fn store(&self) -> &FaultyStore<MemoryStore> {
        &self.store
    }

    /// The registry the kernel runs with.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The stream.
    pub fn cover(&self) -> &Cover {
        &self.cover
    }
}

/// **Then**: what one handling answered.
pub struct Then<D: Domain> {
    handled: Result<Handled, Failure<Rejected>>,
    domain: PhantomData<fn() -> D>,
}

impl<D: Domain> Then<D> {
    /// The events the decision recorded on the stream, decoded. Panics on
    /// anything but an applied command.
    pub fn events(self) -> Vec<D::Event> {
        todo!()
    }

    /// The refusal. Panics on anything else.
    pub fn refused(self) -> Rejected {
        todo!()
    }

    /// The events, or the failure.
    pub fn result(self) -> Result<Vec<D::Event>, Failure<Rejected>> {
        todo!()
    }

    /// The kernel's answer as it gave it.
    pub fn handled(self) -> Result<Handled, Failure<Rejected>> {
        self.handled
    }
}

#[cfg(test)]
#[path = "testing.test.rs"]
mod tests;
