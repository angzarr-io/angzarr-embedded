//! The command handler: a command and every reaction it sets off, as one
//! atomic chain, in process.
//!
//! [`Kernel::handle`] runs the book through the five steps (validate,
//! identify, deduplicate, load and resolve, decode and decide or ingest),
//! then synchronously runs the policies reacting to its events, whose
//! commands take the same five steps in their targets, whose events set off
//! their own policies, and so on down the chain. Every event stays **staged
//! in memory**, in a [`Chain`], across every stream involved; a later step
//! sees the staged state (and command ids) of any stream an earlier step
//! touched, the originating stream included. Nothing is appended until the
//! whole chain has succeeded.
//!
//! - **A duplicate ends it at once.** If step 3 finds the originating
//!   command's events on its stream, the answer is [`Handled::Duplicate`]
//!   with them; nothing runs, nothing is written.
//! - **Any failure fails it all.** A policy's command rejected, a malformed
//!   policy book, a panicking ingest, a gap, the depth limit: the failure
//!   goes back to the caller with the path to the step that failed
//!   ([`Failure::Downstream`]), and nothing is appended anywhere, the
//!   originating command's events included. What angzarr would dead-letter
//!   is also written to the [`DeadLetterLog`].
//! - **On success** every staged stream is appended through the unit of
//!   work, each checked against the length it was loaded at (optimistic
//!   concurrency on every stream touched), and the unit committed: one
//!   transaction. The command ids are written with the events, so
//!   deduplication needs nothing else.
//! - **Then projections run**, on committed events only. A failed chain is
//!   never seen by one, and a failing projection never undoes the commit:
//!   it is reported in [`Recorded::projector_failures`].
//!
//! **This is a deliberate divergence from angzarr** (see
//! `docs/design/audit.md`, *Divergences*): angzarr appends one stream per
//! write and runs reactions asynchronously, each its own command, so a
//! reaction's rejection never fails its trigger there. annals keeps policies
//! inside the write side's consistency boundary because it is the in-process
//! runtime: one process, one transaction, no bus, no outbox and no retry
//! loop. A reaction that must cross a process or a service boundary is not
//! one of these; it reads the committed log (a [`Position`](super::Position)
//! per stream, [`orders_owed`](super::orders_owed)) instead.
//!
//! Which policies run is a [`Routes`] table, keyed by source domain, so a
//! chain can come back to a stream it already touched (A → B → A); a
//! cascade deeper than the chain's limit is a [`KernelError::ChainTooDeep`],
//! which is how a reaction cycle that never settles ends.

use std::any::Any as AnyType;
use std::collections::{BTreeMap, HashMap};

use angzarr_proto::v1::{CommandBook, Cover, EventBook, Outcome};

use super::check::{Checked, Sampled};
use super::dead_letter::DeadLetterLog;
use super::domain::Domain;
use super::error::{Failure, KernelError, Step, StepError};
use super::handler::Handler;
use super::reaction::{Order, Policy, Projection};
use super::registry::Registry;
use super::store::{AsyncUnitOfWork, EventStore, UnitOfWork};
use super::stream::Envelope;

/// How deep a chain may go before it is a [`KernelError::ChainTooDeep`],
/// unless the kernel says otherwise ([`Kernel::limit`]).
pub const DEFAULT_DEPTH_LIMIT: usize = 8;

/// What the application builds once and handles every command through: the
/// registry, where dead letters go, and the chain's depth limit.
pub struct Kernel<'k> {
    registry: &'k Registry,
    dead_letters: &'k dyn DeadLetterLog,
    limit: usize,
}

/// What a handled command came to (angzarr's `Outcome.delivery`).
pub enum Handled {
    /// Business code ran, and the chain committed: everything it recorded
    /// (possibly nothing: a decision with no events).
    Applied(Recorded),
    /// Step 3 found the command's events already on its stream: these, in
    /// sequence order. Nothing ran and nothing was written.
    Duplicate(EventBook),
}

impl Handled {
    /// As angzarr's `Outcome`: APPLIED, with each projector failure as a
    /// `ProjectorFailure` (code INTERNAL), or DUPLICATE. annals never
    /// answers PARKED: in process, a panicking ingest fails the call.
    pub fn outcome(&self) -> Outcome {
        todo!("proposal: APPLIED with projector failures, or DUPLICATE")
    }

    /// The events to answer with: every stream the chain appended to, or
    /// the duplicate's.
    pub fn books(&self) -> Vec<EventBook> {
        todo!("proposal: Recorded::books, or the duplicate's book")
    }
}

impl<'k> Kernel<'k> {
    /// With [`DEFAULT_DEPTH_LIMIT`].
    pub fn new(registry: &'k Registry, dead_letters: &'k dyn DeadLetterLog) -> Kernel<'k> {
        Kernel {
            registry,
            dead_letters,
            limit: DEFAULT_DEPTH_LIMIT,
        }
    }

    /// Chains at most `limit` reaction steps deep.
    pub fn limit(mut self, limit: usize) -> Kernel<'k> {
        self.limit = limit;
        self
    }

    pub fn registry(&self) -> &'k Registry {
        self.registry
    }

    /// Handle one book for the `D` stream it names, with every reaction it
    /// sets off, as one atomic chain: [`check`](super::check) it,
    /// [`Chain::execute`] it, [`Chain::append`] everything staged,
    /// [`UnitOfWork::commit`], then run `projections`. Returns what the
    /// chain recorded, or the duplicate's events. On any failure nothing is
    /// committed and no projection runs. A cover of another domain than
    /// `D` is [`KernelError::WrongDomain`].
    pub fn handle<D, U, Ps>(
        &self,
        unit: U,
        routes: &Routes,
        book: CommandBook,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<D::Rejection>>
    where
        D: Domain,
        U: UnitOfWork,
        Ps: Project,
    {
        todo!("proposal: check, Chain::new(..).execute, append, commit, project")
    }

    /// [`Kernel::handle`], over the async ports: the store's reads, appends
    /// and commit are awaited; deciding stays synchronous and pure.
    pub async fn handle_async<D, U, Ps>(
        &self,
        unit: U,
        routes: &Routes,
        book: CommandBook,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<D::Rejection>>
    where
        D: Domain,
        U: AsyncUnitOfWork,
        Ps: Project,
    {
        todo!("proposal: as handle, awaiting the store")
    }

    /// Deliver one owed order outside a chain (a catch-up from
    /// [`orders_owed`](super::orders_owed)): the target's
    /// [`Reaction::position`](super::Reaction::position) places the source
    /// event first (a redelivery runs nothing and answers
    /// [`Handled::Applied`] with nothing recorded; a gap is
    /// [`KernelError::Gap`]); then the order runs as a chain of its own,
    /// with the derived command id, so a delivery that already landed is
    /// answered DUPLICATE.
    pub fn deliver<P, U, Ps>(
        &self,
        unit: U,
        routes: &Routes,
        order: &Order<P>,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<<P::Target as Domain>::Rejection>>
    where
        P: Policy,
        U: UnitOfWork,
        Ps: Project,
    {
        todo!("proposal: place by position, then order.check and a chain")
    }
}

/// One policy, its types erased so a [`Routes`] table can hold policies of
/// any domains, and so the table names no store: what the policy owes for
/// the events a source stream (a `Stream<Source>`, boxed) staged from
/// `from` on, each order booked and checked through the kernel's reaction
/// entry, with the step it is. Pure: the chain loads, deduplicates and
/// decides the orders' targets itself, the typed step reached through the
/// [`Registry`] by the target's domain name.
type Route = Box<
    dyn Fn(&dyn AnyType, u64, &Registry) -> Result<Vec<(Step, Checked)>, (Vec<Step>, StepError)>
        + Send
        + Sync,
>;

/// Which policies react to which source domain, in the order registered.
/// Built once at start-up, independent of any store, and shared by every
/// command, sync or async.
#[derive(Default)]
pub struct Routes {
    by_source: BTreeMap<&'static str, Vec<Route>>,
}

impl Routes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run `P` after any `P::Source` stream stages events: each order it
    /// owes is checked, identified and executed in its target, in the same
    /// chain. Both domains must be registered.
    pub fn policy<P: Policy>(self) -> Self {
        todo!("proposal: box a closure that owes P's orders and checks each")
    }
}

/// One staged stream: the sequence it was loaded at, and the stream (a
/// `Stream<D>`, boxed) with the chain's events committed to it in memory.
struct Staged {
    loaded_at: u64,
    stream: Box<dyn AnyType + Send>,
}

/// A command chain in progress: every stream it has touched, loaded once
/// from the unit of work and staged since, the path from the originating
/// command to the step running now, and what the application sampled for
/// the whole chain (one instant; one minted command id, for the
/// originating command only).
pub struct Chain<'u, U> {
    unit: &'u mut U,
    registry: &'u Registry,
    routes: &'u Routes,
    sampled: &'u Sampled,
    dead_letters: &'u dyn DeadLetterLog,
    /// Looked up only; the order streams are appended in is `order`.
    staged: HashMap<Cover, Staged>,
    order: Vec<Cover>,
    path: Vec<Step>,
    limit: usize,
    /// Mints each delivery's [`Nonce`](super::Nonce).
    nonces: u64,
}

impl<'u, U> Chain<'u, U> {
    /// An empty chain over `unit`.
    pub fn new(
        kernel: &Kernel<'u>,
        unit: &'u mut U,
        routes: &'u Routes,
        sampled: &'u Sampled,
    ) -> Chain<'u, U> {
        Chain {
            unit,
            registry: kernel.registry,
            routes,
            sampled,
            dead_letters: kernel.dead_letters,
            staged: HashMap::new(),
            order: Vec::new(),
            path: Vec::new(),
            limit: kernel.limit,
            nonces: 0,
        }
    }

    /// `stream`'s state as the chain has it, staged events folded in, if
    /// the chain has touched it and it is a `D` stream.
    pub fn state<D: Domain>(&self, stream: &Cover) -> Option<&D::State> {
        todo!("proposal: downcast the staged Stream<D>")
    }

    /// The path from the originating command to the step running now.
    pub fn path(&self) -> &[Step] {
        &self.path
    }

    /// Run a checked command through steps 2 to 5 in the stream it names
    /// (deduplicating against the store and the chain's staged events;
    /// loading from the store the first time the chain touches the stream,
    /// from the staged copy after), stage its events, then run the routes
    /// for `D` on them, depth first. The chain's first `execute` is the
    /// originating command (a client's command or fact); a route's are the
    /// rest, always policy commands. `Some`: step 3 found the command's
    /// events, which are the answer, and nothing was staged.
    pub fn execute<D>(
        &mut self,
        command: Checked,
    ) -> Result<Option<EventBook>, Failure<D::Rejection>>
    where
        D: Domain,
        U: EventStore,
    {
        todo!(
            "proposal: identify, deduplicate, load or find, decode, decide or ingest, stage, react"
        )
    }

    /// The whole chain succeeded: append every staged stream through the
    /// unit of work, in the order first touched, each expecting the length
    /// it was loaded at. A conflict on any stream is a
    /// [`StoreError::Conflict`](super::StoreError::Conflict) naming it; the
    /// caller then drops the unit of work uncommitted.
    pub fn append(self) -> Result<Recorded, KernelError>
    where
        U: UnitOfWork,
    {
        todo!("proposal: append each staged stream's new pages")
    }
}

/// Everything a chain recorded, every stream, after commit: angzarr's
/// `CommandResponse.events`, one `EventBook` per stream here, since a chain
/// touches several.
pub struct Recorded {
    /// One book per stream touched, in the order first touched: the pages
    /// appended, `next_sequence` the new length.
    books: Vec<EventBook>,
    /// The streams after the chain (each a `Stream<D>`, boxed), with the
    /// length each was loaded at.
    streams: Vec<(Cover, u64, Box<dyn AnyType + Send>)>,
    /// Projections that failed after the commit.
    projector_failures: Vec<ProjectorFailure>,
}

impl Recorded {
    /// What the chain appended, as `EventBook`s: every page with its
    /// `EventMeta`.
    pub fn books(&self) -> &[EventBook] {
        &self.books
    }

    /// The events the chain recorded in `stream`, if it is a `D` stream the
    /// chain touched.
    pub fn events<D: Domain>(&self, stream: &Cover) -> Option<&[Envelope<D::Event>]> {
        todo!("proposal: downcast, slice from the loaded length")
    }

    /// Hand the chain's events in `stream` to `handlers`, each with the
    /// state just after it.
    pub fn dispatch<D: Domain>(
        &self,
        stream: &Cover,
        registry: &Registry,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<(), KernelError> {
        todo!("proposal: Stream::dispatch from the loaded length")
    }

    /// The projections that failed on the chain's events (angzarr's
    /// `Outcome.projector_failures`). The commit stands regardless.
    pub fn projector_failures(&self) -> &[ProjectorFailure] {
        &self.projector_failures
    }
}

/// One projection that failed after the commit.
#[derive(Clone, PartialEq, Debug)]
pub struct ProjectorFailure {
    /// The [`ProjectionRoute::name`].
    pub projector: &'static str,
    pub error: KernelError,
}

/// Projections the command handler runs after commit, over what the chain
/// recorded: [`ProjectionRoute`]s; `()` is none, a pair is both. Each
/// returns the projections that failed; a failure never stops the others.
pub trait Project {
    fn project(&mut self, recorded: &Recorded, registry: &Registry) -> Vec<ProjectorFailure>;
}

/// A projection of one domain's streams, run after commit over every
/// stream of that domain the chain recorded in.
pub struct ProjectionRoute<'p, D: Domain, P: Projection<D>> {
    /// Named in a [`ProjectorFailure`].
    pub name: &'static str,
    pub projection: &'p mut P,
    pub domain: std::marker::PhantomData<fn() -> D>,
}

impl Project for () {
    fn project(&mut self, recorded: &Recorded, registry: &Registry) -> Vec<ProjectorFailure> {
        Vec::new()
    }
}

impl<A: Project, B: Project> Project for (A, B) {
    fn project(&mut self, recorded: &Recorded, registry: &Registry) -> Vec<ProjectorFailure> {
        let mut failures = self.0.project(recorded, registry);
        failures.extend(self.1.project(recorded, registry));
        failures
    }
}

impl<D: Domain, P: Projection<D>> Project for ProjectionRoute<'_, D, P> {
    fn project(&mut self, recorded: &Recorded, registry: &Registry) -> Vec<ProjectorFailure> {
        todo!("proposal: dispatch each D stream recorded through projection()")
    }
}
