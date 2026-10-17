//! The command handler: a command and every reaction it sets off, as one
//! atomic chain, in process.
//!
//! [`handle`] handles the book (deciding a command, or ingesting a fact),
//! then synchronously runs the policies reacting to its events, whose
//! commands are decided in their targets, whose events set off their own
//! policies, and so on down the chain. Every event stays **staged in
//! memory**, in a [`Chain`], across every stream involved; a later step sees
//! the staged state of any stream an earlier step touched, the originating
//! stream included. Nothing is appended until the whole chain has
//! succeeded.
//!
//! - **Any failure fails it all.** A policy's command rejected, a gap, the
//!   depth limit: the failure goes back to the caller with the path to the
//!   step that failed ([`Failure::Downstream`]), and nothing is appended
//!   anywhere, the originating command's events included.
//! - **On success** every staged stream is appended through the unit of
//!   work, each checked against the length it was loaded at (optimistic
//!   concurrency on every stream touched), the originating command's key is
//!   recorded if it has one ([`UnitOfWork::record_command`]), and the unit
//!   of work committed: one transaction.
//! - **Then projections run**, on committed events only. A failed chain is
//!   never seen by one, and a failing projection never undoes the commit:
//!   it is reported in [`Recorded::projector_failures`].
//!
//! **This is a deliberate divergence from angzarr** (see
//! `docs/design/audit.md`, *Divergences*): angzarr appends one stream per
//! write and runs reactions asynchronously, each its own command. annals
//! keeps policies inside the write side's consistency boundary because it
//! is the in-process runtime: one process, one transaction, no bus, no
//! outbox and no retry loop. A reaction that must cross a process or a
//! service boundary is not one of these; it reads the committed log (a
//! [`Position`] per stream, [`orders_owed`]) instead.
//!
//! Which policies run is a [`Routes`] table, keyed by source domain, so a
//! chain can come back to a stream it already touched (A → B → A); a
//! cascade deeper than the chain's limit is a
//! [`KernelError::ChainTooDeep`], which is how a reaction cycle that never
//! settles ends.
//!
//! [`handle`] is request/response: it returns every event the chain
//! recorded ([`Recorded`]), so an API handler can answer with them (a draw
//! command answers with its `Drawn` event).
//!
//! [`Position`]: super::Position
//! [`orders_owed`]: super::orders_owed

use std::any::{Any, TypeId};
use std::collections::BTreeMap;

use super::book::{BookOf, Sampled};
use super::domain::Domain;
use super::error::{Failure, KernelError, Step, StepError};
use super::handler::Handler;
use super::id::{Seq, StreamKey};
use super::meta::Origin;
use super::reaction::{Policy, Projection};
use super::store::{AsyncEventStore, AsyncUnitOfWork, EventStore, UnitOfWork};
use super::stream::Envelope;
use super::version::Rulebook;

/// How deep a chain may go before it is a [`KernelError::ChainTooDeep`],
/// unless the caller says otherwise ([`Chain::limit`]).
pub const DEFAULT_DEPTH_LIMIT: usize = 8;

/// One reaction, its types erased so a [`Routes`] table can hold reactions
/// of any domains: run it on what `source` staged from `from` on.
type Route<'r, U> = Box<
    dyn Fn(&mut Chain<'_, 'r, U>, &StreamKey, Seq) -> Result<(), (Vec<Step>, StepError)>
        + Send
        + Sync
        + 'r,
>;

/// Which policies react to which source domain, in the
/// order registered, with the rules of both ends. Built once; shared by
/// every command. The async command handler takes an [`AsyncRoutes`].
pub struct Routes<'r, U> {
    by_source: BTreeMap<TypeId, Vec<Route<'r, U>>>,
}

impl<'r, U> Default for Routes<'r, U> {
    fn default() -> Self {
        Routes {
            by_source: BTreeMap::new(),
        }
    }
}

impl<'r, U: 'r> Routes<'r, U> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run `P` after any `P::Source` stream stages events: each command it
    /// owes is executed in its target, in the same chain.
    pub fn policy<P: Policy>(
        self,
        source: &'r Rulebook<<P::Source as Domain>::Rules>,
        target: &'r Rulebook<<P::Target as Domain>::Rules>,
    ) -> Self
    where
        U: EventStore<<P::Source as Domain>::Event> + EventStore<<P::Target as Domain>::Event>,
    {
        todo!("proposal: box a closure dispatching through policy() and chain.execute")
    }
}

/// [`Routes`] for the async command handler: the same table, its entries
/// loading through [`AsyncEventStore`] (boxed futures inside, std only).
pub struct AsyncRoutes<'r, U> {
    by_source: BTreeMap<TypeId, Vec<Route<'r, U>>>,
}

impl<'r, U: 'r> AsyncRoutes<'r, U> {
    pub fn new() -> Self {
        AsyncRoutes {
            by_source: BTreeMap::new(),
        }
    }

    /// As [`Routes::policy`].
    pub fn policy<P: Policy>(
        self,
        source: &'r Rulebook<<P::Source as Domain>::Rules>,
        target: &'r Rulebook<<P::Target as Domain>::Rules>,
    ) -> Self
    where
        U: AsyncEventStore<<P::Source as Domain>::Event>
            + AsyncEventStore<<P::Target as Domain>::Event>,
    {
        todo!("proposal: as Routes::policy, async")
    }
}

impl<'r, U: 'r> Default for AsyncRoutes<'r, U> {
    fn default() -> Self {
        Self::new()
    }
}

/// A command chain in progress: every stream it has touched, loaded once
/// from the unit of work and staged since, the path from the originating
/// command to the step running now, and what the application sampled for
/// the whole chain (one instant; the minted correlation, for the origin).
pub struct Chain<'u, 'r, U> {
    store: &'u mut U,
    routes: &'u Routes<'r, U>,
    sampled: &'u Sampled,
    /// Each touched stream: the length it was loaded at, and the stream
    /// (a `Stream<D>`, boxed with how to append it) with the chain's events
    /// committed to it in memory.
    staged: BTreeMap<StreamKey, (Seq, Box<dyn Any + Send>)>,
    /// The order streams were first touched in: the order they are
    /// appended in.
    order: Vec<StreamKey>,
    /// The originating book's stream and origin, recorded at append when
    /// keyed ([`UnitOfWork::record_command`]).
    origin: Option<(StreamKey, Origin)>,
    path: Vec<Step>,
    limit: usize,
    /// Mints each delivery's [`Nonce`](super::Nonce): a counter, so the
    /// kernel needs no randomness.
    nonces: u64,
}

impl<'u, 'r, U> Chain<'u, 'r, U> {
    /// An empty chain over `store`, reacting by `routes`, with
    /// [`DEFAULT_DEPTH_LIMIT`].
    pub fn new(
        store: &'u mut U,
        routes: &'u Routes<'r, U>,
        sampled: &'u Sampled,
    ) -> Chain<'u, 'r, U> {
        Chain {
            store,
            routes,
            sampled,
            staged: BTreeMap::new(),
            order: Vec::new(),
            origin: None,
            path: Vec::new(),
            limit: DEFAULT_DEPTH_LIMIT,
            nonces: 0,
        }
    }

    /// At most `limit` reaction steps deep.
    pub fn limit(mut self, limit: usize) -> Chain<'u, 'r, U> {
        self.limit = limit;
        self
    }

    /// `stream`'s state as the chain has it, staged events folded in, if
    /// the chain has touched it and it is a `D` stream.
    pub fn state<D: Domain>(&self, stream: &StreamKey) -> Option<&D::State> {
        todo!("proposal: downcast the staged Stream<D>")
    }

    /// The path from the originating command to the step running now.
    pub fn path(&self) -> &[Step] {
        &self.path
    }

    /// Handle `book` in the stream it names (loaded from the store the
    /// first time the chain touches it, staged after), as
    /// [`Stream::decide`](super::Stream::decide) does, stage its events,
    /// then run the routes for `D` on them, depth first. A chain's first
    /// `execute` is the originating book (a command or a fact); a policy
    /// route's are the rest, always commands. A key of another domain than
    /// `D` is a [`KernelError::WrongDomain`].
    pub fn execute<D>(
        &mut self,
        book: &BookOf<D>,
        rules: &Rulebook<D::Rules>,
    ) -> Result<(), Failure<D::Rejection>>
    where
        D: Domain,
        U: EventStore<D::Event>,
    {
        todo!("proposal: load or find, decide or ingest, stage, react")
    }

    /// The whole chain succeeded: append every staged stream through the
    /// unit of work, in the order first touched, each expecting the length
    /// it was loaded at, then record the originating book's key if it has
    /// one. A conflict on any stream is a
    /// [`StoreError::Conflict`](super::StoreError::Conflict) naming it, and
    /// a recorded key a [`StoreError::Duplicate`](super::StoreError::Duplicate);
    /// the caller then drops the unit of work uncommitted.
    pub fn append(self) -> Result<Recorded, KernelError>
    where
        U: UnitOfWork,
    {
        todo!("proposal: append each staged stream's new events, then record_command")
    }
}

/// Everything a chain recorded, every stream, after commit: angzarr's
/// `CommandResponse.events` (one book per stream here, since a chain
/// touches several), its delivery APPLIED.
pub struct Recorded {
    /// Each stream touched, in the order first touched: the length it was
    /// loaded at, and the stream (a `Stream<D>`, boxed) after the chain.
    streams: Vec<(StreamKey, Seq, Box<dyn Any + Send>)>,
    /// Projections that failed after the commit.
    projector_failures: Vec<ProjectorFailure>,
}

impl Recorded {
    /// Every stream the chain appended to, in order, with the seqs it
    /// appended: `from..to`.
    pub fn streams(&self) -> Vec<(StreamKey, Seq, Seq)> {
        todo!("proposal: (id, loaded length, new length)")
    }

    /// The events the chain recorded in `stream`, if it is a `D` stream the
    /// chain touched.
    pub fn events<D: Domain>(&self, stream: &StreamKey) -> Option<&[Envelope<D::Event>]> {
        todo!("proposal: downcast, slice from the loaded length")
    }

    /// Hand the chain's events in `stream` to `handlers`, each with the
    /// state just after it.
    pub fn dispatch<D: Domain>(
        &self,
        stream: &StreamKey,
        rules: &Rulebook<D::Rules>,
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
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProjectorFailure {
    /// The [`ProjectionRoute::name`].
    pub projector: &'static str,
    pub error: KernelError,
}

/// Projections the command handler runs after commit, over what the chain
/// recorded: [`ProjectionRoute`]s; `()` is none, a pair is both. Each
/// returns the projections that failed; a failure never stops the others.
pub trait Project {
    fn project(&mut self, recorded: &Recorded) -> Vec<ProjectorFailure>;
}

/// A projection of one domain's streams, run after commit over every
/// stream of that domain the chain recorded in.
pub struct ProjectionRoute<'p, D: Domain, P: Projection<D>> {
    /// Named in a [`ProjectorFailure`].
    pub name: &'static str,
    pub projection: &'p mut P,
    pub rules: &'p Rulebook<D::Rules>,
}

impl Project for () {
    fn project(&mut self, recorded: &Recorded) -> Vec<ProjectorFailure> {
        Vec::new()
    }
}

impl<A: Project, B: Project> Project for (A, B) {
    fn project(&mut self, recorded: &Recorded) -> Vec<ProjectorFailure> {
        let mut failures = self.0.project(recorded);
        failures.extend(self.1.project(recorded));
        failures
    }
}

impl<D: Domain, P: Projection<D>> Project for ProjectionRoute<'_, D, P> {
    fn project(&mut self, recorded: &Recorded) -> Vec<ProjectorFailure> {
        todo!("proposal: dispatch each D stream recorded through projection()")
    }
}

/// Handle one book, with every reaction it sets off, as one atomic chain:
/// [`Chain::execute`] the book, [`Chain::append`] everything staged (and
/// record its key), [`UnitOfWork::commit`], then run `projections`.
/// Returns every event the chain recorded. On any failure nothing is
/// committed and no projection runs.
pub fn handle<D, U, Ps>(
    unit: U,
    routes: &Routes<'_, U>,
    book: &BookOf<D>,
    sampled: &Sampled,
    rules: &Rulebook<D::Rules>,
    projections: &mut Ps,
) -> Result<Recorded, Failure<D::Rejection>>
where
    D: Domain,
    U: UnitOfWork + EventStore<D::Event>,
    Ps: Project,
{
    todo!("proposal: Chain::new(&mut unit, routes, sampled).execute, append, commit, project")
}

/// [`handle`], over the async ports: the store's reads, appends and commit
/// are awaited; deciding stays synchronous and pure.
pub async fn handle_async<D, U, Ps>(
    unit: U,
    routes: &AsyncRoutes<'_, U>,
    book: &BookOf<D>,
    sampled: &Sampled,
    rules: &Rulebook<D::Rules>,
    projections: &mut Ps,
) -> Result<Recorded, Failure<D::Rejection>>
where
    D: Domain,
    U: AsyncUnitOfWork + AsyncEventStore<D::Event>,
    Ps: Project,
{
    todo!("proposal: as handle, awaiting the store")
}
