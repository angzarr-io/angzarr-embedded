//! The command handler: a command and every reaction it sets off, as one
//! atomic chain, in process.
//!
//! [`handle`] decides the command, then synchronously runs the policies
//! and translations reacting to its events, which decide (or admit) in
//! their targets, whose events set off their own reactions, and so on down
//! the chain. Every event stays **staged in memory**, in a [`Chain`], across
//! every stream involved; a later step sees the staged state of any stream
//! an earlier step touched, the originating stream included. Nothing is
//! appended until the whole chain has succeeded.
//!
//! - **Any failure fails it all.** A policy's command rejected, a
//!   translation's fact refused, a gap, the depth limit: the failure goes
//!   back to the caller with the path to the step that failed
//!   ([`Failure::Downstream`]), and nothing is appended anywhere, the
//!   originating command's events included.
//! - **On success** every staged stream is appended through the unit of
//!   work, each checked against the length it was loaded at (optimistic
//!   concurrency on every stream touched), and the unit of work committed:
//!   one transaction.
//! - **Then projections run**, on committed events only. A failed chain is
//!   never seen by one.
//!
//! Policies and translations are thereby inside the write side's
//! consistency boundary, by choice: this is the in-process form of the
//! pattern, where a reaction's target is updated in the same transaction as
//! its source. A reaction that must cross a process or a service boundary
//! is not one of these; it would read the committed log (a [`Position`]
//! per stream, [`orders_owed`] / [`facts_owed`]) instead.
//!
//! Which reactions run is a [`Routes`] table, keyed by source domain, so a
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
//! [`facts_owed`]: super::facts_owed

use std::any::{Any, TypeId};
use std::collections::BTreeMap;

use super::domain::Domain;
use super::error::{Failure, KernelError, Step, StepError};
use super::handler::Handler;
use super::id::{Seq, StreamKey};
use super::meta::CommandMeta;
use super::reaction::{Policy, Projection, Translation};
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

/// Which policies and translations react to which source domain, in the
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

    /// Run `T` after any `T::Source` stream stages events: each fact it
    /// owes is injected into its target, in the same chain.
    pub fn translation<T: Translation>(
        self,
        source: &'r Rulebook<<T::Source as Domain>::Rules>,
        target: &'r Rulebook<<T::Target as Domain>::Rules>,
    ) -> Self
    where
        U: EventStore<<T::Source as Domain>::Event> + EventStore<<T::Target as Domain>::Event>,
    {
        todo!("proposal: box a closure dispatching through translation() and chain.inject")
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

    /// As [`Routes::translation`].
    pub fn translation<T: Translation>(
        self,
        source: &'r Rulebook<<T::Source as Domain>::Rules>,
        target: &'r Rulebook<<T::Target as Domain>::Rules>,
    ) -> Self
    where
        U: AsyncEventStore<<T::Source as Domain>::Event>
            + AsyncEventStore<<T::Target as Domain>::Event>,
    {
        todo!("proposal: as Routes::translation, async")
    }
}

impl<'r, U: 'r> Default for AsyncRoutes<'r, U> {
    fn default() -> Self {
        Self::new()
    }
}

/// A command chain in progress: every stream it has touched, loaded once
/// from the unit of work and staged since, and the path from the
/// originating command to the step running now.
pub struct Chain<'u, 'r, U> {
    store: &'u mut U,
    routes: &'u Routes<'r, U>,
    /// Each touched stream: the length it was loaded at, and the stream
    /// (a `Stream<D>`, boxed with how to append it) with the chain's events
    /// committed to it in memory.
    staged: BTreeMap<StreamKey, (Seq, Box<dyn Any + Send>)>,
    /// The order streams were first touched in: the order they are
    /// appended in.
    order: Vec<StreamKey>,
    path: Vec<Step>,
    limit: usize,
}

impl<'u, 'r, U> Chain<'u, 'r, U> {
    /// An empty chain over `store`, reacting by `routes`, with
    /// [`DEFAULT_DEPTH_LIMIT`].
    pub fn new(store: &'u mut U, routes: &'u Routes<'r, U>) -> Chain<'u, 'r, U> {
        Chain {
            store,
            routes,
            staged: BTreeMap::new(),
            order: Vec::new(),
            path: Vec::new(),
            limit: DEFAULT_DEPTH_LIMIT,
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

    /// Decide `cmd` in the stream `key` names (loaded from the store the
    /// first time the chain touches it, staged after), stage its events,
    /// then run the routes for `D` on them, depth first. A chain's first
    /// `execute` is the originating command; a policy route's are the rest.
    /// A key of another domain than `D` is a [`KernelError::WrongDomain`].
    pub fn execute<D>(
        &mut self,
        key: &StreamKey,
        cmd: &D::Command,
        meta: &CommandMeta,
        rules: &Rulebook<D::Rules>,
    ) -> Result<(), Failure<D::Rejection>>
    where
        D: Domain,
        U: EventStore<D::Event>,
    {
        todo!("proposal: load or find, decide, stage, react")
    }

    /// Admit `fact` into the stream `key` names after placing its source
    /// seq against the target's staged position (a translation route reads
    /// that off the target's staged state), then run the routes for `D`.
    pub fn inject<D>(
        &mut self,
        key: &StreamKey,
        fact: D::Event,
        meta: &CommandMeta,
        rules: &Rulebook<D::Rules>,
    ) -> Result<(), Failure<std::convert::Infallible>>
    where
        D: Domain,
        U: EventStore<D::Event>,
    {
        todo!("proposal: load or find, admit, stage, react")
    }

    /// The whole chain succeeded: append every staged stream through the
    /// unit of work, in the order first touched, each expecting the length
    /// it was loaded at. A conflict on any is a
    /// [`StoreError::Conflict`](super::StoreError::Conflict) naming it; the
    /// caller then drops the unit of work uncommitted.
    pub fn append(self) -> Result<Recorded, KernelError> {
        todo!("proposal: append each staged stream's new events")
    }
}

/// Everything a chain recorded, every stream, after commit.
pub struct Recorded {
    /// Each stream touched, in the order first touched: the length it was
    /// loaded at, and the stream (a `Stream<D>`, boxed) after the chain.
    streams: Vec<(StreamKey, Seq, Box<dyn Any + Send>)>,
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
}

/// Projections the command handler runs after commit, over what the chain
/// recorded: [`ProjectionRoute`]s; `()` is none, a pair is both.
pub trait Project {
    fn project(&mut self, recorded: &Recorded) -> Result<(), KernelError>;
}

/// A projection of one domain's streams, run after commit over every
/// stream of that domain the chain recorded in.
pub struct ProjectionRoute<'p, D: Domain, P: Projection<D>> {
    pub projection: &'p mut P,
    pub rules: &'p Rulebook<D::Rules>,
}

impl Project for () {
    fn project(&mut self, recorded: &Recorded) -> Result<(), KernelError> {
        Ok(())
    }
}

impl<A: Project, B: Project> Project for (A, B) {
    fn project(&mut self, recorded: &Recorded) -> Result<(), KernelError> {
        self.0.project(recorded)?;
        self.1.project(recorded)
    }
}

impl<D: Domain, P: Projection<D>> Project for ProjectionRoute<'_, D, P> {
    fn project(&mut self, recorded: &Recorded) -> Result<(), KernelError> {
        todo!("proposal: dispatch each D stream recorded through projection()")
    }
}

/// Handle one command, with every reaction it sets off, as one atomic
/// chain: [`Chain::execute`] the command, [`Chain::append`] everything
/// staged, [`UnitOfWork::commit`], then run `projections`. Returns every
/// event the chain recorded. On any failure nothing is committed and no
/// projection runs.
pub fn handle<D, U, Ps>(
    unit: U,
    routes: &Routes<'_, U>,
    key: &StreamKey,
    cmd: &D::Command,
    meta: &CommandMeta,
    rules: &Rulebook<D::Rules>,
    projections: &mut Ps,
) -> Result<Recorded, Failure<D::Rejection>>
where
    D: Domain,
    U: UnitOfWork + EventStore<D::Event>,
    Ps: Project,
{
    todo!("proposal: Chain::new(&mut unit, routes).execute, append, unit.commit, project")
}

/// [`handle`], over the async ports: the store's reads, appends and commit
/// are awaited; deciding stays synchronous and pure.
pub async fn handle_async<D, U, Ps>(
    unit: U,
    routes: &AsyncRoutes<'_, U>,
    key: &StreamKey,
    cmd: &D::Command,
    meta: &CommandMeta,
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
