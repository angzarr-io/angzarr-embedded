//! The command handler: a command and every reaction it sets off, as one
//! atomic chain, in process.
//!
//! [`Kernel::handle`] runs the book through the five steps (validate,
//! identify, load then deduplicate, resolve, decode and decide or ingest),
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
//! - **A saga's refused command is not a failure.** The markup's sagas
//!   ([`Registry::saga`]) run in the chain beside the policies; when a
//!   saga's command is refused, its target records nothing, and the
//!   chain delivers the saga's refusal fact to the stream of the
//!   triggering event and goes on (see [`Registry::saga`]).
//! - **Then projections run**, on committed events only. A failed chain is
//!   never seen by one, and a failing projection never undoes the commit:
//!   a [`Project`]ion route's is reported in
//!   [`Recorded::projector_failures`]; a marked-up projector's, on the
//!   async path, is the caller's [`Failure::Projection`].
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

use angzarr_proto::v1::{
    self as v1, CommandBook, Cover, DeliveryOutcome, EventBook, EventPage, Outcome,
};

use super::check::{check, check_reaction, check_refusal, Checked, Identified, Sampled};
use super::coordinator::{
    deduplicate, expect, find_duplicate, load, novel, prepare, read, resolution, run_step,
    unknown_domain, Contextual, Deduplicated,
};
use super::dead_letter::{write_dead_letter, DeadLetterLog, NoTriggerLog, TriggerLog};
use super::domain::Domain;
use super::error::{Failure, KernelError, Step, StepError};
use super::handler::{Admit, Gap, Handler, Position};
use super::markup::{self, ProjectionContext, SagaContext};
use super::payload::type_name;
use super::reaction::{policy, projection, Order, Policy, Projection};
use super::registry::{Registry, StepFailure};
use super::store::{
    block, AsyncPort, AsyncStore, AsyncUnitOfWork, EventStore, Port, SyncPort, UnitOfWork,
};
use super::stream::{Envelope, Stream};
use annals_diagnostic::Report;

/// How deep a chain may go before it is a [`KernelError::ChainTooDeep`],
/// unless the kernel says otherwise ([`Kernel::limit`]).
pub const DEFAULT_DEPTH_LIMIT: usize = 8;

/// What the application builds once and handles every command through: the
/// registry, where dead letters go, where trigger edges go, and the chain's
/// depth limit.
pub struct Kernel<'k> {
    registry: &'k Registry,
    dead_letters: &'k dyn DeadLetterLog,
    triggers: &'k dyn TriggerLog,
    limit: usize,
}

/// angzarr's code for a projector failure: google.rpc.Code INTERNAL.
const INTERNAL: i32 = 13;

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
        match self {
            Handled::Applied(recorded) => Outcome {
                delivery: DeliveryOutcome::Applied as i32,
                projections: Vec::new(),
                projector_failures: recorded
                    .projector_failures
                    .iter()
                    .map(|f| v1::ProjectorFailure {
                        projector: f.projector.to_string(),
                        code: INTERNAL,
                        message: f.error.to_string(),
                    })
                    .collect(),
            },
            Handled::Duplicate(_) => Outcome {
                delivery: DeliveryOutcome::Duplicate as i32,
                ..Outcome::default()
            },
        }
    }

    /// The events to answer with: every stream the chain appended to, or
    /// the duplicate's.
    pub fn books(&self) -> Vec<EventBook> {
        match self {
            Handled::Applied(recorded) => recorded.books.clone(),
            Handled::Duplicate(book) => vec![book.clone()],
        }
    }
}

impl<'k> Kernel<'k> {
    /// With [`DEFAULT_DEPTH_LIMIT`].
    pub fn new(registry: &'k Registry, dead_letters: &'k dyn DeadLetterLog) -> Kernel<'k> {
        Kernel {
            registry,
            dead_letters,
            triggers: &NoTriggerLog,
            limit: DEFAULT_DEPTH_LIMIT,
        }
    }

    /// Report each reaction command the chain dispatches to `triggers`.
    pub fn triggers(mut self, triggers: &'k dyn TriggerLog) -> Kernel<'k> {
        self.triggers = triggers;
        self
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
        let checked = self.check::<D>(book)?;
        let mut unit = unit;
        let handled =
            block(self.chain::<D, _>(&mut SyncPort(&mut unit), routes, sampled, checked))?;
        if let Handled::Applied(_) = handled {
            unit.commit()
                .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
        }
        Ok(self.project(handled, projections))
    }

    /// [`Kernel::handle`], over the async ports: the store's reads, appends
    /// and commit are awaited; deciding stays synchronous and pure. After
    /// the commit and `projections`, every marked-up projector
    /// ([`Registry::projector`]) runs on the chain's events of its source,
    /// awaited before this returns: any failure is
    /// [`Failure::Projection`], with every failing projector and what was
    /// committed, and does not stop the other projectors. (The synchronous
    /// [`Kernel::handle`] runs none: the async path is the one path.)
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
        let handled = self
            .commit_async::<D, U, Ps>(unit, routes, book, sampled, projections)
            .await?;
        if let Handled::Applied(recorded) = &handled {
            let committed = recorded.books.clone();
            let failures = self.run_projectors(&committed).await;
            if !failures.is_empty() {
                return Err(Failure::Projection {
                    committed,
                    failures,
                });
            }
        }
        Ok(handled)
    }

    /// [`Kernel::handle_async`] without the marked-up projectors.
    async fn commit_async<D, U, Ps>(
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
        let checked = self.check::<D>(book)?;
        let mut unit = unit;
        let handled = self
            .chain::<D, _>(&mut AsyncPort(&mut unit), routes, sampled, checked)
            .await?;
        if let Handled::Applied(_) = handled {
            unit.commit()
                .await
                .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
        }
        Ok(self.project(handled, projections))
    }

    /// Every marked-up projector over the `committed` events of its source, in
    /// registration order, each event in stream order: a projector stops
    /// at its first failure, the others go on. The failures, by projector.
    async fn run_projectors(&self, committed: &[EventBook]) -> Vec<(&'static str, Report)> {
        let mut failures = Vec::new();
        for entry in self.registry.projectors() {
            let books = committed.iter().filter(|book| {
                book.cover
                    .as_ref()
                    .is_some_and(|c| c.domain == entry.source)
            });
            if books.clone().all(|book| book.pages.is_empty()) {
                continue;
            }
            // The one instance is out of the lock while it runs; a
            // concurrent command waits for it.
            let mut instance = loop {
                let taken = entry
                    .instance
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                match taken {
                    Some(instance) => break instance,
                    None => Yield(false).await,
                }
            };
            'books: for book in books {
                let cover = book.cover.clone().unwrap_or_default();
                for page in &book.pages {
                    let recorded_at = page
                        .meta
                        .as_ref()
                        .and_then(|m| m.recorded_at)
                        .unwrap_or_default();
                    let ctx = ProjectionContext::new(cover.clone(), page.sequence, recorded_at);
                    let payload = page.payload.clone().unwrap_or_default();
                    let run = instance.project(&payload, &ctx);
                    if let Some(Err(error)) = match run {
                        Some(run) => Some(run.await),
                        None => None,
                    } {
                        failures.push((entry.name, error));
                        break 'books;
                    }
                }
            }
            *entry
                .instance
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(instance);
        }
        failures
    }

    /// Handle `book` as its own step, then every saga step it sets off,
    /// each committed on its own, and wait for them to settle: annals'
    /// sagas ([`Registry::saga`]) run at once, in process, but with the
    /// meaning they have anywhere. Atomicity covers ONE step: one stream's
    /// events. The source's events are committed before a saga's command
    /// runs; each command and each refusal fact is a step of its own,
    /// deduplicated by its id, in its own unit of work begun on `store`. A
    /// step that fails keeps every commit before it: the call is an error
    /// (or a dead letter), never a rollback.
    ///
    /// Each step's marked-up projectors run after its commit; any failure
    /// is reported once the steps have settled, as [`Failure::Projection`]
    /// with every step's committed book. A saga's malformed command (a
    /// saga bug) is dead-lettered and fails the call
    /// ([`Failure::Downstream`], naming its step), as does any step's
    /// fault; a business refusal is not a failure, but its saga's refusal
    /// fact, delivered to the source.
    ///
    /// Not yet: a crash between steps is not resumed (annals keeps no saga
    /// position yet), and the kernel's [`Routes`] policies do not run here.
    pub async fn settle<D, S>(
        &self,
        store: &mut S,
        book: CommandBook,
        sampled: &Sampled,
    ) -> Result<Settled, Failure<D::Rejection>>
    where
        D: Domain,
        S: AsyncStore,
    {
        let routes = Routes::new();
        let unit = store.begin().await;
        let first = self
            .commit_async::<D, _, _>(unit, &routes, book, sampled, &mut ())
            .await?;
        let mut settled = Settled::default();
        let Handled::Applied(recorded) = first else {
            return Ok(settled);
        };
        let mut failures = self.run_projectors(&recorded.books).await;
        let mut pending: std::collections::VecDeque<EventBook> = recorded
            .books
            .into_iter()
            .filter(|book| !book.pages.is_empty())
            .collect();
        while let Some(book) = pending.pop_front() {
            settled.steps.push(book.clone());
            let cover = book.cover.clone().unwrap_or_default();
            for (index, saga) in self.registry.sagas(&cover.domain) {
                for page in &book.pages {
                    let trigger = v1::EventRef {
                        stream: Some(cover.clone()),
                        sequence: page.sequence,
                    };
                    let meta = page.meta.clone().unwrap_or_default();
                    let correlation = meta.correlation_id.unwrap_or_default();
                    let ctx = SagaContext::new(
                        trigger.clone(),
                        correlation.clone(),
                        meta.recorded_at.unwrap_or_default(),
                    );
                    let payload = page.payload.clone().unwrap_or_default();
                    let Some(commands) = (saga.on_event)(&payload, &ctx) else {
                        continue;
                    };
                    for (n, (root, command)) in commands.into_iter().enumerate() {
                        let step = Step {
                            stream: Cover {
                                domain: saga.target.to_string(),
                                root: Some(root),
                                edition: cover.edition.clone(),
                            },
                            cause: trigger.clone(),
                        };
                        let downstream = |error| Failure::Downstream {
                            path: vec![step.clone()],
                            error,
                        };
                        let order = CommandBook {
                            cover: Some(step.stream.clone()),
                            page: Some(v1::CommandPage {
                                body: Some(v1::command_page::Body::Command(v1::Command {
                                    payload: Some(command),
                                })),
                                ..v1::CommandPage::default()
                            }),
                            handling: None,
                        };
                        let checked = check_reaction(
                            order.clone(),
                            self.registry,
                            trigger.clone(),
                            saga.name,
                            u32::try_from(n).unwrap_or(u32::MAX),
                            correlation.clone(),
                        )
                        .map_err(|m| {
                            write_dead_letter(self.dead_letters, order, saga.name, &m.to_string());
                            downstream(StepError::Kernel(KernelError::Malformed(m)))
                        })?;
                        let command = checked.identify(sampled);
                        if let Some(trigger) = command.causation_id() {
                            self.triggers.dispatched(
                                trigger,
                                command.command_id(),
                                command.cover(),
                            );
                        }
                        let books = match self
                            .settle_step(store, &routes, sampled, command.clone())
                            .await
                        {
                            Ok(books) => books,
                            Err(StepFailure {
                                error: StepError::Rejected(report),
                                refusal,
                                ..
                            }) => {
                                let event = EventBook {
                                    cover: Some(cover.clone()),
                                    snapshot: None,
                                    pages: vec![page.clone()],
                                    ..EventBook::default()
                                };
                                self.refusal_step(
                                    store, &routes, sampled, index, event, &command, &report,
                                    refusal,
                                )
                                .await
                                .map_err(downstream)?
                            }
                            Err(failure) => return Err(downstream(failure.error)),
                        };
                        failures.extend(self.run_projectors(&books).await);
                        pending.extend(books);
                    }
                }
            }
        }
        if !failures.is_empty() {
            return Err(Failure::Projection {
                committed: settled.steps,
                failures,
            });
        }
        Ok(settled)
    }

    /// `command` as a step of its own, in a unit of work begun on `store`
    /// and committed: the books it appended (none for a duplicate).
    async fn settle_step<S: AsyncStore>(
        &self,
        store: &mut S,
        routes: &Routes,
        sampled: &Sampled,
        command: Identified,
    ) -> Result<Vec<EventBook>, StepFailure> {
        let kernel = |error| StepFailure {
            error: StepError::Kernel(error),
            refusal: None,
            stream: None,
        };
        let mut unit = store.begin().await;
        let recorded = {
            let mut port = AsyncPort(&mut unit);
            let mut run = Run::new(self, routes, sampled);
            if run.step(&mut port, command).await?.is_none() {
                return Ok(Vec::new());
            }
            run.append(&mut port).await.map_err(kernel)?
        };
        unit.commit()
            .await
            .map_err(|e| kernel(KernelError::Store(e)))?;
        Ok(recorded
            .books
            .into_iter()
            .filter(|book| !book.pages.is_empty())
            .collect())
    }

    /// Saga `saga`'s refusal fact for `delivered`, which its target refused
    /// with `report` (and `refusal`, its typed object), as a step of its own
    /// in the stream of the triggering `event`, with the refused command's
    /// id: the books it appended. A refusal the source has no handler for
    /// is dead-lettered, and appends nothing.
    #[allow(clippy::too_many_arguments)]
    async fn refusal_step<S: AsyncStore>(
        &self,
        store: &mut S,
        routes: &Routes,
        sampled: &Sampled,
        saga: usize,
        event: EventBook,
        delivered: &Identified,
        report: &Report,
        refusal: Option<prost_types::Any>,
    ) -> Result<Vec<EventBook>, StepError> {
        let entry = self.registry.saga_at(saga);
        let answer = (entry.on_refused)(&markup::v1::Refusal {
            event: Some(event),
            command: Some(delivered.to_book()),
            response: Some(markup::v1::RefusalResponse {
                refusal,
                message: report.to_string(),
            }),
        });
        let trigger = delivered.causation_id().cloned().unwrap_or_default();
        let book = CommandBook {
            cover: trigger.stream.clone(),
            page: Some(v1::CommandPage {
                body: Some(v1::command_page::Body::Fact(v1::Fact {
                    payload: Some(answer),
                })),
                fact_origin: Some(v1::FactOrigin {
                    external_id: delivered.command_id().to_string(),
                    description: format!(
                        "{} refused by {}",
                        type_name(&delivered.payload().type_url),
                        delivered.domain()
                    ),
                }),
                ..v1::CommandPage::default()
            }),
            handling: None,
        };
        let checked = check_refusal(
            book.clone(),
            self.registry,
            trigger,
            delivered.correlation_id().clone(),
            delivered.command_id().clone(),
        )
        .map_err(|m| {
            write_dead_letter(self.dead_letters, book, entry.name, &m.to_string());
            StepError::Kernel(KernelError::Malformed(m))
        })?;
        let fact = checked.identify(sampled);
        match self.settle_step(store, routes, sampled, fact.clone()).await {
            Ok(books) => Ok(books),
            Err(StepFailure {
                error: StepError::Kernel(KernelError::UnknownType { type_url, .. }),
                ..
            }) => {
                write_dead_letter(
                    self.dead_letters,
                    fact.to_book(),
                    fact.domain(),
                    &format!(
                        "annals-next-no-refusal-handler: nothing handles the refusal of {type_url}"
                    ),
                );
                Ok(Vec::new())
            }
            Err(failure) => Err(failure.error),
        }
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
        let kernel = Failure::Kernel;
        let checked = order
            .check_logged(self.registry, self.dead_letters)
            .map_err(|m| kernel(KernelError::Malformed(m)))?;
        let mut unit = unit;
        let mut port = SyncPort(&mut unit);
        let mut run = Run::new(self, routes, sampled);
        let identified = checked.clone().identify(sampled);
        let loaded = block(read(&mut port, self.registry, &identified)).map_err(kernel)?;
        let contextual = load(novel(identified, loaded), &mut run.nonces).map_err(kernel)?;
        let head = contextual.head();
        let target = prepare::<P::Target>(contextual, None, self.registry)
            .map_err(kernel)?
            .into_stream();
        match place(P::position(target.state()), order) {
            Ok(Admit::Next) => {}
            Ok(Admit::Redelivery) => return Ok(Handled::Applied(Recorded::default())),
            Err(gap) => {
                return Err(kernel(KernelError::Gap {
                    stream: order.cover().clone(),
                    gap,
                }))
            }
        }
        run.stage(order.cover().clone(), head, Box::new(target), Vec::new());
        let handled = block(run.finish::<P::Target, _>(&mut port, checked))?;
        if let Handled::Applied(_) = handled {
            unit.commit().map_err(|e| kernel(KernelError::Store(e)))?;
        }
        Ok(self.project(handled, projections))
    }

    /// Step 1 for a `D` stream: the book checked, and naming a `D` stream.
    fn check<D: Domain>(&self, book: CommandBook) -> Result<Checked, Failure<D::Rejection>> {
        let checked =
            check(book, self.registry).map_err(|m| Failure::Kernel(KernelError::Malformed(m)))?;
        if checked.domain() != D::NAME {
            return Err(Failure::Kernel(KernelError::WrongDomain {
                stream: checked.cover().clone(),
                expected: D::NAME,
            }));
        }
        Ok(checked)
    }

    /// The chain for `checked`, appended through `port`, uncommitted.
    async fn chain<D: Domain, P: Port>(
        &self,
        port: &mut P,
        routes: &Routes,
        sampled: &Sampled,
        checked: Checked,
    ) -> Result<Handled, Failure<D::Rejection>> {
        Run::new(self, routes, sampled)
            .finish::<D, P>(port, checked)
            .await
    }

    /// Run `projections` over what a committed chain recorded.
    fn project<Ps: Project>(&self, handled: Handled, projections: &mut Ps) -> Handled {
        match handled {
            Handled::Applied(mut recorded) => {
                recorded.projector_failures = projections.project(&recorded, self.registry);
                Handled::Applied(recorded)
            }
            duplicate => duplicate,
        }
    }
}

/// One policy, its types erased so a [`Routes`] table can hold policies of
/// any domains, and so the table names no store: what the policy owes for
/// the events a source stream (a `Stream<Source>`, boxed) staged from
/// `from` on, each order booked and checked through the kernel's reaction
/// entry, with the step it is. Pure: the chain loads, deduplicates and
/// decides the orders' targets itself, the typed step reached through the
/// [`Registry`] by the target's domain name.
type Route = Box<dyn Fn(&dyn AnyType, u64, &Registry, &dyn DeadLetterLog) -> Owed + Send + Sync>;

/// The orders owed, each with its step; or the path to the step that
/// failed, from the step that owes, and why.
type Owed = Result<Vec<(Step, Checked)>, (Vec<Step>, StepError)>;

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
        let route: Route = Box::new(|source, from, registry, dead_letters| {
            let Some(source) = source.downcast_ref::<Stream<P::Source>>() else {
                return Ok(Vec::new());
            };
            let mut orders = Vec::new();
            source
                .dispatch(
                    from,
                    registry,
                    &mut [&mut policy::<P, _>(|order| orders.push(order))],
                )
                .map_err(|e| (Vec::new(), StepError::Kernel(e)))?;
            orders
                .into_iter()
                .map(|order| {
                    let step = Step {
                        stream: order.cover().clone(),
                        cause: order.trigger().clone(),
                    };
                    match order.check_logged(registry, dead_letters) {
                        Ok(checked) => Ok((step, checked)),
                        Err(m) => Err((vec![step], StepError::Kernel(KernelError::Malformed(m)))),
                    }
                })
                .collect()
        });
        let mut routes = self;
        routes
            .by_source
            .entry(<P::Source as Domain>::NAME)
            .or_default()
            .push(route);
        routes
    }
}

/// One staged stream: the sequence it was loaded at, the stream (a
/// `Stream<D>`, boxed) with the chain's events committed to it in memory,
/// and those events' pages, for deduplication and the append. The stream
/// is out only while a step runs on it.
struct Staged {
    loaded_at: u64,
    stream: Option<Box<dyn AnyType + Send>>,
    pages: Vec<EventPage>,
}

/// A command chain in progress: every stream it has touched, loaded once
/// from the unit of work and staged since, the path from the originating
/// command to the step running now, and what the application sampled for
/// the whole chain (one instant; one minted command id, for the
/// originating command only).
pub struct Chain<'u, U> {
    unit: &'u mut U,
    run: Run<'u>,
}

/// A chain's state, apart from its unit of work, so the same chain runs
/// over the sync and the async port.
struct Run<'u> {
    registry: &'u Registry,
    routes: &'u Routes,
    sampled: &'u Sampled,
    dead_letters: &'u dyn DeadLetterLog,
    triggers: &'u dyn TriggerLog,
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
        block(
            self.run
                .execute::<D, _>(&mut SyncPort(&mut *self.unit), command),
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
        block(self.run.append(&mut SyncPort(self.unit)))
    }
}

/// What step 4 found for a command in a chain.
enum Found {
    Duplicate(EventBook),
    /// The command in context, and the chain's staged stream, if any.
    Novel(Box<Contextual>, Option<Box<dyn AnyType + Send>>),
}

impl<'u> Run<'u> {
    fn new(kernel: &Kernel<'u>, routes: &'u Routes, sampled: &'u Sampled) -> Run<'u> {
        Run {
            registry: kernel.registry,
            routes,
            sampled,
            dead_letters: kernel.dead_letters,
            triggers: kernel.triggers,
            staged: HashMap::new(),
            order: Vec::new(),
            path: Vec::new(),
            limit: kernel.limit,
            nonces: 0,
        }
    }

    /// The originating command's chain, then the append: the duplicate's
    /// events, or what the chain recorded.
    async fn finish<D: Domain, P: Port>(
        mut self,
        port: &mut P,
        command: Checked,
    ) -> Result<Handled, Failure<D::Rejection>> {
        if let Some(book) = self.execute::<D, P>(port, command).await? {
            return Ok(Handled::Duplicate(book));
        }
        let recorded = self.append(port).await.map_err(Failure::Kernel)?;
        Ok(Handled::Applied(recorded))
    }

    /// Steps 3 and 4 for `command`, the chain's staged events and streams
    /// consulted before the store.
    async fn find<P: Port>(
        &mut self,
        port: &mut P,
        command: super::Identified,
    ) -> Result<Found, KernelError> {
        let cover = command.cover().clone();
        let Some(staged_pages) = self.staged.get(&cover).map(|s| &s.pages) else {
            // Not yet in the chain: load, then look up (step 3), then step 4.
            return match deduplicate(port, self.registry, command).await? {
                Deduplicated::Duplicate { events, .. } => Ok(Found::Duplicate(events)),
                Deduplicated::Novel(novel) => {
                    let contextual = load(novel, &mut self.nonces)?;
                    Ok(Found::Novel(Box::new(contextual), None))
                }
            };
        };
        // Loaded when the chain first touched it: look up, store and staged.
        if let Some(events) = find_duplicate(port, &command, staged_pages)
            .await
            .map_err(KernelError::Store)?
        {
            return Ok(Found::Duplicate(events));
        }
        let staged = self
            .staged
            .get_mut(&cover)
            .expect("the chain holds the stream it just looked up");
        let head = staged.loaded_at + staged.pages.len() as u64;
        let resolution = resolution(self.registry, &command)?;
        let (expected, nonce) = expect(&command, resolution, head, &mut self.nonces)?;
        let contextual = Contextual {
            command,
            resolution,
            expected,
            snapshot: None,
            pages: Vec::new(),
            head,
            last_recorded_at: None,
            nonce,
        };
        Ok(Found::Novel(Box::new(contextual), staged.stream.take()))
    }

    /// Hold `stream` after a step, with the `pages` it added; `loaded_at`
    /// is its length when the chain first touched it.
    fn stage(
        &mut self,
        cover: Cover,
        loaded_at: u64,
        stream: Box<dyn AnyType + Send>,
        pages: Vec<EventPage>,
    ) {
        match self.staged.get_mut(&cover) {
            Some(staged) => {
                staged.stream = Some(stream);
                staged.pages.extend(pages);
            }
            None => {
                self.order.push(cover.clone());
                self.staged.insert(
                    cover,
                    Staged {
                        loaded_at,
                        stream: Some(stream),
                        pages,
                    },
                );
            }
        }
    }

    /// See [`Chain::execute`].
    async fn execute<D: Domain, P: Port>(
        &mut self,
        port: &mut P,
        command: Checked,
    ) -> Result<Option<EventBook>, Failure<D::Rejection>> {
        let command = command.identify(self.sampled);
        let (contextual, staged) = match self.find(port, command).await.map_err(Failure::Kernel)? {
            Found::Duplicate(book) => return Ok(Some(book)),
            Found::Novel(contextual, staged) => (contextual, staged),
        };
        let cover = contextual.command().cover().clone();
        let head = contextual.head();
        let (stream, pages) = run_step::<D>(
            *contextual,
            staged,
            self.registry,
            self.sampled,
            self.dead_letters,
        )
        .map_err(|(failure, _)| failure)?;
        self.stage(cover.clone(), head, Box::new(stream), pages);
        self.react(port, &cover, head)
            .await
            .map_err(|(path, error)| Failure::Downstream { path, error })?;
        Ok(None)
    }

    /// The orders the routes for `cover`'s domain owe for its events from
    /// `from` on, each with its step.
    fn owed(&self, cover: &Cover, from: u64) -> Owed {
        let Some(stream) = self.staged.get(cover).and_then(|s| s.stream.as_deref()) else {
            return Ok(Vec::new());
        };
        let mut orders = Vec::new();
        for route in self
            .routes
            .by_source
            .get(cover.domain.as_str())
            .into_iter()
            .flatten()
        {
            let owed =
                route(stream, from, self.registry, self.dead_letters).map_err(|(rest, e)| {
                    let mut path = self.path.clone();
                    path.extend(rest);
                    (path, e)
                })?;
            orders.extend(owed);
        }
        Ok(orders)
    }

    /// Every policy command `cover`'s events from `from` on set off, and
    /// theirs, depth first.
    async fn react<P: Port>(
        &mut self,
        port: &mut P,
        cover: &Cover,
        from: u64,
    ) -> Result<(), (Vec<Step>, StepError)> {
        let mut frames = vec![self.owed(cover, from)?.into_iter()];
        while let Some(frame) = frames.last_mut() {
            let Some((step, command)) = frame.next() else {
                frames.pop();
                continue;
            };
            let depth = frames.len();
            self.path.truncate(depth - 1);
            self.path.push(step);
            let failed = |run: &Run<'_>, error| (run.path.clone(), error);
            if depth > self.limit {
                let limit = self.limit;
                return Err(failed(
                    self,
                    StepError::Kernel(KernelError::ChainTooDeep { limit }),
                ));
            }
            let command = command.identify(self.sampled);
            if let Some(trigger) = command.causation_id() {
                self.triggers
                    .dispatched(trigger, command.command_id(), command.cover());
            }
            let (contextual, staged) = match self.find(port, command).await {
                Ok(Found::Duplicate(_)) => continue,
                Ok(Found::Novel(contextual, staged)) => (contextual, staged),
                Err(e) => return Err(failed(self, StepError::Kernel(e))),
            };
            let target = contextual.command().cover().clone();
            let head = contextual.head();
            let step = self
                .registry
                .step(&target.domain)
                .ok_or_else(|| failed(self, StepError::Kernel(unknown_domain(&target.domain))))?;
            let (stream, pages) = step(
                *contextual,
                staged,
                self.registry,
                self.sampled,
                self.dead_letters,
            )
            .map_err(|e| failed(self, e.error))?;
            self.stage(target.clone(), head, stream, pages);
            frames.push(self.owed(&target, head)?.into_iter());
        }
        Ok(())
    }

    /// Steps 2 to 5 for `command` in the stream it names, staged: the
    /// stream and the length it had before, or `None` for a duplicate. On
    /// a failure the stream, as it was, stays staged.
    async fn step<P: Port>(
        &mut self,
        port: &mut P,
        command: Identified,
    ) -> Result<Option<(Cover, u64)>, StepFailure> {
        let kernel = |error| StepFailure {
            error: StepError::Kernel(error),
            refusal: None,
            stream: None,
        };
        let (contextual, staged) = match self.find(port, command).await.map_err(kernel)? {
            Found::Duplicate(_) => return Ok(None),
            Found::Novel(contextual, staged) => (contextual, staged),
        };
        let target = contextual.command().cover().clone();
        let head = contextual.head();
        let step = self
            .registry
            .step(&target.domain)
            .ok_or_else(|| kernel(unknown_domain(&target.domain)))?;
        match step(
            *contextual,
            staged,
            self.registry,
            self.sampled,
            self.dead_letters,
        ) {
            Ok((stream, pages)) => {
                self.stage(target.clone(), head, stream, pages);
                Ok(Some((target, head)))
            }
            Err(mut failure) => {
                if let (Some(stream), Some(staged)) =
                    (failure.stream.take(), self.staged.get_mut(&target))
                {
                    staged.stream = Some(stream);
                }
                Err(failure)
            }
        }
    }

    /// See [`Chain::append`].
    async fn append<P: Port>(mut self, port: &mut P) -> Result<Recorded, KernelError> {
        let mut recorded = Recorded::default();
        for cover in std::mem::take(&mut self.order) {
            let Some(staged) = self.staged.remove(&cover) else {
                continue;
            };
            if !staged.pages.is_empty() {
                port.append(&cover, staged.loaded_at, &staged.pages)
                    .await
                    .map_err(KernelError::Store)?;
            }
            recorded.books.push(EventBook {
                cover: Some(cover.clone()),
                snapshot: None,
                next_sequence: staged.loaded_at + staged.pages.len() as u64,
                pages: staged.pages,
            });
            if let Some(stream) = staged.stream {
                recorded.streams.push((cover, staged.loaded_at, stream));
            }
        }
        Ok(recorded)
    }
}

/// What [`Kernel::settle`] committed: each step's book, in commit order.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct Settled {
    steps: Vec<EventBook>,
}

impl Settled {
    /// Each committed step's events, one book per step (a step that
    /// recorded nothing is not listed), in the order committed.
    pub fn steps(&self) -> &[EventBook] {
        &self.steps
    }
}

/// Everything a chain recorded, every stream, after commit: angzarr's
/// `CommandResponse.events`, one `EventBook` per stream here, since a chain
/// touches several.
#[derive(Default)]
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
        let (_, loaded_at, boxed) = self.streams.iter().find(|(c, ..)| c == stream)?;
        let stream = boxed.downcast_ref::<Stream<D>>()?;
        Some(&stream.events()[(loaded_at - stream.first()) as usize..])
    }

    /// Hand the chain's events in `stream` to `handlers`, each with the
    /// state just after it. Nothing, for a stream the chain did not touch;
    /// [`KernelError::WrongDomain`] for one that is not a `D` stream.
    pub fn dispatch<D: Domain>(
        &self,
        stream: &Cover,
        registry: &Registry,
        handlers: &mut [&mut dyn Handler<D>],
    ) -> Result<(), KernelError> {
        let Some((_, loaded_at, boxed)) = self.streams.iter().find(|(c, ..)| c == stream) else {
            return Ok(());
        };
        let recorded =
            boxed
                .downcast_ref::<Stream<D>>()
                .ok_or_else(|| KernelError::WrongDomain {
                    stream: stream.clone(),
                    expected: D::NAME,
                })?;
        recorded.dispatch(*loaded_at, registry, handlers)
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
    fn project(&mut self, _recorded: &Recorded, _registry: &Registry) -> Vec<ProjectorFailure> {
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
        let mut failures = Vec::new();
        for (cover, ..) in &recorded.streams {
            if cover.domain != D::NAME {
                continue;
            }
            let mut handler = projection(&mut *self.projection);
            if let Err(error) = recorded.dispatch::<D>(cover, registry, &mut [&mut handler]) {
                failures.push(ProjectorFailure {
                    projector: self.name,
                    error,
                });
            }
        }
        failures
    }
}

/// Where `order` falls against its target's `position`, per target: at or
/// before the last source sequence it holds, a redelivery; past it while
/// the order owed before this one ([`Order::previous`]) is not yet held, a
/// gap naming that one; otherwise the next. Sequences the target was never
/// owed (a filtered policy) are no gap.
fn place<P: Policy>(position: Position, order: &Order<P>) -> Result<Admit, Gap> {
    let sequence = order.trigger().sequence;
    if position.last().is_some_and(|last| sequence <= last) {
        return Ok(Admit::Redelivery);
    }
    match order.previous() {
        Some(previous) if position.last().is_none_or(|last| last < previous) => Err(Gap {
            expected: previous,
            found: sequence,
        }),
        _ => Ok(Admit::Next),
    }
}

#[cfg(test)]
#[path = "handle.test.rs"]
mod tests;

/// A future that is pending once, waking itself: a turn for another task.
struct Yield(bool);

impl std::future::Future for Yield {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            return std::task::Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}
