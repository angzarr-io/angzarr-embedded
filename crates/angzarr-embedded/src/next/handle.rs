//! The command handler: a command, then every policy step it sets off, each
//! committed on its own: atomicity covers one handling step, never a
//! chain.
//!
//! [`Kernel::handle`] runs the book through the five steps (validate,
//! identify, load then deduplicate, resolve, decode and decide or ingest)
//! and writes its events: ONE atomic write to the store, at the length the
//! step loaded. Then every policy of its domain runs on those events, each
//! command a step of its own in its target, written with the policy's
//! position on the source; whose events set off their own policies, stream
//! by stream in the order written, until the chain settles. The call waits
//! for it.
//!
//! - **A duplicate** (step 3 finds the command's events on its stream) is
//!   answered [`Handled::Duplicate`] with them, once its stream's policies
//!   have run whatever they still owe (a retry after a crash finishes the
//!   chain).
//! - **A failing step keeps every write before it**: nothing is rolled
//!   back. The failure goes back to the caller with the path to the step
//!   ([`Failure::Downstream`]). A refused command of a policy that does not
//!   answer refusals is dead-lettered and its position moves past it; a bug
//!   (a malformed policy book, the depth limit) or a store failure holds
//!   the position, so [`Kernel::resume`] runs the step again, deduplicated
//!   by its derived command id.
//! - **A refused command of a policy that answers refusals** (every
//!   marked-up one) is no failure: the policy's refusal fact goes to the
//!   stream of the triggering event as a step of its own, with the refused
//!   command's id; a refusal its source has no handler for is
//!   dead-lettered.
//! - **Projections run** on what each step wrote: a [`Project`]ion route's
//!   failure is reported in [`Recorded::projector_failures`]; a marked-up
//!   projector's ([`Registry::projector`]) is the caller's
//!   [`Failure::Projection`]. A failing projection never undoes a write.
//!
//! Which policies run is the registry's one list
//! ([`Registry::policy`]), in registration order, so a chain can come back
//! to a stream it already touched (A → B → A); a cascade deeper than the
//! chain's limit is a [`KernelError::ChainTooDeep`], which is how a cycle
//! that never settles ends.

use std::any::Any as AnyType;
use std::collections::VecDeque;
use std::convert::Infallible;

use angzarr_proto::v1::{
    self as v1, command_page, CommandBook, CommandPage, Cover, DeliveryOutcome, EventBook,
    EventPage, EventRef, Fact, FactOrigin, Outcome,
};

use super::check::{check, check_refusal, Checked, Identified, Sampled};
use super::coordinator::{deduplicate, load, run_step, unknown_domain, Deduplicated};
use super::dead_letter::{write_dead_letter, DeadLetterLog};
use super::domain::Domain;
use super::error::{Failure, KernelError, Step, StepError};
use super::handler::Handler;
use super::markup::{self, PolicyContext, ProjectionContext};
use super::payload::type_name;
use super::policy::{orders, projection, Projection};
use super::registry::{Registry, StepFailure};
use super::store::{Port, Store, StorePort};
use super::stream::{Envelope, Stream};
use angzarr_embedded_diagnostic::Report;

/// How deep a chain may go before it is a [`KernelError::ChainTooDeep`],
/// unless the kernel says otherwise ([`Kernel::limit`]).
pub const DEFAULT_DEPTH_LIMIT: usize = 8;

/// What the application builds once and handles every command through: the
/// registry, where dead letters go, where trigger edges go, and the chain's
/// depth limit.
pub struct Kernel<'k> {
    registry: &'k Registry,
    dead_letters: &'k dyn DeadLetterLog,
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
    /// `ProjectorFailure` (code INTERNAL), or DUPLICATE. angzarr-embedded never
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
            limit: DEFAULT_DEPTH_LIMIT,
        }
    }

    /// Chains at most `limit` policy steps deep.
    pub fn limit(mut self, limit: usize) -> Kernel<'k> {
        self.limit = limit;
        self
    }

    pub fn registry(&self) -> &'k Registry {
        self.registry
    }

    /// Handle one book for the `D` stream it names, then every policy step
    /// it sets off, and wait for the chain to settle (see the module docs).
    /// Each step is ONE atomic write to `store`: one stream's events, with
    /// the step's positions. The command's own failure writes nothing: a
    /// refused write and a lost race are [`KernelError::Store`]. A cover of
    /// another domain than `D` is [`KernelError::WrongDomain`]. Deciding
    /// stays synchronous and pure; the future is `Send` whenever the
    /// store's are.
    pub async fn handle<D, S, Ps>(
        &self,
        store: &S,
        book: CommandBook,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<D::Rejection>>
    where
        D: Domain,
        S: Store,
        Ps: Project,
    {
        let checked = self.check::<D>(book)?;
        let mut port = StorePort(store);
        let mut run = Run::new(self, sampled);
        let identified = checked.identify(sampled);
        let cover = identified.cover().clone();
        let novel = match deduplicate(&mut port, self.registry, identified)
            .await
            .map_err(Failure::Kernel)?
        {
            Deduplicated::Duplicate { events } => {
                // A retry settles what its own events still owe, as the
                // original would have: never the stream's older history.
                if let Some(first) = events.pages.first() {
                    run.queue
                        .push_back((cover, Vec::new(), Some(first.sequence)));
                    run.settle(&mut port).await.map_err(downstream)?;
                }
                let failures = self.run_projectors(&run.recorded.steps).await;
                if !failures.is_empty() {
                    return Err(Failure::Projection {
                        committed: run.recorded.books,
                        failures,
                    });
                }
                return Ok(Handled::Duplicate(events));
            }
            Deduplicated::Novel(novel) => novel,
        };
        let contextual = load(*novel, &mut run.nonces).map_err(Failure::Kernel)?;
        let head = contextual.head();
        let (stream, pages) =
            run_step::<D>(contextual, None, self.registry, sampled, self.dead_letters)?;
        if !pages.is_empty() {
            port.write(&cover, head, &pages, &[])
                .await
                .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
        }
        run.record(cover.clone(), head, Box::new(stream), pages);
        run.queue.push_back((cover, Vec::new(), Some(head)));
        let settled = run.settle(&mut port).await.map_err(downstream);
        let recorded = run.recorded;
        settled?;
        self.finish(Handled::Applied(recorded), projections).await
    }

    /// Run every policy step still owed, by position: for each stream of
    /// each policy's source domain, the events from the lowest of its
    /// policies' positions on, each policy for each event it has yet to
    /// handle, each step as [`Kernel::handle`] runs them (deduplicated, so
    /// a step that landed before a crash is not repeated; one write each,
    /// its position written with it). What an application runs at start,
    /// after a crash, or after adding a policy to a domain with history.
    /// Returns what the steps recorded.
    pub async fn resume<S, Ps>(
        &self,
        store: &S,
        sampled: &Sampled,
        projections: &mut Ps,
    ) -> Result<Recorded, Failure<Infallible>>
    where
        S: Store,
        Ps: Project,
    {
        let mut port = StorePort(store);
        let mut run = Run::new(self, sampled);
        let mut sources: Vec<&'static str> = Vec::new();
        for policy in self.registry.all_policies() {
            if !sources.contains(&policy.source()) {
                sources.push(policy.source());
            }
        }
        for source in sources {
            let covers = port
                .covers(source)
                .await
                .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
            for cover in covers {
                run.queue.push_back((cover, Vec::new(), None));
            }
        }
        let settled = run.settle(&mut port).await.map_err(downstream);
        let recorded = run.recorded;
        settled?;
        match self.finish(Handled::Applied(recorded), projections).await? {
            Handled::Applied(recorded) => Ok(recorded),
            Handled::Duplicate(_) => unreachable!("a resume answers what it recorded"),
        }
    }

    /// Run `projections`, then every marked-up projector, over what was
    /// written.
    async fn finish<R, Ps: Project>(
        &self,
        handled: Handled,
        projections: &mut Ps,
    ) -> Result<Handled, Failure<R>> {
        let handled = self.project(handled, projections);
        let (steps, books) = match &handled {
            Handled::Applied(recorded) => (recorded.steps.clone(), recorded.books.clone()),
            Handled::Duplicate(_) => return Ok(handled),
        };
        let failures = self.run_projectors(&steps).await;
        if !failures.is_empty() {
            return Err(Failure::Projection {
                committed: books,
                failures,
            });
        }
        Ok(handled)
    }

    /// Every marked-up projector over the `committed` events of its
    /// source, in registration order, each event in stream order: a
    /// projector stops at its first failure, the others go on. The
    /// failures, by projector.
    async fn run_projectors(&self, committed: &[EventBook]) -> Vec<(&'static str, Report)> {
        let mut failures = Vec::new();
        for entry in self.registry.projectors() {
            let books: Vec<&EventBook> = committed
                .iter()
                .filter(|book| {
                    book.cover
                        .as_ref()
                        .is_some_and(|c| c.domain == entry.source)
                })
                .collect();
            if books.iter().all(|book| book.pages.is_empty()) {
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

/// A chain's failure, as the caller's: the path to the failing step, and
/// why.
fn downstream<R>((path, error): (Vec<Step>, StepError)) -> Failure<R> {
    Failure::Downstream { path, error }
}

/// The path to a failing step, and why it failed.
type Failed = (Vec<Step>, StepError);

/// A chain in progress: the streams whose policies may owe steps, each with
/// the path of steps that led to it, in the order written; what the
/// application sampled for the whole chain (one instant; one minted
/// command id, for the originating command only); and what it recorded.
struct Run<'k> {
    registry: &'k Registry,
    sampled: &'k Sampled,
    dead_letters: &'k dyn DeadLetterLog,
    limit: usize,
    /// Mints each delivery's [`Nonce`](super::Nonce).
    nonces: u64,
    /// The streams to settle, the path to each, and which of its events:
    /// those a step just wrote (`Some(from)`, from that sequence on), or
    /// every one its policies still owe by their positions (`None`).
    queue: VecDeque<(Cover, Vec<Step>, Option<u64>)>,
    recorded: Recorded,
}

/// What a step came to.
enum Stepped {
    /// It wrote events to its stream, from this sequence on.
    Wrote(Cover, u64),
    /// It wrote no event: a duplicate, or a decision with none.
    Nothing,
}

impl<'k> Run<'k> {
    fn new(kernel: &Kernel<'k>, sampled: &'k Sampled) -> Run<'k> {
        Run {
            registry: kernel.registry,
            sampled,
            dead_letters: kernel.dead_letters,
            limit: kernel.limit,
            nonces: 0,
            queue: VecDeque::new(),
            recorded: Recorded::default(),
        }
    }

    /// Settle every queued stream, in order: each runs its policies on
    /// every event they have yet to handle; a stream a step writes to is
    /// queued in its turn.
    async fn settle<P: Port>(&mut self, port: &mut P) -> Result<(), Failed> {
        while let Some((source, path, written)) = self.queue.pop_front() {
            self.settle_stream(port, &source, &path, written).await?;
        }
        Ok(())
    }

    /// Run `source`'s policies on its events, each event in order, each
    /// policy in registration order: on the events a step just wrote, from
    /// `written` on; or, `written` `None`, on every event from each
    /// policy's position on. A policy whose position is past an event has
    /// handled it.
    async fn settle_stream<P: Port>(
        &mut self,
        port: &mut P,
        source: &Cover,
        path: &[Step],
        written: Option<u64>,
    ) -> Result<(), Failed> {
        let registry = self.registry;
        let policies: Vec<_> = registry.policies(&source.domain).collect();
        if policies.is_empty() {
            return Ok(());
        }
        let store = |e| (path.to_vec(), StepError::Kernel(KernelError::Store(e)));
        let mut positions = Vec::with_capacity(policies.len());
        for policy in &policies {
            positions.push(port.position(policy.name(), source).await.map_err(store)?);
        }
        let owed = positions.iter().copied().min().unwrap_or_default();
        let from = written.map_or(owed, |written| written.max(owed));
        let slice = port.read_from(source, from).await.map_err(store)?;
        let upcaster = registry.upcaster(&source.domain);
        for page in &slice.pages {
            let trigger = EventRef {
                stream: Some(source.clone()),
                sequence: page.sequence,
            };
            let meta = page.meta.clone().unwrap_or_default();
            let ctx = PolicyContext::new(
                trigger,
                meta.correlation_id.clone().unwrap_or_default(),
                meta.recorded_at.unwrap_or_default(),
            );
            let sent = page.payload.clone().unwrap_or_default();
            let event = match upcaster {
                Some(upcaster) => upcaster.upcast_event(&sent).map_err(|error| {
                    let error = KernelError::Upcast {
                        stream: source.clone(),
                        error,
                    };
                    (path.to_vec(), StepError::Kernel(error))
                })?,
                None => sent,
            };
            for (k, policy) in policies.iter().enumerate() {
                if page.sequence < positions[k] {
                    continue;
                }
                let ran = self
                    .run_policy(port, *policy, source, page, &event, &ctx, path)
                    .await;
                if let Err(failed) = ran {
                    // A command owes steps for its own events only: a step
                    // that fails leaves its policy owing from its trigger,
                    // as a success would have moved past the history
                    // before it. Best effort: the step's error is the
                    // answer, and a position left behind only owes more. A
                    // refused step has already moved its position past.
                    if !matches!(failed.1, StepError::Rejected(_)) {
                        let _ = port
                            .write_positions(&[(policy.name(), source, page.sequence)])
                            .await;
                    }
                    return Err(failed);
                }
            }
        }
        Ok(())
    }

    /// One policy on one event of `source`: each command it sends a step
    /// of its own, its position written with the last (or alone, when no
    /// step writes it).
    #[allow(clippy::too_many_arguments)]
    async fn run_policy<P: Port>(
        &mut self,
        port: &mut P,
        policy: &dyn super::registry::ErasedPolicy,
        source: &Cover,
        page: &EventPage,
        event: &prost_types::Any,
        ctx: &PolicyContext,
        path: &[Step],
    ) -> Result<(), Failed> {
        let next = page.sequence + 1;
        let position = [(policy.name(), source, next)];
        let commands = policy.on_event(event, ctx);
        let orders = orders(policy.name(), policy.target(), source, ctx, commands);
        let store = |path: Vec<Step>, e| (path, StepError::Kernel(KernelError::Store(e)));
        if orders.is_empty() {
            return port
                .write_positions(&position)
                .await
                .map_err(|e| store(path.to_vec(), e));
        }
        let last = orders.len() - 1;
        for (index, order) in orders.iter().enumerate() {
            let mut path = path.to_vec();
            path.push(Step {
                stream: order.cover().clone(),
                cause: order.trigger().clone(),
            });
            if path.len() > self.limit {
                let limit = self.limit;
                return Err((path, StepError::Kernel(KernelError::ChainTooDeep { limit })));
            }
            let checked = match order.check_logged(self.registry, self.dead_letters) {
                Ok(checked) => checked,
                Err(m) => return Err((path, StepError::Kernel(KernelError::Malformed(m)))),
            };
            let delivered = checked.identify(self.sampled);
            let positions: &[(&str, &Cover, u64)] = match index == last {
                true => &position,
                false => &[],
            };
            match self.step(port, delivered.clone(), positions).await {
                Ok(Stepped::Wrote(cover, head)) => self.queue.push_back((cover, path, Some(head))),
                Ok(Stepped::Nothing) => {}
                Err(StepFailure {
                    error: StepError::Rejected(report),
                    refusal,
                }) => {
                    if !policy.answers_refusals() {
                        write_dead_letter(
                            self.dead_letters,
                            order.book(),
                            policy.name(),
                            &report.to_string(),
                        );
                        port.write_positions(&position)
                            .await
                            .map_err(|e| store(path.clone(), e))?;
                        return Err((path, StepError::Rejected(report)));
                    }
                    let event = EventBook {
                        cover: Some(source.clone()),
                        pages: vec![page.clone()],
                        ..EventBook::default()
                    };
                    let refusal = markup::v1::Refusal {
                        event: Some(event),
                        command: Some(delivered.to_book()),
                        response: Some(markup::v1::RefusalResponse {
                            refusal,
                            message: report.to_string(),
                        }),
                    };
                    let answer = policy.on_refused(&refusal);
                    match self
                        .refusal_step(port, policy.name(), &delivered, answer, positions)
                        .await
                    {
                        Ok(Stepped::Wrote(cover, head)) => {
                            self.queue.push_back((cover, path, Some(head)))
                        }
                        Ok(Stepped::Nothing) => {}
                        Err(error) => return Err((path, error)),
                    }
                }
                Err(failure) => return Err((path, failure.error)),
            }
        }
        Ok(())
    }

    /// `delivered`'s refusal fact, `answer` the policy's payload for it, as
    /// a step of its own in the stream of the triggering event, with the
    /// refused command's id and correlation. A refusal its source has no
    /// handler for is dead-lettered, and writes only `positions`.
    async fn refusal_step<P: Port>(
        &mut self,
        port: &mut P,
        policy: &'static str,
        delivered: &Identified,
        answer: prost_types::Any,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<Stepped, StepError> {
        let trigger = delivered.causation_id().cloned().unwrap_or_default();
        let book = CommandBook {
            cover: trigger.stream.clone(),
            page: Some(CommandPage {
                body: Some(command_page::Body::Fact(Fact {
                    payload: Some(answer),
                })),
                fact_origin: Some(FactOrigin {
                    external_id: delivered.command_id().to_string(),
                    description: format!(
                        "{} refused by {}",
                        type_name(&delivered.payload().type_url),
                        delivered.domain()
                    ),
                }),
                ..CommandPage::default()
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
            write_dead_letter(self.dead_letters, book, policy, &m.to_string());
            StepError::Kernel(KernelError::Malformed(m))
        })?;
        let fact = checked.identify(self.sampled);
        match self.step(port, fact.clone(), positions).await {
            Ok(stepped) => Ok(stepped),
            Err(StepFailure {
                error: StepError::Kernel(KernelError::UnknownType { type_url, .. }),
                ..
            }) => {
                write_dead_letter(
                    self.dead_letters,
                    fact.to_book(),
                    fact.domain(),
                    &format!("nothing handles the refusal of {type_url}"),
                );
                port.write_positions(positions)
                    .await
                    .map_err(|e| StepError::Kernel(KernelError::Store(e)))?;
                Ok(Stepped::Nothing)
            }
            Err(failure) => Err(failure.error),
        }
    }

    /// `command` as a step of its own in the stream it names: load,
    /// deduplicate, decide or ingest, and write its events with
    /// `positions` (or `positions` alone, when it writes no event).
    async fn step<P: Port>(
        &mut self,
        port: &mut P,
        command: Identified,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<Stepped, StepFailure> {
        let kernel = |error| StepFailure {
            error: StepError::Kernel(error),
            refusal: None,
        };
        let store = |e| kernel(KernelError::Store(e));
        let cover = command.cover().clone();
        let novel = match deduplicate(port, self.registry, command)
            .await
            .map_err(kernel)?
        {
            Deduplicated::Duplicate { .. } => {
                if !positions.is_empty() {
                    port.write_positions(positions).await.map_err(store)?;
                }
                return Ok(Stepped::Nothing);
            }
            Deduplicated::Novel(novel) => novel,
        };
        let contextual = load(*novel, &mut self.nonces).map_err(kernel)?;
        let head = contextual.head();
        let typed = self
            .registry
            .step(&cover.domain)
            .ok_or_else(|| kernel(unknown_domain(&cover.domain)))?;
        let (stream, pages) = typed(
            contextual,
            None,
            self.registry,
            self.sampled,
            self.dead_letters,
        )?;
        if pages.is_empty() {
            if !positions.is_empty() {
                port.write_positions(positions).await.map_err(store)?;
            }
            return Ok(Stepped::Nothing);
        }
        port.write(&cover, head, &pages, positions)
            .await
            .map_err(store)?;
        self.record(cover.clone(), head, stream, pages);
        Ok(Stepped::Wrote(cover, head))
    }

    /// A step's write: its events, as the chain's answer.
    fn record(
        &mut self,
        cover: Cover,
        head: u64,
        stream: Box<dyn AnyType + Send>,
        pages: Vec<EventPage>,
    ) {
        if pages.is_empty() {
            return;
        }
        let next_sequence = head + pages.len() as u64;
        self.recorded.steps.push(EventBook {
            cover: Some(cover.clone()),
            snapshot: None,
            next_sequence,
            pages: pages.clone(),
        });
        match self
            .recorded
            .books
            .iter_mut()
            .find(|book| book.cover.as_ref() == Some(&cover))
        {
            Some(book) => {
                book.pages.extend(pages);
                book.next_sequence = next_sequence;
            }
            None => self.recorded.books.push(EventBook {
                cover: Some(cover.clone()),
                snapshot: None,
                next_sequence,
                pages,
            }),
        }
        match self.recorded.streams.iter_mut().find(|(c, ..)| *c == cover) {
            Some((_, _, held)) => *held = stream,
            None => self.recorded.streams.push((cover, head, stream)),
        }
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
    /// Each step's events, one book per step that wrote any, in the order
    /// written.
    steps: Vec<EventBook>,
}

impl Recorded {
    /// Each committed step's events, one book per step (a step that wrote
    /// no event is not listed), in the order written: the command's first,
    /// then each policy step's and refusal fact's.
    pub fn steps(&self) -> &[EventBook] {
        &self.steps
    }

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
