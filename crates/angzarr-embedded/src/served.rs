//! A bus's handlers through the kernel ([`super::super::bus`]): a served
//! projector's item, retried and dead-lettered as an inline projector's;
//! and a served policy's step through the transactional outbox, as the
//! distributed coordinator takes one (angzarr-project's
//! `docs/design/policies.md` §5): the stamped outputs written to the outbox
//! with the policy's position past the trigger in one write, then relayed
//! (each a step in its target), each cleared once its step is done.

use prost::Message as _;

use angzarr_ports::{Outbox, OutboxEntry};

use super::*;
use crate::policy::Order;

/// An outbox entry's message type: the stamped reaction.
const REACTION: &str = "type.googleapis.com/io.angzarr.v1.ReactionDelivery";

impl<'k> Kernel<'k> {
    /// One policy a bus serves (`policy`, never one of the registry's) on
    /// one event of `source` (`page`, as stored): upcast, reacted to, its
    /// outputs checked and stamped ([`Run::orders`]) and written to the
    /// outbox with the policy's position past the event, in one write; then
    /// relayed, and what they wrote settled (the targets' inline policies
    /// and the inline projectors), as after [`Kernel::handle`]. A framework
    /// meta-event is never a trigger: its position is written alone. Every
    /// step's instant is `now`. Failed before the write, the position is
    /// where the failure left it (past the event when its bug is
    /// dead-lettered and the kernel moves past bugs); failed in the relay,
    /// the position is past the event and an entry not done stays pending
    /// ([`Kernel::relay_pending`]).
    pub(crate) async fn react_served<S: Store + Outbox>(
        &self,
        store: &S,
        policy: &PolicyEntry,
        source: &Cover,
        page: &EventPage,
        now: prost_types::Timestamp,
    ) -> Result<Recorded, Failure<Infallible>> {
        let sampled = sampled(now);
        let letters = CallLetters::new(self.dead_letters, Some(sampled.now));
        let mut port = StorePort::new(store, &letters);
        let run = Run::new(self, &sampled, &letters);
        let reacted = (self.react_outboxed(store, &mut port, run, policy, (source, page))).await;
        match lettered(reacted.map(Handled::Applied), letters.take())? {
            Handled::Applied(recorded) => Ok(recorded),
            Handled::Duplicate(..) => unreachable!("a served policy answers what it recorded"),
        }
    }

    /// [`Kernel::react_served`] on `port`, in `run`, on `page` of `source`.
    async fn react_outboxed<S: Store + Outbox, P: Port>(
        &self,
        store: &S,
        port: &mut P,
        mut run: Run<'_>,
        policy: &PolicyEntry,
        (source, page): (&Cover, &EventPage),
    ) -> Result<Recorded, Failure<Infallible>> {
        let position = [(policy.name(), source, page.sequence + 1)];
        let store_failed = |e| Failure::Kernel(KernelError::Store(e));
        if angzarr_router::page_event(page).is_none() {
            port.write_positions(&position)
                .await
                .map_err(store_failed)?;
            return Ok(Recorded::default());
        }
        let event = self.upcast_trigger(source, page).await?;
        let trigger = EventRef {
            stream: Some(source.clone()),
            sequence: page.sequence,
        };
        let book = EventBook {
            cover: Some(source.clone()),
            pages: vec![event],
            ..EventBook::default()
        };
        let orders = (run.orders(port, policy, &book, &trigger, &position).await)
            .map_err(|error| downstream((Vec::new(), error)))?;
        let entries: Vec<OutboxEntry> = orders.iter().map(|order| entry(policy, order)).collect();
        let stream = StreamRef::try_from(source).map_err(store_failed)?;
        let keyed = [(
            PositionKey {
                handler: policy.name(),
                stream,
            },
            page.sequence + 1,
        )];
        let write = Write {
            positions: &keyed,
            outbox: &entries,
            ..Write::default()
        };
        store.write(write).await.map_err(store_failed)?;
        let mut relayed = Ok(());
        for (order, entry) in orders.into_iter().zip(&entries) {
            if let Err(failed) = run.relay(store, port, policy, &book, order, entry).await {
                relayed = relayed.and(Err(failed));
            }
        }
        self.settled(run, port, relayed).await
    }

    /// Relay every entry `policy` left pending in the outbox (written with
    /// its position, not yet delivered: a crash, or a step that failed and
    /// may be run again), each as [`Kernel::react_served`] relays one, at
    /// `now`. Only a store failure is answered: a step's own failure is the
    /// kernel's, dead-lettered or left pending.
    pub(crate) async fn relay_pending<S: Store + Outbox>(
        &self,
        store: &S,
        policy: &PolicyEntry,
        now: prost_types::Timestamp,
    ) -> Result<(), KernelError> {
        let pending = store.pending(usize::MAX).await?;
        let ours: Vec<OutboxEntry> = (pending.into_iter())
            .filter(|entry| entry.origin == policy.name())
            .collect();
        if ours.is_empty() {
            return Ok(());
        }
        let sampled = sampled(now);
        let letters = CallLetters::new(self.dead_letters, Some(sampled.now));
        let mut port = StorePort::new(store, &letters);
        let mut run = Run::new(self, &sampled, &letters);
        let mut relayed = Ok(());
        for entry in &ours {
            let Ok(delivery) = ReactionDelivery::decode(&entry.message.value[..]) else {
                continue;
            };
            let trigger = delivery.causation.clone().unwrap_or_default();
            let source = trigger.stream.clone().unwrap_or_default();
            let page = port
                .read_from(&source, trigger.sequence)
                .await?
                .pages
                .into_iter()
                .next();
            let Some(page) = page else {
                continue;
            };
            let event = match self.upcast_trigger(&source, &page).await {
                Ok(event) => event,
                Err(_) => continue,
            };
            let book = EventBook {
                cover: Some(source),
                pages: vec![event],
                ..EventBook::default()
            };
            let order = Order { delivery, trigger };
            let relay = run.relay(store, &mut port, policy, &book, order, entry);
            if let Err(failed) = relay.await {
                relayed = relayed.and(Err(failed));
            }
        }
        match self.settled(run, &mut port, relayed).await {
            Err(Failure::Kernel(error @ KernelError::Store(_))) => Err(error),
            _ => {
                let _unkept = port.write_letters(None).await;
                Ok(())
            }
        }
    }

    /// `page` of `source` as its domain's upcaster has it now.
    async fn upcast_trigger(
        &self,
        source: &Cover,
        page: &EventPage,
    ) -> Result<EventPage, Failure<Infallible>> {
        let Some(sidecar) = self.registry.catalog().upcast_port(&source.domain) else {
            return Ok(page.clone());
        };
        upcast_pages(sidecar, &source.domain, std::slice::from_ref(page))
            .await
            .map(|mut upcast| upcast.pop().unwrap_or_else(|| page.clone()))
            .map_err(|error| {
                Failure::Kernel(KernelError::Upcast {
                    stream: source.clone(),
                    error,
                })
            })
    }

    /// Settle what `run`'s relayed steps wrote, run the inline projectors
    /// over it, and answer it, or the relay's failure.
    async fn settled<P: Port>(
        &self,
        mut run: Run<'_>,
        port: &mut P,
        relayed: Result<(), Failed>,
    ) -> Result<Recorded, Failure<Infallible>> {
        let letters = run.letters;
        let settled = match relayed {
            Ok(()) => run.settle(port).await,
            failed => failed,
        };
        let recorded = run.recorded;
        settled.map_err(downstream)?;
        match self
            .finish(Handled::Applied(recorded), port, letters, None)
            .await?
        {
            Handled::Applied(recorded) => Ok(recorded),
            Handled::Duplicate(..) => unreachable!("a relay answers what it recorded"),
        }
    }

    /// A projector a bus serves (`name`, `handler`) handed `book`, as the
    /// inline projectors are ([`Kernel::project_book`]): a retryable
    /// failure retried as the kernel retries, then dead-lettered at its
    /// cap; any other failure dead-lettered at once. Its letters are kept
    /// in the store, placed at `at`, stamped `now`. Failed, the failure and
    /// whether its letter was kept (the bus then moves past it; else it is
    /// held and handed again).
    pub(crate) async fn project_served<S: Store>(
        &self,
        store: &S,
        name: &str,
        handler: &dyn DynProjectorHandler,
        book: &EventBook,
        at: Option<(&Cover, u64)>,
        now: prost_types::Timestamp,
    ) -> Result<(), (Report, bool)> {
        let letters = CallLetters::new(self.dead_letters, Some(now));
        let Err(failure) = self.project_book(name, handler, book, &letters).await else {
            return Ok(());
        };
        if letters.count() == 0 {
            return Err((failure, false));
        }
        let mut port = StorePort::new(store, &letters);
        match port.write_letters(at).await {
            Ok(()) => Err((failure, true)),
            Err(error) => Err((Report::of(&KernelError::Store(error)), false)),
        }
    }
}

impl Run<'_> {
    /// Deliver `order` (outbox `entry`) as a step of its own in its target,
    /// then clear the entry once the step is done: landed, settled by its
    /// dead letter, a refusal translated (or its Translate's bug
    /// dead-lettered), a handler's bug dead-lettered, or a failure given up
    /// at its cap. A failure the kernel would run again (a store's, a
    /// retryable one with retries off) leaves the entry pending.
    async fn relay<S: Outbox, P: Port>(
        &mut self,
        store: &S,
        port: &mut P,
        policy: &PolicyEntry,
        event: &EventBook,
        order: Order,
        entry: &OutboxEntry,
    ) -> Result<(), Failed> {
        let path = vec![Step {
            stream: order.cover().clone(),
            cause: order.trigger().clone(),
        }];
        let (done, failed) = match order.check_logged(self.registry.catalog(), self.letters, event)
        {
            Err(m) => (true, Some(StepError::Kernel(KernelError::Malformed(m)))),
            Ok(checked) => self.step_relayed(port, policy, event, checked, &path).await,
        };
        if done {
            let cleared = store.clear(&[entry.id]).await;
            cleared.map_err(|e| (path.clone(), StepError::Kernel(KernelError::Store(e))))?;
        }
        match failed {
            Some(error) => Err((path, error)),
            None => Ok(()),
        }
    }

    /// `checked`'s step, and what came of it: whether its entry is done,
    /// and its failure.
    async fn step_relayed<P: Port>(
        &mut self,
        port: &mut P,
        policy: &PolicyEntry,
        event: &EventBook,
        checked: Checked,
        path: &[Step],
    ) -> (bool, Option<StepError>) {
        let delivered = checked.identify(self.sampled);
        let raised = self.letters.count();
        let failure = match self.step(port, delivered.clone(), &[]).await {
            Ok(stepped) => {
                self.enqueue(stepped, path.to_vec());
                return (true, None);
            }
            Err(failure) => failure,
        };
        if self.settled_by_letter(&delivered, &failure, raised) {
            return (true, None);
        }
        match failure {
            Failure::Rejected(rejected) if !aborted(&rejected) => {
                match self
                    .refusal_step(port, policy, event, &rejected, &delivered, &[])
                    .await
                {
                    Ok(stepped) => {
                        self.enqueue(stepped, path.to_vec());
                        (true, None)
                    }
                    Err(error) => (true, Some(error)),
                }
            }
            Failure::Kernel(KernelError::Router(error)) if route::is_handler_fault(&error) => {
                let error = KernelError::Router(error);
                if self.letters.count() == raised {
                    dead_letter::write_command(
                        self.letters,
                        self.registry.catalog(),
                        &delivered,
                        &error,
                    );
                }
                (true, Some(StepError::Kernel(error)))
            }
            failure => {
                let done = self.retried(&failure, Caller::Kernel) || given_up(&failure);
                (done, Some(step_error(failure)))
            }
        }
    }
}

/// What a served step samples: `now`, and no minted id (no command of its
/// own is identified by one).
fn sampled(now: prost_types::Timestamp) -> Sampled {
    Sampled {
        now,
        command_id: angzarr_proto::v1::Uuid::default(),
    }
}

/// `order` as `policy`'s outbox entry: its command id the receiver's dedup
/// key, the stamped reaction its message, in its trigger's edition.
fn entry(policy: &PolicyEntry, order: &Order) -> OutboxEntry {
    let id = (order.delivery.command_id.as_ref())
        .and_then(|id| id.to_bytes().ok())
        .unwrap_or_default();
    OutboxEntry {
        id: angzarr_ports::Uuid::from_bytes(id),
        origin: policy.name().to_string(),
        message: prost_types::Any {
            type_url: REACTION.to_string(),
            value: order.delivery.encode_to_vec(),
        },
        edition: order.cover().edition.clone(),
    }
}
