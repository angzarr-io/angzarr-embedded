//! Steps 3 to 5 of the pipeline: the in-process coordinator.
//!
//! angzarr's aggregate coordinator is a service that loads a stream, checks
//! or stamps the command's expected sequence and hands a
//! `ContextualCommand` to business code. annals has no service and sends
//! nothing: the [`Coordinator`] is a struct over the application's unit of
//! work, and it does the same steps in process, domain-blind (it sees
//! `Cover`s, pages and the [`Registry`], never a domain's types):
//!
//! 3. [`Coordinator::deduplicate`]: does the addressed stream already hold
//!    events with this command id? Then the command was applied: answered
//!    [`Deduplicated::Duplicate`] with those events, and nothing below
//!    runs. Before the sequence check, so a retry of an applied STRICT
//!    command whose expected sequence is now stale is a duplicate, never
//!    stale.
//! 4. [`Coordinator::load`]: resolve the conflict resolution (the book's,
//!    the command type's, the domain's, else COMMUTATIVE where the state
//!    supports it and STRICT otherwise: [`Registry::resolve`]); load the
//!    newest usable snapshot and the events after it; stamp an absent
//!    expected sequence with the head, or check a present one: under
//!    STRICT it must be the head ([`KernelError::Stale`]); under
//!    AGGREGATE_HANDLES and COMMUTATIVE the events from it to the head are
//!    the window, delivered as events (the snapshot used is at or below
//!    expected−1, so none of the window is hidden in snapshot state), with
//!    a [`Nonce`]. An expected sequence past the head is stale under every
//!    resolution.
//!
//! Then the typed step, which needs `D`:
//!
//! 5. [`Contextual::decode`]: the snapshot's state (discarded, and the
//!    stream replayed, on a rules-version or state-type mismatch), the
//!    events and the command's payload, each through the domain's upcaster
//!    and decoded strictly; then [`Prepared::decide`] runs decide or ingest
//!    and stamps the events.
//!
//! In a [`Chain`](super::Chain), steps 3 and 4 consult the chain's staged
//! streams before the store, so a later step sees an earlier step's
//! uncommitted events (and its command ids).

use std::any::Any as AnyType;
use std::panic::{catch_unwind, AssertUnwindSafe};

use angzarr_proto::v1::{ConflictResolution, EventBook, EventPage, FactOrigin, Snapshot};
use prost_types::Timestamp;

use super::check::{Body, Identified, Malformed, Sampled};
use super::dead_letter::{write_dead_letter, DeadLetterLog};
use super::domain::{Conflicts, Decision, Domain, IngestCtx, State, Tx};
use super::error::{Failure, KernelError, PayloadError, StoreError};
use super::payload::Payload;
use super::registry::Registry;
use super::store::{block, produced_by, EventStore, Port, SyncPort};
use super::stream::{Envelope, Stream};

/// The in-process conflict nonce (angzarr's `ContextualCommand.conflict_nonce`):
/// minted by the coordinator per delivery under AGGREGATE_HANDLES and
/// COMMUTATIVE, handed to decide with the window under AGGREGATE_HANDLES
/// (the kernel keeps it under COMMUTATIVE and acknowledges it itself when
/// the units are disjoint), and given back through
/// [`Tx::resolve_conflicts`](super::Tx::resolve_conflicts) as decide's
/// declaration that it examined the window. Not constructible, clonable or
/// copyable outside the kernel, so a decide cannot satisfy the check
/// without having received it. A counter, not a random value: the kernel
/// stays deterministic.
#[derive(PartialEq, Eq, Debug)]
pub struct Nonce(pub(crate) u64);

/// Steps 3 and 4 over the application's unit of work.
pub struct Coordinator<'u, U> {
    unit: &'u mut U,
    registry: &'u Registry,
    /// The next [`Nonce`].
    nonces: u64,
}

/// What deduplication found.
pub enum Deduplicated {
    /// The stream holds this command's events already: they are the
    /// answer (angzarr's DUPLICATE), in sequence order, in an `EventBook`
    /// for the addressed stream.
    Duplicate {
        command: Identified,
        events: EventBook,
    },
    /// Nothing found: the command goes on to [`Coordinator::load`]. A
    /// command whose earlier decision emitted nothing lands here again (an
    /// accepted cost: it is decided again, against the stream as it is
    /// now).
    Novel(Novel),
}

/// An [`Identified`] command that deduplication did not find: the only
/// thing [`Coordinator::load`] takes.
pub struct Novel(Identified);

impl Novel {
    pub fn command(&self) -> &Identified {
        &self.0
    }

    pub(crate) fn into_command(self) -> Identified {
        self.0
    }
}

impl<'u, U: EventStore> Coordinator<'u, U> {
    pub fn new(unit: &'u mut U, registry: &'u Registry) -> Coordinator<'u, U> {
        Coordinator {
            unit,
            registry,
            nonces: 0,
        }
    }

    /// Step 3: look the command id up on the addressed stream
    /// ([`EventStore::find_command`], over the index on (stream,
    /// command_id)).
    pub fn deduplicate(&mut self, command: Identified) -> Result<Deduplicated, StoreError> {
        block(deduplicate(&mut SyncPort(self.unit), command, &[]))
    }

    /// Step 4: resolve the conflict resolution, load the snapshot and the
    /// events, and stamp or check the expected sequence (see the module
    /// docs). STRICT's stale command is refused here, before anything is
    /// decoded.
    pub fn load(&mut self, command: Novel) -> Result<Contextual, KernelError> {
        block(load(
            &mut SyncPort(self.unit),
            self.registry,
            command.into_command(),
            &mut self.nonces,
        ))
    }
}

/// A command with its stream's history and its handling resolved: angzarr's
/// `ContextualCommand`, in process. Domain-blind still: the history is
/// pages.
pub struct Contextual {
    pub(crate) command: Identified,
    /// Never UNSPECIFIED.
    pub(crate) resolution: ConflictResolution,
    /// The book's, or the head, stamped.
    pub(crate) expected: u64,
    /// The newest usable snapshot, at or below expected−1 under
    /// AGGREGATE_HANDLES and COMMUTATIVE.
    pub(crate) snapshot: Option<Snapshot>,
    /// The events after the snapshot (or from 0) to the head.
    pub(crate) pages: Vec<EventPage>,
    /// The stream's next sequence.
    pub(crate) head: u64,
    /// The last event's `recorded_at`: the floor for the next.
    pub(crate) last_recorded_at: Option<Timestamp>,
    /// Under AGGREGATE_HANDLES and COMMUTATIVE; `None` under STRICT.
    pub(crate) nonce: Option<Nonce>,
}

impl Contextual {
    pub fn command(&self) -> &Identified {
        &self.command
    }

    /// The resolved conflict resolution: never UNSPECIFIED.
    pub fn conflict_resolution(&self) -> ConflictResolution {
        self.resolution
    }

    /// The resolved expected sequence: the book's, or the head.
    pub fn expected_sequence(&self) -> u64 {
        self.expected
    }

    /// The stream's next sequence.
    pub fn head(&self) -> u64 {
        self.head
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// The events loaded, after the snapshot.
    pub fn pages(&self) -> &[EventPage] {
        &self.pages
    }

    /// The events from the expected sequence to the head: empty under
    /// STRICT and for a stamped command.
    pub fn window(&self) -> &[EventPage] {
        let start = self.pages.partition_point(|p| p.sequence < self.expected);
        &self.pages[start..]
    }

    /// Whether a nonce was minted (AGGREGATE_HANDLES or COMMUTATIVE, even
    /// for an empty window).
    pub fn has_nonce(&self) -> bool {
        self.nonce.is_some()
    }

    /// Step 5's first half, typed: the stream as `D` (from the snapshot if
    /// [`usable`](Registry::snapshot_usable), else from 0 after discarding
    /// it), every event and the command's payload through `D`'s upcaster
    /// and decoded strictly. A command or fact type `D` does not decode is
    /// [`KernelError::UnknownType`], naming its type URL; a stored event,
    /// [`StoreError::Unreadable`]; a cover of another domain,
    /// [`KernelError::WrongDomain`].
    pub fn decode<D: Domain>(self, registry: &Registry) -> Result<Prepared<D>, KernelError> {
        prepare(self, None, registry)
    }
}

/// The decoded input: a command for decide, or a fact for ingest.
pub(crate) enum Input<D: Domain> {
    Command(D::Command),
    Fact(D::Fact, FactOrigin),
}

/// A command ready for business code: its stream at the head, its decoded
/// input, and its resolved handling.
pub struct Prepared<D: Domain> {
    pub(crate) command: Identified,
    pub(crate) input: Input<D>,
    pub(crate) stream: Stream<D>,
    pub(crate) resolution: ConflictResolution,
    pub(crate) expected: u64,
    pub(crate) nonce: Option<Nonce>,
}

impl<D: Domain> Prepared<D> {
    pub fn command(&self) -> &Identified {
        &self.command
    }

    /// The stream at the head.
    pub fn stream(&self) -> &Stream<D> {
        &self.stream
    }

    /// Step 5's second half: the body arm picks the pipeline.
    ///
    /// - A command is decided in a staging [`Tx`](super::Tx), against the
    ///   head. Under AGGREGATE_HANDLES decide is handed the window and the
    ///   nonce and must give the nonce back
    ///   ([`KernelError::Unresolved`] otherwise). Under COMMUTATIVE the
    ///   kernel resolves: `window = changed(state at expected, head)`,
    ///   `mine = changed(head, head with the events)`, both by the state's
    ///   generated [`State::changed`](super::State::changed); the events are
    ///   kept iff the two [do not overlap](super::Units::overlaps), else
    ///   [`KernelError::Stale`].
    /// - A fact is ingested ([`Domain::ingest`](super::Domain::ingest),
    ///   with an [`IngestCtx`](super::IngestCtx)). A panic is caught: the
    ///   chain fails with [`KernelError::IngestPanicked`] and `dead_letters`
    ///   gets a line ([`dead_letter_line`](super::dead_letter_line)).
    ///
    /// Every event is stamped with [`Identified::meta`], at `recorded_at =
    /// max(sampled.now, the stream's last)` and the current rules version.
    pub fn decide(
        &mut self,
        sampled: &Sampled,
        registry: &Registry,
        dead_letters: &dyn DeadLetterLog,
    ) -> Result<Decision<D>, Failure<D::Rejection>> {
        decide_on(
            &self.stream,
            &self.command,
            &self.input,
            self.resolution,
            self.expected,
            self.nonce.take(),
            sampled,
            registry,
            dead_letters,
        )
    }

    /// The stream, to commit the decision to.
    pub fn into_stream(self) -> Stream<D> {
        self.stream
    }
}

/// The refusal for a domain the registry does not hold.
pub(crate) fn unknown_domain(domain: &str) -> KernelError {
    KernelError::Malformed(Malformed::UnknownDomain {
        domain: domain.to_string(),
    })
}

/// Step 3 over `port`, also finding the command's events among `staged`,
/// a chain's uncommitted pages of the same stream.
pub(crate) async fn deduplicate<P: Port>(
    port: &mut P,
    command: Identified,
    staged: &[EventPage],
) -> Result<Deduplicated, StoreError> {
    let id = command.command_id();
    let mut pages = port.find_command(command.cover(), id).await?;
    pages.extend(staged.iter().filter(|p| produced_by(p, id)).cloned());
    let Some(next_sequence) = pages.last().map(|p| p.sequence + 1) else {
        return Ok(Deduplicated::Novel(Novel(command)));
    };
    Ok(Deduplicated::Duplicate {
        events: EventBook {
            cover: Some(command.cover().clone()),
            snapshot: None,
            pages,
            next_sequence,
        },
        command,
    })
}

/// The command's resolution ([`Registry::resolve`]): never UNSPECIFIED.
pub(crate) fn resolution(
    registry: &Registry,
    command: &Identified,
) -> Result<ConflictResolution, KernelError> {
    registry
        .resolve(
            command.domain(),
            &command.payload().type_url,
            command.conflict_resolution(),
        )
        .ok_or_else(|| unknown_domain(command.domain()))
}

/// The expected sequence, stamped with `head` or checked against it, and
/// the nonce (from `nonces`) under AGGREGATE_HANDLES and COMMUTATIVE.
pub(crate) fn expect(
    command: &Identified,
    resolution: ConflictResolution,
    head: u64,
    nonces: &mut u64,
) -> Result<(u64, Option<Nonce>), KernelError> {
    let expected = command.expected_sequence().unwrap_or(head);
    if expected > head || (resolution == ConflictResolution::Strict && expected != head) {
        return Err(KernelError::Stale {
            stream: command.cover().clone(),
            expected,
            actual: head,
        });
    }
    let nonce = (resolution != ConflictResolution::Strict).then(|| {
        *nonces += 1;
        Nonce(*nonces)
    });
    Ok((expected, nonce))
}

/// Step 4 over `port`.
pub(crate) async fn load<P: Port>(
    port: &mut P,
    registry: &Registry,
    command: Identified,
    nonces: &mut u64,
) -> Result<Contextual, KernelError> {
    let resolution = resolution(registry, &command)?;
    let cover = command.cover();
    let windowed = resolution != ConflictResolution::Strict;
    let snapshot = port
        .read_snapshot(cover)
        .await
        .map_err(KernelError::Store)?
        .filter(|s| {
            registry.snapshot_usable(command.domain(), s)
                && !(windowed && command.expected_sequence().is_some_and(|e| s.sequence >= e))
        });
    let from = snapshot.as_ref().map_or(0, |s| s.sequence + 1);
    let (snapshot, pages, head, last_recorded_at) = match port
        .read_from(cover, from)
        .await
        .map_err(KernelError::Store)?
    {
        Some(slice) => (
            snapshot,
            slice.pages,
            slice.next_sequence,
            slice.last_recorded_at,
        ),
        None => (None, Vec::new(), 0, None),
    };
    let (expected, nonce) = expect(&command, resolution, head, nonces)?;
    Ok(Contextual {
        command,
        resolution,
        expected,
        snapshot,
        pages,
        head,
        last_recorded_at,
        nonce,
    })
}

/// Step 5's first half: the stream as `D` (`staged`, a chain's
/// `Stream<D>`, if it has one; else from the snapshot or from 0) and the
/// command's input, upcast and decoded.
pub(crate) fn prepare<D: Domain>(
    command: Contextual,
    staged: Option<Box<dyn AnyType + Send>>,
    registry: &Registry,
) -> Result<Prepared<D>, KernelError> {
    let cover = command.command.cover().clone();
    let wrong = || KernelError::WrongDomain {
        stream: cover.clone(),
        expected: D::NAME,
    };
    if cover.domain != D::NAME {
        return Err(wrong());
    }
    let stream = match (staged, &command.snapshot) {
        (Some(staged), _) => *staged.downcast::<Stream<D>>().map_err(|_| wrong())?,
        (None, Some(snapshot)) => {
            // The stream's last instant is the floor only if no event
            // follows the snapshot; otherwise the events carry their own.
            let floor = command
                .last_recorded_at
                .filter(|_| command.pages.is_empty());
            Stream::resume(cover, snapshot, floor, &command.pages, registry)?
        }
        (None, None) => Stream::replay(cover, &command.pages, registry)?,
    };
    let input = decode_input::<D>(&command.command, registry)?;
    Ok(Prepared {
        command: command.command,
        input,
        stream,
        resolution: command.resolution,
        expected: command.expected,
        nonce: command.nonce,
    })
}

/// The command's payload through `D`'s upcaster, decoded strictly.
pub(crate) fn decode_input<D: Domain>(
    command: &Identified,
    registry: &Registry,
) -> Result<Input<D>, KernelError> {
    let stream = || command.cover().clone();
    let upcaster = registry
        .upcaster(D::NAME)
        .ok_or_else(|| unknown_domain(D::NAME))?;
    let payload = upcaster
        .upcast_command(command.payload())
        .map_err(|error| KernelError::Upcast {
            stream: stream(),
            error,
        })?;
    let input = match command.body() {
        Body::Command { .. } => D::Command::from_any(&payload).map(Input::Command),
        Body::Fact { source, .. } => {
            D::Fact::from_any(&payload).map(|fact| Input::Fact(fact, source.clone()))
        }
    };
    input.map_err(|e| KernelError::UnknownType {
        stream: stream(),
        type_url: match e {
            PayloadError::UnknownType { type_url } | PayloadError::Decode { type_url, .. } => {
                type_url
            }
        },
    })
}

/// Step 5's second half on `stream` (see [`Prepared::decide`]).
#[allow(clippy::too_many_arguments)]
pub(crate) fn decide_on<D: Domain>(
    stream: &Stream<D>,
    command: &Identified,
    input: &Input<D>,
    resolution: ConflictResolution,
    expected: u64,
    nonce: Option<Nonce>,
    sampled: &Sampled,
    registry: &Registry,
    dead_letters: &dyn DeadLetterLog,
) -> Result<Decision<D>, Failure<D::Rejection>> {
    let kernel = Failure::Kernel;
    let cover = stream.cover();
    let head = stream.len();
    let recorded_at = crate::next::not_before(stream.last_recorded_at().as_ref(), sampled.now);
    let (rules_version, rules) = registry
        .rules::<D>()
        .ok_or_else(|| kernel(unknown_domain(D::NAME)))?
        .current();
    let events = match input {
        Input::Fact(fact, source) => {
            let ctx = IngestCtx {
                stream: cover,
                recorded_at,
                command_id: command.command_id(),
                correlation_id: command.correlation_id(),
                source,
                rules,
                rules_version,
            };
            match catch_unwind(AssertUnwindSafe(|| D::ingest(stream.state(), fact, &ctx))) {
                Ok(events) => events,
                Err(_) => {
                    write_dead_letter(dead_letters, command.to_book(), D::NAME, "ingest panicked");
                    return Err(kernel(KernelError::IngestPanicked {
                        stream: cover.clone(),
                        command_id: command.command_id().clone(),
                    }));
                }
            }
        }
        Input::Command(cmd) => {
            let given = nonce.as_ref().map(|n| n.0);
            let conflicts = match nonce {
                Some(nonce) if resolution == ConflictResolution::AggregateHandles => {
                    let start = expected.checked_sub(stream.first()).ok_or_else(|| {
                        kernel(KernelError::BeforeSnapshot {
                            stream: cover.clone(),
                            first: stream.first(),
                        })
                    })?;
                    Some(Conflicts {
                        window: &stream.events()[start as usize..],
                        nonce,
                    })
                }
                _ => None,
            };
            let mut tx = Tx {
                committed: stream.state(),
                staged: None,
                rules,
                rules_version,
                stream: cover,
                head,
                recorded_at,
                command_id: command.command_id(),
                correlation_id: command.correlation_id(),
                causation_id: command.causation_id(),
                conflicts,
                resolved: None,
                events: Vec::new(),
            };
            D::decide(cmd, &mut tx).map_err(Failure::Rejected)?;
            let stale = |unresolved: bool| {
                let (stream, actual) = (cover.clone(), head);
                kernel(if unresolved {
                    KernelError::Unresolved {
                        stream,
                        expected,
                        actual,
                    }
                } else {
                    KernelError::Stale {
                        stream,
                        expected,
                        actual,
                    }
                })
            };
            match resolution {
                ConflictResolution::AggregateHandles
                    if tx.resolved.as_ref().map(|n| n.0) != given =>
                {
                    write_dead_letter(
                        dead_letters,
                        command.to_book(),
                        D::NAME,
                        "decide never acknowledged its conflict window",
                    );
                    return Err(stale(true));
                }
                ConflictResolution::Commutative if expected < head => {
                    let before = stream.state_at(expected, registry).map_err(kernel)?;
                    let window = D::State::changed(&before, stream.state());
                    let mine = D::State::changed(stream.state(), tx.state());
                    if window.overlaps(&mine) {
                        return Err(stale(false));
                    }
                }
                _ => {}
            }
            tx.events
        }
    };
    let meta = command.meta(recorded_at, rules_version);
    let events = (head..)
        .zip(events)
        .map(|(sequence, event)| Envelope::stamped(sequence, meta.clone(), event))
        .collect::<Result<_, _>>()
        .map_err(|e| kernel(KernelError::Replay(e)))?;
    Ok(Decision {
        stream: cover.clone(),
        head,
        command_id: command.command_id().clone(),
        events,
    })
}

/// A step's stream after it, and the pages it adds.
pub(crate) type Stepped<D> = (Stream<D>, Vec<EventPage>);

/// Step 5 for a chain: prepare, decide, commit in memory; the stream
/// after, and the pages to append.
pub(crate) fn run_step<D: Domain>(
    command: Contextual,
    staged: Option<Box<dyn AnyType + Send>>,
    registry: &Registry,
    sampled: &Sampled,
    dead_letters: &dyn DeadLetterLog,
) -> Result<Stepped<D>, Failure<D::Rejection>> {
    let mut prepared = prepare::<D>(command, staged, registry).map_err(Failure::Kernel)?;
    let decision = prepared.decide(sampled, registry, dead_letters)?;
    let pages = decision.events().iter().map(Envelope::page).collect();
    let mut stream = prepared.into_stream();
    stream
        .commit(decision, registry, &mut [])
        .map_err(Failure::Kernel)?;
    Ok((stream, pages))
}
