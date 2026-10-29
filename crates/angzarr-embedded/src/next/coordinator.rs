//! Steps 3 to 5 of the pipeline: the in-process coordinator, over
//! angzarr-router.
//!
//! angzarr's aggregate coordinator is a service that loads a stream, checks
//! or stamps the command's expected sequence and hands a
//! `ContextualCommand` to business code. angzarr-embedded has no service and
//! sends nothing: the chain does the same steps in process, through the
//! store, domain-blind (it sees `Cover`s, pages and the [`Registry`], never
//! a domain's types), and hands the `ContextualCommand` to the domain's
//! router (its `CommandHandler`), which owns the business rules:
//!
//! 3. [`deduplicate`]: **load the stream first** (resolve the conflict
//!    resolution: the book's, the command type's, the domain's, else
//!    COMMUTATIVE where the state supports it and STRICT otherwise,
//!    [`Registry::resolve`]; then the newest usable snapshot (under the
//!    current rules and state type, and below the head) and the events
//!    after it), **then** look the command id up (the store's
//!    `find_command`), and ask the router to admit the input: it decides
//!    what an input meets before its handler runs (a refusal fact's
//!    address, deduplication by the events found, a process manager's
//!    start rule). Events found are answered [`Deduplicated::Duplicate`],
//!    and nothing below runs. The load comes first so that a concurrent
//!    commit of the same command (a retry racing its original) can never
//!    slip between the two reads unseen: if it lands after the load, either
//!    the lookup sees its events, or this command's append, expecting the
//!    head it loaded, fails as a conflict and the retry then finds the
//!    duplicate. Before the sequence check, so a retry of an applied STRICT
//!    command whose expected sequence is now stale is a duplicate, never
//!    stale.
//! 4. [`load`]: stamp an absent expected sequence with the head, or check a
//!    present one: under STRICT it must be the head ([`KernelError::Stale`]);
//!    under AGGREGATE_HANDLES and COMMUTATIVE the events from it to the head
//!    are the window, delivered as events (the snapshot used is at or below
//!    expected−1, so none of the window is hidden in snapshot state), with a
//!    nonce. An expected sequence past the head is stale under every
//!    resolution.
//! 5. [`dispatch`]: the `ContextualCommand` (the stream's snapshot and
//!    events, upcast; the book with its ids and its handling resolved; the
//!    nonce) through the router, whose handler decides or records; the
//!    kernel stamps every event it answers with the input's
//!    [`Identified::meta`]. A panicking fact handler is caught.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use angzarr_proto::v1::{ConflictResolution, EventBook, EventPage, Handling, Snapshot};
use angzarr_router::domain::{Admission, Arrival, StreamStatus};
use angzarr_router::error::{codes, CodedError};
use angzarr_router::pb::ContextualCommand;
use prost_types::Timestamp;

use super::check::{Body, Identified, Malformed, Sampled};
use super::dead_letter::{write_dead_letter, write_handler_fault, DeadLetterLog};
use super::error::{Failure, KernelError};
use super::markup::STEP_LIMIT;
use super::registry::Registry;
use super::route::{self, Rejected};
use super::store::Port;

/// What step 3 found.
pub(crate) enum Deduplicated {
    /// The stream holds this command's events already: they are the
    /// answer (angzarr's DUPLICATE), in sequence order, in an `EventBook`
    /// for the addressed stream.
    Duplicate { events: EventBook },
    /// A process manager's fact for a stream with no instance, of a type
    /// that does not start one: skipped, answered applied with no events.
    Skipped,
    /// Nothing found: the command goes on to [`load`]. A command whose
    /// earlier decision emitted nothing lands here again (an accepted cost:
    /// it is decided again, against the stream as it is now).
    Novel(Box<Novel>),
}

/// An [`Identified`] command that deduplication did not find, with the
/// stream it loaded first: the only thing [`load`] takes.
pub(crate) struct Novel {
    command: Identified,
    loaded: Loaded,
}

/// What step 3 read, before its lookup: the resolution, the snapshot, the
/// events after it, the head and the last instant.
pub(crate) struct Loaded {
    resolution: ConflictResolution,
    snapshot: Option<Snapshot>,
    pages: Vec<EventPage>,
    head: u64,
    last_recorded_at: Option<Timestamp>,
}

/// A command with its stream's history and its handling resolved: angzarr's
/// `ContextualCommand`, in process. Domain-blind: the history is pages.
pub(crate) struct Contextual {
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
    pub(crate) nonce: Option<Vec<u8>>,
}

impl Contextual {
    /// The stream's next sequence.
    pub fn head(&self) -> u64 {
        self.head
    }
}

/// The refusal for an incoming book whose domain the registry does not
/// hold: the caller's.
pub(crate) fn unknown_domain(domain: &str) -> KernelError {
    KernelError::Malformed(Malformed::UnknownDomain {
        domain: domain.to_string(),
    })
}

/// The router's refusal `error` of an input to `command`'s stream, in the
/// kernel's terms: a business refusal (FAILED_PRECONDITION, whatever its
/// code) is [`Failure::Rejected`]; a decision past its step limit
/// [`KernelError::TooManySteps`]; anything else, a handler's own code under
/// any other status and a policy's failure in its source's `#[on_refused]`
/// handler included (the chain makes it
/// [`StepError::PolicyFailed`](super::StepError::PolicyFailed)),
/// [`KernelError::Router`].
pub(crate) fn refused(command: &Identified, error: CodedError) -> Failure<Rejected> {
    if error.code == route::TOO_MANY_STEPS {
        return Failure::Kernel(KernelError::TooManySteps {
            stream: command.cover().clone(),
            limit: STEP_LIMIT,
        });
    }
    if error.code == route::HANDLER_UNAVAILABLE {
        return Failure::Kernel(KernelError::HandlerUnavailable {
            stream: command.cover().clone(),
            cause: error.extras.get("cause").cloned().unwrap_or_default(),
        });
    }
    if let Some(error) = route::upcast_failure(&error) {
        return Failure::Kernel(KernelError::Upcast {
            stream: command.cover().clone(),
            error,
        });
    }
    if route::is_rejection(&error) {
        return Failure::Rejected(Rejected::of(&error));
    }
    Failure::Kernel(KernelError::Router(error))
}

/// Step 3 over `port`: load the stream, look the command up, and have the
/// router admit it.
pub(crate) async fn deduplicate<P: Port>(
    port: &mut P,
    registry: &Registry,
    command: Identified,
) -> Result<Deduplicated, Failure<Rejected>> {
    let loaded = read(port, registry, &command)
        .await
        .map_err(Failure::Kernel)?;
    let recorded = port
        .find_command(command.cover(), command.command_id())
        .await
        .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
    let handler = registry
        .handler(command.domain())
        .ok_or_else(|| Failure::Kernel(unknown_domain(command.domain())))?;
    let book = command.to_book();
    let arrival = Arrival {
        command: &book,
        recorded: &recorded,
        stream: StreamStatus {
            exists: loaded.head > 0,
        },
    };
    match handler.admit(&arrival) {
        Ok(Admission::Duplicate(pages)) => Ok(Deduplicated::Duplicate {
            events: EventBook {
                cover: Some(command.cover().clone()),
                snapshot: None,
                next_sequence: pages.last().map_or(0, |p| p.sequence + 1),
                pages,
            },
        }),
        Ok(Admission::Skip) => Ok(Deduplicated::Skipped),
        Ok(Admission::Dispatch) => Ok(Deduplicated::Novel(Box::new(Novel { command, loaded }))),
        Err(error) => Err(refused(&command, error)),
    }
}

/// The command's resolution ([`Registry::resolve`]): never UNSPECIFIED.
/// The configuration per command type is looked up by the command's
/// current type, after the domain's upcaster, so an old type has the
/// setting of the type it upcasts to. A payload whose upcast fails is
/// looked up as sent; the router then reports the failure.
pub(crate) fn resolution(
    registry: &Registry,
    command: &Identified,
) -> Result<ConflictResolution, KernelError> {
    let domain = command.domain();
    let sent = command.payload();
    let current = registry
        .upcaster(domain)
        .and_then(|u| u.upcast_command(sent).ok())
        .map_or_else(|| sent.type_url.clone(), |any| any.type_url);
    registry
        .resolve(domain, &current, command.conflict_resolution())
        .ok_or_else(|| unknown_domain(domain))
}

/// The expected sequence, stamped with `head` or checked against it, and
/// the nonce (from `nonces`) under AGGREGATE_HANDLES and COMMUTATIVE.
pub(crate) fn expect(
    command: &Identified,
    resolution: ConflictResolution,
    head: u64,
    nonces: &mut u64,
) -> Result<(u64, Option<Vec<u8>>), KernelError> {
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
        nonces.to_be_bytes().to_vec()
    });
    Ok((expected, nonce))
}

/// The read half of step 3: the resolution, the newest usable snapshot and
/// the events after it. A snapshot at or past the head is not usable: the
/// stream is read from 0.
pub(crate) async fn read<P: Port>(
    port: &mut P,
    registry: &Registry,
    command: &Identified,
) -> Result<Loaded, KernelError> {
    let resolution = resolution(registry, command)?;
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
    let mut slice = port
        .read_from(cover, from)
        .await
        .map_err(KernelError::Store)?;
    let snapshot = match (snapshot, &slice) {
        // At or past the head: it claims events the stream does not hold
        // (a purge left it, or it was written ahead of them), so it is
        // unusable, like one under other rules, and the stream is read
        // from its events.
        (Some(s), read) if s.sequence >= read.next_sequence => {
            slice = port.read_from(cover, 0).await.map_err(KernelError::Store)?;
            None
        }
        (snapshot, _) => snapshot,
    };
    Ok(Loaded {
        resolution,
        snapshot,
        pages: slice.pages,
        head: slice.next_sequence,
        last_recorded_at: slice.last_recorded_at,
    })
}

/// Step 4: the expected sequence stamped or checked against the head step 3
/// loaded.
pub(crate) fn load(novel: Novel, nonces: &mut u64) -> Result<Contextual, KernelError> {
    let Novel { command, loaded } = novel;
    let (expected, nonce) = expect(&command, loaded.resolution, loaded.head, nonces)?;
    Ok(Contextual {
        command,
        resolution: loaded.resolution,
        expected,
        snapshot: loaded.snapshot,
        pages: loaded.pages,
        head: loaded.head,
        last_recorded_at: loaded.last_recorded_at,
        nonce,
    })
}

/// Step 5: `command` through its domain's router, against its stream's
/// history: the events it answers, stamped at `recorded_at = max(sampled
/// now, the stream's last)` with the domain's rules version, from the
/// head on. A panicking fact handler fails the chain
/// ([`KernelError::IngestPanicked`]) and `dead_letters` gets a line; a
/// panicking command handler panics on. A handler's fault under a code of
/// its own ([`route::is_handler_fault`]) fails it as
/// [`KernelError::Router`], a bug, and `dead_letters` gets a line of its
/// code and args.
pub(crate) fn dispatch(
    command: &Contextual,
    sampled: &Sampled,
    registry: &Registry,
    dead_letters: &dyn DeadLetterLog,
) -> Result<Vec<EventPage>, Failure<Rejected>> {
    let identified = &command.command;
    let domain = identified.domain();
    let kernel = Failure::Kernel;
    let handler = registry
        .handler(domain)
        .ok_or_else(|| kernel(unknown_domain(domain)))?;
    let recorded_at = crate::next::not_before(command.last_recorded_at.as_ref(), sampled.now);
    let contextual = contextual_command(command, registry).map_err(kernel)?;
    let answered = catch_unwind(AssertUnwindSafe(|| {
        let delivery = route::Delivery { recorded_at };
        route::recording(delivery, || handler.dispatch(&contextual))
    }));
    let answered = match answered {
        Ok(answered) => answered,
        Err(panic) if matches!(identified.body(), Body::Fact { .. }) => {
            drop(panic);
            write_dead_letter(
                dead_letters,
                identified.to_book(),
                domain,
                "ingest panicked",
            );
            return Err(kernel(KernelError::IngestPanicked {
                stream: identified.cover().clone(),
                command_id: identified.command_id().clone(),
            }));
        }
        Err(panic) => resume_unwind(panic),
    };
    let response = answered.map_err(|error| {
        if error.code == codes::CONFLICT_UNRESOLVED {
            write_dead_letter(
                dead_letters,
                identified.to_book(),
                domain,
                "decide never acknowledged its conflict window",
            );
        }
        let (stream, expected, actual) =
            (identified.cover().clone(), command.expected, command.head);
        match error.code.as_str() {
            codes::CONFLICT_OVERLAP => Failure::Kernel(KernelError::Stale {
                stream,
                expected,
                actual,
            }),
            codes::CONFLICT_UNRESOLVED => Failure::Kernel(KernelError::Unresolved {
                stream,
                expected,
                actual,
            }),
            _ => {
                let failure = refused(identified, error);
                if let Failure::Kernel(KernelError::Router(error)) = &failure {
                    if route::is_handler_fault(error) {
                        write_handler_fault(dead_letters, identified.to_book(), domain, error);
                    }
                }
                failure
            }
        }
    })?;
    let rules_version = registry
        .rules_version(domain)
        .map(str::to_string)
        .unwrap_or(response.rules_version);
    let meta = identified.meta(recorded_at, &rules_version);
    Ok((command.head..)
        .zip(
            response
                .events
                .unwrap_or_default()
                .pages
                .into_iter()
                .filter(|page| !route::is_nothing(page)),
        )
        .map(|(sequence, page)| EventPage {
            sequence,
            meta: Some(meta.clone()),
            ..page
        })
        .collect())
}

/// The `ContextualCommand` step 5 hands `command`'s domain: its stream's
/// history upcast, its handling resolved (COMMUTATIVE at the head is handed
/// over as STRICT, with no nonce), and the delivery's nonce.
pub(crate) fn contextual_command(
    command: &Contextual,
    registry: &Registry,
) -> Result<ContextualCommand, KernelError> {
    let identified = &command.command;
    let upcaster = registry.upcaster(identified.domain());
    let pages = command
        .pages
        .iter()
        .map(|page| match upcaster {
            Some(upcaster) => upcaster.upcast_page(page),
            None => Ok(page.clone()),
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| KernelError::Upcast {
            stream: identified.cover().clone(),
            error,
        })?;
    // At the head there is no window: nothing for COMMUTATIVE to compare,
    // so the command is handed over as STRICT, with no nonce.
    let (resolution, nonce) = match command.resolution {
        ConflictResolution::Commutative if command.expected == command.head => {
            (ConflictResolution::Strict, None)
        }
        resolution => (resolution, command.nonce.clone()),
    };
    let mut book = identified.to_book();
    book.handling = Some(Handling {
        expected_sequence: Some(command.expected),
        conflict_resolution: resolution as i32,
    });
    Ok(ContextualCommand {
        events: Some(EventBook {
            cover: Some(identified.cover().clone()),
            snapshot: command.snapshot.clone(),
            pages,
            next_sequence: command.head,
        }),
        command: Some(book),
        conflict_nonce: nonce.unwrap_or_default().into(),
    })
}

#[cfg(test)]
#[path = "steps.test.rs"]
pub(crate) mod steps;

#[cfg(test)]
#[path = "coordinator.test.rs"]
mod tests;
