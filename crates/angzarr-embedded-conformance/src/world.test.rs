//! The coordinator a scenario runs: angzarr-embedded's kernel over an
//! in-memory store, every domain behind a scripted business process
//! ([`Business`]) registered by its descriptor, as a child's is. Givens
//! describe the domains and write their history; whens hand the kernel
//! inputs, keeping each answer and what the dead-letter log was told;
//! thens read the answers, the deliveries and the store.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use angzarr_embedded::testing::{MemoryStore, Ready as _, Samples};
use angzarr_embedded::v1::{
    command_page, Command, CommandBook, CommandPage, ComponentDescriptor, ComponentKind,
    ConflictResolution, Cover, EventPage, Fact, FactOrigin, Handling, Snapshot, StateDescriptor,
    StateOptions, Uuid,
};
use angzarr_embedded::{
    Any, EventStore, Failure, Handled, Kernel, MemoryDeadLetters, OnRetryable, Registry, Rejected,
    SnapshotStore, StreamRef, Wait, Write, DEAD_LETTER_PREFIX,
};

use crate::tests::answer::Answered;
use crate::tests::business::{Business, Process};

/// A domain as its descriptor registers it, and the business process
/// behind it.
pub(crate) struct Domain {
    pub(crate) kind: ComponentKind,
    pub(crate) rules_version: String,
    pub(crate) state_type: String,
    pub(crate) resolutions: Vec<ConflictResolution>,
    pub(crate) business: Arc<Business>,
}

/// The waits the kernel backs off with, kept, each over at once; every
/// jitter draw 0, so each wait is its backoff.
#[derive(Default)]
pub(crate) struct Waits(Mutex<Vec<Duration>>);

impl Waits {
    pub(crate) fn waited(&self) -> Vec<Duration> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Wait for Waits {
    fn wait(
        &self,
        backoff: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// The world: one coordinator, its store and its domains.
#[derive(cucumber::World)]
#[world(init = Self::new)]
pub(crate) struct Coordinator {
    pub(crate) store: MemoryStore,
    pub(crate) domains: BTreeMap<String, Domain>,
    pub(crate) retry: OnRetryable,
    pub(crate) waits: Waits,
    pub(crate) log: MemoryDeadLetters,
    /// Inputs given, not yet handled.
    pub(crate) pending: Vec<CommandBook>,
    /// Every input handled, with its answer, in order.
    pub(crate) handled: Vec<(CommandBook, Result<Handled, Failure<Rejected>>)>,
    /// Each stream's length when the first when began: what the givens
    /// wrote.
    baseline: Option<BTreeMap<String, u64>>,
    /// The order stream the scenario speaks of, when it names none.
    current: Option<Cover>,
    samples: Samples,
    inputs: u32,
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Coordinator")
            .field("domains", &self.domains.keys().collect::<Vec<_>>())
            .field("pending", &self.pending)
            .field("answers", &self.answers())
            .finish_non_exhaustive()
    }
}

/// The type URL package of `domain`'s messages: `orders.v1.` for `order`.
fn package(domain: &str) -> String {
    let plural = match domain {
        "order" => "orders",
        other => other,
    };
    format!("{}.v1.", plural.replace('-', ""))
}

/// The stream `cover` names, as the store's ports take it.
pub(crate) fn sref(cover: &Cover) -> StreamRef<'_> {
    StreamRef::try_from(cover).expect("a well-formed stream")
}

/// A stream's key in the baseline.
fn key(cover: &Cover) -> String {
    format!("{cover:?}")
}

/// A root by its name in a scenario: a UUID as written, else 16 bytes
/// derived from the name, the same each time.
pub(crate) fn root(name: &str) -> Uuid {
    if let Ok(uuid) = name.parse::<Uuid>() {
        return uuid;
    }
    let mut bytes = [0u8; 16];
    for (i, b) in name.bytes().enumerate() {
        bytes[i % 16] = bytes[i % 16].wrapping_mul(31).wrapping_add(b);
    }
    bytes[15] |= 1;
    Uuid::from_bytes(bytes)
}

/// The resolution by its spec name.
pub(crate) fn resolution(name: &str) -> ConflictResolution {
    ConflictResolution::from_str_name(&format!("CONFLICT_RESOLUTION_{name}"))
        .or_else(|| ConflictResolution::from_str_name(name))
        .unwrap_or_else(|| panic!("no conflict resolution {name}"))
}

impl Coordinator {
    fn new() -> Coordinator {
        Coordinator {
            store: MemoryStore::new(),
            domains: BTreeMap::new(),
            retry: OnRetryable::default(),
            waits: Waits::default(),
            log: MemoryDeadLetters::new(),
            pending: Vec::new(),
            handled: Vec::new(),
            baseline: None,
            current: None,
            samples: Samples::starting_at(1_700_000_000),
            inputs: 0,
        }
    }

    /// `name`'s aggregate, registered at rules version `1.4.2+build.7`
    /// with state `/<package>State`, supporting every resolution, unless a
    /// given has said otherwise.
    pub(crate) fn domain(&mut self, name: &str) -> &mut Domain {
        let store = self.store.clone();
        self.domains.entry(name.to_string()).or_insert_with(|| {
            let package = package(name);
            let state_type = format!("/{package}State");
            let rules_version = "1.4.2+build.7";
            Domain {
                kind: ComponentKind::Aggregate,
                rules_version: rules_version.to_string(),
                business: Arc::new(Business::new(&package, rules_version, &state_type, &store)),
                state_type,
                resolutions: vec![
                    ConflictResolution::Strict,
                    ConflictResolution::AggregateHandles,
                    ConflictResolution::Commutative,
                ],
            }
        })
    }

    /// `name`'s business process.
    pub(crate) fn business(&mut self, name: &str) -> Arc<Business> {
        Arc::clone(&self.domain(name).business)
    }

    /// The stream of `domain` root `name`, on the main timeline.
    pub(crate) fn cover(&self, domain: &str, name: &str) -> Cover {
        Cover {
            domain: domain.to_string(),
            root: Some(root(name)),
            edition: None,
        }
    }

    /// The order stream a scenario speaks of when it names none:
    /// `order-1`, unless a given has spoken of another.
    pub(crate) fn order(&self) -> Cover {
        self.current
            .clone()
            .unwrap_or_else(|| self.cover("order", "order-1"))
    }

    /// From here on the scenario speaks of the order stream `name`.
    pub(crate) fn speak_of(&mut self, name: &str) {
        self.current = Some(self.cover("order", name));
    }

    /// The type URL of `name` in `domain`'s package.
    pub(crate) fn type_url(&mut self, domain: &str, name: &str) -> String {
        self.business(domain).type_url(name)
    }

    /// Events of the types `names` appended to `cover`, at its head, as a
    /// command of the past recorded them.
    pub(crate) fn write(&mut self, cover: &Cover, names: &[&str]) {
        let head = self.pages(cover).len() as u64;
        let pages: Vec<EventPage> = (head..)
            .zip(names)
            .map(|(sequence, name)| EventPage {
                sequence,
                payload: Some(Any {
                    type_url: self.type_url(&cover.domain, name),
                    value: Vec::new(),
                }),
                ..EventPage::default()
            })
            .collect();
        self.store
            .write(Write::append(sref(cover), head, &pages))
            .ready()
            .expect("the given events are written");
    }

    /// A snapshot of `cover` at `sequence`, of the state type
    /// `state_type`, recorded under `rules_version`.
    pub(crate) fn snapshot(
        &mut self,
        cover: &Cover,
        sequence: u64,
        state_type: &str,
        rules_version: &str,
    ) {
        let snapshot = Snapshot {
            sequence,
            state: Some(Any {
                type_url: state_type.to_string(),
                value: b"given".to_vec(),
            }),
            rules_version: rules_version.to_string(),
        };
        self.store
            .write_snapshot(sref(cover), &snapshot)
            .ready()
            .expect("the given snapshot is written");
    }

    /// `cover`'s pages as stored.
    pub(crate) fn pages(&self, cover: &Cover) -> Vec<EventPage> {
        self.store
            .read(sref(cover), 0, None)
            .ready()
            .expect("the in-memory store reads")
            .pages
    }

    /// The pages appended to `cover` since the first when, except the
    /// other writes a race landed (`OrderNoteAdded`).
    pub(crate) fn persisted(&self, cover: &Cover) -> Vec<EventPage> {
        let from = self
            .baseline
            .as_ref()
            .and_then(|b| b.get(&key(cover)).copied())
            .unwrap_or(0);
        self.pages(cover)
            .into_iter()
            .filter(|page| page.sequence >= from)
            .filter(|page| {
                !page
                    .payload
                    .as_ref()
                    .is_some_and(|p| p.type_url.ends_with(".OrderNoteAdded"))
            })
            .collect()
    }

    /// A client's command of the type `name` to `cover`, with a command id
    /// of its own and no handling.
    pub(crate) fn command(&mut self, cover: &Cover, name: &str) -> CommandBook {
        let payload = Any {
            type_url: self.type_url(&cover.domain, name),
            value: Vec::new(),
        };
        self.book(
            cover,
            command_page::Body::Command(Command {
                payload: Some(payload),
            }),
            None,
        )
    }

    /// A fact of the type `name` to `cover` from `external_id`.
    pub(crate) fn fact(&mut self, cover: &Cover, name: &str, external_id: &str) -> CommandBook {
        let payload = Any {
            type_url: self.type_url(&cover.domain, name),
            value: Vec::new(),
        };
        let origin = FactOrigin {
            external_id: external_id.to_string(),
            description: "a webhook".to_string(),
        };
        self.book(
            cover,
            command_page::Body::Fact(Fact {
                payload: Some(payload),
            }),
            Some(origin),
        )
    }

    fn book(
        &mut self,
        cover: &Cover,
        body: command_page::Body,
        fact_origin: Option<FactOrigin>,
    ) -> CommandBook {
        self.inputs += 1;
        let command_id = match fact_origin {
            Some(_) => None,
            None => {
                let mut bytes = [0x0c; 16];
                bytes[12..].copy_from_slice(&self.inputs.to_be_bytes());
                Some(Uuid::from_bytes(bytes))
            }
        };
        CommandBook {
            cover: Some(cover.clone()),
            page: Some(CommandPage {
                body: Some(body),
                command_id,
                fact_origin,
                ..CommandPage::default()
            }),
            handling: None,
        }
    }

    /// `book` expecting `expected` (`None`: unset), resolved by
    /// `resolution` (`None`: by configuration).
    pub(crate) fn handled_as(
        mut book: CommandBook,
        expected: Option<u64>,
        resolution: Option<ConflictResolution>,
    ) -> CommandBook {
        book.handling = Some(Handling {
            expected_sequence: expected,
            conflict_resolution: resolution.unwrap_or(ConflictResolution::Unspecified) as i32,
        });
        book
    }

    /// Every domain, registered by its descriptor.
    fn registry(&self) -> Registry {
        let mut registry = Registry::new();
        for (name, domain) in &self.domains {
            let descriptor = ComponentDescriptor {
                name: name.clone(),
                kind: domain.kind as i32,
                rules_version: domain.rules_version.clone(),
                state: Some(StateDescriptor {
                    type_url: domain.state_type.clone(),
                    options: Some(StateOptions {
                        conflict_resolutions: domain
                            .resolutions
                            .iter()
                            .map(|r| *r as i32)
                            .collect(),
                    }),
                }),
                ..ComponentDescriptor::default()
            };
            registry
                .command_handler(name, Process(Arc::clone(&domain.business)), descriptor)
                .unwrap_or_else(|e| panic!("{name} registers: {e}"));
        }
        registry
    }

    /// The first when begins: what the givens wrote is the baseline.
    pub(crate) fn begin(&mut self) {
        if self.baseline.is_some() {
            return;
        }
        let covers: Vec<Cover> = self
            .handled
            .iter()
            .filter_map(|(book, _)| book.cover.clone())
            .chain(self.pending.iter().filter_map(|book| book.cover.clone()))
            .chain(std::iter::once(self.order()))
            .collect();
        let baseline = covers
            .iter()
            .map(|cover| (key(cover), self.pages(cover).len() as u64))
            .collect();
        self.baseline = Some(baseline);
        for domain in self.domains.values() {
            domain.business.begin();
        }
    }

    /// `book`, handled by the kernel as its caller sends it, and kept with
    /// its answer.
    pub(crate) async fn handle(&mut self, book: CommandBook) {
        let registry = self.registry();
        let sampled = self.samples.next_sample();
        let store = self.store.clone();
        let result = Kernel::new(&registry)
            .log_with(&self.log)
            .on_retryable(self.retry)
            .wait_with(&self.waits)
            .handle_any(&store, book.clone(), &sampled)
            .await;
        self.handled.push((book, result));
    }

    /// Every pending input, handled in order.
    pub(crate) async fn handle_pending(&mut self) {
        self.begin();
        for book in std::mem::take(&mut self.pending) {
            self.handle(book).await;
        }
    }

    /// The book of the input handled last.
    pub(crate) fn last_book(&self) -> CommandBook {
        self.handled.last().expect("an input was handled").0.clone()
    }

    /// Every answer, in order.
    pub(crate) fn answers(&self) -> Vec<Answered> {
        self.handled
            .iter()
            .map(|(_, result)| Answered::of(result))
            .collect()
    }

    /// The last answer.
    pub(crate) fn answer(&self) -> Answered {
        self.answers().pop().expect("an input was handled")
    }

    /// The dead-letter log's alerts: its lines, each prefixed with its
    /// warning.
    pub(crate) fn alerts(&self) -> Vec<String> {
        let lines = self.log.lines();
        for line in &lines {
            assert!(line.starts_with(DEAD_LETTER_PREFIX), "{line}");
        }
        lines
    }
}
