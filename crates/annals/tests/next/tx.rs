//! What a decide reads through its `Tx`: a probe domain records every
//! accessor into its event, reached as a policy's target so it has a
//! causation too.

use std::convert::Infallible;

use annals::next::v1::{ConflictResolution, StateDescriptor, StateOptions, Uuid};
use annals::next::{
    ids, pack, Committed, Decoder, Domain, InMemoryStore, IngestCtx, Kernel, MemoryDeadLetters,
    Payload, PayloadError, Policy, Position, Reaction, Registry, Rulebook, State, Tx, Upcaster,
};

use crate::toys::{at, command, cover, fees, opened, Account, AccountCmd, Edit};

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProbeState {
    #[prost(uint32, tag = "1")]
    pub bumps: u32,
}

impl prost::Name for ProbeState {
    const NAME: &'static str = "ProbeState";
    const PACKAGE: &'static str = "toy.probe.v1";
}

/// What decide saw.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Probed {
    #[prost(string, tag = "1")]
    pub rules_version: String,
    #[prost(string, tag = "2")]
    pub domain: String,
    #[prost(int64, tag = "3")]
    pub recorded_seconds: i64,
    #[prost(bytes = "vec", tag = "4")]
    pub command_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub correlation_id: Vec<u8>,
    #[prost(uint64, optional, tag = "6")]
    pub causation: Option<u64>,
    #[prost(uint64, tag = "7")]
    pub first: u64,
    #[prost(uint64, tag = "8")]
    pub next: u64,
    #[prost(uint64, tag = "9")]
    pub emitted: u64,
    #[prost(uint32, tag = "10")]
    pub bumps_seen: u32,
    #[prost(uint32, tag = "11")]
    pub bumps_committed: u32,
    #[prost(bool, tag = "12")]
    pub conflicts: bool,
}

impl prost::Name for Probed {
    const NAME: &'static str = "Probed";
    const PACKAGE: &'static str = "toy.probe.v1";
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Bumped {}

impl prost::Name for Bumped {
    const NAME: &'static str = "Bumped";
    const PACKAGE: &'static str = "toy.probe.v1";
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Probe {}

impl prost::Name for Probe {
    const NAME: &'static str = "Probe";
    const PACKAGE: &'static str = "toy.probe.cmd.v1";
}

impl State for ProbeState {
    fn descriptor() -> StateDescriptor {
        StateDescriptor {
            type_url: "/toy.probe.v1.ProbeState".to_string(),
            options: Some(StateOptions {
                conflict_resolutions: vec![
                    ConflictResolution::Strict as i32,
                    ConflictResolution::AggregateHandles as i32,
                ],
            }),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum ProbeEvent {
    Bumped,
    Probed(Probed),
}

impl Payload for ProbeEvent {
    fn to_any(&self) -> annals::next::Any {
        match self {
            ProbeEvent::Bumped => pack(&Bumped {}),
            ProbeEvent::Probed(p) => pack(p),
        }
    }

    fn from_any(any: &annals::next::Any) -> Result<Self, PayloadError> {
        Decoder::new()
            .on::<Bumped>(|_| ProbeEvent::Bumped)
            .on::<Probed>(ProbeEvent::Probed)
            .decode(any)
    }

    fn type_urls() -> Vec<String> {
        Vec::new()
    }
}

impl Payload for Probe {
    fn to_any(&self) -> annals::next::Any {
        pack(self)
    }

    fn from_any(any: &annals::next::Any) -> Result<Self, PayloadError> {
        Decoder::new().on::<Probe>(|p| p).decode(any)
    }

    fn type_urls() -> Vec<String> {
        Vec::new()
    }
}

pub struct ProbeDomain;

impl Domain for ProbeDomain {
    type State = ProbeState;
    type Command = Probe;
    type Fact = Infallible;
    type Event = ProbeEvent;
    type Rejection = Infallible;
    type Rules = ();

    const NAME: &'static str = "probe";

    fn decide(_: &Probe, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
        let conflicts = tx.conflicts().is_some();
        let committed = tx.state().bumps;
        let first = tx.emit(ProbeEvent::Bumped);
        let probed = Probed {
            rules_version: tx.rules_version().to_string(),
            domain: tx.stream().domain.clone(),
            recorded_seconds: tx.recorded_at().seconds,
            command_id: tx.command_id().value.to_vec(),
            correlation_id: tx.correlation_id().value.to_vec(),
            causation: tx.causation_id().map(|c| c.sequence),
            first,
            next: tx.next_sequence(),
            emitted: tx.emitted() as u64,
            bumps_seen: tx.state().bumps,
            bumps_committed: committed,
            conflicts,
        };
        let () = *tx.rules();
        tx.emit(ProbeEvent::Probed(probed));
        Ok(())
    }

    fn ingest(_: &ProbeState, fact: &Infallible, _: &IngestCtx<'_, Self>) -> Vec<ProbeEvent> {
        match *fact {}
    }

    fn apply(s: &mut ProbeState, e: &ProbeEvent, (): &()) {
        if let ProbeEvent::Bumped = e {
            s.bumps += 1;
        }
    }
}

fn probe_root() -> Uuid {
    ids::compute_root("probe", "one").unwrap()
}

/// Every account event → the probe.
struct Probing;

impl Reaction for Probing {
    type Source = Account;
    type Target = ProbeDomain;
    type TargetId = ();

    const NAME: &'static str = "probing";

    fn targets(_: &crate::toys::account::AccountState) -> Vec<()> {
        vec![()]
    }

    fn root((): &()) -> Uuid {
        probe_root()
    }

    fn position(_: &ProbeState) -> Position {
        Position::START
    }
}

impl Policy for Probing {
    fn command((): &(), _: &Committed<'_, Account>) -> Probe {
        Probe {}
    }
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry
        .register::<Account>(fees(), Upcaster::new("u", "account"))
        .unwrap();
    registry
        .register::<ProbeDomain>(Rulebook::new("p7", ()), Upcaster::new("u", "probe"))
        .unwrap();
    registry
}

fn probes(books: &[annals::next::v1::EventBook]) -> Vec<Probed> {
    let probe: Vec<_> = books
        .iter()
        .filter(|b| b.cover.as_ref().is_some_and(|c| c.domain == "probe"))
        .cloned()
        .collect();
    crate::toys::payloads::<ProbeEvent>(&probe)
        .into_iter()
        .filter_map(|e| match e {
            ProbeEvent::Probed(p) => Some(p),
            ProbeEvent::Bumped => None,
        })
        .collect()
}

#[test]
fn decide_reads_the_rules_the_stream_the_instant_and_the_ids() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry, &log);
    let mut store = InMemoryStore::new();
    opened(&mut store, &registry, 1, 0);
    let routes = annals::next::Routes::new().policy::<Probing>();
    let book = command(cover("account", 1), AccountCmd::Deposit(5).to_any())
        .id(3)
        .correlated(6);
    let handled = kernel
        .handle::<Account, _, _>(store.begin(), &routes, book, &at(50), &mut ())
        .unwrap();
    let trigger = annals::next::v1::EventRef {
        stream: Some(cover("account", 1)),
        sequence: 2,
    };
    assert_eq!(
        probes(&handled.books()),
        [Probed {
            rules_version: "p7".to_string(),
            domain: "probe".to_string(),
            recorded_seconds: 50,
            command_id: ids::reaction_command_id(&trigger, "probing", 0)
                .unwrap()
                .value
                .to_vec(),
            correlation_id: crate::toys::correlation(6).value.to_vec(),
            causation: Some(2),
            first: 0,
            next: 1,
            emitted: 1,
            bumps_seen: 1,
            bumps_committed: 0,
            conflicts: false,
        }]
    );

    // Again: the probe stream is at 2 now, and decide sees its state.
    let book = command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(4);
    let handled = kernel
        .handle::<Account, _, _>(store.begin(), &routes, book, &at(51), &mut ())
        .unwrap();
    let probed = &probes(&handled.books())[0];
    assert_eq!((probed.first, probed.next), (2, 3));
    assert_eq!((probed.bumps_committed, probed.bumps_seen), (1, 2));
    assert_eq!(
        probed.correlation_id,
        crate::toys::command_id(4).value.to_vec()
    );
}
