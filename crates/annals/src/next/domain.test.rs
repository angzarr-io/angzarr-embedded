#[allow(unused_imports)]
use crate::next::toys::prelude::*;
mod ingest {
    //! Facts: ingested, never decided and never refused, with a read-only
    //! context.
    use crate::next::toys::prelude::*;
    use crate::next::v1::CommandBook;
    use crate::next::{ids, Payload};

    use crate::next::toys::{
        account, at, command, cover, fact, handle, opened, payloads, registry, Account, AccountCmd,
        AccountEvent, AccountFact,
    };

    fn payment(cents: i64, external_id: &str) -> CommandBook {
        fact(
            cover("account", 1),
            AccountFact::PaymentReceived(cents).to_any(),
            external_id,
        )
    }

    #[test]
    fn ingest_gets_a_read_only_context() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);

        let handled = handle::<Account>(&store, &registry, payment(70, "pi_7"), &at(40)).unwrap();
        assert_eq!(
            payloads::<AccountEvent>(&handled.books()),
            [AccountEvent::Received(account::Received {
                cents: 70,
                external_id: "pi_7".to_string(),
                recorded_seconds: 40,
                rules_version: "v2".to_string(),
                matched: true,
            })],
            "the context's recorded_at, source and rules version, copied by the toy"
        );
        let meta = handled.books()[0].pages[0].meta.clone().unwrap();
        assert_eq!(
            meta.causation,
            Some(crate::next::v1::event_meta::Causation::CommandId(
                ids::fact_command_id("pi_7")
            ))
        );
        assert_eq!(meta.fact.unwrap().external_id, "pi_7");
    }

    #[test]
    fn a_fact_is_never_refused() {
        let registry = registry();
        let store = MemoryStore::new();

        // The account was never opened: a Deposit command would be refused.
        let handled = handle::<Account>(&store, &registry, payment(70, "pi_8"), &at(40)).unwrap();
        let [AccountEvent::Received(received)] = &payloads::<AccountEvent>(&handled.books())[..]
        else {
            panic!("one event");
        };
        assert!(!received.matched, "recorded as unmatched");
        assert!(
            handle::<Account>(
                &store,
                &registry,
                command(cover("account", 1), AccountCmd::Deposit(70).to_any()),
                &at(41)
            )
            .is_err(),
            "where the command is refused"
        );
    }

    #[test]
    fn a_fact_is_stamped_at_the_head() {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);
        let handled = handle::<Account>(&store, &registry, payment(1, "pi_9"), &at(40)).unwrap();
        assert_eq!(handled.books()[0].pages[0].sequence, 2);
    }
}

mod tx {
    //! What a decide reads through its `Tx`: a probe domain records every
    //! accessor into its event, reached as a policy's target so it has a
    //! causation too.
    use crate::next::toys::prelude::*;

    use std::convert::Infallible;

    use crate::next::v1::{ConflictResolution, StateDescriptor, StateOptions, Uuid};
    use crate::next::{
        ids, pack, Committed, Decoder, Domain, IngestCtx, Kernel, MemoryDeadLetters, Payload,
        PayloadError, Policy, Registry, Rulebook, State, Tx, Upcaster,
    };

    use crate::next::toys::{at, command, cover, fees, opened, Account, AccountCmd, Edit};

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
        #[prost(uint64, tag = "13")]
        pub emitted_before: u64,
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

    /// `stash`: 0 acknowledges any window with its own nonce; 1 keeps the
    /// nonce in [`STASHED`] and acknowledges nothing; 2 acknowledges the window
    /// with the stashed nonce instead of its own: a decide smuggling a nonce
    /// from one delivery into another.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Probe {
        #[prost(uint32, tag = "1")]
        pub stash: u32,
    }

    /// The nonce a probe kept from an earlier delivery.
    static STASHED: std::sync::Mutex<Option<crate::next::Nonce>> = std::sync::Mutex::new(None);

    impl prost::Name for Probe {
        const NAME: &'static str = "Probe";
        const PACKAGE: &'static str = "toy.probe.cmd.v1";
    }

    impl State for ProbeState {
        type Snapshot = Self;

        fn to_snapshot(&self) -> Self {
            self.clone()
        }

        fn from_snapshot(snapshot: Self) -> Result<Self, annals_diagnostic::Report> {
            Ok(snapshot)
        }

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
        fn to_any(&self) -> crate::next::Any {
            match self {
                ProbeEvent::Bumped => pack(&Bumped {}),
                ProbeEvent::Probed(p) => pack(p),
            }
        }

        fn from_any(any: &crate::next::Any) -> Result<Self, PayloadError> {
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
        fn to_any(&self) -> crate::next::Any {
            pack(self)
        }

        fn from_any(any: &crate::next::Any) -> Result<Self, PayloadError> {
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

        fn decide(probe: &Probe, tx: &mut Tx<'_, Self>) -> Result<(), Infallible> {
            let window = tx.conflicts();
            let conflicts = window.is_some();
            if let Some(c) = window {
                let mut stashed = STASHED.lock().unwrap();
                match probe.stash {
                    1 => *stashed = Some(c.nonce),
                    2 => {
                        if let Some(nonce) = stashed.take() {
                            tx.resolve_conflicts(c.window, nonce);
                        }
                    }
                    _ => tx.resolve_conflicts(c.window, c.nonce),
                }
            }
            let emitted_before = tx.emitted() as u64;
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
                emitted_before,
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

    impl Policy for Probing {
        type Source = Account;
        type Target = ProbeDomain;
        type TargetId = ();

        const NAME: &'static str = "probing";

        fn targets(_: &crate::next::Committed<'_, Account>) -> Vec<()> {
            vec![()]
        }

        fn root((): &()) -> Uuid {
            probe_root()
        }

        fn command((): &(), _: &Committed<'_, Account>) -> Probe {
            Probe { stash: 0 }
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

    fn probes(books: &[crate::next::v1::EventBook]) -> Vec<Probed> {
        let probe: Vec<_> = books
            .iter()
            .filter(|b| b.cover.as_ref().is_some_and(|c| c.domain == "probe"))
            .cloned()
            .collect();
        crate::next::toys::payloads::<ProbeEvent>(&probe)
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
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);
        let routes = crate::next::Routes::new().policy::<Probing>();
        let book = command(cover("account", 1), AccountCmd::Deposit(5).to_any())
            .id(3)
            .correlated(6);
        let handled = kernel
            .handle::<Account, _, _>(&store, &routes, book, &at(50), &mut ())
            .ready()
            .unwrap();
        let trigger = crate::next::v1::EventRef {
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
                correlation_id: crate::next::toys::correlation(6).value.to_vec(),
                causation: Some(2),
                first: 0,
                next: 1,
                emitted: 1,
                bumps_seen: 1,
                bumps_committed: 0,
                conflicts: false,
                emitted_before: 0,
            }]
        );

        // Again: the probe stream is at 2 now, and decide sees its state.
        let book = command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(4);
        let handled = kernel
            .handle::<Account, _, _>(&store, &routes, book, &at(51), &mut ())
            .ready()
            .unwrap();
        let probed = &probes(&handled.books())[0];
        assert_eq!((probed.first, probed.next), (2, 3));
        assert_eq!((probed.bumps_committed, probed.bumps_seen), (1, 2));
        assert_eq!(
            probed.correlation_id,
            crate::next::toys::command_id(4).value.to_vec()
        );
    }

    /// Each probe event → the ledger: the probe emits two per command, so
    /// this sets off two sibling steps, two deep.
    struct ProbeToLedger;

    impl Policy for ProbeToLedger {
        type Source = ProbeDomain;
        type Target = crate::next::toys::Ledger;
        type TargetId = ();

        const NAME: &'static str = "probe-to-ledger";

        fn targets(_: &crate::next::Committed<'_, ProbeDomain>) -> Vec<()> {
            vec![()]
        }

        fn root((): &()) -> Uuid {
            crate::next::toys::ledger_root()
        }

        fn command((): &(), c: &Committed<'_, ProbeDomain>) -> crate::next::toys::ledger::Record {
            crate::next::toys::ledger::Record {
                source_sequence: c.sequence(),
                cents: 1,
            }
        }
    }

    /// A chain two steps deep, each step committed: within the limit it
    /// settles; one shallower, the step past it fails with the path to it,
    /// and the steps before it stand.
    #[test]
    fn a_two_deep_chain_keeps_the_path_to_the_step_running_and_its_limit() {
        let mut registry = registry();
        registry
            .register::<crate::next::toys::Ledger>(
                Rulebook::new("v1", ()),
                Upcaster::new("u", "ledger"),
            )
            .unwrap();
        let log = MemoryDeadLetters::new();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);
        let routes = crate::next::Routes::new()
            .policy::<Probing>()
            .policy::<ProbeToLedger>();
        let deposit = || command(cover("account", 1), AccountCmd::Deposit(5).to_any()).id(3);

        let kernel = Kernel::new(&registry, &log).limit(2);
        kernel
            .handle::<Account, _, _>(&store, &routes, deposit(), &at(50), &mut ())
            .ready()
            .unwrap();
        let probe = crate::next::v1::Cover {
            domain: "probe".to_string(),
            root: Some(probe_root()),
            edition: None,
        };
        assert_eq!(
            pages(&store, &probe).len(),
            2,
            "the probe step's two events"
        );
        let ledger = crate::next::v1::Cover {
            domain: "ledger".to_string(),
            root: Some(crate::next::toys::ledger_root()),
            edition: None,
        };
        assert_eq!(
            pages(&store, &ledger).len(),
            2,
            "then the ledger, once per probe event"
        );

        let store = MemoryStore::new();
        opened(&store, &registry, 1, 0);
        let shallow = Kernel::new(&registry, &log).limit(1);
        let failure = shallow
            .handle::<Account, _, _>(&store, &routes, deposit(), &at(50), &mut ())
            .ready()
            .err()
            .unwrap();
        assert!(matches!(
            &failure,
            crate::next::Failure::Downstream {
                path,
                error: crate::next::StepError::Kernel(crate::next::KernelError::ChainTooDeep {
                    limit: 1
                }),
            } if path.len() == 2
                && path[0].stream.domain == "probe"
                && path[1].stream.domain == "ledger"
                && path[1].cause.sequence == 0
        ));
        assert_eq!(
            streams(&store).len(),
            2,
            "the account's and the probe's commits stand; no ledger step ran"
        );
        assert_eq!(shallow.registry().domains().count(), 3);
    }

    #[test]
    fn a_nonce_acknowledges_only_the_delivery_it_was_issued_for() {
        use crate::next::coordinator::{steps::Coordinator, Deduplicated};
        use crate::next::v1::ConflictResolution;
        use crate::next::{check, Failure, KernelError};

        let registry = registry();
        let log = MemoryDeadLetters::new();
        let store = MemoryStore::new();
        let mut coordinator = Coordinator::new(&store, &registry);
        let mut probe = |n: u128, stash: u32| {
            let book = command(cover("probe", 1), Probe { stash }.to_any())
                .id(n)
                .resolved_by(ConflictResolution::AggregateHandles);
            let identified = check(book, &registry).unwrap().identify(&at(60));
            let Deduplicated::Novel(novel) = coordinator.deduplicate(identified).unwrap() else {
                panic!("new");
            };
            coordinator
                .load(novel)
                .unwrap()
                .decode::<ProbeDomain>(&registry)
                .unwrap()
        };
        // Two deliveries by one coordinator: the first's decide keeps its nonce,
        // the second's presents it instead of its own.
        let mut first = probe(1, 1);
        let mut second = probe(2, 2);
        let unresolved = || {
            Failure::Kernel(KernelError::Unresolved {
                stream: cover("probe", 1),
                expected: 0,
                actual: 0,
            })
        };
        assert_eq!(
            first.decide(&at(60), &registry, &log).err(),
            Some(unresolved()),
            "kept, not acknowledged"
        );
        assert_eq!(
            second.decide(&at(60), &registry, &log).err(),
            Some(unresolved()),
            "another delivery's nonce acknowledges nothing"
        );
        assert_eq!(log.lines().len(), 2, "each a dead-lettered handler bug");
        let mut honest = probe(3, 0);
        assert!(honest.decide(&at(60), &registry, &log).is_ok());
    }
}

/// A hand-written domain's rejection carries no typed refusal object:
/// `Domain::refusal`'s default.
#[test]
fn a_hand_written_rejection_has_no_typed_refusal_object() {
    use crate::next::toys::{Account, Refused};
    assert_eq!(
        <Account as crate::next::Domain>::refusal(&Refused::NotOpen),
        None
    );
}
