//! The registry: angzarr-embedded's mirror of `ComponentDescriptor.state`, looked up
//! by `cover.domain`, and the conflict-resolution defaults. These run.

use crate::next::v1::{ConflictResolution, Snapshot};
use crate::next::{pack, Misconfigured, Registry, Rulebook, Upcaster};

use crate::next::toys::{account, fees, registry, shelf, Account, Shelf};

#[test]
fn registration_fills_the_state_descriptor_from_the_generated_impl() {
    let registry = registry();
    let shelf = registry.state("shelf").unwrap();
    assert_eq!(shelf.type_url, "/toy.shelf.v1.ShelfState");
    assert!(registry.supports("shelf", ConflictResolution::Commutative));
    assert!(registry.supports("shelf", ConflictResolution::Strict));
    assert!(registry.supports("account", ConflictResolution::AggregateHandles));
    assert!(
        !registry.supports("account", ConflictResolution::Commutative),
        "no conflict units"
    );
    assert!(!registry.supports("nowhere", ConflictResolution::Strict));
    assert_eq!(registry.rules_version("account"), Some("v2"));
    assert_eq!(
        registry.domains().collect::<Vec<_>>(),
        ["account", "ledger", "shelf"]
    );
    assert!(registry.rules::<Account>().is_some());
}

#[test]
fn commutative_is_the_default_where_the_state_declares_units_else_strict() {
    let registry = registry();
    let set = "/toy.shelf.cmd.v1.Set";
    let deposit = "/toy.account.cmd.v1.Deposit";
    assert_eq!(
        registry.resolve("shelf", set, None),
        Some(ConflictResolution::Commutative),
        "the built-in default where the state supports it"
    );
    assert_eq!(
        registry.resolve("account", deposit, None),
        Some(ConflictResolution::Strict),
        "and STRICT where it does not"
    );
    assert_eq!(
        registry.resolve("shelf", "/toy.shelf.cmd.v1.Note", None),
        Some(ConflictResolution::Strict),
        "a command type's configuration is above the default"
    );
    assert_eq!(
        registry.resolve(
            "shelf",
            "type.googleapis.com/toy.shelf.cmd.v1.Note",
            Some(ConflictResolution::AggregateHandles)
        ),
        Some(ConflictResolution::AggregateHandles),
        "and the book's request above that"
    );
    assert_eq!(registry.resolve("nowhere", set, None), None);
}

#[test]
fn misconfiguration_is_refused_at_registration() {
    let mut registry = registry();
    assert_eq!(
        registry.register::<Account>(fees(), Upcaster::new("u", "account")),
        Err(Misconfigured::AlreadyRegistered {
            domain: "account".to_string()
        })
    );

    let mut fresh = Registry::new();
    assert_eq!(
        fresh.register::<Account>(fees(), Upcaster::new("u", "ledger")),
        Err(Misconfigured::UpcasterForAnotherDomain {
            domain: "account".to_string(),
            upcaster: "ledger".to_string()
        })
    );
    assert_eq!(
        fresh.register::<Shelf>(Rulebook::new("", ()), Upcaster::new("u", "shelf")),
        Err(Misconfigured::EmptyRulesVersion {
            domain: "shelf".to_string()
        })
    );
    assert!(fresh.domains().next().is_none(), "nothing half-registered");
}

#[test]
fn a_configured_resolution_the_state_lacks_is_refused() {
    use crate::next::toys::account::AccountState;
    use crate::next::{Domain, IngestCtx, Tx};

    /// An account configured COMMUTATIVE although its state has no units.
    struct Eager;
    impl Domain for Eager {
        type State = AccountState;
        type Command = std::convert::Infallible;
        type Fact = std::convert::Infallible;
        type Event = crate::next::toys::AccountEvent;
        type Rejection = std::convert::Infallible;
        type Rules = ();
        const NAME: &'static str = "eager";
        const CONFLICT_RESOLUTION: Option<ConflictResolution> =
            Some(ConflictResolution::Commutative);
        fn decide(c: &Self::Command, _: &mut Tx<'_, Self>) -> Result<(), Self::Rejection> {
            match *c {}
        }
        fn ingest(_: &AccountState, f: &Self::Fact, _: &IngestCtx<'_, Self>) -> Vec<Self::Event> {
            match *f {}
        }
        fn apply(_: &mut AccountState, _: &Self::Event, (): &()) {}
    }

    assert_eq!(
        Registry::new().register::<Eager>(Rulebook::new("v1", ()), Upcaster::new("u", "eager")),
        Err(Misconfigured::UnsupportedResolution {
            domain: "eager".to_string(),
            command_type: None,
            resolution: ConflictResolution::Commutative
        })
    );
}

#[test]
fn a_snapshot_is_usable_only_under_the_current_rules_and_state_type() {
    let registry = registry();
    let state = pack(&account::AccountState::default());
    let snapshot = |rules: &str, state: &crate::next::Any| Snapshot {
        sequence: 3,
        state: Some(state.clone()),
        rules_version: rules.to_string(),
    };
    assert!(registry.snapshot_usable("account", &snapshot("v2", &state)));
    assert!(
        !registry.snapshot_usable("account", &snapshot("v1", &state)),
        "folded under other rules: discarded"
    );
    assert!(
        !registry.snapshot_usable(
            "account",
            &snapshot("v2", &pack(&shelf::ShelfState::default()))
        ),
        "another state type: discarded"
    );
    assert!(!registry.snapshot_usable("nowhere", &snapshot("v2", &state)));
}

#[test]
fn a_state_descriptor_without_the_implicit_base_is_refused() {
    use crate::next::v1::{ConflictResolution, StateDescriptor, StateOptions};

    /// Lists STRICT only: not the base every generated descriptor has.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct StrictOnly {}

    impl prost::Name for StrictOnly {
        const NAME: &'static str = "StrictOnly";
        const PACKAGE: &'static str = "toy.bad.v1";
    }

    impl crate::next::State for StrictOnly {
        type Snapshot = Self;

        fn to_snapshot(&self) -> Self {
            self.clone()
        }

        fn from_snapshot(snapshot: Self) -> Result<Self, angzarr_embedded_diagnostic::Report> {
            Ok(snapshot)
        }

        fn descriptor() -> StateDescriptor {
            StateDescriptor {
                type_url: "/toy.bad.v1.StrictOnly".to_string(),
                options: Some(StateOptions {
                    conflict_resolutions: vec![ConflictResolution::Strict as i32],
                }),
            }
        }
    }

    struct Bad;

    impl crate::next::Domain for Bad {
        type State = StrictOnly;
        type Command = std::convert::Infallible;
        type Fact = std::convert::Infallible;
        type Event = std::convert::Infallible;
        type Rejection = std::convert::Infallible;
        type Rules = ();

        const NAME: &'static str = "bad";

        fn decide(
            cmd: &std::convert::Infallible,
            _: &mut crate::next::Tx<'_, Self>,
        ) -> Result<(), std::convert::Infallible> {
            match *cmd {}
        }

        fn ingest(
            _: &StrictOnly,
            fact: &std::convert::Infallible,
            _: &crate::next::IngestCtx<'_, Self>,
        ) -> Vec<std::convert::Infallible> {
            match *fact {}
        }

        fn apply(_: &mut StrictOnly, event: &std::convert::Infallible, (): &()) {
            match *event {}
        }
    }

    let mut registry = crate::next::Registry::new();
    assert_eq!(
        registry.register::<Bad>(
            crate::next::Rulebook::new("v1", ()),
            crate::next::Upcaster::new("u", "bad")
        ),
        Err(crate::next::Misconfigured::BadStateDescriptor {
            domain: "bad".to_string()
        })
    );
}

#[test]
fn a_state_descriptor_naming_another_type_than_its_snapshot_is_refused() {
    use crate::next::v1::{ConflictResolution, StateDescriptor, StateOptions};

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Snap {}

    impl prost::Name for Snap {
        const NAME: &'static str = "Snap";
        const PACKAGE: &'static str = "toy.bad.v1";
    }

    /// Its descriptor names a type its snapshots are not: a snapshot it
    /// wrote would never be found usable.
    #[derive(Clone, Default)]
    pub struct Misnamed;

    impl crate::next::State for Misnamed {
        type Snapshot = Snap;

        fn to_snapshot(&self) -> Snap {
            Snap {}
        }

        fn from_snapshot(_: Snap) -> Result<Self, angzarr_embedded_diagnostic::Report> {
            Ok(Misnamed)
        }

        fn descriptor() -> StateDescriptor {
            StateDescriptor {
                type_url: "/toy.bad.v1.Elsewhere".to_string(),
                options: Some(StateOptions {
                    conflict_resolutions: vec![
                        ConflictResolution::Strict as i32,
                        ConflictResolution::AggregateHandles as i32,
                    ],
                }),
            }
        }
    }

    struct Bad;

    impl crate::next::Domain for Bad {
        type State = Misnamed;
        type Command = std::convert::Infallible;
        type Fact = std::convert::Infallible;
        type Event = std::convert::Infallible;
        type Rejection = std::convert::Infallible;
        type Rules = ();

        const NAME: &'static str = "bad";

        fn decide(
            cmd: &std::convert::Infallible,
            _: &mut crate::next::Tx<'_, Self>,
        ) -> Result<(), std::convert::Infallible> {
            match *cmd {}
        }

        fn ingest(
            _: &Misnamed,
            fact: &std::convert::Infallible,
            _: &crate::next::IngestCtx<'_, Self>,
        ) -> Vec<std::convert::Infallible> {
            match *fact {}
        }

        fn apply(_: &mut Misnamed, event: &std::convert::Infallible, (): &()) {
            match *event {}
        }
    }

    let mut registry = crate::next::Registry::new();
    assert_eq!(
        registry.register::<Bad>(
            crate::next::Rulebook::new("v1", ()),
            crate::next::Upcaster::new("u", "bad")
        ),
        Err(crate::next::Misconfigured::BadStateDescriptor {
            domain: "bad".to_string()
        })
    );
}
