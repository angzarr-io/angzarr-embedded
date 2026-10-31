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
        registry.register::<Account>(
            fees(),
            Upcaster::new("u", "account"),
            crate::next::toys::handlers::<crate::next::toys::Account>(
                crate::next::toys::Fees { withdrawal: 1 },
                "v2"
            )
        ),
        Err(Misconfigured::AlreadyRegistered {
            domain: "account".to_string()
        })
    );

    let mut fresh = Registry::new();
    assert_eq!(
        fresh.register::<Account>(
            fees(),
            Upcaster::new("u", "ledger"),
            crate::next::toys::handlers::<crate::next::toys::Account>(
                crate::next::toys::Fees { withdrawal: 1 },
                "v2"
            )
        ),
        Err(Misconfigured::UpcasterForAnotherDomain {
            domain: "account".to_string(),
            upcaster: "ledger".to_string()
        })
    );
    assert_eq!(
        fresh.register::<Shelf>(
            Rulebook::new("", ()),
            Upcaster::new("u", "shelf"),
            crate::next::toys::handlers::<crate::next::toys::Shelf>((), "v1")
        ),
        Err(Misconfigured::EmptyRulesVersion {
            domain: "shelf".to_string()
        })
    );
    assert!(fresh.domains().next().is_none(), "nothing half-registered");
}

#[test]
fn a_configured_resolution_the_state_lacks_is_refused() {
    use crate::next::toys::account::AccountState;
    use crate::next::Domain;

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
        fn apply(_: &mut AccountState, _: &Self::Event, (): &()) {}
    }

    assert_eq!(
        Registry::new().register::<Eager>(
            Rulebook::new("v1", ()),
            Upcaster::new("u", "eager"),
            |t| t
        ),
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

        fn apply(_: &mut StrictOnly, event: &std::convert::Infallible, (): &()) {
            match *event {}
        }
    }

    let mut registry = crate::next::Registry::new();
    assert_eq!(
        registry.register::<Bad>(
            crate::next::Rulebook::new("v1", ()),
            crate::next::Upcaster::new("u", "bad"),
            |t| t
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

        fn apply(_: &mut Misnamed, event: &std::convert::Infallible, (): &()) {
            match *event {}
        }
    }

    let mut registry = crate::next::Registry::new();
    assert_eq!(
        registry.register::<Bad>(
            crate::next::Rulebook::new("v1", ()),
            crate::next::Upcaster::new("u", "bad"),
            |t| t
        ),
        Err(crate::next::Misconfigured::BadStateDescriptor {
            domain: "bad".to_string()
        })
    );
}

/// A hand-written handler of `kind`, registered by its descriptor.
struct Hello {
    kind: angzarr_router::pb::ComponentKind,
}

impl angzarr_router::router::CommandHandler for Hello {
    fn name(&self) -> &str {
        "hello"
    }

    fn kind(&self) -> angzarr_router::pb::ComponentKind {
        self.kind
    }

    fn command_types(&self) -> Vec<String> {
        Vec::new()
    }

    async fn dispatch(
        &self,
        _: &angzarr_router::pb::ContextualCommand,
    ) -> Result<angzarr_router::pb::CommandHandlerResponse, angzarr_router::error::CodedError> {
        Ok(Default::default())
    }
}

fn hello(
    state: Option<crate::next::v1::StateDescriptor>,
) -> angzarr_router::pb::ComponentDescriptor {
    angzarr_router::pb::ComponentDescriptor {
        name: "hello".to_string(),
        rules_version: "h1".to_string(),
        state,
        ..Default::default()
    }
}

fn base_state() -> crate::next::v1::StateDescriptor {
    use crate::next::v1::{ConflictResolution, StateDescriptor, StateOptions};
    StateDescriptor {
        type_url: "/toy.hello.v1.State".to_string(),
        options: Some(StateOptions {
            conflict_resolutions: vec![
                ConflictResolution::Strict as i32,
                ConflictResolution::AggregateHandles as i32,
            ],
        }),
    }
}

/// A handler registers on what its descriptor says: refused without a
/// rules version or a state type, a process manager's kind kept.
#[test]
fn a_command_handler_registers_by_its_descriptor() {
    use angzarr_router::pb::ComponentKind;
    let refused = |descriptor| {
        crate::next::Registry::new()
            .command_handler(
                "hello",
                Hello {
                    kind: ComponentKind::Aggregate,
                },
                descriptor,
            )
            .err()
    };
    let unversioned = angzarr_router::pb::ComponentDescriptor {
        rules_version: String::new(),
        ..hello(Some(base_state()))
    };
    assert_eq!(
        refused(unversioned),
        Some(Misconfigured::EmptyRulesVersion {
            domain: "hello".to_string()
        })
    );
    let untyped = crate::next::v1::StateDescriptor {
        type_url: String::new(),
        ..base_state()
    };
    assert_eq!(
        refused(hello(Some(untyped))),
        Some(Misconfigured::BadStateDescriptor {
            domain: "hello".to_string()
        })
    );
    assert_eq!(
        refused(hello(None)),
        Some(Misconfigured::BadStateDescriptor {
            domain: "hello".to_string()
        })
    );

    let mut registry = crate::next::Registry::new();
    registry
        .command_handler(
            "hello",
            Hello {
                kind: ComponentKind::ProcessManager,
            },
            hello(Some(base_state())),
        )
        .unwrap();
    assert!(registry.is_process_manager("hello"));
    assert_eq!(registry.kind("hello"), ComponentKind::ProcessManager);
    assert_eq!(registry.kind("nowhere"), ComponentKind::Unspecified);
    assert_eq!(registry.rules_version("hello"), Some("h1"));
    let mut aggregate = crate::next::Registry::new();
    aggregate
        .command_handler(
            "hello",
            Hello {
                kind: ComponentKind::Aggregate,
            },
            hello(Some(base_state())),
        )
        .unwrap();
    assert!(!aggregate.is_process_manager("hello"));
    assert_eq!(aggregate.kind("hello"), ComponentKind::Aggregate);
}

/// A domain's name as a component of an application is decision 14.50's
/// default: an aggregate's is its domain, a process manager's `pmg-` and
/// its domain; a domain not registered here has none.
#[test]
fn a_domain_is_named_as_a_component_by_its_kind() {
    use angzarr_router::pb::ComponentKind;
    let named = |kind| {
        let mut registry = crate::next::Registry::new();
        registry
            .command_handler("hello", Hello { kind }, hello(Some(base_state())))
            .unwrap();
        (
            registry.component_name("hello"),
            registry.component_name("nowhere"),
        )
    };
    assert_eq!(
        named(ComponentKind::Aggregate),
        (Some("hello".to_string()), None)
    );
    assert_eq!(
        named(ComponentKind::ProcessManager),
        (Some("pmg-hello".to_string()), None)
    );
}

/// A projector, as the router's `ProjectorHandler`, named `name`, that
/// projects nothing.
struct Named(&'static str);

impl angzarr_router::router::ProjectorHandler for Named {
    fn name(&self) -> &str {
        self.0
    }

    async fn dispatch(
        &self,
        _: &crate::next::v1::EventBook,
    ) -> Result<crate::next::v1::Projection, angzarr_router::error::CodedError> {
        Ok(crate::next::v1::Projection::default())
    }
}

/// The registry names its policies and its projectors, each in
/// registration order.
#[test]
fn the_registry_names_its_policies_and_projectors_in_order() {
    use angzarr_router::policy::PolicyDispatch;
    let mut registry = Registry::new();
    assert_eq!(registry.policy_names().count(), 0);
    assert_eq!(registry.projector_names().count(), 0);
    registry
        .policy_dispatch(PolicyDispatch::new("b-policy", "x", "y"))
        .policy_dispatch(PolicyDispatch::new("a-policy", "x", "y"))
        .projector_handler(&["x"], Named("b-view"))
        .projector_handler(&["x", "y"], Named("a-view"));
    assert_eq!(
        registry.policy_names().collect::<Vec<_>>(),
        ["b-policy", "a-policy"]
    );
    assert_eq!(
        registry.projector_names().collect::<Vec<_>>(),
        ["b-view", "a-view"]
    );
}

/// A projector projects at least one source domain: one naming none is
/// a composition-root bug, refused at startup by its name.
#[test]
#[should_panic(expected = "no-view")]
fn a_projector_with_no_source_panics() {
    Registry::new().projector_handler(&[], Named("no-view"));
}
