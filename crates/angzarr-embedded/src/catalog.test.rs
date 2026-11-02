//! The catalog: what is registered, listed from each component's
//! descriptor; every registration fallible the one way, a refused one
//! listing and keeping nothing.

use angzarr_diagnostic::Diagnostic;
use angzarr_router::policy::PolicyDispatch;

use crate::toys::registry;
use crate::v1::{ComponentKind, EventBook, Projection};
use crate::{Misconfigured, Registry, Upcaster};

/// A projector that projects nothing, named `name`.
struct Named(&'static str);

impl angzarr_router::router::ProjectorHandler for Named {
    fn name(&self) -> &str {
        self.0
    }

    async fn dispatch(
        &self,
        _: &EventBook,
    ) -> Result<Projection, angzarr_router::error::CodedError> {
        Ok(Projection::default())
    }
}

/// A coordinator elsewhere that is never called.
struct Elsewhere;

impl crate::CoordinatorPort for Elsewhere {
    fn handle(&self, _: crate::v1::CommandBook) -> crate::Coordinated<'_> {
        unreachable!("never called")
    }
}

/// A policy or a projector of a name registered already is refused,
/// named; the first is kept, alone.
#[test]
fn a_policy_or_projector_registered_twice_is_refused() {
    let mut registry = Registry::new();
    registry
        .policy_dispatch(PolicyDispatch::new("audit", "x", "y"))
        .unwrap()
        .projector_handler(&["x"], Named("view"))
        .unwrap();
    let twice = || Misconfigured::ComponentRegistered {
        component: "audit".to_string(),
    };
    assert_eq!(
        registry
            .policy_dispatch(PolicyDispatch::new("audit", "z", "y"))
            .err(),
        Some(twice())
    );
    assert_eq!(
        registry.projector_handler(&["z"], Named("view")).err(),
        Some(Misconfigured::ComponentRegistered {
            component: "view".to_string()
        })
    );
    assert_eq!(
        registry.catalog().policy_names().collect::<Vec<_>>(),
        ["audit"]
    );
    assert_eq!(
        registry.catalog().projector_names().collect::<Vec<_>>(),
        ["view"]
    );
    assert_eq!(registry.policies("z").count(), 0, "nothing kept");
    assert_eq!(registry.projectors().len(), 1);
    assert_eq!(twice().code(), "ROUTER_REASON_DUPLICATE_COMPONENT");
}

/// A projector naming no domain it reads is refused under the spec's key
/// (decision 14.55), its name the arg `component`; nothing is kept.
#[test]
fn a_projector_with_no_source_is_refused_under_the_spec_key() {
    let mut registry = Registry::new();
    let refused = registry.projector_handler(&[], Named("no-view")).err();
    assert_eq!(
        refused,
        Some(Misconfigured::ProjectorNoInputDomain {
            component: "no-view".to_string()
        })
    );
    let refused = refused.unwrap();
    assert_eq!(refused.code(), "HOST_REASON_PROJECTOR_NO_INPUT_DOMAIN");
    assert_eq!(
        refused.to_string(),
        "HOST_REASON_PROJECTOR_NO_INPUT_DOMAIN component=no-view"
    );
    assert_eq!(registry.catalog().projector_names().count(), 0);
    assert!(registry.projectors().is_empty());
}

/// An upcaster given for a domain not registered is refused, the domain
/// named; one for a registered domain is its port.
#[test]
fn an_upcaster_for_a_domain_not_registered_is_refused() {
    let mut registry = registry();
    assert_eq!(
        registry
            .upcast_with("nowhere", Upcaster::new("u", "nowhere"))
            .err(),
        Some(Misconfigured::UpcasterUnknownDomain {
            domain: "nowhere".to_string()
        })
    );
    assert!(registry.catalog().upcast_port("nowhere").is_none());
    registry
        .upcast_with("account", Upcaster::new("sidecar", "account"))
        .unwrap();
    assert!(registry.catalog().upcast_port("account").is_some());
}

/// A remote domain is listed apart from the domains handled here, and is
/// no kind here; a policy or projector of its events, refused, keeps
/// nothing, and a domain of its name is refused as taken.
#[test]
fn a_remote_domain_is_listed_apart_and_keeps_its_readers_out() {
    let mut registry = registry();
    registry.remote("bank", Elsewhere).unwrap();
    let catalog = registry.catalog();
    assert_eq!(catalog.remotes().collect::<Vec<_>>(), ["bank"]);
    assert!(!catalog.domains().any(|d| d == "bank"));
    assert_eq!(catalog.kind("bank"), ComponentKind::Unspecified);
    assert_eq!(catalog.component_name("bank"), None);
    assert!(catalog.is_remote("bank") && !catalog.is_here("bank"));
    assert!(catalog.is_here("account") && !catalog.is_remote("account"));

    let remote_source = Some(Misconfigured::RemoteSource {
        domain: "bank".to_string(),
        component: "till".to_string(),
    });
    assert_eq!(
        registry
            .policy_dispatch(PolicyDispatch::new("till", "bank", "account"))
            .err(),
        remote_source
    );
    assert_eq!(
        registry
            .projector_handler(&["account", "bank"], Named("till"))
            .err(),
        remote_source
    );
    assert_eq!(registry.catalog().policy_names().count(), 0);
    assert_eq!(registry.catalog().projector_names().count(), 0);
    assert_eq!(registry.all_policies().count(), 0);
    assert!(registry.projectors().is_empty());
    assert_eq!(
        registry.remote("account", Elsewhere).err(),
        Some(Misconfigured::AlreadyRegistered {
            domain: "account".to_string()
        })
    );
    assert_eq!(
        registry.remote("bank", Elsewhere).err(),
        Some(Misconfigured::AlreadyRegistered {
            domain: "bank".to_string()
        })
    );
}

/// Decision 14.65 (the owner's ruling on decision 21 of the 2026-10-31
/// run): every misconfiguration, a Rust registration's or a child's
/// descriptor's, is refused under a spec key, its args exactly the key's.
#[test]
fn every_misconfiguration_is_a_spec_key_with_its_declared_args() {
    use angzarr_diagnostic::Diagnostic as _;
    for misconfigured in crate::misconfigured_samples() {
        let declared = angzarr_proto::reason_key_args(misconfigured.code())
            .unwrap_or_else(|| panic!("{} is no spec key", misconfigured.code()));
        let given: Vec<&str> = misconfigured.args().iter().map(|(n, _)| *n).collect();
        for name in &given {
            assert!(declared.contains(name), "{misconfigured:?}: {name}");
        }
    }
}
