//! The one validator every component meets (decision 14.65): each of
//! 14.42's descriptor rules, under its spec key.

use angzarr_diagnostic::Diagnostic;
use angzarr_proto::v1::{
    ComponentDescriptor, ComponentKind, StateDescriptor, StateOptions, Target,
};

use super::validate_descriptor;

fn state(resolutions: &[i32]) -> StateDescriptor {
    StateDescriptor {
        type_url: "/toy.State".to_string(),
        options: Some(StateOptions {
            conflict_resolutions: resolutions.to_vec(),
        }),
    }
}

fn of(kind: ComponentKind) -> ComponentDescriptor {
    let stateful = matches!(
        kind,
        ComponentKind::Aggregate | ComponentKind::ProcessManager
    );
    ComponentDescriptor {
        name: "order".to_string(),
        kind: kind as i32,
        rules_version: "1.0.0".to_string(),
        state: stateful.then(|| state(&[2, 3])),
        inputs: match kind {
            ComponentKind::Policy => vec![Target {
                domain: "cart".to_string(),
            }],
            _ => Vec::new(),
        },
        output_domain: match kind {
            ComponentKind::Policy => "inventory".to_string(),
            _ => String::new(),
        },
        ..ComponentDescriptor::default()
    }
}

fn refused(descriptor: ComponentDescriptor) -> String {
    validate_descriptor(&descriptor)
        .expect_err("refused")
        .code()
        .to_string()
}

#[test]
fn a_well_formed_descriptor_of_each_kind_passes() {
    for kind in [
        ComponentKind::Aggregate,
        ComponentKind::ProcessManager,
        ComponentKind::Policy,
        ComponentKind::Projector,
    ] {
        assert_eq!(validate_descriptor(&of(kind)), Ok(()), "{kind:?}");
    }
}

#[test]
fn each_rule_refuses_under_its_key() {
    let aggregate = || of(ComponentKind::Aggregate);
    let policy = || of(ComponentKind::Policy);
    for (descriptor, key) in [
        (
            ComponentDescriptor {
                rules_version: String::new(),
                ..aggregate()
            },
            "COORDINATOR_REASON_MISSING_RULES_VERSION",
        ),
        (
            ComponentDescriptor {
                starting_types: vec!["/toy.Start".to_string()],
                ..aggregate()
            },
            "COORDINATOR_REASON_STARTING_TYPES_ON_NON_PM",
        ),
        (
            ComponentDescriptor {
                state: None,
                ..aggregate()
            },
            "COORDINATOR_REASON_MISSING_STATE_TYPE",
        ),
        (
            ComponentDescriptor {
                state: Some(StateDescriptor {
                    type_url: String::new(),
                    ..state(&[2, 3])
                }),
                ..of(ComponentKind::ProcessManager)
            },
            "COORDINATOR_REASON_MISSING_STATE_TYPE",
        ),
        (
            ComponentDescriptor {
                state: Some(state(&[2, 3])),
                ..of(ComponentKind::Projector)
            },
            "COORDINATOR_REASON_STATE_ON_STATELESS_KIND",
        ),
        (
            ComponentDescriptor {
                state: Some(state(&[2, 3])),
                ..policy()
            },
            "COORDINATOR_REASON_STATE_ON_STATELESS_KIND",
        ),
        (
            ComponentDescriptor {
                inputs: Vec::new(),
                ..policy()
            },
            "COORDINATOR_REASON_POLICY_INPUT_COUNT",
        ),
        (
            ComponentDescriptor {
                output_domain: String::new(),
                ..policy()
            },
            "COORDINATOR_REASON_POLICY_NO_OUTPUT_DOMAIN",
        ),
        (
            ComponentDescriptor {
                output_domain: "inventory".to_string(),
                ..aggregate()
            },
            "COORDINATOR_REASON_OUTPUT_DOMAIN_ON_NON_POLICY",
        ),
        (
            ComponentDescriptor {
                state: Some(state(&[2])),
                ..aggregate()
            },
            "COORDINATOR_REASON_STATE_RESOLUTIONS_INVALID",
        ),
        (
            ComponentDescriptor {
                state: Some(state(&[2, 3, 0])),
                ..aggregate()
            },
            "COORDINATOR_REASON_STATE_RESOLUTIONS_INVALID",
        ),
        (
            ComponentDescriptor {
                state: Some(state(&[2, 3, 99])),
                ..aggregate()
            },
            "COORDINATOR_REASON_STATE_RESOLUTIONS_INVALID",
        ),
    ] {
        assert_eq!(refused(descriptor.clone()), key, "{descriptor:?}");
    }
}
