//! Why a component could not be registered, and the one validator that
//! decides it (decision 14.65, the owner's ruling on decision 21 of the
//! 2026-10-31 run): a domain, policy or projector registered in Rust and a
//! child's descriptor are held to the same rules, each refusal under a spec
//! key, verbatim, with that key's args (decision 14.60).

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_proto::v1::reason_args as args;
use angzarr_proto::v1::{
    ComponentDescriptor, ComponentKind, ConflictResolution, CoordinatorReason, HostReason,
    RouterReason, StateDescriptor,
};

/// Why a component could not be registered: angzarr refuses the same
/// configuration when it is loaded or the component registers. Every
/// registration answers one of these; none panics.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Misconfigured {
    /// `domain` is registered already: `ROUTER_REASON_DUPLICATE_COMPONENT`,
    /// naming it as the one registered and the one refused.
    AlreadyRegistered { domain: String },
    /// A policy or a projector named `component` is registered already:
    /// `ROUTER_REASON_DUPLICATE_COMPONENT`, as a domain's is.
    ComponentRegistered { component: String },
    /// An upcaster is given for `domain`, which is not registered:
    /// `HOST_REASON_UPCASTER_UNKNOWN_DOMAIN`.
    UpcasterUnknownDomain { domain: String },
    /// The upcaster names another domain:
    /// `HOST_REASON_UPCASTER_OTHER_DOMAIN`.
    UpcasterForAnotherDomain { domain: String, upcaster: String },
    /// An empty rules version: every event is stamped with it, so it must
    /// say something. `COORDINATOR_REASON_MISSING_RULES_VERSION`.
    MissingRulesVersion { component: String },
    /// Starting types on a kind (`kind`) that is not a process manager:
    /// `COORDINATOR_REASON_STARTING_TYPES_ON_NON_PM`.
    StartingTypesOnNonPm { component: String, kind: String },
    /// An aggregate or process manager with no state (or a state with no
    /// type): `COORDINATOR_REASON_MISSING_STATE_TYPE`.
    MissingStateType { component: String, kind: String },
    /// A policy or projector with a state:
    /// `COORDINATOR_REASON_STATE_ON_STATELESS_KIND`.
    StateOnStatelessKind { component: String, kind: String },
    /// A policy with other than exactly one input:
    /// `COORDINATOR_REASON_POLICY_INPUT_COUNT`.
    PolicyInputCount { component: String, input_count: u64 },
    /// A policy with no output domain:
    /// `COORDINATOR_REASON_POLICY_NO_OUTPUT_DOMAIN`.
    PolicyNoOutputDomain { component: String },
    /// An output domain on a kind other than a policy:
    /// `COORDINATOR_REASON_OUTPUT_DOMAIN_ON_NON_POLICY`.
    OutputDomainOnNonPolicy { component: String, kind: String },
    /// A state listing a value that is not a resolution, or lacking the
    /// implicit base (STRICT and AGGREGATE_HANDLES), which every generated
    /// descriptor includes: `COORDINATOR_REASON_STATE_RESOLUTIONS_INVALID`.
    StateResolutionsInvalid { component: String },
    /// Configuration naming a resolution the state does not support:
    /// COMMUTATIVE for a state with no conflict units, or UNSPECIFIED.
    /// `command_type` is the command's full type name, or `None` for the
    /// domain's own default. `HOST_REASON_RESOLUTION_UNSUPPORTED`.
    UnsupportedResolution {
        domain: String,
        command_type: Option<String>,
        resolution: ConflictResolution,
    },
    /// A policy or projector here, `component`, handles the events of
    /// `domain`, a remote domain ([`Registry::remote`](super::Registry::remote)):
    /// a remote domain's events are its coordinator's, never written or
    /// read here, so a local component of them would never run. Remote
    /// stays remote. `HOST_REASON_REMOTE_SOURCE`.
    RemoteSource { domain: String, component: String },
    /// A state whose descriptor names one type (`type_url`) and whose
    /// snapshot form is another (`state_type_url`): its snapshots would
    /// never be found usable. `HOST_REASON_STATE_TYPE_MISMATCH`.
    StateTypeMismatch {
        component: String,
        type_url: String,
        state_type_url: String,
    },
    /// A projector that names no domain it reads: there is no wildcard
    /// domain, so it would fold nothing. `HOST_REASON_PROJECTOR_NO_INPUT_DOMAIN`
    /// (decision 14.55).
    ProjectorNoInputDomain { component: String },
}

impl fmt::Display for Misconfigured {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for Misconfigured {}

impl Diagnostic for Misconfigured {
    fn code(&self) -> &str {
        use Misconfigured as M;
        match self {
            M::AlreadyRegistered { .. } | M::ComponentRegistered { .. } => {
                RouterReason::DuplicateComponent.as_str_name()
            }
            M::UpcasterUnknownDomain { .. } => HostReason::UpcasterUnknownDomain.as_str_name(),
            M::UpcasterForAnotherDomain { .. } => HostReason::UpcasterOtherDomain.as_str_name(),
            M::MissingRulesVersion { .. } => CoordinatorReason::MissingRulesVersion.as_str_name(),
            M::StartingTypesOnNonPm { .. } => CoordinatorReason::StartingTypesOnNonPm.as_str_name(),
            M::MissingStateType { .. } => CoordinatorReason::MissingStateType.as_str_name(),
            M::StateOnStatelessKind { .. } => CoordinatorReason::StateOnStatelessKind.as_str_name(),
            M::PolicyInputCount { .. } => CoordinatorReason::PolicyInputCount.as_str_name(),
            M::PolicyNoOutputDomain { .. } => CoordinatorReason::PolicyNoOutputDomain.as_str_name(),
            M::OutputDomainOnNonPolicy { .. } => {
                CoordinatorReason::OutputDomainOnNonPolicy.as_str_name()
            }
            M::StateResolutionsInvalid { .. } => {
                CoordinatorReason::StateResolutionsInvalid.as_str_name()
            }
            M::UnsupportedResolution { .. } => HostReason::ResolutionUnsupported.as_str_name(),
            M::RemoteSource { .. } => HostReason::RemoteSource.as_str_name(),
            M::StateTypeMismatch { .. } => HostReason::StateTypeMismatch.as_str_name(),
            M::ProjectorNoInputDomain { .. } => HostReason::ProjectorNoInputDomain.as_str_name(),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        use Misconfigured as M;
        let text = |name, value: &str| (name, Arg::from(value));
        match self {
            M::AlreadyRegistered { domain: name } | M::ComponentRegistered { component: name } => {
                vec![text(args::REGISTERED, name), text(args::REFUSED, name)]
            }
            M::UpcasterUnknownDomain { domain } => vec![text(args::DOMAIN, domain)],
            M::UpcasterForAnotherDomain { domain, upcaster } => {
                vec![text(args::DOMAIN, domain), text(args::UPCASTER, upcaster)]
            }
            M::MissingRulesVersion { component }
            | M::PolicyNoOutputDomain { component }
            | M::StateResolutionsInvalid { component }
            | M::ProjectorNoInputDomain { component } => vec![text(args::COMPONENT, component)],
            M::StartingTypesOnNonPm { component, kind }
            | M::MissingStateType { component, kind }
            | M::StateOnStatelessKind { component, kind }
            | M::OutputDomainOnNonPolicy { component, kind } => vec![
                text(args::COMPONENT, component),
                text(args::COMPONENT_KIND, kind),
            ],
            M::PolicyInputCount {
                component,
                input_count,
            } => vec![
                text(args::COMPONENT, component),
                (args::INPUT_COUNT, (*input_count).into()),
            ],
            M::UnsupportedResolution {
                domain,
                command_type,
                resolution,
            } => {
                let mut found = vec![
                    text(args::COMPONENT, domain),
                    text(args::RESOLUTION, resolution.as_str_name()),
                ];
                found.extend(command_type.as_deref().map(|c| text(args::COMMAND_TYPE, c)));
                found
            }
            M::StateTypeMismatch {
                component,
                type_url,
                state_type_url,
            } => vec![
                text(args::COMPONENT, component),
                text(args::TYPE_URL, type_url),
                text(args::STATE_TYPE_URL, state_type_url),
            ],
            M::RemoteSource { domain, component } => {
                vec![text(args::COMPONENT, component), text(args::DOMAIN, domain)]
            }
        }
    }
}

/// `descriptor`, held to 14.42's rules, in order: a rules version; starting
/// types only on a process manager; a state with a type on an aggregate and
/// a process manager, its resolutions resolutions with the implicit base
/// ([`supported`]), and none on a policy or a projector; a policy's one
/// input and its output domain, and no output domain on any other kind.
/// The one check every component meets: a Rust registration's descriptor
/// as a child's (decision 14.65).
pub fn validate_descriptor(descriptor: &ComponentDescriptor) -> Result<(), Misconfigured> {
    let component = || descriptor.name.clone();
    let kind = descriptor.kind();
    let kind_name = || kind.as_str_name().to_string();
    if descriptor.rules_version.is_empty() {
        return Err(Misconfigured::MissingRulesVersion {
            component: component(),
        });
    }
    if kind != ComponentKind::ProcessManager && !descriptor.starting_types.is_empty() {
        return Err(Misconfigured::StartingTypesOnNonPm {
            component: component(),
            kind: kind_name(),
        });
    }
    let state = descriptor.state.as_ref();
    match kind {
        ComponentKind::Aggregate | ComponentKind::ProcessManager => {
            let state = state.filter(|s| !s.type_url.is_empty()).ok_or_else(|| {
                Misconfigured::MissingStateType {
                    component: component(),
                    kind: kind_name(),
                }
            })?;
            supported(&descriptor.name, state)?;
        }
        ComponentKind::Policy | ComponentKind::Projector if state.is_some() => {
            return Err(Misconfigured::StateOnStatelessKind {
                component: component(),
                kind: kind_name(),
            });
        }
        _ => {}
    }
    if kind == ComponentKind::Policy {
        if descriptor.inputs.len() != 1 {
            return Err(Misconfigured::PolicyInputCount {
                component: component(),
                input_count: descriptor.inputs.len() as u64,
            });
        }
        if descriptor.output_domain.is_empty() {
            return Err(Misconfigured::PolicyNoOutputDomain {
                component: component(),
            });
        }
    } else if !descriptor.output_domain.is_empty() {
        return Err(Misconfigured::OutputDomainOnNonPolicy {
            component: component(),
            kind: kind_name(),
        });
    }
    Ok(())
}

/// The resolutions `state`, `component`'s, lists, refused unless every one
/// is a resolution and the implicit base (STRICT and AGGREGATE_HANDLES) is
/// there.
pub(crate) fn supported(
    component: &str,
    state: &StateDescriptor,
) -> Result<Vec<ConflictResolution>, Misconfigured> {
    let invalid = || Misconfigured::StateResolutionsInvalid {
        component: component.to_string(),
    };
    let listed = state
        .options
        .as_ref()
        .map(|o| o.conflict_resolutions.as_slice())
        .unwrap_or_default();
    let mut supported = Vec::with_capacity(listed.len());
    for value in listed {
        match ConflictResolution::try_from(*value) {
            Ok(ConflictResolution::Unspecified) | Err(_) => return Err(invalid()),
            Ok(resolution) => supported.push(resolution),
        }
    }
    if !supported.contains(&ConflictResolution::Strict)
        || !supported.contains(&ConflictResolution::AggregateHandles)
    {
        return Err(invalid());
    }
    Ok(supported)
}

#[cfg(test)]
#[path = "misconfigured.test.rs"]
mod tests;
