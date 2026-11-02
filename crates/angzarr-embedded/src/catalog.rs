//! What is registered, by name: angzarr-embedded's mirror of the
//! `ComponentDescriptor`s angzarr's coordinator holds, and of the
//! configuration it reads. Read-only to everything but the [`Registry`]
//! that fills it ([`Registry::catalog`]).
//!
//! The pipeline's domain-blind steps ([`check`](super::check), the
//! coordinator's) look a domain up here by `cover.domain` and see only
//! what angzarr's coordinator sees: the state's `StateDescriptor` (its
//! type URL and the conflict resolutions it supports), the configured
//! resolutions, the rules version, a process manager's starting types, and
//! what its payloads are upcast through. A policy and a projector are
//! listed by their descriptors too: their names and the domains they read.
//! Every entry is made from a `ComponentDescriptor`, generated for a Rust
//! component (from its markup or its `Domain`) or answered by a child to
//! `Describe`, and is held to the same rules either way; the handlers
//! behind them are the [`Registry`]'s.
//!
//! [`Registry`]: super::Registry
//! [`Registry::catalog`]: super::Registry::catalog

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use angzarr_diagnostic::{Arg, Diagnostic};
use angzarr_proto::v1::{
    ComponentDescriptor, ComponentKind, ConflictResolution, Snapshot, StateDescriptor, Target,
};

use super::payload::type_name;
use super::upcast::UpcastPort;

/// What a domain's descriptor tells the kernel: replaced whole, under
/// its lock, when the domain is described again ([`Description::replace`]).
struct Described {
    /// The state's descriptor.
    state: StateDescriptor,
    /// The resolutions it lists, read when it is described.
    supported: Vec<ConflictResolution>,
    /// The registered rules version.
    rules_version: String,
    /// A process manager's starting input types, by full name, as its
    /// descriptor lists them (`ComponentDescriptor.starting_types`): the
    /// kernel's start rule. `None` for an aggregate.
    starting: Option<BTreeSet<String>>,
}

/// One domain handled here.
struct Listed {
    /// What its descriptor says, shared with its [`Description`].
    described: Arc<RwLock<Described>>,
    /// Its own configured resolution (`Domain::CONFLICT_RESOLUTION`).
    component: Option<ConflictResolution>,
    /// Its configured resolutions by command, by full type name.
    by_command: BTreeMap<String, ConflictResolution>,
    /// What its incoming payloads are upcast through (decision 14.39): a
    /// typed domain's [`Upcaster`](super::Upcaster), or the port
    /// [`Registry::upcast_with`](super::Registry::upcast_with) gave.
    /// `None`: nothing is upcast.
    upcast: Option<Arc<dyn UpcastPort>>,
}

impl Listed {
    /// What its descriptor says, read under its lock: never held across an
    /// await.
    fn described(&self) -> RwLockReadGuard<'_, Described> {
        self.described
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The configuration a domain is listed with beside its descriptor: its
/// own resolution and its resolutions by command, each held to what its
/// state supports.
#[derive(Default)]
pub(crate) struct Configured {
    pub(crate) component: Option<ConflictResolution>,
    pub(crate) by_command: BTreeMap<String, ConflictResolution>,
}

/// A domain listed by its descriptor: what its coordinator holds to
/// describe it again. A business process that reconnects is described
/// again (component.proto, Registration; C-0563), and
/// [`Description::replace`] lists its new descriptor whole, at once,
/// before anything more is delivered to it.
#[derive(Clone)]
pub struct Description {
    domain: String,
    kind: ComponentKind,
    described: Arc<RwLock<Described>>,
}

impl fmt::Debug for Description {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Description")
            .field("domain", &self.domain)
            .finish_non_exhaustive()
    }
}

impl Description {
    /// List `descriptor` as the domain's in place of the one it was
    /// registered by: its state, its resolutions, its rules version and a
    /// process manager's starting types, all at once. Refused, the old
    /// one kept, as registering a domain refuses a descriptor.
    pub fn replace(&self, descriptor: ComponentDescriptor) -> Result<(), Misconfigured> {
        let described = described(&self.domain, self.kind, descriptor)?;
        *self
            .described
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = described;
        Ok(())
    }
}

/// What `descriptor` says of `domain`, a handler of `kind`, refused
/// without a rules version or a state type, or with a state whose
/// resolutions are not resolutions.
fn described(
    domain: &str,
    kind: ComponentKind,
    descriptor: ComponentDescriptor,
) -> Result<Described, Misconfigured> {
    if descriptor.rules_version.is_empty() {
        return Err(Misconfigured::EmptyRulesVersion {
            domain: domain.to_string(),
        });
    }
    let starting = (kind == ComponentKind::ProcessManager).then(|| {
        // A listed type URL counts by the name it ends in.
        (descriptor.starting_types.iter())
            .map(|t| type_name(t).to_string())
            .collect()
    });
    let state = descriptor
        .state
        .filter(|state| !state.type_url.is_empty())
        .ok_or_else(|| Misconfigured::BadStateDescriptor {
            domain: domain.to_string(),
        })?;
    let supported = supported(domain, &state)?;
    Ok(Described {
        state,
        supported,
        rules_version: descriptor.rules_version,
        starting,
    })
}

/// The resolutions `state` lists, refused unless every one is a resolution
/// and the implicit base (STRICT and AGGREGATE_HANDLES) is there.
fn supported(
    domain: &str,
    state: &StateDescriptor,
) -> Result<Vec<ConflictResolution>, Misconfigured> {
    let bad = || Misconfigured::BadStateDescriptor {
        domain: domain.to_string(),
    };
    let listed = state
        .options
        .as_ref()
        .map(|o| o.conflict_resolutions.as_slice())
        .unwrap_or_default();
    let mut supported = Vec::with_capacity(listed.len());
    for value in listed {
        match ConflictResolution::try_from(*value) {
            Ok(ConflictResolution::Unspecified) | Err(_) => return Err(bad()),
            Ok(resolution) => supported.push(resolution),
        }
    }
    if !supported.contains(&ConflictResolution::Strict)
        || !supported.contains(&ConflictResolution::AggregateHandles)
    {
        return Err(bad());
    }
    Ok(supported)
}

/// Why a component could not be registered: angzarr refuses the same
/// configuration when it is loaded or the component registers. Every
/// registration answers one of these; none panics.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Misconfigured {
    /// `domain` is registered already.
    AlreadyRegistered { domain: String },
    /// A policy or a projector named `component` is registered already.
    ComponentRegistered { component: String },
    /// An upcaster is given for `domain`, which is not registered.
    NotRegistered { domain: String },
    /// The upcaster names another domain.
    UpcasterForAnotherDomain { domain: String, upcaster: String },
    /// The domain's rules version is empty: every event is stamped with
    /// it, so it must say something.
    EmptyRulesVersion { domain: String },
    /// The state's descriptor lists a value that is not a resolution, or
    /// lacks the implicit base (STRICT and AGGREGATE_HANDLES), which every
    /// generated descriptor includes.
    BadStateDescriptor { domain: String },
    /// Configuration names a resolution the state does not support:
    /// COMMUTATIVE for a state with no conflict units, or UNSPECIFIED.
    /// `command_type` is the command's full type name, or `None` for the
    /// domain's own default.
    UnsupportedResolution {
        domain: String,
        command_type: Option<String>,
        resolution: ConflictResolution,
    },
    /// A policy or projector here, `component`, handles the events of
    /// `domain`, a remote domain ([`Registry::remote`](super::Registry::remote)):
    /// a remote domain's events are its coordinator's, never written or
    /// read here, so a local component of them would never run. Remote
    /// stays remote.
    RemoteSource { domain: String, component: String },
    /// A projector that names no domain it reads: there is no wildcard
    /// domain, so it would fold nothing. The spec's
    /// `HOST_REASON_PROJECTOR_NO_INPUT_DOMAIN` (decision 14.55).
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
        match self {
            Misconfigured::AlreadyRegistered { .. } => {
                "angzarr-embedded-misconfigured-already-registered"
            }
            Misconfigured::ComponentRegistered { .. } => {
                "angzarr-embedded-misconfigured-component-registered"
            }
            Misconfigured::NotRegistered { .. } => "angzarr-embedded-misconfigured-not-registered",
            Misconfigured::UpcasterForAnotherDomain { .. } => {
                "angzarr-embedded-misconfigured-upcaster-for-another-domain"
            }
            Misconfigured::EmptyRulesVersion { .. } => {
                "angzarr-embedded-misconfigured-empty-rules-version"
            }
            Misconfigured::BadStateDescriptor { .. } => {
                "angzarr-embedded-misconfigured-bad-state-descriptor"
            }
            Misconfigured::UnsupportedResolution {
                command_type: None, ..
            } => "angzarr-embedded-misconfigured-unsupported-resolution",
            Misconfigured::UnsupportedResolution {
                command_type: Some(_),
                ..
            } => "angzarr-embedded-misconfigured-unsupported-command-resolution",
            Misconfigured::RemoteSource { .. } => "angzarr-embedded-misconfigured-remote-source",
            Misconfigured::ProjectorNoInputDomain { .. } => {
                angzarr_proto::v1::HostReason::ProjectorNoInputDomain.as_str_name()
            }
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            Misconfigured::AlreadyRegistered { domain }
            | Misconfigured::NotRegistered { domain }
            | Misconfigured::EmptyRulesVersion { domain }
            | Misconfigured::BadStateDescriptor { domain } => {
                vec![("domain", domain.as_str().into())]
            }
            Misconfigured::ComponentRegistered { component } => {
                vec![("component", component.as_str().into())]
            }
            Misconfigured::ProjectorNoInputDomain { component } => {
                vec![(
                    angzarr_proto::v1::reason_args::COMPONENT,
                    component.as_str().into(),
                )]
            }
            Misconfigured::RemoteSource { domain, component } => vec![
                ("domain", domain.as_str().into()),
                ("component", component.as_str().into()),
            ],
            Misconfigured::UpcasterForAnotherDomain { domain, upcaster } => vec![
                ("domain", domain.as_str().into()),
                ("upcaster", upcaster.as_str().into()),
            ],
            Misconfigured::UnsupportedResolution {
                domain,
                command_type,
                resolution,
            } => {
                let mut args = vec![("domain", domain.as_str().into())];
                if let Some(command_type) = command_type {
                    args.push(("command_type", command_type.as_str().into()));
                }
                args.push(("resolution", resolution.as_str_name().into()));
                args
            }
        }
    }
}

/// types.proto's default snapshot compatibility rule: strip ONE leading
/// "v" or "V" from each version; if both then parse as SemVer 2.0.0, their
/// major and minor numbers are equal (patch, pre-release and build
/// metadata ignored); otherwise the versions, as recorded, are equal.
pub(crate) fn compatible(snapshot: &str, registered: &str) -> bool {
    let stripped = |v: &str| v.strip_prefix(['v', 'V']).unwrap_or(v).to_string();
    match (
        major_minor(&stripped(snapshot)),
        major_minor(&stripped(registered)),
    ) {
        (Some(a), Some(b)) => a == b,
        _ => snapshot == registered,
    }
}

/// `version`'s major and minor numbers, if it is a SemVer 2.0.0 version:
/// `MAJOR.MINOR.PATCH`, each a number with no leading zero, then an
/// optional `-` pre-release and an optional `+` build, each of
/// dot-separated non-empty `[0-9A-Za-z-]` identifiers (a numeric
/// pre-release identifier with no leading zero).
fn major_minor(version: &str) -> Option<(u64, u64)> {
    let (rest, build) = match version.split_once('+') {
        Some((rest, build)) => (rest, Some(build)),
        None => (version, None),
    };
    let (core, pre) = match rest.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (rest, None),
    };
    let identifier =
        |id: &str| !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    let numeric = |n: &str| {
        !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) && (n == "0" || !n.starts_with('0'))
    };
    if let Some(build) = build {
        if !build.split('.').all(identifier) {
            return None;
        }
    }
    if let Some(pre) = pre {
        let valid =
            |id: &str| identifier(id) && (!id.bytes().all(|b| b.is_ascii_digit()) || numeric(id));
        if !pre.split('.').all(valid) {
            return None;
        }
    }
    let mut parts = core.split('.');
    let (major, minor, patch) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || ![major, minor, patch].into_iter().all(numeric) {
        return None;
    }
    Some((major.parse().ok()?, minor.parse().ok()?))
}

/// Every component an application runs, by name, as its descriptor
/// describes it: its domains (handled here, or by a coordinator elsewhere),
/// its policies and its projectors.
#[derive(Default)]
pub struct Catalog {
    domains: BTreeMap<String, Listed>,
    /// The domains whose streams a coordinator elsewhere owns
    /// ([`Registry::remote`](super::Registry::remote)): never one of
    /// `domains`.
    remotes: BTreeSet<String>,
    /// The policies' descriptors, in registration order.
    policies: Vec<ComponentDescriptor>,
    /// The projectors' descriptors, in registration order.
    projectors: Vec<ComponentDescriptor>,
}

impl Catalog {
    /// List domain `descriptor.name`, its handler of `kind`, by its
    /// descriptor, configured as `configured`, its payloads upcast through
    /// `upcast`. Refused when the name is taken (here or remote), without
    /// a rules version or a state type, with a state whose resolutions are
    /// not resolutions, or configured with one its state does not support;
    /// refused, nothing is listed.
    pub(crate) fn list_domain(
        &mut self,
        kind: ComponentKind,
        descriptor: ComponentDescriptor,
        configured: Configured,
        upcast: Option<Arc<dyn UpcastPort>>,
    ) -> Result<Description, Misconfigured> {
        let domain = descriptor.name.clone();
        self.free(&domain)?;
        let described = described(&domain, kind, descriptor)?;
        let unsupported = |command_type: Option<&str>, resolution| {
            Err(Misconfigured::UnsupportedResolution {
                domain: domain.clone(),
                command_type: command_type.map(str::to_string),
                resolution,
            })
        };
        if let Some(resolution) = configured.component {
            if !described.supported.contains(&resolution) {
                return unsupported(None, resolution);
            }
        }
        for (command_type, resolution) in &configured.by_command {
            if !described.supported.contains(resolution) {
                return unsupported(Some(command_type), *resolution);
            }
        }
        let described = Arc::new(RwLock::new(described));
        self.domains.insert(
            domain.clone(),
            Listed {
                described: Arc::clone(&described),
                component: configured.component,
                by_command: configured.by_command,
                upcast,
            },
        );
        Ok(Description {
            domain,
            kind,
            described,
        })
    }

    /// Upcast `domain`'s incoming payloads through `port`; refused for a
    /// domain not listed here.
    pub(crate) fn upcast_with(
        &mut self,
        domain: &str,
        port: Arc<dyn UpcastPort>,
    ) -> Result<(), Misconfigured> {
        let listed = self
            .domains
            .get_mut(domain)
            .ok_or_else(|| Misconfigured::NotRegistered {
                domain: domain.to_string(),
            })?;
        listed.upcast = Some(port);
        Ok(())
    }

    /// List `domain` as a coordinator elsewhere's. Refused when the name is
    /// taken, and when a policy or projector here reads its events.
    pub(crate) fn list_remote(&mut self, domain: &str) -> Result<(), Misconfigured> {
        self.free(domain)?;
        let reader = self
            .policies
            .iter()
            .chain(&self.projectors)
            .find(|reader| reader.inputs.iter().any(|input| input.domain == domain));
        if let Some(reader) = reader {
            return Err(Misconfigured::RemoteSource {
                domain: domain.to_string(),
                component: reader.name.clone(),
            });
        }
        self.remotes.insert(domain.to_string());
        Ok(())
    }

    /// List policy `name`, of `source`'s events, its commands to
    /// `output_domain`. Refused for a name listed already and for a remote
    /// source.
    pub(crate) fn list_policy(
        &mut self,
        name: &str,
        source: &str,
        output_domain: &str,
    ) -> Result<(), Misconfigured> {
        let descriptor = ComponentDescriptor {
            output_domain: output_domain.to_string(),
            ..self.reader(name, ComponentKind::Policy, &[source], &self.policies)?
        };
        self.policies.push(descriptor);
        Ok(())
    }

    /// List projector `name`, of the events of `sources`, at least one.
    /// Refused for no source, a name listed already and a remote source.
    pub(crate) fn list_projector(
        &mut self,
        name: &str,
        sources: &[&str],
    ) -> Result<(), Misconfigured> {
        if sources.is_empty() {
            return Err(Misconfigured::ProjectorNoInputDomain {
                component: name.to_string(),
            });
        }
        let descriptor = self.reader(name, ComponentKind::Projector, sources, &self.projectors)?;
        self.projectors.push(descriptor);
        Ok(())
    }

    /// The descriptor of a reader of `sources`' events (a policy, a
    /// projector), named `name` and of `kind`: refused when `listed`
    /// holds its name, or a source is remote.
    fn reader(
        &self,
        name: &str,
        kind: ComponentKind,
        sources: &[&str],
        listed: &[ComponentDescriptor],
    ) -> Result<ComponentDescriptor, Misconfigured> {
        if listed.iter().any(|reader| reader.name == name) {
            return Err(Misconfigured::ComponentRegistered {
                component: name.to_string(),
            });
        }
        if let Some(remote) = sources.iter().find(|s| self.remotes.contains(**s)) {
            return Err(Misconfigured::RemoteSource {
                domain: remote.to_string(),
                component: name.to_string(),
            });
        }
        Ok(ComponentDescriptor {
            name: name.to_string(),
            kind: kind as i32,
            inputs: (sources.iter())
                .map(|domain| Target {
                    domain: domain.to_string(),
                })
                .collect(),
            ..ComponentDescriptor::default()
        })
    }

    /// Refused when `domain` is handled somewhere already: here or remote.
    fn free(&self, domain: &str) -> Result<(), Misconfigured> {
        match self.is_here(domain) || self.is_remote(domain) {
            true => Err(Misconfigured::AlreadyRegistered {
                domain: domain.to_string(),
            }),
            false => Ok(()),
        }
    }

    /// Whether `domain` is handled here.
    pub(crate) fn is_here(&self, domain: &str) -> bool {
        self.domains.contains_key(domain)
    }

    /// Whether `domain` is a coordinator elsewhere's.
    pub(crate) fn is_remote(&self, domain: &str) -> bool {
        self.remotes.contains(domain)
    }

    /// Whether `domain` is a process manager's.
    pub(crate) fn is_process_manager(&self, domain: &str) -> bool {
        self.kind(domain) == ComponentKind::ProcessManager
    }

    /// `domain`'s kind: a process manager's or an aggregate's, as
    /// listed here; `UNSPECIFIED` for a domain handled elsewhere (its
    /// descriptor is its coordinator's) or not registered.
    pub fn kind(&self, domain: &str) -> ComponentKind {
        match self.domains.get(domain) {
            Some(listed) if listed.described().starting.is_some() => ComponentKind::ProcessManager,
            Some(_) => ComponentKind::Aggregate,
            None => ComponentKind::Unspecified,
        }
    }

    /// `domain`'s state descriptor, if registered: what [`check`](super::check)
    /// reads capabilities from.
    pub fn state(&self, domain: &str) -> Option<StateDescriptor> {
        self.domains
            .get(domain)
            .map(|listed| listed.described().state.clone())
    }

    /// Whether `domain`'s state supports `resolution`. `false` for a domain
    /// not registered.
    pub fn supports(&self, domain: &str, resolution: ConflictResolution) -> bool {
        self.domains
            .get(domain)
            .is_some_and(|listed| listed.described().supported.contains(&resolution))
    }

    /// The resolution for a command of type `command_type_url` to `domain`
    /// whose book requested `requested` (angzarr's precedence, highest
    /// first; the kernel passes the command's type after upcasting): the
    /// book's request; the configuration for its command type; the
    /// domain's; the built-in default, **COMMUTATIVE where the state
    /// supports it, otherwise STRICT**. Never UNSPECIFIED. `None` for a
    /// domain not registered.
    pub fn resolve(
        &self,
        domain: &str,
        command_type_url: &str,
        requested: Option<ConflictResolution>,
    ) -> Option<ConflictResolution> {
        let listed = self.domains.get(domain)?;
        let built_in = match self.supports(domain, ConflictResolution::Commutative) {
            true => ConflictResolution::Commutative,
            false => ConflictResolution::Strict,
        };
        Some(
            requested
                .or_else(|| listed.by_command.get(type_name(command_type_url)).copied())
                .or(listed.component)
                .unwrap_or(built_in),
        )
    }

    /// `domain`'s registered rules version: what its events are stamped
    /// with.
    pub fn rules_version(&self, domain: &str) -> Option<String> {
        self.domains
            .get(domain)
            .map(|listed| listed.described().rules_version.clone())
    }

    /// `domain`'s starting input types, by full name, if it is a process
    /// manager's: what the kernel's start rule admits with no instance.
    pub(crate) fn starting_types(&self, domain: &str) -> Option<BTreeSet<String>> {
        self.domains.get(domain)?.described().starting.clone()
    }

    /// What `domain`'s incoming payloads are upcast through, if anything.
    pub(crate) fn upcast_port(&self, domain: &str) -> Option<&dyn UpcastPort> {
        self.domains.get(domain)?.upcast.as_deref()
    }

    /// Whether a load may start from `snapshot` of a `domain` stream
    /// (types.proto, the Snapshot region's validity): its state's type URL
    /// is the registered state's, and its rules version passes the default
    /// compatibility rule against the registered one ([`compatible`]). A
    /// snapshot that fails is not offered: the stream is replayed from its
    /// events, never migrated.
    pub fn snapshot_usable(&self, domain: &str, snapshot: &Snapshot) -> bool {
        let Some(listed) = self.domains.get(domain) else {
            return false;
        };
        let described = listed.described();
        compatible(&snapshot.rules_version, &described.rules_version)
            && snapshot
                .state
                .as_ref()
                .is_some_and(|s| type_name(&s.type_url) == type_name(&described.state.type_url))
    }

    /// The registered policies' names, in registration order: what a
    /// host holds its config's in-process policies to.
    pub fn policy_names(&self) -> impl Iterator<Item = &str> {
        self.policies.iter().map(|policy| policy.name.as_str())
    }

    /// The registered projectors' names, in registration order: what a
    /// host holds its config's in-process projectors to.
    pub fn projector_names(&self) -> impl Iterator<Item = &str> {
        self.projectors
            .iter()
            .map(|projector| projector.name.as_str())
    }

    /// The domains handled here, in name order.
    pub fn domains(&self) -> impl Iterator<Item = &str> {
        self.domains.keys().map(String::as_str)
    }

    /// The domains a coordinator elsewhere handles, in name order.
    pub fn remotes(&self) -> impl Iterator<Item = &str> {
        self.remotes.iter().map(String::as_str)
    }

    /// `domain`'s name as a component of an application, what a host's
    /// `component` entry lists it by (decision 14.50's default
    /// `component_name`): an aggregate's is its domain, a process
    /// manager's `pmg-` and its domain. `None` for a domain not handled
    /// here.
    pub fn component_name(&self, domain: &str) -> Option<String> {
        match self.kind(domain) {
            ComponentKind::ProcessManager => Some(format!("pmg-{domain}")),
            ComponentKind::Unspecified => None,
            _ => Some(domain.to_string()),
        }
    }
}

#[cfg(test)]
#[path = "catalog.test.rs"]
mod tests;
