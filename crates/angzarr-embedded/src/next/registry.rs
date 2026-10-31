//! What the kernel knows about each domain, by name: angzarr-embedded's mirror of
//! angzarr's `ComponentDescriptor.state`, and of the configuration
//! angzarr's coordinator reads.
//!
//! The pipeline's domain-blind steps ([`check`](super::check), the
//! [`Coordinator`](super::Coordinator)) look a domain up by `cover.domain`
//! and see only what angzarr's coordinator sees: the state's
//! `StateDescriptor` (its type URL and the conflict resolutions it
//! supports), the configured resolutions, and the rules version. The typed
//! parts (the rulebook, the upcaster, the decoders) are held here too, for
//! the typed step.
//!
//! Built once at start-up and shared: it is `Send + Sync`.

use std::any::Any as AnyType;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use angzarr_proto::v1::{ConflictResolution, EventPage, Snapshot, StateDescriptor};
use angzarr_router::domain::DomainDispatch;
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::router::{
    self as router, DynCommandHandler, DynPolicyComponent, DynProjectorHandler,
};

use angzarr_embedded_diagnostic::{Arg, Diagnostic};

use super::coordinator::Contextual;
use super::domain::{Domain, State};
use super::error::{KernelError, UpcastChainError};
use super::markup::{self, AsyncProjector, Projecting, ProjectionContext};
use super::payload::{type_name, Payload};
use super::route;
use super::stream::Stream;
use super::upcast::{UpcastPort, Upcaster};
use super::version::Rulebook;

/// One domain's entry.
struct Entry {
    /// The state's descriptor: generated (`D::State::descriptor()`), or a
    /// hand-built handler's.
    state: StateDescriptor,
    /// The resolutions it lists, read once at registration.
    supported: Vec<ConflictResolution>,
    /// `D::CONFLICT_RESOLUTION`.
    component: Option<ConflictResolution>,
    /// `D::CONFLICT_RESOLUTION_BY_COMMAND`, by full type name.
    by_command: BTreeMap<String, ConflictResolution>,
    /// The current rules version.
    rules_version: String,
    /// What dispatches the domain's inputs: angzarr-router's.
    handler: Box<dyn DynCommandHandler>,
    /// What only a typed domain has: its rulebook, upcaster, payload types,
    /// factory and typed replay.
    typed: Option<Typed>,
    /// Whether it is a process manager: an absent correlation is its root.
    process_manager: bool,
    /// What its incoming payloads are upcast through (decision 14.39): a
    /// typed domain's [`Upcaster`], or the port [`Registry::upcast_with`]
    /// gave. `None`: nothing is upcast.
    upcast: Option<Arc<dyn UpcastPort>>,
    /// A process manager's starting input types, by full name, as its
    /// descriptor lists them (`ComponentDescriptor.starting_types`): the
    /// kernel's start rule. `None` for an aggregate.
    starting: Option<BTreeSet<String>>,
}

/// A process manager's starting types, by name, from its descriptor:
/// `None` for any other kind. A listed type URL counts by the name it ends
/// in.
fn starting_types(
    kind: angzarr_router::pb::ComponentKind,
    descriptor: &angzarr_router::pb::ComponentDescriptor,
) -> Option<BTreeSet<String>> {
    (kind == angzarr_router::pb::ComponentKind::ProcessManager).then(|| {
        descriptor
            .starting_types
            .iter()
            .map(|t| type_name(t).to_string())
            .collect()
    })
}

/// A typed [`Domain`]'s parts, erased.
struct Typed {
    /// The `Arc<Rulebook<D::Rules>>`, boxed.
    rules: Box<dyn AnyType + Send + Sync>,
    upcaster: Arc<Upcaster>,
    /// The current event, command and fact type URLs: what an upcast chain
    /// must end at.
    current: fn() -> Vec<String>,
    /// Every fresh `D::State`: a `Factory<D::State>`, boxed.
    fresh: Box<dyn AnyType + Send + Sync>,
    /// The chain's `Stream<D>` after a step: [`replay::<D>`].
    replay: TypedReplay,
}

/// A factory of `S`.
type Factory<S> = Arc<dyn Fn() -> S + Send + Sync>;

/// A marked-up projector, erased, its one instance held here: out of the
/// lock while it runs.
pub(crate) struct ProjectorEntry {
    pub(crate) name: String,
    pub(crate) source: String,
    pub(crate) instance: ProjectorInstance,
}

/// What a registered projector runs.
pub(crate) enum ProjectorInstance {
    /// A marked-up projector's ONE instance, out of the lock while it runs.
    Marked(Mutex<Option<Box<dyn ErasedProjector>>>),
    /// A projector written as angzarr-router's `ProjectorHandler` (one in
    /// another process, behind its `ProjectorService`), handed each
    /// committed book of its source whole.
    Handler(Box<dyn DynProjectorHandler>),
}

/// A registered policy, as angzarr-router dispatches it, held in the
/// registry's one list in registration order.
pub(crate) struct PolicyEntry {
    pub(crate) dispatch: Box<dyn DynPolicyComponent>,
    /// The one domain its commands may go to (`ComponentDescriptor.output_domain`):
    /// what the kernel holds them to (decision 14.42).
    pub(crate) output_domain: String,
    /// Whether a refusal of its command is answered by its refusal fact
    /// (the router's OnRefused); a policy that does not has its refused
    /// command fail the step.
    pub(crate) answers_refusals: bool,
}

impl PolicyEntry {
    /// Its name: its commands' component and its position's handler.
    pub(crate) fn name(&self) -> &str {
        self.dispatch.name()
    }

    /// The domain whose events it handles.
    pub(crate) fn source(&self) -> &str {
        self.dispatch.input_domain()
    }
}

/// [`AsyncProjector`] with its constant and type erased.
pub(crate) trait ErasedProjector: Send {
    fn project<'a>(
        &'a mut self,
        event: &'a prost_types::Any,
        ctx: &'a ProjectionContext,
    ) -> Option<Projecting<'a>>;
}

impl<P: AsyncProjector> ErasedProjector for P {
    fn project<'a>(
        &'a mut self,
        event: &'a prost_types::Any,
        ctx: &'a ProjectionContext,
    ) -> Option<Projecting<'a>> {
        AsyncProjector::project(self, event, ctx)
    }
}

/// A typed domain's stream after a step, its type erased so a chain can
/// hold it for a domain it knows only by name: the stream as the step
/// loaded it ([`Contextual`]) with the step's `pages` folded on, a
/// `Stream<D>`, boxed.
pub(crate) type TypedReplay =
    fn(&Contextual, &[EventPage], &Registry) -> Result<Box<dyn AnyType + Send>, KernelError>;

/// [`TypedReplay`] for `D`.
fn replay<D: Domain>(
    loaded: &Contextual,
    pages: &[EventPage],
    registry: &Registry,
) -> Result<Box<dyn AnyType + Send>, KernelError> {
    let cover = loaded.command.cover().clone();
    let mut stream = match &loaded.snapshot {
        Some(snapshot) => {
            // The stream's last instant is the floor only if no event
            // follows the snapshot; otherwise the events carry their own.
            let floor = loaded.last_recorded_at.filter(|_| loaded.pages.is_empty());
            Stream::<D>::resume(cover, snapshot, floor, &loaded.pages, registry)?
        }
        None => Stream::<D>::replay(cover, &loaded.pages, registry)?,
    };
    stream.extend(pages, registry)?;
    Ok(Box::new(stream))
}

/// Why a domain could not be registered: angzarr refuses the same
/// configuration when it is loaded or the component registers.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Misconfigured {
    /// `domain` is registered already.
    AlreadyRegistered { domain: String },
    /// The upcaster names another domain.
    UpcasterForAnotherDomain { domain: String, upcaster: String },
    /// The rulebook's current version is empty: every event is stamped with
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
    /// `domain`, a remote domain ([`Registry::remote`]): a remote domain's
    /// events are its coordinator's, never written or read here, so a
    /// local component of them would never run. Remote stays remote.
    RemoteSource { domain: String, component: String },
}

impl fmt::Display for Misconfigured {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_embedded_diagnostic::display(self, f)
    }
}

impl Diagnostic for Misconfigured {
    fn code(&self) -> &'static str {
        match self {
            Misconfigured::AlreadyRegistered { .. } => {
                "angzarr-embedded-next-misconfigured-already-registered"
            }
            Misconfigured::UpcasterForAnotherDomain { .. } => {
                "angzarr-embedded-next-misconfigured-upcaster-for-another-domain"
            }
            Misconfigured::EmptyRulesVersion { .. } => {
                "angzarr-embedded-next-misconfigured-empty-rules-version"
            }
            Misconfigured::BadStateDescriptor { .. } => {
                "angzarr-embedded-next-misconfigured-bad-state-descriptor"
            }
            Misconfigured::UnsupportedResolution {
                command_type: None, ..
            } => "angzarr-embedded-next-misconfigured-unsupported-resolution",
            Misconfigured::UnsupportedResolution {
                command_type: Some(_),
                ..
            } => "angzarr-embedded-next-misconfigured-unsupported-command-resolution",
            Misconfigured::RemoteSource { .. } => {
                "angzarr-embedded-next-misconfigured-remote-source"
            }
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            Misconfigured::AlreadyRegistered { domain }
            | Misconfigured::EmptyRulesVersion { domain }
            | Misconfigured::BadStateDescriptor { domain } => {
                vec![("domain", domain.as_str().into())]
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

/// Every domain an application runs, by name.
#[derive(Default)]
pub struct Registry {
    domains: BTreeMap<String, Entry>,
    /// The domains whose streams a coordinator elsewhere owns, by name
    /// ([`Registry::remote`]): never one of `domains`.
    remotes: BTreeMap<String, Box<dyn super::CoordinatorPort>>,
    /// The registered policies, in registration order: the one list.
    policies: Vec<PolicyEntry>,
    projectors: Vec<ProjectorEntry>,
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

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `D` under `D::NAME`, with its rules (whose current version
    /// is stamped on every event, and under which each event folds), its
    /// upcaster (for events and, discouraged, commands), and its handlers:
    /// `handlers` registers them on the router's table for `D`
    /// (`DomainDispatch`, over `D::State`, already folding, restoring
    /// snapshots and resolving COMMUTATIVE through `D`), with
    /// `on_command`, `on_fact` and `on_refused`. Fills the entry from
    /// `D::State`'s generated descriptor, as `registry.register::<D>()`
    /// fills angzarr's `ComponentDescriptor.state`, and refuses
    /// configuration the state does not support.
    pub fn register<D: Domain>(
        &mut self,
        rules: Rulebook<D::Rules>,
        upcaster: Upcaster,
        handlers: impl FnOnce(DomainDispatch<D::State>) -> DomainDispatch<D::State>,
    ) -> Result<(), Misconfigured> {
        let fresh: Factory<D::State> = Arc::new(D::State::default);
        self.typed::<D>(rules, upcaster, fresh, None, handlers)
    }

    /// Register typed `D`, built fresh by `fresh`, a process manager if it
    /// has `starting` types (its descriptor's), its handlers registered by
    /// `handlers`.
    fn typed<D: Domain>(
        &mut self,
        rules: Rulebook<D::Rules>,
        upcaster: Upcaster,
        fresh: Factory<D::State>,
        starting: Option<BTreeSet<String>>,
        handlers: impl FnOnce(DomainDispatch<D::State>) -> DomainDispatch<D::State>,
    ) -> Result<(), Misconfigured> {
        let domain = D::NAME.to_string();
        if self.taken(&domain) {
            return Err(Misconfigured::AlreadyRegistered { domain });
        }
        if upcaster.domain() != domain {
            return Err(Misconfigured::UpcasterForAnotherDomain {
                domain,
                upcaster: upcaster.domain().to_string(),
            });
        }
        if rules.current().0.is_empty() {
            return Err(Misconfigured::EmptyRulesVersion { domain });
        }
        let state = D::State::descriptor();
        // A snapshot is found usable by the descriptor's type URL, and is
        // written as the snapshot form's: they must be one type.
        if type_name(&state.type_url) != <<D::State as State>::Snapshot as prost::Name>::full_name()
        {
            return Err(Misconfigured::BadStateDescriptor { domain });
        }
        let supported = supported(&domain, &state)?;
        let unsupported = |command_type: Option<&str>, resolution: ConflictResolution| {
            Misconfigured::UnsupportedResolution {
                domain: domain.clone(),
                command_type: command_type.map(str::to_string),
                resolution,
            }
        };
        if let Some(resolution) = D::CONFLICT_RESOLUTION {
            if !supported.contains(&resolution) {
                return Err(unsupported(None, resolution));
            }
        }
        let mut by_command = BTreeMap::new();
        for (command_type, resolution) in D::CONFLICT_RESOLUTION_BY_COMMAND {
            if !supported.contains(resolution) {
                return Err(unsupported(Some(command_type), *resolution));
            }
            by_command.insert(command_type.to_string(), *resolution);
        }
        let rules = Arc::new(rules);
        let upcaster = Arc::new(upcaster);
        let port: Arc<dyn UpcastPort> = upcaster.clone();
        let process_manager = starting.is_some();
        let table = route::table::<D>(Arc::clone(&rules), Arc::clone(&fresh), process_manager);
        let handler: Box<dyn DynCommandHandler> = Box::new(handlers(table));
        let entry = Entry {
            state,
            supported,
            component: D::CONFLICT_RESOLUTION,
            by_command,
            rules_version: rules.current().0.to_string(),
            handler,
            typed: Some(Typed {
                rules: Box::new(rules),
                upcaster,
                current: || {
                    let mut urls = D::Event::type_urls();
                    urls.extend(D::Command::type_urls());
                    urls.extend(D::Fact::type_urls());
                    urls
                },
                fresh: Box::new(fresh),
                replay: replay::<D>,
            }),
            process_manager,
            upcast: Some(port),
            starting,
        };
        self.domains.insert(domain, entry);
        Ok(())
    }

    /// Register `handler` as domain `domain`: any angzarr-router
    /// `CommandHandler` (a `DomainDispatch` built by hand, or a handler in
    /// another process behind one), described by `descriptor`, its
    /// generated `ComponentDescriptor` (decision 14.42: angzarr-build's
    /// `angzarr-descriptors.rs`, or the one a child answers `Describe`
    /// with). The kernel knows only what the descriptor says: its state's
    /// descriptor, its rules version, which its events are stamped with,
    /// and a process manager's starting types. Refused without a rules
    /// version or a state type. Its streams are dispatched like any
    /// domain's; with no typed [`Domain`] behind it, a chain holds no typed
    /// stream of it ([`Recorded::events`](super::Recorded::events) is
    /// `None`).
    pub fn command_handler(
        &mut self,
        domain: &str,
        handler: impl router::CommandHandler + 'static,
        descriptor: angzarr_router::pb::ComponentDescriptor,
    ) -> Result<(), Misconfigured> {
        let name = domain.to_string();
        if self.taken(&name) {
            return Err(Misconfigured::AlreadyRegistered { domain: name });
        }
        if descriptor.rules_version.is_empty() {
            return Err(Misconfigured::EmptyRulesVersion { domain: name });
        }
        let starting = starting_types(handler.kind(), &descriptor);
        let state = descriptor
            .state
            .filter(|state| !state.type_url.is_empty())
            .ok_or_else(|| Misconfigured::BadStateDescriptor {
                domain: name.clone(),
            })?;
        let supported = supported(&name, &state)?;
        let process_manager = handler.kind() == angzarr_router::pb::ComponentKind::ProcessManager;
        self.domains.insert(
            name,
            Entry {
                state,
                supported,
                component: None,
                by_command: BTreeMap::new(),
                rules_version: descriptor.rules_version,
                handler: Box::new(handler),
                typed: None,
                process_manager,
                upcast: None,
                starting,
            },
        );
        Ok(())
    }

    /// Upcast `domain`'s incoming payloads through `port` (decision
    /// 14.39): its upcaster sidecar, for a domain registered by
    /// [`Registry::command_handler`]. A typed domain's is the [`Upcaster`]
    /// it was registered with. A domain not registered PANICS: a bug in the
    /// composition root.
    pub fn upcast_with(&mut self, domain: &str, port: impl UpcastPort + 'static) -> &mut Self {
        let Some(entry) = self.domains.get_mut(domain) else {
            panic!("an upcaster is given for \"{domain}\", which is not registered");
        };
        entry.upcast = Some(Arc::new(port));
        self
    }

    /// Register the marked-up aggregate `D` (angzarr-embedded-macros' `#[aggregate]`)
    /// with `factory`, which builds every fresh instance: `D::default` for
    /// one with no configuration, or a closure over immutable reference
    /// configuration (`registry.aggregate::<Wallet>(move || Wallet::with(config.clone()))`).
    /// Replay starts from its output, and resuming from a snapshot builds
    /// from it, then [restores](super::State::restore), so the configuration
    /// survives. The factory is called once per stream and reads nothing
    /// that changes.
    ///
    /// This is the whole registration of a marked-up aggregate, with no
    /// error path: its handlers are the router's table its markup
    /// generates ([`Routed`](super::markup::Routed)), its rules are `()`,
    /// recorded under the fixed
    /// [`MARKUP_RULES_VERSION`](super::markup::MARKUP_RULES_VERSION), and
    /// its upcaster an empty one unless [`Aggregate::upcaster`] gives one.
    /// What [`Registry::register`] would refuse (a domain registered twice,
    /// a bad descriptor) is a bug in the composition root, which PANICS
    /// there, at startup. A hand-written domain keeps `register`.
    ///
    /// It returns the registration, to give an upcaster; it derefs to the
    /// registry, so registrations chain:
    /// `registry.aggregate::<Wallet>(f).upcaster(u).aggregate::<Lamp>(g)`.
    pub fn aggregate<D>(
        &mut self,
        factory: impl Fn() -> D + Send + Sync + 'static,
    ) -> Aggregate<'_, D>
    where
        D: Domain<State = D, Rules = ()> + markup::Routed,
    {
        self.marked_up::<D>(Arc::new(factory), Upcaster::new(D::NAME, D::NAME), None);
        Aggregate {
            registry: self,
            domain: std::marker::PhantomData,
        }
    }

    /// Register the marked-up process manager `P` (`#[process_manager]`)
    /// with `factory`, which builds every fresh instance. A process manager
    /// is a domain, registered as [`Registry::aggregate`] registers one
    /// (the markup's rules version, an empty upcaster, a second
    /// registration PANICS at startup), on the router's process-manager
    /// table: only a starting input type may start an instance.
    pub fn process_manager<P>(
        &mut self,
        factory: impl Fn() -> P + Send + Sync + 'static,
    ) -> &mut Self
    where
        P: super::markup::ProcessManager + markup::Routed,
    {
        // Its starting types are its markup's (the inputs marked `starts`),
        // as its generated descriptor lists them.
        let starting = P::STARTS.iter().map(|t| t.full_name()).collect();
        self.marked_up::<P>(
            Arc::new(factory),
            Upcaster::new(P::NAME, P::NAME),
            Some(starting),
        );
        self
    }

    /// Register `D`, marked up, with `factory` and `upcaster`; a process
    /// manager if it has `starting` types. A registration
    /// [`Registry::register`] refuses panics: a bug in the composition root.
    fn marked_up<D>(
        &mut self,
        factory: Factory<D>,
        upcaster: Upcaster,
        starting: Option<BTreeSet<String>>,
    ) where
        D: Domain<State = D, Rules = ()> + markup::Routed,
    {
        let rules = Rulebook::new(markup::MARKUP_RULES_VERSION, ());
        if let Err(problem) = self.typed::<D>(rules, upcaster, factory, starting, D::route) {
            panic!("{problem}");
        }
    }

    /// Register `policy`, written by hand or marked up (`#[policy]`), as the
    /// router's table for it ([`PolicyDispatch`]): ONE list, in
    /// registration order, which is the order the kernel runs every policy
    /// of a source in, for each of its events. Registering one twice (by
    /// name), or one of a remote domain's events, PANICS at startup.
    pub fn policy<P: super::Policy>(&mut self, policy: P) -> &mut Self {
        let answers_refusals = P::ANSWERS_REFUSALS;
        let dispatch = super::policy::dispatch(policy);
        let output_domain = dispatch.output_domain().to_string();
        self.push_policy(Box::new(dispatch), output_domain, answers_refusals)
    }

    /// Register a policy built as angzarr-router's dispatch table, written
    /// by hand: in the ONE list, beside every other policy, its source its
    /// input domain and its target its output domain. Its refused commands
    /// are answered by its OnRefused. Registering one twice (by name)
    /// PANICS at startup.
    pub fn policy_dispatch(&mut self, dispatch: PolicyDispatch) -> &mut Self {
        let output_domain = dispatch.output_domain().to_string();
        self.push_policy(Box::new(dispatch), output_domain, true)
    }

    /// Register a policy written as angzarr-router's `PolicyComponent`: a
    /// policy in another process, behind its `PolicyService` (the host's
    /// remote policy), or any other. In the ONE list, beside every other
    /// policy, its source its input domain and its commands held to
    /// `output_domain` (its descriptor's); its refused commands are
    /// answered by its OnRefused. Registering one twice (by name) PANICS
    /// at startup.
    pub fn policy_component(
        &mut self,
        policy: impl router::PolicyComponent + 'static,
        output_domain: &str,
    ) -> &mut Self {
        self.push_policy(Box::new(policy), output_domain.to_string(), true)
    }

    fn push_policy(
        &mut self,
        dispatch: Box<dyn DynPolicyComponent>,
        output_domain: String,
        answers_refusals: bool,
    ) -> &mut Self {
        self.refuse_remote_source(dispatch.input_domain(), dispatch.name());
        if self
            .policies
            .iter()
            .any(|entry| entry.name() == dispatch.name())
        {
            panic!("policy \"{}\" is registered already", dispatch.name());
        }
        self.policies.push(PolicyEntry {
            dispatch,
            output_domain,
            answers_refusals,
        });
        self
    }

    /// The policies of `source`'s events, in registration order.
    pub(crate) fn policies<'r>(
        &'r self,
        source: &'r str,
    ) -> impl Iterator<Item = &'r PolicyEntry> + 'r {
        self.policies
            .iter()
            .filter(move |entry| entry.source() == source)
    }

    /// Every registered policy, in registration order.
    pub(crate) fn all_policies(&self) -> impl Iterator<Item = &PolicyEntry> {
        self.policies.iter()
    }

    /// `domain`'s handler: what the kernel dispatches its inputs through.
    pub(crate) fn handler(&self, domain: &str) -> Option<&dyn DynCommandHandler> {
        self.domains.get(domain).map(|e| &*e.handler)
    }

    /// `domain`'s typed replay, for a typed domain.
    pub(crate) fn typed_replay(&self, domain: &str) -> Option<TypedReplay> {
        self.domains.get(domain)?.typed.as_ref().map(|t| t.replay)
    }

    /// Register `projector`, the ONE instance of a marked-up projector
    /// (`#[projector]`) this process runs, holding its ports. The kernel
    /// ([`Kernel::handle`](super::Kernel::handle)) runs it after each commit that records its source's events, and
    /// awaits it before the call returns: a failure is the caller's error
    /// ([`Failure::Projection`](super::Failure::Projection); the events stay committed), and does not
    /// stop the other projectors. Registering one twice PANICS at startup.
    pub fn projector<P: AsyncProjector>(&mut self, projector: P) -> &mut Self {
        self.push_projector(
            P::NAME,
            <P::Source as Domain>::NAME,
            ProjectorInstance::Marked(Mutex::new(Some(Box::new(projector)))),
        )
    }

    /// Register `projector`, written as angzarr-router's
    /// `ProjectorHandler` (a projector in another process, behind its
    /// `ProjectorService`: the host's remote projector), projecting
    /// `source`'s events. The kernel runs it as it runs a marked-up one,
    /// after each commit that records its source's events, handing it each
    /// committed book of them; its failures follow their class (decision
    /// 14.37, 14.41): a retryable one and an ABORTED one are run again
    /// under the kernel's [`OnRetryable`](super::OnRetryable), then
    /// dead-lettered, and any other is dead-lettered at once. Registering
    /// one twice (by name) PANICS at startup.
    pub fn projector_handler(
        &mut self,
        source: &str,
        projector: impl router::ProjectorHandler + 'static,
    ) -> &mut Self {
        let name = projector.name().to_string();
        self.push_projector(
            &name,
            source,
            ProjectorInstance::Handler(Box::new(projector)),
        )
    }

    fn push_projector(
        &mut self,
        name: &str,
        source: &str,
        instance: ProjectorInstance,
    ) -> &mut Self {
        self.refuse_remote_source(source, name);
        if self.projectors.iter().any(|entry| entry.name == name) {
            panic!("projector \"{name}\" is registered already");
        }
        self.projectors.push(ProjectorEntry {
            name: name.to_string(),
            source: source.to_string(),
            instance,
        });
        self
    }

    /// A fresh `D` state: the registered factory's, else the default.
    pub(crate) fn fresh<D: Domain>(&self) -> D::State {
        self.domains
            .get(D::NAME)
            .and_then(|entry| entry.typed.as_ref())
            .and_then(|typed| typed.fresh.downcast_ref::<Factory<D::State>>())
            .map_or_else(D::State::default, |factory| factory())
    }

    /// Whether `domain` is a process manager's.
    pub(crate) fn is_process_manager(&self, domain: &str) -> bool {
        self.domains.get(domain).is_some_and(|e| e.process_manager)
    }

    /// `domain`'s kind: a process manager's or an aggregate's, as
    /// registered here; `UNSPECIFIED` for a domain handled elsewhere
    /// ([`Registry::remote`]: its descriptor is its coordinator's) or not
    /// registered.
    pub(crate) fn kind(&self, domain: &str) -> angzarr_router::pb::ComponentKind {
        let _ = domain;
        angzarr_router::pb::ComponentKind::Unspecified
    }

    /// Every projector, in registration order.
    pub(crate) fn projectors(&self) -> &[ProjectorEntry] {
        &self.projectors
    }

    /// `domain`'s state descriptor, if registered: what [`check`](super::check)
    /// reads capabilities from.
    pub fn state(&self, domain: &str) -> Option<&StateDescriptor> {
        self.domains.get(domain).map(|e| &e.state)
    }

    /// Whether `domain`'s state supports `resolution`. `false` for a domain
    /// not registered.
    pub fn supports(&self, domain: &str, resolution: ConflictResolution) -> bool {
        self.domains
            .get(domain)
            .is_some_and(|e| e.supported.contains(&resolution))
    }

    /// The resolution for a command of type `command_type_url` to `domain`
    /// whose book requested `requested` (angzarr's precedence, highest
    /// first; the kernel passes the command's type after upcasting): the book's request; the configuration for its command type;
    /// the domain's; the built-in default, **COMMUTATIVE where the state
    /// supports it, otherwise STRICT**. Never UNSPECIFIED. `None` for a
    /// domain not registered.
    pub fn resolve(
        &self,
        domain: &str,
        command_type_url: &str,
        requested: Option<ConflictResolution>,
    ) -> Option<ConflictResolution> {
        let entry = self.domains.get(domain)?;
        let built_in = if entry.supported.contains(&ConflictResolution::Commutative) {
            ConflictResolution::Commutative
        } else {
            ConflictResolution::Strict
        };
        Some(
            requested
                .or_else(|| entry.by_command.get(type_name(command_type_url)).copied())
                .or(entry.component)
                .unwrap_or(built_in),
        )
    }

    /// `domain`'s current rules version: what its events are stamped with.
    pub fn rules_version(&self, domain: &str) -> Option<&str> {
        self.domains.get(domain).map(|e| e.rules_version.as_str())
    }

    /// `D`'s rulebook, if `D` is registered.
    pub fn rules<D: Domain>(&self) -> Option<&Rulebook<D::Rules>> {
        self.domains
            .get(D::NAME)?
            .typed
            .as_ref()?
            .rules
            .downcast_ref::<Arc<Rulebook<D::Rules>>>()
            .map(|rules| &**rules)
    }

    /// `domain`'s upcaster, if registered.
    pub fn upcaster(&self, domain: &str) -> Option<&Upcaster> {
        self.domains
            .get(domain)?
            .typed
            .as_ref()
            .map(|t| &*t.upcaster)
    }

    /// `domain`'s starting input types, by full name, if it is a process
    /// manager's: what the kernel's start rule admits with no instance.
    pub(crate) fn starting_types(&self, domain: &str) -> Option<&BTreeSet<String>> {
        self.domains.get(domain)?.starting.as_ref()
    }

    /// What `domain`'s incoming payloads are upcast through, if anything.
    pub(crate) fn upcast_port(&self, domain: &str) -> Option<&dyn UpcastPort> {
        self.domains.get(domain)?.upcast.as_deref()
    }

    /// Whether a load may start from `snapshot` of a `domain` stream: its
    /// state's type URL is the registered state's, and its rules version
    /// the current one. On a mismatch the snapshot is discarded and the
    /// stream replayed from its events, never migrated.
    pub fn snapshot_usable(&self, domain: &str, snapshot: &Snapshot) -> bool {
        let Some(entry) = self.domains.get(domain) else {
            return false;
        };
        snapshot.rules_version == entry.rules_version
            && snapshot
                .state
                .as_ref()
                .is_some_and(|s| type_name(&s.type_url) == type_name(&entry.state.type_url))
    }

    /// Check every domain's upcaster against the types it decodes
    /// ([`Upcaster::check`]). For a test that runs in CI beside the golden
    /// replay fixtures ([`assert_replay_fixtures`](super::assert_replay_fixtures)).
    pub fn check(&self) -> Result<(), Vec<UpcastChainError>> {
        let problems: Vec<UpcastChainError> = self
            .domains
            .values()
            .filter_map(|entry| entry.typed.as_ref())
            .filter_map(|typed| typed.upcaster.check(&(typed.current)()).err())
            .flatten()
            .collect();
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems)
        }
    }

    /// The registered domains, in name order.
    pub fn domains(&self) -> impl Iterator<Item = &str> {
        self.domains.keys().map(String::as_str)
    }

    /// Route `domain` to `coordinator`, a coordinator elsewhere that owns
    /// its streams ([`super::target`]): every command naming it, the
    /// application's and every policy step's, is handed to the port, and
    /// nothing of it is loaded or written here. Refused for a domain
    /// registered already, here or remote, and for one whose events a
    /// policy or projector here handles ([`Misconfigured::RemoteSource`]):
    /// a remote domain stays remote, its events never here.
    pub fn remote(
        &mut self,
        domain: &str,
        coordinator: impl super::CoordinatorPort + 'static,
    ) -> Result<(), Misconfigured> {
        if self.taken(domain) {
            return Err(Misconfigured::AlreadyRegistered {
                domain: domain.to_string(),
            });
        }
        let policies = self.policies.iter().map(|p| (p.source(), p.name()));
        let projectors = self
            .projectors
            .iter()
            .map(|p| (p.source.as_str(), p.name.as_str()));
        if let Some((_, component)) = policies
            .chain(projectors)
            .find(|(source, _)| *source == domain)
        {
            return Err(Misconfigured::RemoteSource {
                domain: domain.to_string(),
                component: component.to_string(),
            });
        }
        self.remotes
            .insert(domain.to_string(), Box::new(coordinator));
        Ok(())
    }

    /// Where a command naming `domain` is handled: here, through its
    /// handler; remote, through its coordinator; `None` when it is
    /// neither (the caller's [`Malformed::UnknownDomain`](super::Malformed::UnknownDomain)).
    pub fn target(&self, domain: &str) -> Option<super::Target<'_>> {
        match self.domains.get(domain) {
            Some(entry) => Some(super::Target::Local(&*entry.handler)),
            None => self
                .remotes
                .get(domain)
                .map(|port| super::Target::Remote(&**port)),
        }
    }

    /// PANICS at startup, [`Misconfigured::RemoteSource`], when `source`
    /// is a remote domain: `component` here would handle events that are
    /// never here.
    fn refuse_remote_source(&self, source: &str, component: &str) {
        if self.remotes.contains_key(source) {
            panic!(
                "{}",
                Misconfigured::RemoteSource {
                    domain: source.to_string(),
                    component: component.to_string(),
                }
            );
        }
    }

    /// Whether `domain` is handled somewhere already: here or remote.
    fn taken(&self, domain: &str) -> bool {
        self.domains.contains_key(domain) || self.remotes.contains_key(domain)
    }
}

#[cfg(test)]
#[path = "registry.test.rs"]
mod tests;

/// A marked-up aggregate's registration, made by [`Registry::aggregate`]:
/// already registered, with an empty upcaster, until
/// [`Aggregate::upcaster`] replaces it. Derefs to the registry, so
/// registrations chain.
pub struct Aggregate<'r, D: Domain> {
    registry: &'r mut Registry,
    domain: std::marker::PhantomData<fn() -> D>,
}

impl<'r, D: Domain<State = D, Rules = ()> + markup::Routed> Aggregate<'r, D> {
    /// Read the aggregate's old events through `upcaster` (its domain must
    /// be `D::NAME`; another panics, as a composition-root bug).
    pub fn upcaster(self, upcaster: super::Upcaster) -> Aggregate<'r, D> {
        if upcaster.domain() != D::NAME {
            panic!(
                "the upcaster given for domain \"{}\" is for \"{}\"",
                D::NAME,
                upcaster.domain()
            );
        }
        let Some(entry) = self.registry.domains.remove(D::NAME) else {
            return self;
        };
        let starting = entry.starting;
        let fresh = (entry.typed)
            .and_then(|typed| typed.fresh.downcast::<Factory<D>>().ok())
            .map(|fresh| *fresh);
        if let Some(fresh) = fresh {
            self.registry.marked_up::<D>(fresh, upcaster, starting);
        }
        self
    }
}

impl<D: Domain> std::ops::Deref for Aggregate<'_, D> {
    type Target = Registry;

    fn deref(&self) -> &Registry {
        self.registry
    }
}

impl<D: Domain> std::ops::DerefMut for Aggregate<'_, D> {
    fn deref_mut(&mut self) -> &mut Registry {
        self.registry
    }
}
