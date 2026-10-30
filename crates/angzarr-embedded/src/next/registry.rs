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
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use angzarr_proto::v1::{ConflictResolution, EventPage, Snapshot, StateDescriptor};
use angzarr_router::domain::DomainDispatch;
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::router::CommandHandler;

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
    handler: Box<dyn CommandHandler>,
    /// What only a typed domain has: its rulebook, upcaster, payload types,
    /// factory and typed replay.
    typed: Option<Typed>,
    /// Whether it is a process manager: an absent correlation is its root.
    process_manager: bool,
    /// What its incoming payloads are upcast through (decision 14.39): a
    /// typed domain's [`Upcaster`], or the port [`Registry::upcast_with`]
    /// gave. `None`: nothing is upcast.
    upcast: Option<Arc<dyn UpcastPort>>,
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
    pub(crate) name: &'static str,
    pub(crate) source: &'static str,
    pub(crate) instance: Mutex<Option<Box<dyn ErasedProjector>>>,
}

/// A registered policy, as angzarr-router dispatches it, held in the
/// registry's one list in registration order.
pub(crate) struct PolicyEntry {
    pub(crate) dispatch: PolicyDispatch,
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
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            Misconfigured::AlreadyRegistered { domain }
            | Misconfigured::EmptyRulesVersion { domain }
            | Misconfigured::BadStateDescriptor { domain } => {
                vec![("domain", domain.as_str().into())]
            }
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
        self.typed::<D>(rules, upcaster, fresh, false, handlers)
    }

    /// Register typed `D`, built fresh by `fresh`, a process manager if
    /// `process_manager`, its handlers registered by `handlers`.
    fn typed<D: Domain>(
        &mut self,
        rules: Rulebook<D::Rules>,
        upcaster: Upcaster,
        fresh: Factory<D::State>,
        process_manager: bool,
        handlers: impl FnOnce(DomainDispatch<D::State>) -> DomainDispatch<D::State>,
    ) -> Result<(), Misconfigured> {
        let domain = D::NAME.to_string();
        if self.domains.contains_key(&domain) {
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
        let table = route::table::<D>(
            Arc::clone(&rules),
            Arc::clone(&upcaster),
            Arc::clone(&fresh),
            process_manager,
        );
        let entry = Entry {
            state,
            supported,
            component: D::CONFLICT_RESOLUTION,
            by_command,
            rules_version: rules.current().0.to_string(),
            handler: Box::new(handlers(table)),
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
        };
        self.domains.insert(domain, entry);
        Ok(())
    }

    /// Register `handler` as domain `domain`: any angzarr-router
    /// `CommandHandler` (a `DomainDispatch` built by hand, or a handler in
    /// another process behind one). The kernel knows only what its
    /// registration hello (`ComponentDescriptor`) says: its state's
    /// descriptor and its rules version, which its events are stamped with.
    /// Its streams are dispatched like any domain's; with no typed
    /// [`Domain`] behind it, a chain holds no typed stream of it
    /// ([`Recorded::events`](super::Recorded::events) is `None`).
    pub fn command_handler(
        &mut self,
        domain: &str,
        handler: impl CommandHandler + 'static,
    ) -> Result<(), Misconfigured> {
        let name = domain.to_string();
        if self.domains.contains_key(&name) {
            return Err(Misconfigured::AlreadyRegistered { domain: name });
        }
        let descriptor = handler.component_descriptor().map_err(|error| {
            match error.code == angzarr_router::error::codes::MISSING_RULES_VERSION {
                true => Misconfigured::EmptyRulesVersion {
                    domain: name.clone(),
                },
                false => Misconfigured::BadStateDescriptor {
                    domain: name.clone(),
                },
            }
        })?;
        let state = descriptor
            .state
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
        self.marked_up::<D>(Arc::new(factory), Upcaster::new(D::NAME, D::NAME), false);
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
        self.marked_up::<P>(Arc::new(factory), Upcaster::new(P::NAME, P::NAME), true);
        self
    }

    /// Register `D`, marked up, with `factory` and `upcaster`; a process
    /// manager if `pm`. A registration [`Registry::register`] refuses
    /// panics: a bug in the composition root.
    fn marked_up<D>(&mut self, factory: Factory<D>, upcaster: Upcaster, pm: bool)
    where
        D: Domain<State = D, Rules = ()> + markup::Routed,
    {
        let rules = Rulebook::new(markup::MARKUP_RULES_VERSION, ());
        if let Err(problem) = self.typed::<D>(rules, upcaster, factory, pm, D::route) {
            panic!("{problem}");
        }
    }

    /// Register `policy`, written by hand or marked up (`#[policy]`), as the
    /// router's table for it ([`PolicyDispatch`]): ONE list, in
    /// registration order, which is the order the kernel runs every policy
    /// of a source in, for each of its events. Registering one twice (by
    /// name) PANICS at startup.
    pub fn policy<P: super::Policy>(&mut self, policy: P) -> &mut Self {
        let answers_refusals = P::ANSWERS_REFUSALS;
        self.push_policy(super::policy::dispatch(policy), answers_refusals)
    }

    /// Register a policy built as angzarr-router's dispatch table, written
    /// by hand: in the ONE list, beside every other policy, its source its
    /// input domain and its target its output domain. Its refused commands
    /// are answered by its OnRefused. Registering one twice (by name)
    /// PANICS at startup.
    pub fn policy_dispatch(&mut self, dispatch: PolicyDispatch) -> &mut Self {
        self.push_policy(dispatch, true)
    }

    fn push_policy(&mut self, dispatch: PolicyDispatch, answers_refusals: bool) -> &mut Self {
        if self
            .policies
            .iter()
            .any(|entry| entry.name() == dispatch.name())
        {
            panic!("policy \"{}\" is registered already", dispatch.name());
        }
        self.policies.push(PolicyEntry {
            dispatch,
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
    pub(crate) fn handler(&self, domain: &str) -> Option<&dyn CommandHandler> {
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
        if self.projectors.iter().any(|entry| entry.name == P::NAME) {
            panic!("projector \"{}\" is registered already", P::NAME);
        }
        self.projectors.push(ProjectorEntry {
            name: P::NAME,
            source: <P::Source as Domain>::NAME,
            instance: Mutex::new(Some(Box::new(projector))),
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
        let process_manager = self.registry.is_process_manager(D::NAME);
        let fresh = self
            .registry
            .domains
            .remove(D::NAME)
            .and_then(|entry| entry.typed)
            .and_then(|typed| typed.fresh.downcast::<Factory<D>>().ok())
            .map(|fresh| *fresh);
        if let Some(fresh) = fresh {
            self.registry
                .marked_up::<D>(fresh, upcaster, process_manager);
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
