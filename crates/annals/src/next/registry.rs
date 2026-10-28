//! What the kernel knows about each domain, by name: annals' mirror of
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
use std::sync::Mutex;

use angzarr_proto::v1::{ConflictResolution, EventPage, Snapshot, StateDescriptor};
use prost_types::Any;

use annals_diagnostic::{Arg, Diagnostic, Report};

use super::check::Sampled;
use super::coordinator::{run_step, Contextual};
use super::dead_letter::DeadLetterLog;
use super::domain::{Domain, State};
use super::error::{Failure, StepError, UpcastChainError};
use super::markup::{self, AsyncProjector, Projecting, ProjectionContext};
use super::payload::{type_name, Payload};
use super::upcast::Upcaster;
use super::version::Rulebook;

/// One domain's entry.
struct Entry {
    /// `D::State::descriptor()`, as generated.
    state: StateDescriptor,
    /// The resolutions it lists, read once at registration.
    supported: Vec<ConflictResolution>,
    /// `D::CONFLICT_RESOLUTION`.
    component: Option<ConflictResolution>,
    /// `D::CONFLICT_RESOLUTION_BY_COMMAND`, by full type name.
    by_command: BTreeMap<String, ConflictResolution>,
    /// The rulebook's current version.
    rules_version: String,
    /// The `Rulebook<D::Rules>`, boxed.
    rules: Box<dyn AnyType + Send + Sync>,
    upcaster: Upcaster,
    /// The current event, command and fact type URLs: what an upcast chain
    /// must end at.
    current: fn() -> Vec<String>,
    /// `D`'s typed step 5, erased: [`typed_step::<D>`].
    step: TypedStep,
    /// A marked-up domain's factory: every fresh `D::State`, boxed.
    fresh: Option<Fresh>,
    /// Whether it is a process manager: its root is the correlation.
    process_manager: bool,
}

/// A marked-up domain's factory, erased: a `Factory<D::State>`, boxed.
type Fresh = Box<dyn AnyType + Send + Sync>;

/// A factory of `S`.
type Factory<S> = Box<dyn Fn() -> S + Send + Sync>;

/// A marked-up projector, erased, its one instance held here: out of the
/// lock while it runs.
pub(crate) struct ProjectorEntry {
    pub(crate) name: &'static str,
    pub(crate) source: &'static str,
    pub(crate) instance: Mutex<Option<Box<dyn ErasedProjector>>>,
}

/// [`AsyncProjector`] with its constant and type erased.
pub(crate) trait ErasedProjector: Send {
    fn project<'a>(
        &'a mut self,
        event: &'a Any,
        ctx: &'a ProjectionContext,
    ) -> Option<Projecting<'a>>;
}

impl<P: AsyncProjector> ErasedProjector for P {
    fn project<'a>(
        &'a mut self,
        event: &'a Any,
        ctx: &'a ProjectionContext,
    ) -> Option<Projecting<'a>> {
        AsyncProjector::project(self, event, ctx)
    }
}

/// A domain's typed step 5, its types erased so a chain can run a policy's
/// command in a target it knows only by name: decode (from the chain's
/// staged `Stream<D>`, boxed, when it has one), decide or ingest, and hand
/// back the stream after (boxed) and the pages to append. A rejection
/// comes back as its report.
pub(crate) type TypedStep = fn(
    Contextual,
    Option<Box<dyn AnyType + Send>>,
    &Registry,
    &Sampled,
    &dyn DeadLetterLog,
) -> Result<(Box<dyn AnyType + Send>, Vec<EventPage>), StepError>;

/// [`TypedStep`] for `D`.
fn typed_step<D: Domain>(
    command: Contextual,
    staged: Option<Box<dyn AnyType + Send>>,
    registry: &Registry,
    sampled: &Sampled,
    dead_letters: &dyn DeadLetterLog,
) -> Result<(Box<dyn AnyType + Send>, Vec<EventPage>), StepError> {
    let (stream, pages) =
        run_step::<D>(command, staged, registry, sampled, dead_letters).map_err(|failure| {
            match failure {
                Failure::Rejected(rejection) => StepError::Rejected(Report::of(&rejection)),
                Failure::Kernel(error) => StepError::Kernel(error),
                Failure::Downstream { error, .. } => error,
                Failure::Projection { .. } => unreachable!("a chain step runs no projector"),
            }
        })?;
    Ok((Box::new(stream), pages))
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
        match self {
            Misconfigured::AlreadyRegistered { domain } => {
                write!(f, "domain \"{domain}\" is registered already")
            }
            Misconfigured::UpcasterForAnotherDomain { domain, upcaster } => write!(
                f,
                "the upcaster registered for domain \"{domain}\" is for \"{upcaster}\""
            ),
            Misconfigured::EmptyRulesVersion { domain } => {
                write!(f, "domain \"{domain}\"'s current rules version is empty")
            }
            Misconfigured::BadStateDescriptor { domain } => write!(
                f,
                "domain \"{domain}\"'s state descriptor lists a value that is not a resolution, or lacks STRICT or AGGREGATE_HANDLES"
            ),
            Misconfigured::UnsupportedResolution {
                domain,
                command_type: None,
                resolution,
            } => write!(
                f,
                "domain \"{domain}\" is configured {}, which its state does not support",
                resolution.as_str_name()
            ),
            Misconfigured::UnsupportedResolution {
                domain,
                command_type: Some(command_type),
                resolution,
            } => write!(
                f,
                "domain \"{domain}\" configures {command_type} {}, which its state does not support",
                resolution.as_str_name()
            ),
        }
    }
}

impl Diagnostic for Misconfigured {
    fn code(&self) -> &'static str {
        match self {
            Misconfigured::AlreadyRegistered { .. } => {
                "annals-next-misconfigured-already-registered"
            }
            Misconfigured::UpcasterForAnotherDomain { .. } => {
                "annals-next-misconfigured-upcaster-for-another-domain"
            }
            Misconfigured::EmptyRulesVersion { .. } => {
                "annals-next-misconfigured-empty-rules-version"
            }
            Misconfigured::BadStateDescriptor { .. } => {
                "annals-next-misconfigured-bad-state-descriptor"
            }
            Misconfigured::UnsupportedResolution {
                command_type: None, ..
            } => "annals-next-misconfigured-unsupported-resolution",
            Misconfigured::UnsupportedResolution {
                command_type: Some(_),
                ..
            } => "annals-next-misconfigured-unsupported-command-resolution",
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
    /// The registered policies' names.
    policies: Vec<&'static str>,
    projectors: Vec<ProjectorEntry>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `D` under `D::NAME`, with its rules (whose current version
    /// is stamped on every event) and its upcaster (for events and,
    /// discouraged, commands). Fills the entry from `D::State`'s generated
    /// descriptor, as `registry.register::<D>()` fills angzarr's
    /// `ComponentDescriptor.state`, and refuses configuration the state
    /// does not support.
    pub fn register<D: Domain>(
        &mut self,
        rules: Rulebook<D::Rules>,
        upcaster: Upcaster,
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
        let listed = state
            .options
            .as_ref()
            .map(|o| o.conflict_resolutions.as_slice())
            .unwrap_or_default();
        let mut supported = Vec::with_capacity(listed.len());
        for value in listed {
            match ConflictResolution::try_from(*value) {
                Ok(ConflictResolution::Unspecified) | Err(_) => {
                    return Err(Misconfigured::BadStateDescriptor { domain })
                }
                Ok(resolution) => supported.push(resolution),
            }
        }
        if !supported.contains(&ConflictResolution::Strict)
            || !supported.contains(&ConflictResolution::AggregateHandles)
        {
            return Err(Misconfigured::BadStateDescriptor { domain });
        }
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
        let entry = Entry {
            state,
            supported,
            component: D::CONFLICT_RESOLUTION,
            by_command,
            rules_version: rules.current().0.to_string(),
            rules: Box::new(rules),
            upcaster,
            current: || {
                let mut urls = D::Event::type_urls();
                urls.extend(D::Command::type_urls());
                urls.extend(D::Fact::type_urls());
                urls
            },
            step: typed_step::<D>,
            fresh: None,
            process_manager: false,
        };
        self.domains.insert(domain, entry);
        Ok(())
    }

    /// Register the marked-up aggregate `D` (annals-macros' `#[aggregate]`)
    /// with `factory`, which builds every fresh instance: `D::default` for
    /// one with no configuration, or a closure over immutable reference
    /// configuration (`registry.aggregate::<Wallet>(move || Wallet::with(config.clone()))`).
    /// Replay starts from its output, and resuming from a snapshot builds
    /// from it, then [restores](super::State::restore), so the configuration
    /// survives. The factory is called once per stream and reads nothing
    /// that changes.
    ///
    /// This is the whole registration of a marked-up aggregate, with no
    /// error path: its rules are `()`, recorded under the fixed
    /// [`MARKUP_RULES_VERSION`](super::markup::MARKUP_RULES_VERSION), and
    /// its upcaster an empty one unless [`Aggregate::upcaster`] gives one.
    /// What [`Registry::register`] would refuse (a domain registered twice,
    /// a bad descriptor) is a bug in the composition root, which PANICS
    /// there, at startup. A hand-written domain keeps `register`.
    ///
    /// It returns the registration, to give an upcaster; it derefs to the
    /// registry, so registrations chain:
    /// `registry.aggregate::<Wallet>(f).upcaster(u).aggregate::<Lamp>(g)`.
    ///
    pub fn aggregate<D>(
        &mut self,
        factory: impl Fn() -> D + Send + Sync + 'static,
    ) -> Aggregate<'_, D>
    where
        D: Domain<State = D, Rules = ()>,
    {
        self.marked_up::<D>(factory, false);
        Aggregate {
            registry: self,
            domain: std::marker::PhantomData,
        }
    }

    /// Register the marked-up process manager `P` (`#[process_manager]`)
    /// with `factory`, which builds every fresh instance. A process manager
    /// is a domain, registered as [`Registry::aggregate`] registers one
    /// (the markup's rules version, an empty upcaster, a second
    /// registration PANICS at startup), plus the two rules of
    /// [`ProcessManager`](super::markup::ProcessManager): an input
    /// addressed to it must name the instance its correlation names
    /// ([`check`](super::check) refuses any other), and only a starting
    /// input type may start an instance (its generated `Domain` impl).
    pub fn process_manager<P: super::markup::ProcessManager>(
        &mut self,
        factory: impl Fn() -> P + Send + Sync + 'static,
    ) -> &mut Self {
        self.marked_up::<P>(factory, true);
        self
    }

    /// Register `D`, marked up, with `factory`; a process manager if `pm`.
    /// A registration [`Registry::register`] refuses panics: a bug in the
    /// composition root.
    fn marked_up<D>(&mut self, factory: impl Fn() -> D + Send + Sync + 'static, pm: bool)
    where
        D: Domain<State = D, Rules = ()>,
    {
        let rules = Rulebook::new(markup::MARKUP_RULES_VERSION, ());
        if let Err(problem) = self.register::<D>(rules, Upcaster::new(D::NAME, D::NAME)) {
            panic!("{problem}");
        }
        let entry = self
            .domains
            .get_mut(D::NAME)
            .expect("the domain was just registered");
        let factory: Factory<D> = Box::new(factory);
        entry.fresh = Some(Box::new(factory));
        entry.process_manager = pm;
    }

    /// Register `policy`, a marked-up policy (`#[policy]`). Registering one twice
    /// PANICS at startup.
    ///
    /// annals does not run a policy yet: its steps, each committed on its
    /// own (the source's events before the policy's command; a refusal fact
    /// as a step of its own), arrive with the kernel's committed-step
    /// dispatch. What a policy answers is its markup's
    /// ([`Policy::on_event`](super::markup::Policy::on_event),
    /// [`Policy::on_refused`](super::markup::Policy::on_refused)), whatever
    /// runs it.
    pub fn policy<S: super::markup::Policy>(&mut self, policy: S) -> &mut Self {
        if self.policies.contains(&S::NAME) {
            panic!("policy \"{}\" is registered already", S::NAME);
        }
        drop(policy);
        self.policies.push(S::NAME);
        self
    }

    /// Register `projector`, the ONE instance of a marked-up projector
    /// (`#[projector]`) this process runs, holding its ports. The kernel's
    /// async command path ([`Kernel::handle_async`](super::Kernel::handle_async))
    /// runs it after each commit that records its source's events, and
    /// awaits it before the call returns: a failure is the caller's error
    /// ([`Failure::Projection`]; the events stay committed), and does not
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
            .and_then(|entry| entry.fresh.as_ref())
            .and_then(|fresh| fresh.downcast_ref::<Factory<D::State>>())
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
        self.domains.get(D::NAME)?.rules.downcast_ref()
    }

    /// `domain`'s upcaster, if registered.
    pub fn upcaster(&self, domain: &str) -> Option<&Upcaster> {
        self.domains.get(domain).map(|e| &e.upcaster)
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
            .filter_map(|entry| entry.upcaster.check(&(entry.current)()).err())
            .flatten()
            .collect();
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems)
        }
    }

    /// `domain`'s typed step 5, erased.
    pub(crate) fn step(&self, domain: &str) -> Option<TypedStep> {
        self.domains.get(domain).map(|e| e.step)
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

impl<'r, D: Domain> Aggregate<'r, D> {
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
        if let Some(entry) = self.registry.domains.get_mut(D::NAME) {
            entry.upcaster = upcaster;
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
