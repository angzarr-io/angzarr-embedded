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

use angzarr_proto::v1::{ConflictResolution, EventPage, Snapshot, StateDescriptor};

use super::check::Sampled;
use super::coordinator::Contextual;
use super::dead_letter::DeadLetterLog;
use super::domain::{Domain, State};
use super::error::{StepError, UpcastChainError};
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
    todo!(
        "proposal: decode::<{}> or resume the staged stream, decide, commit in memory, pages",
        D::NAME
    )
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

/// Every domain an application runs, by name.
#[derive(Default)]
pub struct Registry {
    domains: BTreeMap<String, Entry>,
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
        };
        self.domains.insert(domain, entry);
        Ok(())
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
    /// first): the book's request; the configuration for its command type;
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
        todo!("proposal: each entry's upcaster.check((entry.current)())")
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
