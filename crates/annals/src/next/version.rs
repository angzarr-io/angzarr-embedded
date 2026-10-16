//! Versions: of the rules (reference data) an event was decided under, and
//! of the schema its payload was written at.
//!
//! - **Rules** are resolved per event by the version its [`Meta`] records,
//!   through a [`Rulebook`]: replay never trusts the caller to pass the
//!   right ones.
//! - **Schemas** move forward by upcasting at read time, between decoding a
//!   stored event and replaying it: a chain of steps, each from version `N`
//!   to `N + 1`, checked whole when built ([`Upcasts::build`]), as
//!   annals-yaml's versioned documents are. A step works on the stored
//!   form (bytes, or anything a domain stores), one event in, one event
//!   out, seq and metadata kept. Stored events are never migrated in place;
//!   what was written stays what was written.
//! - **Golden replay fixtures** ([`assert_replay_fixtures`]) prove an old
//!   log still replays to the states it did: old log in, expected states
//!   out.

use std::collections::BTreeMap;
use std::fmt::Debug;

use annals_diagnostic::Report;

use super::domain::Domain;
use super::error::KernelError;
use super::id::{RulesVersion, Seq};
use super::meta::Meta;
use super::stream::{Envelope, Header, Stream};

/// Every version of a domain's rules a log may name, and the current one,
/// which new commands are decided under.
pub struct Rulebook<R> {
    current: RulesVersion,
    versions: BTreeMap<RulesVersion, R>,
}

impl<R> Rulebook<R> {
    /// One version, current.
    pub fn new(version: impl Into<RulesVersion>, rules: R) -> Rulebook<R> {
        let current = version.into();
        let mut versions = BTreeMap::new();
        versions.insert(current.clone(), rules);
        Rulebook { current, versions }
    }

    /// Also hold `version`, an older one a log may name. Replaces any rules
    /// already held under that version.
    pub fn with(mut self, version: impl Into<RulesVersion>, rules: R) -> Rulebook<R> {
        self.versions.insert(version.into(), rules);
        self
    }

    /// The current version and its rules.
    pub fn current(&self) -> (&RulesVersion, &R) {
        let rules = &self.versions[&self.current];
        (&self.current, rules)
    }

    /// The rules `version` names, if held.
    pub fn get(&self, version: &RulesVersion) -> Option<&R> {
        self.versions.get(version)
    }
}

/// One upcast step: an event's stored form at version `N`, to `N + 1`. It
/// reads the event's metadata (its rules version, say) and returns the
/// domain's own error, as a report, when it cannot.
pub type UpcastStep<R> = fn(&Meta, R) -> Result<R, Report>;

/// An upcast chain being built.
pub struct Upcasts<R> {
    kind: String,
    current: u32,
    steps: Vec<(u32, UpcastStep<R>)>,
}

/// A chain of steps that cannot upcast.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChainError {
    /// The domain, named in every error.
    pub kind: String,
    pub problem: ChainProblem,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ChainProblem {
    /// Two steps from the same version.
    Duplicate { from: u32 },
    /// A step from a version at or past the current one.
    BeyondCurrent { from: u32, current: u32 },
    /// No step from this version, though older versions have one.
    Gap { from: u32 },
}

impl<R> Upcasts<R> {
    /// No steps yet, for `kind` (a domain name) at schema `current`.
    pub fn new(kind: &str, current: u32) -> Upcasts<R> {
        Upcasts {
            kind: kind.to_string(),
            current,
            steps: Vec::new(),
        }
    }

    /// Register the step from `from` to `from + 1`.
    pub fn step(mut self, from: u32, step: UpcastStep<R>) -> Upcasts<R> {
        self.steps.push((from, step));
        self
    }

    /// Check the chain is whole: no duplicate, nothing at or past current,
    /// no gap between the oldest step and current.
    pub fn build(self) -> Result<Upcaster<R>, ChainError> {
        todo!("proposal: as annals_yaml::Migrations::build")
    }
}

/// A whole upcast chain, from [`Upcaster::oldest`] to [`Upcaster::current`].
pub struct Upcaster<R> {
    kind: String,
    current: u32,
    oldest: u32,
    steps: Vec<UpcastStep<R>>,
}

/// Why a stored event could not be brought to the current schema.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum UpcastError {
    /// Older than the oldest step: support for it was dropped.
    TooOld { seq: Seq, found: u32, oldest: u32 },
    /// Newer than this code: written by a later version.
    TooNew { seq: Seq, found: u32, current: u32 },
    /// The step from `from` failed for the domain's own reason.
    Step { seq: Seq, from: u32, error: Report },
    /// The upcast form did not decode.
    Decode { seq: Seq, error: Report },
}

impl<R> Upcaster<R> {
    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn current(&self) -> u32 {
        self.current
    }

    pub fn oldest(&self) -> u32 {
        self.oldest
    }

    /// One stored event at the current schema: each step from its recorded
    /// schema up, then `meta.schema` set to current. Seq, payload identity
    /// and every other metadata field kept.
    pub fn upcast(&self, stored: Envelope<R>) -> Result<Envelope<R>, UpcastError> {
        todo!("proposal: run steps[found - oldest..]")
    }

    /// A stored log, upcast and decoded: what [`Stream::replay`] takes.
    /// Never writes anything back.
    pub fn read<E>(
        &self,
        stored: Vec<Envelope<R>>,
        decode: impl FnMut(R) -> Result<E, Report>,
    ) -> Result<Vec<Envelope<E>>, UpcastError> {
        todo!("proposal: upcast, then decode, each event")
    }
}

/// A stored log to a stream: upcast, decode, then replay with each event's
/// own rules.
pub fn replay_stored<D: Domain, R>(
    header: Header,
    stored: Vec<Envelope<R>>,
    upcaster: &Upcaster<R>,
    decode: impl FnMut(R) -> Result<D::Event, Report>,
    rules: &Rulebook<D::Rules>,
) -> Result<Stream<D>, KernelError> {
    todo!("proposal: upcaster.read, then Stream::replay")
}

/// An old log, kept as it was written, and the states it must still replay
/// to.
pub struct ReplayFixture<'a, R, S> {
    /// Named in failures.
    pub name: &'a str,
    pub header: Header,
    /// The log as stored, at whatever schemas it was written.
    pub log: Vec<Envelope<R>>,
    /// The state just after each listed seq.
    pub states: Vec<(Seq, S)>,
}

/// Replay every fixture through the upcaster and the rulebook, and compare
/// each listed state. Every failure is listed, one line each.
pub fn check_replay_fixtures<D, R>(
    upcaster: &Upcaster<R>,
    decode: fn(R) -> Result<D::Event, Report>,
    rules: &Rulebook<D::Rules>,
    fixtures: &[ReplayFixture<'_, R, D::State>],
) -> Result<(), Vec<String>>
where
    D: Domain,
    D::State: PartialEq + Debug,
    R: Clone,
{
    todo!("proposal: replay_stored each fixture, dispatch, compare states")
}

/// [`check_replay_fixtures`], panicking with every failure: for tests.
#[track_caller]
pub fn assert_replay_fixtures<D, R>(
    upcaster: &Upcaster<R>,
    decode: fn(R) -> Result<D::Event, Report>,
    rules: &Rulebook<D::Rules>,
    fixtures: &[ReplayFixture<'_, R, D::State>],
) where
    D: Domain,
    D::State: PartialEq + Debug,
    R: Clone,
{
    if let Err(failures) = check_replay_fixtures::<D, R>(upcaster, decode, rules, fixtures) {
        panic!(
            "{} replay fixtures do not replay:\n{}",
            upcaster.kind(),
            failures.join("\n")
        );
    }
}
