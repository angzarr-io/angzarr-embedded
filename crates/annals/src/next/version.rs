//! The version of the rules (reference data) an event was decided under.
//!
//! Rules are resolved per event by the version its [`Meta`] records, through
//! a [`Rulebook`]: replay never trusts the caller to pass the right ones.
//!
//! The version of an event's *payload* is not the kernel's: it is the
//! payload's type (a protobuf type URL, `pkg.v1.Foo` then `pkg.v2.Foo`), and
//! the store adapter upcasts old types at read time, before the kernel sees
//! them (`annals-angzarr`).
//!
//! [`Meta`]: super::Meta

use std::collections::BTreeMap;

use super::id::RulesVersion;

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
