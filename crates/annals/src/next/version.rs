//! The rules (reference data) an event was decided under, by version.
//!
//! A rules version is a short label the domain's author chooses (`"v3"`)
//! and bumps whenever decide, ingest or apply semantics change. The
//! rulebook's current version is registered with the domain
//! ([`Registry::register`](super::Registry::register)) and stamped on every
//! event (`EventMeta.rules_version`). In annals it is **load-bearing**, a
//! documented divergence from angzarr, where it only explains a decision:
//! replay folds each event with the rules its version names, and a
//! snapshot folded under another version is discarded.
//!
//! The version of an event's *payload* is not this: it is the payload's
//! type URL (`pkg.v1.Foo` then `pkg.v2.Foo`), upcast before decoding
//! ([`Upcaster`](super::Upcaster)).

use std::collections::BTreeMap;

/// Every version of a domain's rules a log may name, and the current one,
/// which new commands are decided under.
pub struct Rulebook<R> {
    current: String,
    versions: BTreeMap<String, R>,
}

impl<R> Rulebook<R> {
    /// One version, current.
    pub fn new(version: &str, rules: R) -> Rulebook<R> {
        let mut versions = BTreeMap::new();
        versions.insert(version.to_string(), rules);
        Rulebook {
            current: version.to_string(),
            versions,
        }
    }

    /// Also hold `version`, an older one a log may name. Replaces any rules
    /// already held under that version.
    pub fn with(mut self, version: &str, rules: R) -> Rulebook<R> {
        self.versions.insert(version.to_string(), rules);
        self
    }

    /// The current version and its rules.
    pub fn current(&self) -> (&str, &R) {
        let rules = &self.versions[&self.current];
        (&self.current, rules)
    }

    /// The rules `version` names, if held.
    pub fn get(&self, version: &str) -> Option<&R> {
        self.versions.get(version)
    }
}
