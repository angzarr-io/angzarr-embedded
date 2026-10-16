//! Golden replay fixtures: an old log, kept exactly as it was stored, and
//! the states it must still replay to. With [`Registry::check`], what CI
//! runs so a missing upcaster or a drifted fold fails a build rather than
//! a replay in production.

use std::fmt::Debug;

use annals::next::{Domain, Envelope, Rulebook, Seq, StreamKey};

use crate::payload::Payload;
use crate::registry::Registry;
use crate::store::Stored;

/// An old log, kept as it was stored, and the states it must still replay
/// to.
pub struct ReplayFixture<'a, S> {
    /// Named in failures.
    pub name: &'a str,
    pub key: StreamKey,
    /// The rows as stored, at whatever types they were written.
    pub log: Vec<Envelope<Stored>>,
    /// The state just after each listed seq.
    pub states: Vec<(Seq, S)>,
}

/// Replay every fixture through the registry's upcasters and the rulebook,
/// and compare each listed state. Every failure is listed, one line each,
/// naming its fixture (and the type URL, for one that does not read).
pub fn check_replay_fixtures<D>(
    registry: &Registry,
    rules: &Rulebook<D::Rules>,
    fixtures: &[ReplayFixture<'_, D::State>],
) -> Result<(), Vec<String>>
where
    D: Domain,
    D::Event: Payload,
    D::State: PartialEq + Debug,
{
    todo!("proposal: replay_stored each fixture, dispatch, compare states")
}

/// [`check_replay_fixtures`], panicking with every failure: for tests.
#[track_caller]
pub fn assert_replay_fixtures<D>(
    registry: &Registry,
    rules: &Rulebook<D::Rules>,
    fixtures: &[ReplayFixture<'_, D::State>],
) where
    D: Domain,
    D::Event: Payload,
    D::State: PartialEq + Debug,
{
    if let Err(failures) = check_replay_fixtures::<D>(registry, rules, fixtures) {
        panic!(
            "{} replay fixtures do not replay:\n{}",
            D::NAME,
            failures.join("\n")
        );
    }
}
