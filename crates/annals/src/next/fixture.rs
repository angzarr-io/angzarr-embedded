//! Golden replay fixtures: an old log, kept exactly as it was stored, and
//! the states it must still replay to. With [`Registry::check`], what CI
//! runs so a missing upcaster or a drifted fold fails a build rather than a
//! replay in production.

use std::fmt::Debug;

use angzarr_proto::v1::{Cover, EventPage};

use super::domain::Domain;
use super::registry::Registry;

/// An old log, kept as it was stored, and the states it must still replay
/// to.
pub struct ReplayFixture<'a, S> {
    /// Named in failures.
    pub name: &'a str,
    pub stream: Cover,
    /// The pages as stored, at whatever types they were written.
    pub log: Vec<EventPage>,
    /// The state just after each listed sequence.
    pub states: Vec<(u64, S)>,
}

/// Replay every fixture through the registry's upcaster and rulebook for
/// `D`, and compare each listed state. Every failure is listed, one line
/// each, naming its fixture (and the type URL, for a page that does not
/// read).
pub fn check_replay_fixtures<D>(
    registry: &Registry,
    fixtures: &[ReplayFixture<'_, D::State>],
) -> Result<(), Vec<String>>
where
    D: Domain,
    D::State: PartialEq + Debug,
{
    todo!("proposal: Stream::replay each fixture, dispatch, compare states")
}

/// [`check_replay_fixtures`], panicking with every failure: for tests.
#[track_caller]
pub fn assert_replay_fixtures<D>(registry: &Registry, fixtures: &[ReplayFixture<'_, D::State>])
where
    D: Domain,
    D::State: PartialEq + Debug,
{
    if let Err(failures) = check_replay_fixtures::<D>(registry, fixtures) {
        panic!(
            "{} replay fixtures do not replay:\n{}",
            D::NAME,
            failures.join("\n")
        );
    }
}
