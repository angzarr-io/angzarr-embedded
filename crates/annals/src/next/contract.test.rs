//! The store contract's checks against stores broken on purpose: each check
//! must fail a store with the bug it exists to catch, so a check that checks
//! nothing is caught. (The purge, read and watch sections' own broken stores
//! are in `store.test.rs`.)

use crate::next::contract::{self, block};
use crate::next::v1::{Cover, EventPage, Snapshot, Uuid};
use crate::next::UnitOfWork;
use crate::next::{EventStore, InMemoryStore, InMemoryTx, Ready, Slice, Store, StoreError};

/// What a [`Broken`] store gets wrong.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bug {
    /// None: every check passes it.
    Honest,
    /// Its append takes pages that do not run on from the stream: it
    /// numbers them itself.
    AcceptsAGap,
    /// It keeps a page without its rules version: not as written.
    DropsTheRulesVersion,
    /// It keys a stream by domain and root, so an edition's stream is the
    /// main timeline's.
    IgnoresTheEdition,
    /// It lists a domain's streams by root, then by edition.
    ListsByRoot,
    /// It answers a command's events from every stream of the domain.
    FindsAcrossStreams,
    /// Its unit of work appends straight to the store.
    UnitWritesThrough,
    /// Its unit of work's commit lands nothing.
    CommitLandsNothing,
    /// Its unit of work reads, lists and checks against the store alone,
    /// blind to its own pending appends.
    UnitIsBlind,
    /// Its empty append checks no expected length.
    EmptyAppendUnchecked,
    /// Its empty append creates the stream, holding no events.
    EmptyAppendCreates,
    /// A read past the end gives where the read began as the end.
    ForgetsTheEnd,
    /// Its last instant is the stream's first event's.
    FirstInstant,
    /// It keeps the first snapshot written, not the newest.
    KeepsTheFirstSnapshot,
    /// Its unit of work reads no snapshot.
    UnitSeesNoSnapshot,
    /// Its commit position never moves, whatever is committed.
    PositionStandsStill,
}

/// A store (`S` an [`InMemoryStore`]) or unit of work (an [`InMemoryTx`])
/// with `bug`.
struct Broken<S> {
    inner: S,
    bug: Bug,
    /// The streams an empty append created, under
    /// [`Bug::EmptyAppendCreates`].
    empty: Vec<Cover>,
}

fn broken(bug: Bug) -> Broken<InMemoryStore> {
    Broken {
        inner: InMemoryStore::new(),
        bug,
        empty: Vec::new(),
    }
}

impl<S> Broken<S> {
    /// The stream as this store keys it.
    fn keyed(&self, stream: &Cover) -> Cover {
        let mut stream = stream.clone();
        if self.bug == Bug::IgnoresTheEdition {
            stream.edition = None;
        }
        stream
    }
}

impl<S: EventStore> EventStore for Broken<S> {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        let stream = self.keyed(stream);
        if pages.is_empty() {
            match self.bug {
                Bug::EmptyAppendUnchecked => return Ok(expected),
                Bug::EmptyAppendCreates => self.empty.push(stream.clone()),
                _ => {}
            }
        }
        let mut pages = pages.to_vec();
        for (due, page) in (expected..).zip(&mut pages) {
            if self.bug == Bug::AcceptsAGap {
                page.sequence = due;
            }
            if let (Bug::DropsTheRulesVersion, Some(meta)) = (self.bug, page.meta.as_mut()) {
                meta.rules_version.clear();
            }
        }
        self.inner.append(&stream, expected, &pages)
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        let stream = self.keyed(stream);
        let Some(mut slice) = self.inner.read_from(&stream, from)? else {
            let created = self.empty.contains(&stream);
            return Ok(created.then(|| Slice {
                stream,
                next_sequence: 0,
                last_recorded_at: None,
                pages: Vec::new(),
            }));
        };
        if self.bug == Bug::ForgetsTheEnd && slice.pages.is_empty() {
            slice.next_sequence = from;
        }
        if self.bug == Bug::FirstInstant {
            let whole = self.inner.read_from(&stream, 0)?.unwrap_or(slice.clone());
            let first = whole.pages.first().and_then(|p| p.meta.as_ref());
            slice.last_recorded_at = first.and_then(|m| m.recorded_at);
        }
        Ok(Some(slice))
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let stream = self.keyed(stream);
        if self.bug != Bug::FindsAcrossStreams {
            return self.inner.find_command(&stream, command_id);
        }
        let mut found = Vec::new();
        for each in self.inner.covers(&stream.domain)? {
            found.extend(self.inner.find_command(&each, command_id)?);
        }
        Ok(found)
    }

    fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        let mut covers = self.inner.covers(domain)?;
        covers.extend(self.empty.iter().filter(|c| c.domain == domain).cloned());
        if self.bug == Bug::ListsByRoot {
            covers.sort_by_key(|c| (c.root.clone().map(|r| r.value), c.edition.clone()));
        }
        Ok(covers)
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        let stream = self.keyed(stream);
        self.inner.read_snapshot(&stream)
    }

    fn write_snapshot(&mut self, stream: &Cover, snapshot: &Snapshot) -> Result<(), StoreError> {
        let stream = self.keyed(stream);
        if self.bug == Bug::KeepsTheFirstSnapshot && self.inner.read_snapshot(&stream)?.is_some() {
            return Ok(());
        }
        self.inner.write_snapshot(&stream, snapshot)
    }
}

/// A unit of work that is no unit: it reads the store alone, and its
/// appends go straight to the store (`through`) or wait for commit,
/// checked against the store only.
struct Loose<'s> {
    store: &'s mut InMemoryStore,
    through: bool,
    pending: Vec<(Cover, u64, Vec<EventPage>)>,
}

impl EventStore for Loose<'_> {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        if self.through {
            return self.store.append(stream, expected, pages);
        }
        let length = self
            .store
            .read_from(stream, 0)?
            .map_or(0, |s| s.next_sequence);
        if expected != length {
            return Err(StoreError::Conflict {
                stream: stream.clone(),
                expected,
                actual: length,
            });
        }
        self.pending
            .push((stream.clone(), expected, pages.to_vec()));
        Ok(length + pages.len() as u64)
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        self.store.read_from(stream, from)
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.store.find_command(stream, command_id)
    }

    fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.store.covers(domain)
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        self.store.read_snapshot(stream)
    }
}

/// A [`Broken`] store's unit of work.
enum BrokenUnit<'s> {
    Tx(Broken<InMemoryTx<'s>>),
    Loose(Loose<'s>),
}

impl BrokenUnit<'_> {
    fn store(&mut self) -> &mut dyn EventStore {
        match self {
            BrokenUnit::Tx(tx) => tx,
            BrokenUnit::Loose(loose) => loose,
        }
    }
}

impl EventStore for BrokenUnit<'_> {
    fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        self.store().append(stream, expected, pages)
    }

    fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        self.store().read_from(stream, from)
    }

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.store().find_command(stream, command_id)
    }

    fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.store().covers(domain)
    }

    fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        match self {
            BrokenUnit::Tx(tx) if tx.bug == Bug::UnitSeesNoSnapshot => Ok(None),
            _ => self.store().read_snapshot(stream),
        }
    }
}

impl UnitOfWork for BrokenUnit<'_> {
    fn commit(self) -> Result<(), StoreError> {
        match self {
            BrokenUnit::Tx(tx) if tx.bug == Bug::CommitLandsNothing => Ok(()),
            BrokenUnit::Tx(tx) => tx.inner.commit(),
            BrokenUnit::Loose(Loose { store, pending, .. }) => {
                for (stream, expected, pages) in pending {
                    store.append(&stream, expected, &pages)?;
                }
                Ok(())
            }
        }
    }
}

impl Store for Broken<InMemoryStore> {
    type Unit<'s> = BrokenUnit<'s>;

    fn begin(&mut self) -> BrokenUnit<'_> {
        let loose = |store, through| {
            BrokenUnit::Loose(Loose {
                store,
                through,
                pending: Vec::new(),
            })
        };
        match self.bug {
            Bug::UnitWritesThrough => loose(&mut self.inner, true),
            Bug::UnitIsBlind => loose(&mut self.inner, false),
            bug => BrokenUnit::Tx(Broken {
                empty: self.empty.clone(),
                inner: self.inner.begin(),
                bug,
            }),
        }
    }

    fn commit_position(&mut self) -> Result<u64, StoreError> {
        match self.bug {
            Bug::PositionStandsStill => Ok(7),
            _ => self.inner.commit_position(),
        }
    }
}

/// Whether `check` fails on a fresh store with `bug`.
fn fails<F: std::future::Future>(
    bug: Bug,
    check: impl FnOnce(Ready<Broken<InMemoryStore>>) -> F,
) -> bool {
    let store = Ready(broken(bug));
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block(check(store)))).is_err()
}

/// Require `check` to fail a store with each of `bugs`.
macro_rules! fails_on {
    ($check:ident, $($bug:ident),+ $(,)?) => {
        $(
            assert!(
                fails(Bug::$bug, contract::$check),
                "{} passes a store with {:?}",
                stringify!($check),
                Bug::$bug,
            );
        )+
    };
}

#[test]
fn the_append_check_fails_a_store_that_accepts_a_gap_or_rewrites_a_page() {
    fails_on!(
        append_checks_the_expected_length_and_continuity,
        AcceptsAGap,
        DropsTheRulesVersion,
        FirstInstant,
    );
}

#[test]
fn the_cover_checks_fail_a_store_that_ignores_the_edition_or_lists_by_root() {
    fails_on!(a_cover_is_domain_edition_and_root, IgnoresTheEdition);
    fails_on!(
        covers_orders_main_first_then_by_edition_and_root,
        ListsByRoot
    );
}

#[test]
fn the_unit_listing_check_fails_a_blind_unit_or_one_that_lists_an_empty_append() {
    fails_on!(
        a_unit_of_work_lists_its_pending_streams,
        UnitIsBlind,
        EmptyAppendCreates,
    );
}

#[test]
fn the_find_command_check_fails_a_store_that_finds_across_streams() {
    fails_on!(
        find_command_finds_a_commands_events_on_its_stream,
        FindsAcrossStreams
    );
}

#[test]
fn the_landing_check_fails_a_unit_that_writes_through_or_never_lands() {
    fails_on!(
        a_unit_of_work_lands_at_commit_or_not_at_all,
        UnitWritesThrough,
        CommitLandsNothing,
    );
}

#[test]
fn the_pending_append_check_fails_a_blind_unit() {
    fails_on!(
        a_unit_of_work_checks_and_reads_its_pending_appends,
        UnitIsBlind,
        CommitLandsNothing,
    );
}

#[test]
fn the_empty_append_check_fails_one_that_checks_nothing_or_creates_a_stream() {
    fails_on!(
        an_empty_append_checks_and_creates_nothing,
        EmptyAppendUnchecked,
        EmptyAppendCreates,
    );
}

#[test]
fn the_past_the_end_check_fails_a_store_that_forgets_the_end() {
    fails_on!(
        a_read_past_the_end_is_empty_but_knows_the_end,
        ForgetsTheEnd,
        FirstInstant,
    );
}

#[test]
fn the_snapshot_check_fails_a_store_that_keeps_the_first_or_a_unit_that_sees_none() {
    fails_on!(
        a_store_keeps_the_newest_snapshot_per_stream,
        KeepsTheFirstSnapshot,
        UnitSeesNoSnapshot,
    );
}

#[test]
fn the_commit_position_check_fails_a_position_that_stands_still() {
    fails_on!(
        the_commit_position_rises_with_every_commit,
        PositionStandsStill
    );
}

/// A broken store is broken only where its bug says: with none, it passes
/// every check, so a check that fails it above fails it for its bug.
#[test]
fn a_broken_store_with_no_bug_passes_every_check() {
    use contract::*;
    let fresh = || Ready(broken(Bug::Honest));
    block(append_checks_the_expected_length_and_continuity(fresh()));
    block(a_cover_is_domain_edition_and_root(fresh()));
    block(covers_orders_main_first_then_by_edition_and_root(fresh()));
    block(a_unit_of_work_lists_its_pending_streams(fresh()));
    block(find_command_finds_a_commands_events_on_its_stream(fresh()));
    block(a_unit_of_work_lands_at_commit_or_not_at_all(fresh()));
    block(a_unit_of_work_checks_and_reads_its_pending_appends(fresh()));
    block(an_empty_append_checks_and_creates_nothing(fresh()));
    block(a_read_past_the_end_is_empty_but_knows_the_end(fresh()));
    block(a_store_keeps_the_newest_snapshot_per_stream(fresh()));
    block(the_commit_position_rises_with_every_commit(fresh()));
}
