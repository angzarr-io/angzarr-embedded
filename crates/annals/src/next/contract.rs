//! The store contract: what every store must do, as checks a store's own
//! tests run, one test per check: [`store_contract!`](crate::store_contract)
//! for a synchronous [`Store`](super::Store),
//! [`async_store_contract!`](crate::async_store_contract) for an
//! [`AsyncStore`](super::AsyncStore). [`InMemoryStore`](super::InMemoryStore)
//! is held to it in annals' tests; an application's store adapter is held to
//! the same, so the kernel behaves alike over either.
//!
//! The checks are written once, over the async port: a synchronous store runs
//! them through [`Ready`](super::Ready), whose futures are ready at once, so
//! [`block`] completes them without a runtime.
//!
//! Behind the `contract` feature: a store's crate enables it for its tests
//! (`annals = { ..., features = ["contract"] }` under dev-dependencies).
//!
//! A store that can purge ([`AsyncPurge`](super::AsyncPurge), or
//! [`Purge`](super::Purge) through `Ready`) also runs the purge checks, an
//! opt-in section: `store_contract!(purge, fresh_store)`,
//! `async_store_contract!(purge, #[tokio::test], fresh_store)`. A store that
//! cannot purge runs only the rest.
//!
//! A store that opens read-only units of work
//! ([`AsyncReadStore`](super::AsyncReadStore), or
//! [`ReadStore`](super::ReadStore) through `Ready`) also runs the read
//! checks, another opt-in section. Each takes two handles on one fresh,
//! empty store, a reader and a writer (`(reader, writer)`), and writes
//! through the writer while the reader's unit is open, between its reads,
//! so a unit that does not read one moment is caught rather than lucky.
//! `store_contract!(read, fresh_pair)` runs them on a synchronous store; an
//! async store calls each check itself with its pair (as annals-store-sql's
//! tests do), so it can remove whatever the pair made once the check is
//! done: two handles on one store are two connections to one database,
//! which a test makes on a file.
//!
//! Each check takes a fresh, empty store and panics, naming what failed.
//! The pages are opaque to a store, so the checks write their own: a store
//! never decodes a payload.

use std::future::Future;

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EventMeta, EventPage, Snapshot, Uuid};
use prost_types::{Any, Timestamp};

use super::error::StoreError;
use super::store::{
    AsyncEventStore, AsyncPurge, AsyncReadStore, AsyncReadUnit, AsyncStore, AsyncUnitOfWork,
};

/// Complete a future whose every await is ready at once: a check run over a
/// synchronous store through [`Ready`](super::Ready).
pub fn block<F: Future>(future: F) -> F::Output {
    super::store::block(future)
}

/// The event at `sequence`, produced by command `command`.
pub fn page(sequence: u64, command: u8) -> EventPage {
    let id = Uuid::from_bytes([command; 16]);
    EventPage {
        payload: Some(Any {
            type_url: "/annals.contract.v1.Page".to_string(),
            value: sequence.to_be_bytes().to_vec(),
        }),
        meta: Some(EventMeta {
            causation: Some(Causation::CommandId(id.clone())),
            correlation_id: Some(id),
            recorded_at: Some(at(sequence)),
            rules_version: "v1".to_string(),
            ..EventMeta::default()
        }),
        sequence,
    }
}

/// `domain`'s stream `n` on the main timeline.
pub fn cover(domain: &str, n: u8) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(Uuid::from_bytes([n; 16])),
        edition: None,
    }
}

/// `domain`'s stream `n` in `edition`.
pub fn cover_in(domain: &str, n: u8, edition: &str) -> Cover {
    Cover {
        edition: Some(edition.to_string()),
        ..cover(domain, n)
    }
}

fn command(n: u8) -> Uuid {
    Uuid::from_bytes([n; 16])
}

/// The instant `page(sequence, _)` records.
fn at(sequence: u64) -> Timestamp {
    Timestamp {
        seconds: sequence as i64,
        nanos: 0,
    }
}

pub async fn append_checks_the_expected_length_and_continuity<S: AsyncStore>(mut store: S) {
    let stream = cover("account", 1);
    assert_eq!(
        store.append(&stream, 0, &[page(0, 1), page(1, 1)]).await,
        Ok(2)
    );
    assert_eq!(
        store.append(&stream, 1, &[page(1, 2)]).await,
        Err(StoreError::Conflict {
            stream: stream.clone(),
            expected: 1,
            actual: 2
        })
    );
    assert_eq!(
        store.append(&stream, 2, &[page(3, 2)]).await,
        Err(StoreError::Discontinuous {
            stream: stream.clone(),
            expected: 2,
            found: 3
        })
    );
    let slice = store.read_from(&stream, 1).await.unwrap().unwrap();
    assert_eq!(slice.pages, [page(1, 1)]);
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(slice.last_recorded_at, Some(at(1)));
    assert_eq!(store.read_from(&cover("account", 2), 0).await, Ok(None));
}

pub async fn a_cover_is_domain_edition_and_root<S: AsyncStore>(mut store: S) {
    store
        .append(&cover("account", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    store
        .append(&cover("ledger", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    store
        .append(&cover_in("account", 1, "what-if"), 0, &[page(0, 1)])
        .await
        .unwrap();
    assert_eq!(
        store.covers("account").await,
        Ok(vec![cover("account", 1), cover_in("account", 1, "what-if")])
    );
    assert_eq!(store.covers("ledger").await, Ok(vec![cover("ledger", 1)]));
    assert_eq!(store.covers("nowhere").await, Ok(vec![]));
}

pub async fn covers_orders_main_first_then_by_edition_and_root<S: AsyncStore>(mut store: S) {
    for stream in [
        cover_in("account", 1, "b"),
        cover("account", 2),
        cover_in("account", 2, "a"),
        cover("account", 1),
        cover_in("account", 1, "a"),
    ] {
        store.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    }
    assert_eq!(
        store.covers("account").await,
        Ok(vec![
            cover("account", 1),
            cover("account", 2),
            cover_in("account", 1, "a"),
            cover_in("account", 2, "a"),
            cover_in("account", 1, "b"),
        ])
    );
}

pub async fn a_unit_of_work_lists_its_pending_streams<S: AsyncStore>(mut store: S) {
    store
        .append(&cover("account", 2), 0, &[page(0, 1)])
        .await
        .unwrap();
    let mut unit = store.begin().await;
    unit.append(&cover("account", 1), 0, &[page(0, 2)])
        .await
        .unwrap();
    unit.append(&cover("account", 3), 0, &[]).await.unwrap();
    assert_eq!(
        unit.covers("account").await,
        Ok(vec![cover("account", 1), cover("account", 2)]),
        "its own append, the store's, and no empty one"
    );
}

pub async fn find_command_finds_a_commands_events_on_its_stream<S: AsyncStore>(mut store: S) {
    let stream = cover("account", 1);
    store
        .append(&stream, 0, &[page(0, 1), page(1, 2), page(2, 2)])
        .await
        .unwrap();
    assert_eq!(
        store.find_command(&stream, &command(2)).await,
        Ok(vec![page(1, 2), page(2, 2)])
    );
    assert_eq!(store.find_command(&stream, &command(3)).await, Ok(vec![]));
    assert_eq!(
        store.find_command(&cover("account", 2), &command(2)).await,
        Ok(vec![]),
        "per stream"
    );
}

pub async fn a_unit_of_work_lands_at_commit_or_not_at_all<S: AsyncStore>(mut store: S) {
    let stream = cover("account", 1);
    {
        let mut unit = store.begin().await;
        unit.append(&stream, 0, &[page(0, 1)]).await.unwrap();
        assert_eq!(
            unit.find_command(&stream, &command(1)).await,
            Ok(vec![page(0, 1)])
        );
    }
    assert_eq!(
        store.read_from(&stream, 0).await,
        Ok(None),
        "dropped: rolled back"
    );
    assert_eq!(
        store.covers("account").await,
        Ok(vec![]),
        "dropped: no stream"
    );
    let mut unit = store.begin().await;
    unit.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    unit.commit().await.unwrap();
    assert_eq!(
        store.read_from(&stream, 0).await.unwrap().unwrap().pages,
        [page(0, 1)]
    );
}

pub async fn a_unit_of_work_checks_and_reads_its_pending_appends<S: AsyncStore>(mut store: S) {
    let stream = cover("account", 1);
    store.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    let other = cover("account", 2);
    {
        let mut unit = store.begin().await;
        assert_eq!(unit.read_from(&other, 0).await, Ok(None));
        assert_eq!(unit.append(&stream, 1, &[page(1, 2)]).await, Ok(2));
        assert_eq!(
            unit.append(&stream, 1, &[page(1, 3)]).await,
            Err(StoreError::Conflict {
                stream: stream.clone(),
                expected: 1,
                actual: 2
            }),
            "the pending append counts"
        );
        assert_eq!(
            unit.append(&stream, 2, &[page(2, 3), page(4, 3)]).await,
            Err(StoreError::Discontinuous {
                stream: stream.clone(),
                expected: 3,
                found: 4
            })
        );
        assert_eq!(unit.append(&other, 0, &[page(0, 4)]).await, Ok(1));
        let slice = unit.read_from(&stream, 1).await.unwrap().unwrap();
        assert_eq!(slice.pages, [page(1, 2)]);
        assert_eq!(slice.next_sequence, 2);
        assert_eq!(
            unit.read_from(&other, 0).await.unwrap().unwrap().pages,
            [page(0, 4)]
        );
        assert_eq!(
            unit.find_command(&stream, &command(2)).await,
            Ok(vec![page(1, 2)])
        );
        unit.commit().await.unwrap();
    }
    assert_eq!(
        store
            .read_from(&stream, 0)
            .await
            .unwrap()
            .unwrap()
            .next_sequence,
        2
    );
    assert_eq!(store.covers("account").await, Ok(vec![stream, other]));
}

pub async fn an_empty_append_checks_and_creates_nothing<S: AsyncStore>(mut store: S) {
    let stream = cover("account", 1);
    assert_eq!(store.append(&stream, 0, &[]).await, Ok(0));
    assert_eq!(store.covers("account").await, Ok(vec![]));
    assert!(matches!(
        store.append(&stream, 1, &[]).await,
        Err(StoreError::Conflict { .. })
    ));
    {
        let mut unit = store.begin().await;
        assert_eq!(unit.append(&stream, 0, &[]).await, Ok(0));
        assert_eq!(unit.read_from(&stream, 0).await, Ok(None));
        unit.commit().await.unwrap();
    }
    assert_eq!(store.covers("account").await, Ok(vec![]));
}

pub async fn a_read_past_the_end_is_empty_but_knows_the_end<S: AsyncStore>(mut store: S) {
    let stream = cover("account", 1);
    store
        .append(&stream, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    let slice = store.read_from(&stream, 7).await.unwrap().unwrap();
    assert!(slice.pages.is_empty());
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(slice.last_recorded_at, Some(at(1)));
    assert_eq!(slice.stream, stream);
}

pub async fn a_store_keeps_the_newest_snapshot_per_stream<S: AsyncStore>(mut store: S) {
    let stream = cover("account", 1);
    let snapshot = |sequence| Snapshot {
        sequence,
        state: None,
        rules_version: "v1".to_string(),
    };
    assert_eq!(store.read_snapshot(&stream).await, Ok(None));
    store.write_snapshot(&stream, &snapshot(1)).await.unwrap();
    store.write_snapshot(&stream, &snapshot(4)).await.unwrap();
    assert_eq!(store.read_snapshot(&stream).await, Ok(Some(snapshot(4))));
    assert_eq!(store.read_snapshot(&cover("account", 2)).await, Ok(None));
    let mut unit = store.begin().await;
    assert_eq!(unit.read_snapshot(&stream).await, Ok(Some(snapshot(4))));
}

// ----------------------------------------------------------- purge (opt-in)

fn snapshot(sequence: u64) -> Snapshot {
    Snapshot {
        sequence,
        state: None,
        rules_version: "v1".to_string(),
    }
}

/// Whether `stream` reads as never written: no pages, no snapshot, no
/// command's events, and in no listing of its domain.
async fn assert_gone<S: AsyncEventStore>(store: &mut S, stream: &Cover, what: &str) {
    assert_eq!(store.read_from(stream, 0).await, Ok(None), "{what}: read");
    assert_eq!(
        store.read_snapshot(stream).await,
        Ok(None),
        "{what}: snapshot"
    );
    assert_eq!(
        store.find_command(stream, &command(1)).await,
        Ok(vec![]),
        "{what}: command ids"
    );
    let covers = store.covers(&stream.domain).await.unwrap();
    assert!(!covers.contains(stream), "{what}: covers {covers:?}");
}

pub async fn a_purge_deletes_a_streams_events_snapshot_and_command_ids<S>(mut store: S)
where
    S: AsyncStore + AsyncPurge,
{
    let purged = cover("account", 1);
    let kept = cover("account", 2);
    for stream in [&purged, &kept] {
        store
            .append(stream, 0, &[page(0, 1), page(1, 2)])
            .await
            .unwrap();
        store.write_snapshot(stream, &snapshot(1)).await.unwrap();
    }
    assert_eq!(store.purge(&purged).await, Ok(2), "its two events");
    assert_gone(&mut store, &purged, "purged").await;
    assert_eq!(store.covers("account").await, Ok(vec![kept.clone()]));
    assert_eq!(
        store.read_from(&kept, 0).await.unwrap().unwrap().pages,
        [page(0, 1), page(1, 2)],
        "another stream untouched"
    );
    assert_eq!(
        store
            .read_snapshot(&kept)
            .await
            .map(|s| s.map(|s| s.sequence)),
        Ok(Some(1))
    );
    assert_eq!(
        store.find_command(&kept, &command(1)).await,
        Ok(vec![page(0, 1)])
    );
    assert_eq!(store.purge(&purged).await, Ok(0), "nothing left to purge");
    assert_eq!(store.purge(&cover("account", 3)).await, Ok(0), "no stream");
}

pub async fn a_purge_deletes_a_snapshot_with_no_events<S>(mut store: S)
where
    S: AsyncStore + AsyncPurge,
{
    let stream = cover("account", 1);
    store.write_snapshot(&stream, &snapshot(4)).await.unwrap();
    assert_eq!(store.purge(&stream).await, Ok(0));
    assert_eq!(store.read_snapshot(&stream).await, Ok(None));
}

pub async fn a_purged_stream_starts_again_from_nothing<S>(mut store: S)
where
    S: AsyncStore + AsyncPurge,
{
    // An append that raced the purge and lost, then one made after it: the
    // stale writer is refused, and the stream begins again at sequence 0,
    // its old command ids forgotten.
    let stream = cover("account", 1);
    store
        .append(&stream, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    store.purge(&stream).await.unwrap();
    assert_eq!(
        store.append(&stream, 2, &[page(2, 2)]).await,
        Err(StoreError::Conflict {
            stream: stream.clone(),
            expected: 2,
            actual: 0
        })
    );
    assert_gone(&mut store, &stream, "after the refused append").await;
    assert_eq!(store.append(&stream, 0, &[page(0, 3)]).await, Ok(1));
    let slice = store.read_from(&stream, 0).await.unwrap().unwrap();
    assert_eq!(slice.pages, [page(0, 3)]);
    assert_eq!(slice.next_sequence, 1);
    assert_eq!(slice.last_recorded_at, Some(at(0)));
    assert_eq!(store.find_command(&stream, &command(1)).await, Ok(vec![]));
    assert_eq!(store.covers("account").await, Ok(vec![stream]));
}

pub async fn a_purge_in_a_unit_of_work_lands_at_commit_or_not_at_all<S>(mut store: S)
where
    S: AsyncStore + AsyncPurge,
    for<'s> S::Unit<'s>: AsyncPurge,
{
    let stream = cover("account", 1);
    store
        .append(&stream, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    store.write_snapshot(&stream, &snapshot(1)).await.unwrap();
    {
        let mut unit = store.begin().await;
        assert_eq!(unit.purge(&stream).await, Ok(2));
        assert_gone(&mut unit, &stream, "inside the unit").await;
    }
    assert_eq!(
        store.read_from(&stream, 0).await.unwrap().unwrap().pages,
        [page(0, 1), page(1, 1)],
        "dropped: nothing deleted"
    );
    assert_eq!(store.read_snapshot(&stream).await, Ok(Some(snapshot(1))));
    assert_eq!(
        store.find_command(&stream, &command(1)).await,
        Ok(vec![page(0, 1), page(1, 1)])
    );
    assert_eq!(store.covers("account").await, Ok(vec![stream.clone()]));
    let mut unit = store.begin().await;
    unit.purge(&stream).await.unwrap();
    unit.commit().await.unwrap();
    assert_gone(&mut store, &stream, "committed").await;
}

pub async fn a_unit_of_work_purges_its_own_appends_and_appends_after<S>(mut store: S)
where
    S: AsyncStore + AsyncPurge,
    for<'s> S::Unit<'s>: AsyncPurge,
{
    let stream = cover("account", 1);
    let other = cover("account", 2);
    store.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    store.write_snapshot(&stream, &snapshot(0)).await.unwrap();
    let mut unit = store.begin().await;
    unit.append(&stream, 1, &[page(1, 2)]).await.unwrap();
    unit.append(&other, 0, &[page(0, 2)]).await.unwrap();
    assert_eq!(unit.purge(&stream).await, Ok(2), "stored and pending");
    assert_eq!(unit.purge(&other).await, Ok(1), "only pending");
    assert_eq!(unit.covers("account").await, Ok(vec![]));
    assert_eq!(
        unit.append(&stream, 1, &[page(1, 3)]).await,
        Err(StoreError::Conflict {
            stream: stream.clone(),
            expected: 1,
            actual: 0
        })
    );
    assert_eq!(unit.append(&stream, 0, &[page(0, 3)]).await, Ok(1));
    assert_eq!(
        unit.read_from(&stream, 0).await.unwrap().unwrap().pages,
        [page(0, 3)]
    );
    assert_eq!(unit.read_snapshot(&stream).await, Ok(None));
    unit.commit().await.unwrap();
    let slice = store.read_from(&stream, 0).await.unwrap().unwrap();
    assert_eq!(slice.pages, [page(0, 3)]);
    assert_eq!(slice.next_sequence, 1);
    assert_eq!(store.read_snapshot(&stream).await, Ok(None));
    assert_eq!(store.find_command(&stream, &command(2)).await, Ok(vec![]));
    assert_eq!(store.read_from(&other, 0).await, Ok(None));
    assert_eq!(store.covers("account").await, Ok(vec![stream]));
}

// ------------------------------------------------------------ read (opt-in)

pub async fn a_read_unit_sees_the_store_as_it_began<R, W>((mut reader, mut writer): (R, W))
where
    R: AsyncReadStore,
    W: AsyncEventStore,
{
    let early = cover("account", 1);
    let late = cover("account", 2);
    writer.append(&early, 0, &[page(0, 1)]).await.unwrap();
    let mut reads = reader.begin_read().await;
    // Written after the unit began, before its first read.
    writer.append(&early, 1, &[page(1, 2)]).await.unwrap();
    writer.append(&late, 0, &[page(0, 2)]).await.unwrap();
    assert_eq!(
        reads.covers("account").await,
        Ok(vec![early.clone()]),
        "covers as the unit began"
    );
    // Written between the unit's reads.
    writer.append(&early, 2, &[page(2, 3)]).await.unwrap();
    let slice = reads.read_from(&early, 0).await.unwrap().unwrap();
    assert_eq!(slice.pages, [page(0, 1)], "pages as the unit began");
    assert_eq!(slice.next_sequence, 1);
    assert_eq!(slice.last_recorded_at, Some(at(0)));
    let tail = reads.read_from(&early, 1).await.unwrap().unwrap();
    assert_eq!((tail.next_sequence, tail.pages), (1, vec![]), "its end");
    assert_eq!(
        reads.read_from(&late, 0).await,
        Ok(None),
        "a stream begun after"
    );
    drop(reads);
    let mut later = reader.begin_read().await;
    assert_eq!(later.covers("account").await, Ok(vec![early.clone(), late]));
    let slice = later.read_from(&early, 0).await.unwrap().unwrap();
    assert_eq!(slice.pages, [page(0, 1), page(1, 2), page(2, 3)]);
    assert_eq!(slice.last_recorded_at, Some(at(2)));
}

pub async fn a_read_unit_keeps_a_stream_purged_after_it_began<R, W>(
    (mut reader, mut writer): (R, W),
) where
    R: AsyncReadStore,
    W: AsyncPurge,
{
    let purged = cover("account", 1);
    let kept = cover("account", 2);
    writer
        .append(&purged, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    writer.append(&kept, 0, &[page(0, 3)]).await.unwrap();
    let mut reads = reader.begin_read().await;
    let listed = reads.covers("account").await.unwrap();
    assert_eq!(listed, [purged.clone(), kept.clone()]);
    // Purged between the listing and the reads of what it listed.
    assert_eq!(writer.purge(&purged).await, Ok(2));
    let slice = reads.read_from(&purged, 0).await.unwrap().unwrap();
    assert_eq!(
        slice.pages,
        [page(0, 1), page(1, 2)],
        "whole, never a stream holding none of its events"
    );
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(slice.last_recorded_at, Some(at(1)));
    assert_eq!(reads.covers("account").await, Ok(listed), "listed still");
    drop(reads);
    let mut later = reader.begin_read().await;
    assert_eq!(later.covers("account").await, Ok(vec![kept]));
    assert_eq!(later.read_from(&purged, 0).await, Ok(None));
}

/// One `#[test]` per contract check, each on a fresh synchronous store from
/// `$fresh` (an expression evaluated once per test), run through
/// [`Ready`](crate::next::Ready). `store_contract!(purge, $fresh)` runs the
/// purge checks instead, for a store and unit of work that implement
/// [`Purge`](crate::next::Purge): a store that purges invokes both.
/// `store_contract!(read, $pair)` runs the read checks, for a store that
/// implements [`ReadStore`](crate::next::ReadStore), on a fresh
/// `(reader, writer)` pair from `$pair`: two handles on one empty store.
#[macro_export]
macro_rules! store_contract {
    (read, $pair:expr) => {
        $crate::store_contract!(@pair $pair;
            a_read_unit_sees_the_store_as_it_began,
            a_read_unit_keeps_a_stream_purged_after_it_began,
        );
    };
    (purge, $fresh:expr) => {
        $crate::store_contract!(@each $fresh;
            a_purge_deletes_a_streams_events_snapshot_and_command_ids,
            a_purge_deletes_a_snapshot_with_no_events,
            a_purged_stream_starts_again_from_nothing,
            a_purge_in_a_unit_of_work_lands_at_commit_or_not_at_all,
            a_unit_of_work_purges_its_own_appends_and_appends_after,
        );
    };
    ($fresh:expr) => {
        $crate::store_contract!(@each $fresh;
            append_checks_the_expected_length_and_continuity,
            a_cover_is_domain_edition_and_root,
            covers_orders_main_first_then_by_edition_and_root,
            a_unit_of_work_lists_its_pending_streams,
            find_command_finds_a_commands_events_on_its_stream,
            a_unit_of_work_lands_at_commit_or_not_at_all,
            a_unit_of_work_checks_and_reads_its_pending_appends,
            an_empty_append_checks_and_creates_nothing,
            a_read_past_the_end_is_empty_but_knows_the_end,
            a_store_keeps_the_newest_snapshot_per_stream,
        );
    };
    (@pair $pair:expr; $($check:ident),* $(,)?) => {
        $(
            #[test]
            fn $check() {
                let (reader, writer) = $pair;
                $crate::next::contract::block($crate::next::contract::$check((
                    $crate::next::Ready(reader),
                    $crate::next::Ready(writer),
                )));
            }
        )*
    };
    (@each $fresh:expr; $($check:ident),* $(,)?) => {
        $(
            #[test]
            fn $check() {
                $crate::next::contract::block($crate::next::contract::$check(
                    $crate::next::Ready($fresh),
                ));
            }
        )*
    };
}

/// One test per contract check for an [`AsyncStore`](crate::next::AsyncStore),
/// each an `async fn` under the test attribute given (the caller's runtime:
/// `#[tokio::test]`), on a fresh store from `$fresh`, which may `.await`.
/// `async_store_contract!(purge, #[$test], $fresh)` runs the purge checks
/// instead, for a store and unit of work that implement
/// [`AsyncPurge`](crate::next::AsyncPurge): a store that purges invokes both.
#[macro_export]
macro_rules! async_store_contract {
    (purge, #[$test:meta], $fresh:expr) => {
        $crate::async_store_contract!(@each #[$test], $fresh;
            a_purge_deletes_a_streams_events_snapshot_and_command_ids,
            a_purge_deletes_a_snapshot_with_no_events,
            a_purged_stream_starts_again_from_nothing,
            a_purge_in_a_unit_of_work_lands_at_commit_or_not_at_all,
            a_unit_of_work_purges_its_own_appends_and_appends_after,
        );
    };
    (#[$test:meta], $fresh:expr) => {
        $crate::async_store_contract!(@each #[$test], $fresh;
            append_checks_the_expected_length_and_continuity,
            a_cover_is_domain_edition_and_root,
            covers_orders_main_first_then_by_edition_and_root,
            a_unit_of_work_lists_its_pending_streams,
            find_command_finds_a_commands_events_on_its_stream,
            a_unit_of_work_lands_at_commit_or_not_at_all,
            a_unit_of_work_checks_and_reads_its_pending_appends,
            an_empty_append_checks_and_creates_nothing,
            a_read_past_the_end_is_empty_but_knows_the_end,
            a_store_keeps_the_newest_snapshot_per_stream,
        );
    };
    (@each #[$test:meta], $fresh:expr; $($check:ident),* $(,)?) => {
        $(
            #[$test]
            async fn $check() {
                $crate::next::contract::$check($fresh).await;
            }
        )*
    };
}
