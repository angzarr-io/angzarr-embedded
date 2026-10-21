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
//! Each check takes a fresh, empty store and panics, naming what failed.
//! The pages are opaque to a store, so the checks write their own: a store
//! never decodes a payload.

use std::future::Future;

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EventMeta, EventPage, Snapshot, Uuid};
use prost_types::{Any, Timestamp};

use super::error::StoreError;
use super::store::{AsyncEventStore, AsyncStore, AsyncUnitOfWork};

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

/// One `#[test]` per contract check, each on a fresh synchronous store from
/// `$fresh` (an expression evaluated once per test), run through
/// [`Ready`](crate::next::Ready).
#[macro_export]
macro_rules! store_contract {
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
#[macro_export]
macro_rules! async_store_contract {
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
