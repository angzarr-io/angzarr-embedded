//! The store contract: what every [`Store`] must do, as checks a store's
//! own tests run through [`store_contract!`](crate::store_contract), one
//! `#[test]` per check. [`InMemoryStore`](super::InMemoryStore) is held to
//! it in annals' tests; an application's store adapter is held to the same,
//! so the kernel behaves alike over either.
//!
//! Behind the `contract` feature: a store's crate enables it for its tests
//! (`annals = { ..., features = ["contract"] }` under dev-dependencies).
//!
//! Each check takes a fresh, empty store and panics, naming what failed.
//! The pages are opaque to a store, so the checks write their own: a store
//! never decodes a payload.

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EventMeta, EventPage, Snapshot, Uuid};
use prost_types::{Any, Timestamp};

use super::error::StoreError;
use super::store::{EventStore, Store, UnitOfWork};

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
            recorded_at: Some(Timestamp {
                seconds: sequence as i64,
                nanos: 0,
            }),
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

pub fn append_checks_the_expected_length_and_continuity<S: Store>(mut store: S) {
    let stream = cover("account", 1);
    assert_eq!(store.append(&stream, 0, &[page(0, 1), page(1, 1)]), Ok(2));
    assert_eq!(
        store.append(&stream, 1, &[page(1, 2)]),
        Err(StoreError::Conflict {
            stream: stream.clone(),
            expected: 1,
            actual: 2
        })
    );
    assert_eq!(
        store.append(&stream, 2, &[page(3, 2)]),
        Err(StoreError::Discontinuous {
            stream: stream.clone(),
            expected: 2,
            found: 3
        })
    );
    let slice = store.read_from(&stream, 1).unwrap().unwrap();
    assert_eq!(slice.pages, [page(1, 1)]);
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(
        slice.last_recorded_at,
        Some(page(1, 1).meta.unwrap().recorded_at.unwrap())
    );
    assert_eq!(store.read(&cover("account", 2)).unwrap(), None);
}

pub fn a_cover_is_domain_edition_and_root<S: Store>(mut store: S) {
    store
        .append(&cover("account", 1), 0, &[page(0, 1)])
        .unwrap();
    store.append(&cover("ledger", 1), 0, &[page(0, 1)]).unwrap();
    store
        .append(&cover_in("account", 1, "what-if"), 0, &[page(0, 1)])
        .unwrap();
    assert_eq!(
        store.covers("account"),
        Ok(vec![cover("account", 1), cover_in("account", 1, "what-if")])
    );
    assert_eq!(store.covers("ledger"), Ok(vec![cover("ledger", 1)]));
    assert_eq!(store.covers("nowhere"), Ok(vec![]));
}

pub fn covers_orders_main_first_then_by_edition_and_root<S: Store>(mut store: S) {
    for stream in [
        cover_in("account", 1, "b"),
        cover("account", 2),
        cover_in("account", 2, "a"),
        cover("account", 1),
        cover_in("account", 1, "a"),
    ] {
        store.append(&stream, 0, &[page(0, 1)]).unwrap();
    }
    assert_eq!(
        store.covers("account"),
        Ok(vec![
            cover("account", 1),
            cover("account", 2),
            cover_in("account", 1, "a"),
            cover_in("account", 2, "a"),
            cover_in("account", 1, "b"),
        ])
    );
}

pub fn a_unit_of_work_lists_its_pending_streams<S: Store>(mut store: S) {
    store
        .append(&cover("account", 2), 0, &[page(0, 1)])
        .unwrap();
    let mut unit = store.begin();
    unit.append(&cover("account", 1), 0, &[page(0, 2)]).unwrap();
    unit.append(&cover("account", 3), 0, &[]).unwrap();
    assert_eq!(
        unit.covers("account"),
        Ok(vec![cover("account", 1), cover("account", 2)]),
        "its own append, the store's, and no empty one"
    );
}

pub fn find_command_finds_a_commands_events_on_its_stream<S: Store>(mut store: S) {
    let stream = cover("account", 1);
    store
        .append(&stream, 0, &[page(0, 1), page(1, 2), page(2, 2)])
        .unwrap();
    assert_eq!(
        store.find_command(&stream, &command(2)),
        Ok(vec![page(1, 2), page(2, 2)])
    );
    assert_eq!(store.find_command(&stream, &command(3)), Ok(vec![]));
    assert_eq!(
        store.find_command(&cover("account", 2), &command(2)),
        Ok(vec![]),
        "per stream"
    );
}

pub fn a_unit_of_work_lands_at_commit_or_not_at_all<S: Store>(mut store: S) {
    let stream = cover("account", 1);
    {
        let mut unit = store.begin();
        unit.append(&stream, 0, &[page(0, 1)]).unwrap();
        assert_eq!(
            unit.find_command(&stream, &command(1)),
            Ok(vec![page(0, 1)])
        );
    }
    assert_eq!(store.read(&stream).unwrap(), None, "dropped: rolled back");
    assert_eq!(store.covers("account"), Ok(vec![]), "dropped: no stream");
    let mut unit = store.begin();
    unit.append(&stream, 0, &[page(0, 1)]).unwrap();
    unit.commit().unwrap();
    assert_eq!(store.read(&stream).unwrap().unwrap().pages, [page(0, 1)]);
}

pub fn a_unit_of_work_checks_and_reads_its_pending_appends<S: Store>(mut store: S) {
    let stream = cover("account", 1);
    store.append(&stream, 0, &[page(0, 1)]).unwrap();
    let other = cover("account", 2);
    {
        let mut unit = store.begin();
        assert_eq!(unit.read(&other), Ok(None));
        assert_eq!(unit.append(&stream, 1, &[page(1, 2)]), Ok(2));
        assert_eq!(
            unit.append(&stream, 1, &[page(1, 3)]),
            Err(StoreError::Conflict {
                stream: stream.clone(),
                expected: 1,
                actual: 2
            }),
            "the pending append counts"
        );
        assert_eq!(
            unit.append(&stream, 2, &[page(2, 3), page(4, 3)]),
            Err(StoreError::Discontinuous {
                stream: stream.clone(),
                expected: 3,
                found: 4
            })
        );
        assert_eq!(unit.append(&other, 0, &[page(0, 4)]), Ok(1));
        let slice = unit.read_from(&stream, 1).unwrap().unwrap();
        assert_eq!(slice.pages, [page(1, 2)]);
        assert_eq!(slice.next_sequence, 2);
        assert_eq!(unit.read(&other).unwrap().unwrap().pages, [page(0, 4)]);
        assert_eq!(
            unit.find_command(&stream, &command(2)),
            Ok(vec![page(1, 2)])
        );
        unit.commit().unwrap();
    }
    assert_eq!(store.read(&stream).unwrap().unwrap().next_sequence, 2);
    assert_eq!(store.covers("account"), Ok(vec![stream, other]));
}

pub fn an_empty_append_checks_and_creates_nothing<S: Store>(mut store: S) {
    let stream = cover("account", 1);
    assert_eq!(store.append(&stream, 0, &[]), Ok(0));
    assert_eq!(store.covers("account"), Ok(vec![]));
    assert!(matches!(
        store.append(&stream, 1, &[]),
        Err(StoreError::Conflict { .. })
    ));
    {
        let mut unit = store.begin();
        assert_eq!(unit.append(&stream, 0, &[]), Ok(0));
        assert_eq!(unit.read(&stream), Ok(None));
        unit.commit().unwrap();
    }
    assert_eq!(store.covers("account"), Ok(vec![]));
}

pub fn a_read_past_the_end_is_empty_but_knows_the_end<S: Store>(mut store: S) {
    let stream = cover("account", 1);
    store.append(&stream, 0, &[page(0, 1), page(1, 1)]).unwrap();
    let slice = store.read_from(&stream, 7).unwrap().unwrap();
    assert!(slice.pages.is_empty());
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(
        slice.last_recorded_at,
        Some(page(1, 1).meta.unwrap().recorded_at.unwrap())
    );
    assert_eq!(slice.stream, stream);
}

pub fn a_store_keeps_the_newest_snapshot_per_stream<S: Store>(mut store: S) {
    let stream = cover("account", 1);
    let snapshot = |sequence| Snapshot {
        sequence,
        state: None,
        rules_version: "v1".to_string(),
    };
    assert_eq!(store.read_snapshot(&stream), Ok(None));
    store.write_snapshot(&stream, &snapshot(1)).unwrap();
    store.write_snapshot(&stream, &snapshot(4)).unwrap();
    assert_eq!(store.read_snapshot(&stream), Ok(Some(snapshot(4))));
    assert_eq!(store.read_snapshot(&cover("account", 2)), Ok(None));
    assert_eq!(store.begin().read_snapshot(&stream), Ok(Some(snapshot(4))));
}

/// One `#[test]` per contract check, each on a fresh store from `$fresh`
/// (an expression evaluated once per test).
#[macro_export]
macro_rules! store_contract {
    ($fresh:expr) => {
        $crate::store_contract!(@checks $fresh;
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
    (@checks $fresh:expr; $($check:ident),* $(,)?) => {
        $(
            #[test]
            fn $check() {
                $crate::next::contract::$check($fresh);
            }
        )*
    };
}
