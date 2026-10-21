//! The SQLite store held to annals' store contract, the same checks
//! InMemoryStore passes, so the kernel behaves alike over either; then what
//! is particular to a store on a file.

use std::sync::atomic::{AtomicUsize, Ordering};

use annals::next::contract::{cover, page};
use annals::next::v1::Snapshot;
use annals::next::{AsyncEventStore, AsyncStore, AsyncUnitOfWork};

use super::SqliteStore;

annals::async_store_contract!(#[tokio::test], SqliteStore::open_in_memory().await.unwrap());

/// A path for a database file of this test run's own.
fn database() -> std::path::PathBuf {
    static RUN: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "annals-store-sql-test-{}-{}.db",
        std::process::id(),
        RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

#[tokio::test]
async fn what_was_committed_survives_reopening_the_file() {
    let path = database();
    let stream = cover("tasks", 1);
    let snapshot = Snapshot {
        sequence: 1,
        state: None,
        rules_version: "v1".to_string(),
    };
    {
        let mut store = SqliteStore::open(&path).await.unwrap();
        let mut unit = store.begin().await;
        unit.append(&stream, 0, &[page(0, 1), page(1, 2)])
            .await
            .unwrap();
        unit.commit().await.unwrap();
        store.write_snapshot(&stream, &snapshot).await.unwrap();
    }
    let mut store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(
        store.read_from(&stream, 0).await.unwrap().unwrap().pages,
        [page(0, 1), page(1, 2)]
    );
    assert_eq!(store.read_snapshot(&stream).await, Ok(Some(snapshot)));
    assert_eq!(store.covers("tasks").await, Ok(vec![stream]));
    drop(store);
    std::fs::remove_file(&path).unwrap();
}

#[tokio::test]
async fn a_dropped_unit_leaves_the_file_as_it_was() {
    let path = database();
    let stream = cover("tasks", 1);
    {
        let mut store = SqliteStore::open(&path).await.unwrap();
        let mut unit = store.begin().await;
        unit.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    }
    let mut store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(store.read_from(&stream, 0).await, Ok(None));
    drop(store);
    std::fs::remove_file(&path).unwrap();
}

#[tokio::test]
async fn reopening_a_migrated_file_migrates_nothing_twice() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    let mut store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
    drop(store);
    std::fs::remove_file(&path).unwrap();
}
