//! The SQLite store held to annals' store contract, the same checks
//! InMemoryStore passes, so the kernel behaves alike over either; then what
//! is particular to a store on a file.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use annals::next::contract::{cover, page};
use annals::next::v1::{Cover, Snapshot};
use annals::next::{AsyncEventStore, AsyncStore, AsyncUnitOfWork, StoreError};
use annals_diagnostic::Diagnostic;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};

use super::{SqliteStore, MIGRATOR};

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

/// Remove the database at `path`, and its WAL files if a connection is yet
/// to let them go.
fn remove(path: &Path) {
    std::fs::remove_file(path).unwrap();
    for suffix in ["-wal", "-shm"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        match std::fs::remove_file(&file) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            other => other.unwrap(),
        }
    }
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
    remove(&path);
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
    remove(&path);
}

#[tokio::test]
async fn reopening_a_migrated_file_migrates_nothing_twice() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    let mut store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
    drop(store);
    remove(&path);
}

#[tokio::test]
async fn stores_opening_a_new_file_at_once_all_open_it_migrated() {
    // Processes racing to open a project's database for the first time:
    // each connection is its own SQLite handle on its own thread, as each
    // process's is. Every one must open it, migrated once.
    for _ in 0..5 {
        let path = database();
        let open = || async { SqliteStore::open(&path).await.map(drop) };
        let opened = tokio::join!(
            open(),
            open(),
            open(),
            open(),
            open(),
            open(),
            open(),
            open()
        );
        let opened = [
            opened.0, opened.1, opened.2, opened.3, opened.4, opened.5, opened.6, opened.7,
        ];
        assert_eq!(opened, [const { Ok(()) }; 8]);
        assert_eq!(journal_mode(&path).await, "wal");
        let mut store = SqliteStore::open(&path).await.unwrap();
        assert_eq!(store.covers("tasks").await, Ok(vec![]));
        drop(store);
        remove(&path);
    }
}

// ------------------------------------------------- opening, by those who read

/// What a reader that answers empty for a store never written does: `list`
/// on a project with no database.
async fn tasks_in(path: &Path) -> Result<Vec<Cover>, StoreError> {
    match SqliteStore::open_existing(path).await? {
        None => Ok(vec![]),
        Some(mut store) => store.covers("tasks").await,
    }
}

/// The code of the report `error` carries, if it is the store's own failure.
fn code(error: &StoreError) -> Option<&'static str> {
    match error {
        StoreError::Unavailable(report) => Some(report.code()),
        _ => None,
    }
}

/// A connection of the test's own, under the store's feet.
async fn raw(path: &Path) -> SqliteConnection {
    SqliteConnectOptions::new()
        .filename(path)
        .connect()
        .await
        .unwrap()
}

async fn journal_mode(path: &Path) -> String {
    let mut conn = raw(path).await;
    sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut conn)
        .await
        .unwrap()
}

/// A migrated database in rollback (`DELETE`) journal mode, as a store
/// opened before WAL left it.
async fn migrated_in_rollback_mode(path: &Path) {
    let mut conn = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    MIGRATOR.run(&mut conn).await.unwrap();
    conn.close().await.unwrap();
    assert_eq!(journal_mode(path).await, "delete");
}

#[tokio::test]
async fn reading_a_store_never_written_finds_nothing_and_creates_nothing() {
    let path = database();
    assert!(matches!(SqliteStore::open_existing(&path).await, Ok(None)));
    assert_eq!(tasks_in(&path).await, Ok(vec![]));
    assert!(!path.exists());
    let nested = database().join("no-such-directory").join("state.db");
    assert!(matches!(
        SqliteStore::open_existing(&nested).await,
        Ok(None)
    ));
}

#[tokio::test]
async fn reading_what_is_there_but_no_database_fails() {
    let dir = database();
    std::fs::create_dir(&dir).unwrap();
    let opened = SqliteStore::open_existing(&dir).await.map(drop);
    std::fs::remove_dir(&dir).unwrap();
    assert_eq!(
        opened.map_err(|e| code(&e)),
        Err(Some("annals-store-sql-unavailable"))
    );
}

#[tokio::test]
async fn reading_finds_what_was_written() {
    let path = database();
    let stream = cover("tasks", 1);
    {
        let mut store = SqliteStore::open(&path).await.unwrap();
        store.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    }
    assert_eq!(tasks_in(&path).await, Ok(vec![stream]));
    remove(&path);
}

#[tokio::test]
async fn a_read_only_file_in_a_read_only_directory_opens_for_reading() {
    use std::os::unix::fs::PermissionsExt;
    let dir = database();
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.db");
    let stream = cover("tasks", 1);
    {
        let mut store = SqliteStore::open(&path).await.unwrap();
        store.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    }
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap()
    };
    mode(&path, 0o444);
    mode(&dir, 0o555);
    let mut store = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let read = store.read_from(&stream, 0).await;
    let written = store.append(&stream, 1, &[page(1, 2)]).await;
    drop(store);
    mode(&dir, 0o755);
    mode(&path, 0o644);
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(read.unwrap().unwrap().pages, [page(0, 1)]);
    assert_eq!(
        written.as_ref().map_err(code),
        Err(Some("annals-store-sql-unavailable"))
    );
}

#[tokio::test]
async fn reading_an_unmigrated_file_fails_and_leaves_it_unmigrated() {
    let path = database();
    std::fs::write(&path, b"").unwrap();
    let opened = SqliteStore::open_existing(&path).await;
    assert_eq!(
        opened.as_ref().err().and_then(code),
        Some("annals-store-sql-unmigrated")
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    remove(&path);
}

/// Each way a database's applied migrations can disagree with this build's,
/// which sqlx's migrator refuses: neither open passes it by.
#[tokio::test]
async fn a_database_whose_migrations_disagree_opens_neither_way() {
    for tamper in [
        "UPDATE _sqlx_migrations SET success = FALSE",
        "UPDATE _sqlx_migrations SET checksum = x'00'",
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
         VALUES (9999, 'from a newer build', TRUE, x'00', 0)",
    ] {
        let path = database();
        drop(SqliteStore::open(&path).await.unwrap());
        let mut conn = raw(&path).await;
        sqlx::query(tamper).execute(&mut conn).await.unwrap();
        conn.close().await.unwrap();
        let written = SqliteStore::open(&path).await.map(drop);
        let read = SqliteStore::open_existing(&path).await.map(drop);
        remove(&path);
        assert_eq!(
            written.map_err(|e| code(&e)),
            Err(Some("annals-store-sql-unavailable")),
            "{tamper}"
        );
        assert_eq!(
            read.map_err(|e| code(&e)),
            Err(Some("annals-store-sql-unavailable")),
            "{tamper}"
        );
    }
}

#[tokio::test]
async fn a_store_opens_and_reads_while_another_holds_the_write_lock() {
    // No open takes the write lock once the file is migrated, so neither
    // waits on a writer; WAL lets both read past its uncommitted write.
    let path = database();
    let stream = cover("tasks", 1);
    let mut writer = SqliteStore::open(&path).await.unwrap();
    let mut unit = writer.begin().await;
    unit.append(&stream, 0, &[page(0, 1)]).await.unwrap();

    let mut reader = SqliteStore::open(&path).await.unwrap();
    assert_eq!(reader.read_from(&stream, 0).await, Ok(None));
    let mut read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    assert_eq!(read_only.read_from(&stream, 0).await, Ok(None));

    unit.commit().await.unwrap();
    assert_eq!(reader.covers("tasks").await, Ok(vec![stream.clone()]));
    assert_eq!(read_only.covers("tasks").await, Ok(vec![stream]));
    drop((writer, reader, read_only));
    remove(&path);
}

// ------------------------------------------------------------ journal mode

#[tokio::test]
async fn an_opened_file_is_in_wal_mode() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    assert_eq!(journal_mode(&path).await, "wal");
    remove(&path);
}

#[tokio::test]
async fn opening_a_migrated_file_in_rollback_mode_moves_it_to_wal() {
    let path = database();
    migrated_in_rollback_mode(&path).await;
    drop(SqliteStore::open(&path).await.unwrap());
    assert_eq!(journal_mode(&path).await, "wal");
    remove(&path);
}

#[tokio::test]
async fn moving_into_wal_waits_for_a_writer_to_let_go() {
    // In a rollback journal the move into WAL does not wait on a writer's
    // lock; the open waits for it all the same.
    let path = database();
    migrated_in_rollback_mode(&path).await;
    let mut writer = raw(&path).await;
    let write = writer.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let (opened, ()) = tokio::join!(SqliteStore::open(&path), async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        write.rollback().await.unwrap();
    });
    drop(opened.unwrap());
    drop(writer);
    assert_eq!(journal_mode(&path).await, "wal");
    remove(&path);
}

#[tokio::test]
async fn reading_a_file_in_rollback_mode_leaves_its_journal_mode() {
    let path = database();
    migrated_in_rollback_mode(&path).await;
    assert_eq!(tasks_in(&path).await, Ok(vec![]));
    assert_eq!(journal_mode(&path).await, "delete");
    remove(&path);
}

#[tokio::test]
async fn stores_opening_a_file_in_rollback_mode_at_once_all_move_it_to_wal() {
    // Moving into WAL takes the exclusive lock without waiting the busy
    // timeout, so stores racing to make the move each meet the others'.
    for _ in 0..5 {
        let path = database();
        migrated_in_rollback_mode(&path).await;
        let open = || async { SqliteStore::open(&path).await.map(drop) };
        let opened = tokio::join!(
            open(),
            open(),
            open(),
            open(),
            open(),
            open(),
            open(),
            open()
        );
        let opened = [
            opened.0, opened.1, opened.2, opened.3, opened.4, opened.5, opened.6, opened.7,
        ];
        assert_eq!(opened, [const { Ok(()) }; 8]);
        assert_eq!(journal_mode(&path).await, "wal");
        remove(&path);
    }
}

#[test]
fn every_migration_runs_inside_the_open_transaction() {
    // `open` migrates inside `BEGIN IMMEDIATE`: a migration outside any
    // transaction, or one changing the journal mode, cannot run there.
    for migration in MIGRATOR.iter() {
        assert!(!migration.no_tx, "{}", migration.version);
        assert!(
            !migration.sql.to_ascii_lowercase().contains("journal_mode"),
            "{}",
            migration.version
        );
    }
}
