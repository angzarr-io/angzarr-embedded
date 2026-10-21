//! The SQLite store held to annals' store contract, the same checks
//! InMemoryStore passes, so the kernel behaves alike over either; then what
//! is particular to a store on a file.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use annals::next::contract::{cover, page};
use annals::next::v1::{Cover, Snapshot};
use annals::next::{AsyncEventStore, AsyncStore, AsyncUnitOfWork, StoreError};
use annals_diagnostic::Diagnostic;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};

use super::{into_wal, migrate, pause, read_only, SqliteStore, BUSY_TIMEOUT, MIGRATOR};
use crate::{Damage, Integrity};

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
    // The writer stays open, so its WAL files stay: without them SQLite
    // cannot read a WAL file in a directory it cannot write. Dropped
    // here, it would close on sqlx's worker thread in its own time,
    // deleting them, racing the read.
    let mut writer = SqliteStore::open(&path).await.unwrap();
    writer.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap()
    };
    mode(&path, 0o444);
    mode(&dir, 0o555);
    let mut store = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let read = store.read_from(&stream, 0).await;
    let written = store.append(&stream, 1, &[page(1, 2)]).await;
    drop(store);
    writer.conn.close().await.unwrap();
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
async fn reading_a_database_older_than_the_build_fails_at_once() {
    // It holds what was committed to it, so it is no file being made: the
    // read refuses it as it stands, waiting on no writer.
    let path = database();
    let mut conn = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    sqlx::query("CREATE TABLE older (x)")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let waits = AtomicUsize::new(0);
    let opened = read_only(&path, BUSY_TIMEOUT, || {
        waits.fetch_add(1, Ordering::Relaxed);
    })
    .await;
    assert_eq!(
        opened.as_ref().err().and_then(code),
        Some("annals-store-sql-unmigrated")
    );
    assert_eq!(waits.load(Ordering::Relaxed), 0);
    assert_eq!(journal_mode(&path).await, "delete");
    remove(&path);
}

// --------------------------------------- reading a file a writer is making

/// A file `open` is making: created, and no further.
async fn created(path: &Path) -> SqliteConnection {
    let conn = SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(BUSY_TIMEOUT)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    conn
}

/// Read the file at `path` as `open_existing` does, alongside `writer`,
/// which runs only once the read has found the file being made and begun
/// to wait for it: the read meets the file mid-creation, every run. A read
/// that ends without waiting is returned as it is, the writer never run.
async fn read_while<W: std::future::Future<Output = ()>>(
    path: &Path,
    writer: impl FnOnce() -> W,
) -> Result<Option<SqliteStore>, StoreError> {
    let waiting = AtomicBool::new(false);
    let read = AtomicBool::new(false);
    let (opened, ()) = tokio::join!(
        async {
            let opened = read_only(path, BUSY_TIMEOUT, || {
                waiting.store(true, Ordering::Relaxed)
            })
            .await;
            read.store(true, Ordering::Relaxed);
            opened
        },
        async {
            while !waiting.load(Ordering::Relaxed) {
                if read.load(Ordering::Relaxed) {
                    return;
                }
                tokio::task::yield_now().await;
            }
            writer().await;
        }
    );
    opened
}

#[tokio::test]
async fn reading_a_file_created_but_not_yet_in_wal_waits_for_its_writer() {
    let path = database();
    let mut writer = created(&path).await;
    let read = read_while(&path, || async {
        into_wal(&mut writer).await.unwrap();
        migrate(&mut writer).await.unwrap();
    })
    .await;
    let mut store = read.unwrap().expect("the database its writer made");
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
    drop((store, writer));
    remove(&path);
}

#[tokio::test]
async fn reading_a_file_its_writer_is_migrating_waits_for_the_commit() {
    let path = database();
    let mut writer = created(&path).await;
    into_wal(&mut writer).await.unwrap();
    let mut tx = writer.begin_with("BEGIN IMMEDIATE").await.unwrap();
    MIGRATOR.run_direct(&mut *tx).await.unwrap();
    let read = read_while(&path, || async { tx.commit().await.unwrap() }).await;
    let mut store = read.unwrap().expect("the database its writer made");
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
    drop((store, writer));
    remove(&path);
}

#[tokio::test]
async fn reading_a_file_whose_writer_never_finishes_fails_unmigrated() {
    // An empty file nobody finishes, as a writer that died making it left
    // it: the read waits out its patience, then refuses it, changing
    // nothing. Each wait doubles, from a millisecond, and lasts at least as
    // long as it says, so 20ms holds five at most however slow the machine:
    // four take 15ms, and the fifth is cut to what is left.
    let path = database();
    std::fs::write(&path, b"").unwrap();
    let waits = AtomicUsize::new(0);
    let opened = read_only(&path, Duration::from_millis(20), || {
        waits.fetch_add(1, Ordering::Relaxed);
    })
    .await;
    assert_eq!(
        opened.as_ref().err().and_then(code),
        Some("annals-store-sql-unmigrated")
    );
    assert!((1..=5).contains(&waits.load(Ordering::Relaxed)));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    remove(&path);
}

#[tokio::test]
async fn a_pause_lasts_as_long_as_it_says() {
    let started = Instant::now();
    pause(Duration::from_millis(30)).await;
    assert!(started.elapsed() >= Duration::from_millis(30));
    pause(Duration::ZERO).await;
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

// --------------------------------------------------------------- integrity

#[tokio::test]
async fn a_healthy_database_is_intact_however_it_is_opened() {
    let path = database();
    let stream = cover("tasks", 1);
    let mut store = SqliteStore::open(&path).await.unwrap();
    store
        .append(&stream, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    assert_eq!(store.integrity_check().await, Ok(Integrity::Intact));
    let mut read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    assert_eq!(read_only.integrity_check().await, Ok(Integrity::Intact));
    let mut memory = SqliteStore::open_in_memory().await.unwrap();
    assert_eq!(memory.integrity_check().await, Ok(Integrity::Intact));
    drop((store, read_only));
    remove(&path);
}

#[tokio::test]
async fn checking_a_read_only_store_waits_on_no_writer() {
    // The writer holds the write lock throughout: a check that took it
    // would wait the busy timeout and fail.
    let path = database();
    let stream = cover("tasks", 1);
    let mut writer = SqliteStore::open(&path).await.unwrap();
    writer.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    let mut unit = writer.begin().await;
    unit.append(&stream, 1, &[page(1, 2)]).await.unwrap();

    let mut read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let started = Instant::now();
    assert_eq!(read_only.integrity_check().await, Ok(Integrity::Intact));
    assert!(started.elapsed() < BUSY_TIMEOUT, "{:?}", started.elapsed());

    unit.commit().await.unwrap();
    drop((writer, read_only));
    remove(&path);
}

/// Change the page of `path` holding `index`'s root with `tamper`: the
/// index is damaged, and the tables `open` reads are not.
async fn damage_index(path: &Path, index: &str, tamper: impl FnOnce(&mut [u8])) {
    let mut conn = raw(path).await;
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let root: i64 = sqlx::query_scalar("SELECT rootpage FROM sqlite_master WHERE name = ?")
        .bind(index)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    // Everything into the file itself, so the bytes changed are the page's
    // only copy.
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let mut bytes = std::fs::read(path).unwrap();
    let start = usize::try_from((root - 1) * page_size).unwrap();
    let end = start + usize::try_from(page_size).unwrap();
    tamper(&mut bytes[start..end]);
    std::fs::write(path, bytes).unwrap();
}

/// A file holding one stream of two events, its index then changed by
/// `tamper`, checked opened each way: the damage each check found.
async fn check_damaged(tamper: impl FnOnce(&mut [u8])) -> Vec<Damage> {
    let path = database();
    let stream = cover("tasks", 1);
    {
        let mut store = SqliteStore::open(&path).await.unwrap();
        store
            .append(&stream, 0, &[page(0, 1), page(1, 2)])
            .await
            .unwrap();
    }
    damage_index(&path, "events_by_command", tamper).await;
    let mut found = Vec::new();
    for mut store in [
        SqliteStore::open_existing(&path).await.unwrap().unwrap(),
        SqliteStore::open(&path).await.unwrap(),
    ] {
        match store.integrity_check().await {
            Ok(Integrity::Damaged(damage)) => found.push(damage),
            other => panic!("the damaged index went unseen: {other:?}"),
        }
    }
    remove(&path);
    found
}

#[tokio::test]
async fn an_index_entry_changed_is_reported_as_sqlite_words_it() {
    // The page's last byte is the last entry's last key byte: the page
    // still reads, and the entry no longer matches its row.
    for damage in check_damaged(|page| *page.last_mut().unwrap() ^= 1).await {
        assert_eq!(damage.code(), "annals-store-sql-damaged");
        assert!(
            damage
                .problems()
                .iter()
                .any(|p| p.contains("events_by_command")),
            "{:?}",
            damage.problems()
        );
        assert!(damage.problems().iter().all(|p| p != "ok"));
    }
}

#[tokio::test]
async fn a_page_beyond_reading_is_damage_not_a_failure_to_check() {
    // SQLite stops the check with SQLITE_CORRUPT rather than reporting
    // rows: that is damage found, in SQLite's words.
    for damage in check_damaged(|page| page.fill(0xff)).await {
        assert_eq!(damage.problems(), ["database disk image is malformed"]);
    }
}

#[tokio::test]
async fn a_check_that_cannot_run_is_a_failure_not_damage() {
    // A file in its rollback journal, locked exclusively by another
    // connection: a store that waits on no lock cannot read it.
    let path = database();
    migrated_in_rollback_mode(&path).await;
    let mut holder = raw(&path).await;
    sqlx::query("PRAGMA locking_mode = EXCLUSIVE")
        .execute(&mut holder)
        .await
        .unwrap();
    let held = holder.begin_with("BEGIN EXCLUSIVE").await.unwrap();
    let conn = SqliteConnectOptions::new()
        .filename(&path)
        .busy_timeout(Duration::ZERO)
        .read_only(true)
        .connect()
        .await
        .unwrap();
    let checked = SqliteStore { conn }.integrity_check().await;
    assert_eq!(
        checked.as_ref().map_err(code),
        Err(Some("annals-store-sql-unavailable")),
        "{checked:?}"
    );
    held.rollback().await.unwrap();
    drop(holder);
    remove(&path);
}

// ------------------------------------------------------------------ Send

/// Compiles only if `future` is `Send`, so a store can be opened on a
/// multi-threaded runtime's task (`tokio::spawn`) without a blocking thread.
fn send<F: std::future::Future + Send>(future: F) -> F {
    future
}

#[test]
fn every_open_is_a_send_future() {
    let path = database();
    drop(send(SqliteStore::open(&path)));
    drop(send(SqliteStore::open_existing(&path)));
    drop(send(SqliteStore::open_in_memory()));
    drop(send(async {
        let mut store = SqliteStore::open_in_memory().await?;
        store.integrity_check().await
    }));
}
