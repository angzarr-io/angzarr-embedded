//! The SQLite store held to annals' store contract, the same checks
//! InMemoryStore passes, so the kernel behaves alike over either; then what
//! is particular to a store on a file.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use annals::next::contract::{cover, page};
use annals::next::v1::{Cover, Snapshot};
use annals::next::{
    AsyncEventStore, AsyncPurge, AsyncReadStore, AsyncReadUnit, AsyncStore, AsyncUnitOfWork,
    AsyncWatch, ChangeToken, Slice, StoreError,
};
use annals_diagnostic::Diagnostic;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};

use super::{
    covers, into_wal, migrate, pause, read_alone, read_from, read_only, unavailable, SqliteRead,
    SqliteStore, BUSY_TIMEOUT, MIGRATOR,
};
use crate::{Damage, Integrity};

annals::async_store_contract!(#[tokio::test], SqliteStore::open_in_memory().await.unwrap());
annals::async_store_contract!(purge, #[tokio::test], SqliteStore::open_in_memory().await.unwrap());
annals::async_store_contract!(watch, #[tokio::test], SqliteStore::open_in_memory().await.unwrap());

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

// ------------------------------------------------------------------ purge

fn snapshot(sequence: u64) -> Snapshot {
    Snapshot {
        sequence,
        state: None,
        rules_version: "v1".to_string(),
    }
}

/// A store on `path` holding `stream` with two events and a snapshot, and
/// a trigger refusing to delete any snapshot: a purge's second statement
/// fails after its first has deleted the events.
async fn refusing_to_delete_snapshots(path: &Path, stream: &Cover) -> SqliteStore {
    let mut store = SqliteStore::open(path).await.unwrap();
    store
        .append(stream, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    store.write_snapshot(stream, &snapshot(1)).await.unwrap();
    let mut conn = raw(path).await;
    sqlx::query(
        "CREATE TRIGGER keep_snapshots BEFORE DELETE ON snapshots \
         BEGIN SELECT RAISE(ABORT, 'snapshots are kept'); END",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    conn.close().await.unwrap();
    store
}

/// `stream` as `refusing_to_delete_snapshots` left it: nothing deleted.
async fn assert_whole(store: &mut SqliteStore, stream: &Cover, what: &str) {
    assert_eq!(
        store.read_from(stream, 0).await.unwrap().map(|s| s.pages),
        Some(vec![page(0, 1), page(1, 2)]),
        "{what}"
    );
    assert_eq!(
        store
            .find_command(stream, &page(0, 2).meta.unwrap().correlation_id.unwrap())
            .await,
        Ok(vec![page(1, 2)]),
        "{what}: its command ids"
    );
    assert_eq!(
        store.read_snapshot(stream).await,
        Ok(Some(snapshot(1))),
        "{what}"
    );
    assert_eq!(
        store.covers("tasks").await,
        Ok(vec![stream.clone()]),
        "{what}"
    );
}

#[tokio::test]
async fn a_purge_that_fails_part_way_deletes_nothing() {
    let path = database();
    let stream = cover("tasks", 1);
    let mut store = refusing_to_delete_snapshots(&path, &stream).await;
    let purged = store.purge(&stream).await;
    assert_eq!(
        purged.as_ref().map_err(code),
        Err(Some("annals-store-sql-unavailable")),
        "{purged:?}"
    );
    assert_whole(&mut store, &stream, "after the failed purge").await;
    drop(store);
    remove(&path);
}

#[tokio::test]
async fn a_purge_that_fails_part_way_in_a_unit_leaves_the_unit_as_it_was() {
    // The unit is committed after the failure all the same: the events the
    // purge deleted before failing are not deleted with it.
    let path = database();
    let stream = cover("tasks", 1);
    let other = cover("tasks", 2);
    let mut store = refusing_to_delete_snapshots(&path, &stream).await;
    let mut unit = store.begin().await;
    unit.append(&other, 0, &[page(0, 3)]).await.unwrap();
    let purged = unit.purge(&stream).await;
    assert_eq!(
        purged.as_ref().map_err(code),
        Err(Some("annals-store-sql-unavailable")),
        "{purged:?}"
    );
    assert_eq!(
        unit.read_from(&stream, 0)
            .await
            .unwrap()
            .unwrap()
            .next_sequence,
        2,
        "the unit still holds the stream"
    );
    unit.commit().await.unwrap();
    assert_eq!(
        store.read_from(&other, 0).await.unwrap().unwrap().pages,
        [page(0, 3)],
        "the unit's own append, kept"
    );
    store.purge(&other).await.unwrap();
    assert_whole(&mut store, &stream, "after the unit committed").await;
    drop(store);
    remove(&path);
}

/// A second store on `path` whose connection waits on no lock: whatever it
/// tries while another holds the write lock fails at once, rather than
/// waiting its turn.
async fn impatient(path: &Path) -> SqliteStore {
    let conn = SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(Duration::ZERO)
        .connect()
        .await
        .unwrap();
    SqliteStore { conn }
}

#[tokio::test]
async fn a_purge_racing_an_append_lands_after_it_and_purges_it() {
    // The append's unit holds the write lock: a purge cannot land inside
    // it. Issued while it is in flight, the purge waits, and deletes what
    // the unit appended with the rest.
    let path = database();
    let stream = cover("tasks", 1);
    let mut writer = SqliteStore::open(&path).await.unwrap();
    writer.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    let mut purger = SqliteStore::open(&path).await.unwrap();
    let mut unit = writer.begin().await;
    unit.append(&stream, 1, &[page(1, 2)]).await.unwrap();

    let mut hasty = impatient(&path).await;
    let refused = hasty.purge(&stream).await;
    assert_eq!(
        refused.as_ref().map_err(code),
        Err(Some("annals-store-sql-unavailable")),
        "{refused:?}"
    );

    let (purged, committed) = tokio::join!(purger.purge(&stream), unit.commit());
    committed.unwrap();
    assert_eq!(purged, Ok(2), "appended, then purged");
    assert_eq!(writer.read_from(&stream, 0).await, Ok(None));
    assert_eq!(writer.covers("tasks").await, Ok(vec![]));
    assert_eq!(
        writer.append(&stream, 2, &[page(2, 3)]).await,
        Err(StoreError::Conflict {
            stream: stream.clone(),
            expected: 2,
            actual: 0
        })
    );
    drop((writer, purger, hasty));
    remove(&path);
}

#[tokio::test]
async fn an_append_racing_a_purge_lands_after_it_on_a_new_stream() {
    // The purge's unit holds the write lock: an append cannot land inside
    // it, and a reader still sees the stream whole. Issued while it is in
    // flight, the append's unit waits, then finds no stream, and starts
    // one at sequence 0.
    let path = database();
    let stream = cover("tasks", 1);
    let mut purger = SqliteStore::open(&path).await.unwrap();
    purger
        .append(&stream, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    let mut writer = SqliteStore::open(&path).await.unwrap();
    let mut unit = purger.begin().await;
    assert_eq!(unit.purge(&stream).await, Ok(2));

    let mut hasty = impatient(&path).await;
    let refused = hasty.append(&stream, 2, &[page(2, 2)]).await;
    assert_eq!(
        refused.as_ref().map_err(code),
        Err(Some("annals-store-sql-unavailable")),
        "{refused:?}"
    );
    assert_eq!(
        hasty
            .read_from(&stream, 0)
            .await
            .unwrap()
            .unwrap()
            .next_sequence,
        2,
        "the purge not yet committed"
    );

    let (appended, committed) = tokio::join!(
        async {
            let mut racing = writer.begin().await;
            let read = racing.read_from(&stream, 0).await;
            let appended = racing.append(&stream, 0, &[page(0, 2)]).await;
            racing.commit().await.unwrap();
            (read, appended)
        },
        unit.commit()
    );
    committed.unwrap();
    assert_eq!(appended, (Ok(None), Ok(1)), "purged, then appended");
    let slice = writer.read_from(&stream, 0).await.unwrap().unwrap();
    assert_eq!(slice.pages, [page(0, 2)]);
    assert_eq!(writer.covers("tasks").await, Ok(vec![stream]));
    drop((writer, purger, hasty));
    remove(&path);
}

// ------------------------------------------------------- reading at once

/// Run `future` to its end on a thread and runtime of its own, blocking
/// until it is done: a write made through a second connection from inside
/// a read's hook, while the read waits between its statements.
fn elsewhere<F: std::future::Future + Send>(future: F) -> F::Output
where
    F::Output: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(future)
            })
            .join()
            .unwrap()
    })
}

#[tokio::test]
async fn a_read_outside_a_unit_is_one_moment_across_a_purge() {
    // The purge commits after the read has the stream's length and before
    // it has the pages: the read still answers the stream whole, never a
    // stream of two events holding none.
    let path = database();
    let stream = cover("tasks", 1);
    let mut writer = SqliteStore::open(&path).await.unwrap();
    writer
        .append(&stream, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    let mut reader = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let mut purged = None;
    let read = read_alone(&mut reader.conn, &stream, 0, || {
        purged = Some(elsewhere(writer.purge(&stream)));
    })
    .await;
    assert_eq!(purged, Some(Ok(2)), "the purge ran in between");
    let slice = read.unwrap().unwrap();
    assert_eq!(slice.pages, [page(0, 1), page(1, 2)]);
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(reader.read_from(&stream, 0).await, Ok(None), "then gone");
    drop((writer, reader));
    remove(&path);
}

#[tokio::test]
async fn a_read_outside_a_unit_is_one_moment_across_an_append() {
    // The append commits after the read has the stream's length and before
    // it has the pages: the read answers the stream as it was, its pages
    // ending at its length and its last `recorded_at` its last page's.
    let path = database();
    let stream = cover("tasks", 1);
    let mut writer = SqliteStore::open(&path).await.unwrap();
    writer.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    let mut reader = SqliteStore::open(&path).await.unwrap();
    let mut appended = None;
    let read = read_alone(&mut reader.conn, &stream, 0, || {
        appended = Some(elsewhere(writer.append(&stream, 1, &[page(1, 2)])));
    })
    .await;
    assert_eq!(appended, Some(Ok(2)), "the append ran in between");
    let slice = read.unwrap().unwrap();
    assert_eq!(slice.pages, [page(0, 1)]);
    assert_eq!(slice.next_sequence, 1);
    assert_eq!(slice.last_recorded_at, page(0, 1).meta.unwrap().recorded_at);
    drop((writer, reader));
    remove(&path);
}

// --------------------------------------------------- reading one moment

/// Run the read check `check` on a reader and a writer on one new file,
/// the reader from `open_reader`, then remove the file.
async fn on_file<R, F>(
    open_reader: impl AsyncFnOnce(&Path) -> R,
    check: impl FnOnce((R, SqliteStore)) -> F,
) where
    F: std::future::Future<Output = ()>,
{
    let path = database();
    let writer = SqliteStore::open(&path).await.unwrap();
    let reader = open_reader(&path).await;
    check((reader, writer)).await;
    remove(&path);
}

async fn writable(path: &Path) -> SqliteStore {
    SqliteStore::open(path).await.unwrap()
}

async fn read_only_store(path: &Path) -> SqliteStore {
    SqliteStore::open_existing(path).await.unwrap().unwrap()
}

mod read_contract {
    use annals::next::contract as checks;

    use super::{on_file, read_only_store, writable};

    #[tokio::test]
    async fn a_read_unit_sees_the_store_as_it_began() {
        on_file(writable, checks::a_read_unit_sees_the_store_as_it_began).await;
    }

    #[tokio::test]
    async fn a_read_unit_refuses_an_edition_never_created() {
        on_file(
            writable,
            checks::a_read_unit_refuses_an_edition_never_created,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_only_stores_read_unit_refuses_an_edition_never_created() {
        on_file(
            read_only_store,
            checks::a_read_unit_refuses_an_edition_never_created,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_unit_keeps_a_stream_purged_after_it_began() {
        on_file(
            writable,
            checks::a_read_unit_keeps_a_stream_purged_after_it_began,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_unit_on_a_read_only_store_sees_the_store_as_it_began() {
        on_file(
            read_only_store,
            checks::a_read_unit_sees_the_store_as_it_began,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_unit_on_a_read_only_store_keeps_a_stream_purged_after_it_began() {
        on_file(
            read_only_store,
            checks::a_read_unit_keeps_a_stream_purged_after_it_began,
        )
        .await;
    }
}

/// A SQLite store whose read unit is not one moment, as `fault` says: the
/// read checks must fail on it, so they are seen to force their
/// interleaving through a second connection rather than pass by luck.
struct Faulty {
    store: SqliteStore,
    fault: ReadFault,
}

#[derive(Clone, Copy)]
enum ReadFault {
    /// No transaction: each read sees its own moment.
    Live,
    /// A deferred `BEGIN` and nothing read: its moment is its first read's.
    Unpinned,
}

/// [`Faulty`]'s read unit.
enum FaultyRead<'s> {
    Live(&'s mut SqliteConnection),
    Unpinned(SqliteRead<'s>),
}

impl AsyncReadUnit for FaultyRead<'_> {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        match self {
            FaultyRead::Live(conn) => covers(conn, domain).await,
            FaultyRead::Unpinned(reads) => reads.covers(domain).await,
        }
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        match self {
            FaultyRead::Live(conn) => read_from(conn, stream, from, || {}).await,
            FaultyRead::Unpinned(reads) => reads.read_from(stream, from).await,
        }
    }
}

impl AsyncReadStore for Faulty {
    type Reads<'s> = FaultyRead<'s>;

    async fn begin_read(&mut self) -> FaultyRead<'_> {
        let conn = &mut self.store.conn;
        match self.fault {
            ReadFault::Live => FaultyRead::Live(conn),
            ReadFault::Unpinned => FaultyRead::Unpinned(SqliteRead {
                tx: conn.begin().await.map_err(unavailable),
            }),
        }
    }
}

/// Whether `check` fails on a reader with `fault` and a writer on one file.
async fn fails<F>(fault: ReadFault, check: fn((Faulty, SqliteStore)) -> F) -> bool
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let path = database();
    let writer = SqliteStore::open(&path).await.unwrap();
    let store = SqliteStore::open(&path).await.unwrap();
    let failed = tokio::spawn(check((Faulty { store, fault }, writer)))
        .await
        .is_err();
    remove(&path);
    failed
}

#[tokio::test]
async fn every_read_check_fails_a_read_unit_that_is_no_transaction() {
    use annals::next::contract as checks;
    assert!(
        fails(
            ReadFault::Live,
            checks::a_read_unit_sees_the_store_as_it_began
        )
        .await
    );
    assert!(
        fails(
            ReadFault::Live,
            checks::a_read_unit_keeps_a_stream_purged_after_it_began
        )
        .await
    );
}

#[tokio::test]
async fn the_read_checks_fail_a_read_unit_whose_moment_is_its_first_read() {
    use annals::next::contract as checks;
    assert!(
        fails(
            ReadFault::Unpinned,
            checks::a_read_unit_sees_the_store_as_it_began
        )
        .await
    );
}

#[tokio::test]
async fn a_writer_waits_on_no_open_read_unit() {
    // The writer waits on no lock at all: had the read unit taken the
    // write lock, its unit of work would fail at once.
    let path = database();
    let stream = cover("tasks", 1);
    let mut writable_reader = SqliteStore::open(&path).await.unwrap();
    let mut read_only_reader = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let mut first = writable_reader.begin_read().await;
    let mut second = read_only_reader.begin_read().await;
    let mut hasty = impatient(&path).await;
    let mut unit = hasty.begin().await;
    assert_eq!(unit.append(&stream, 0, &[page(0, 1)]).await, Ok(1));
    unit.commit().await.unwrap();
    assert_eq!(hasty.purge(&stream).await, Ok(1));
    assert_eq!(first.covers("tasks").await, Ok(vec![]));
    assert_eq!(second.read_from(&stream, 0).await, Ok(None));
    drop((first, second));
    drop((writable_reader, read_only_reader, hasty));
    remove(&path);
}

// ------------------------------------------------------ commit position

mod position_contract {
    use annals::next::contract as checks;

    use super::{on_file, read_only_store, writable};

    #[tokio::test]
    async fn the_commit_position_counts_every_handles_commits() {
        on_file(
            writable,
            checks::the_commit_position_counts_every_handles_commits,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_only_stores_commit_position_counts_every_handles_commits() {
        on_file(
            read_only_store,
            checks::the_commit_position_counts_every_handles_commits,
        )
        .await;
    }
}

/// Every event's commit position, as `(domain, sequence, position)`, in
/// domain and sequence order.
async fn positions(store: &mut SqliteStore) -> Vec<(String, i64, i64)> {
    sqlx::query_as("SELECT domain, sequence, position FROM events ORDER BY domain, sequence")
        .fetch_all(&mut store.conn)
        .await
        .unwrap()
}

#[tokio::test]
async fn every_event_is_stamped_with_the_position_it_was_committed_at() {
    let path = database();
    let mut store = SqliteStore::open(&path).await.unwrap();
    let empty = store.commit_position().await.unwrap();
    store
        .append(&cover("account", 1), 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    let first = store.commit_position().await.unwrap();
    let mut unit = store.begin().await;
    unit.append(&cover("ledger", 1), 0, &[page(0, 2)])
        .await
        .unwrap();
    unit.commit().await.unwrap();
    let second = store.commit_position().await.unwrap();
    let stamped = positions(&mut store).await;
    let at = |i: usize| u64::try_from(stamped[i].2).unwrap();
    assert_eq!(stamped.len(), 3);
    assert_eq!(at(0), at(1), "one append, one position");
    assert!(
        empty <= at(0) && at(0) < first,
        "{empty} <= {} < {first}",
        at(0)
    );
    assert!(
        first <= at(2) && at(2) < second,
        "{first} <= {} < {second}",
        at(2)
    );
    // Kept on the file, and never lowered by a purge.
    store.purge(&cover("account", 1)).await.unwrap();
    drop(store);
    let mut reopened = SqliteStore::open(&path).await.unwrap();
    assert_eq!(reopened.commit_position().await, Ok(second));
    drop(reopened);
    remove(&path);
}

#[tokio::test]
async fn a_rolled_back_unit_stamps_nothing_and_moves_no_position() {
    let mut store = SqliteStore::open_in_memory().await.unwrap();
    let before = store.commit_position().await.unwrap();
    {
        let mut unit = store.begin().await;
        unit.append(&cover("account", 1), 0, &[page(0, 1)])
            .await
            .unwrap();
    }
    assert_eq!(store.commit_position().await, Ok(before));
    assert_eq!(positions(&mut store).await, vec![]);
}

// ------------------------------------------------------------- watching

mod watch_contract {
    use annals::next::contract as checks;

    use super::{database, on_file, read_only_store, remove, writable, SqliteStore};

    #[tokio::test]
    async fn a_change_token_changes_when_another_handle_commits() {
        on_file(
            writable,
            checks::a_change_token_changes_when_another_handle_commits,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_only_stores_change_token_changes_when_another_handle_commits() {
        on_file(
            read_only_store,
            checks::a_change_token_changes_when_another_handle_commits,
        )
        .await;
    }

    #[tokio::test]
    async fn a_change_token_on_a_file_changes_when_its_own_store_commits() {
        let path = database();
        checks::a_change_token_changes_when_its_own_store_commits(
            SqliteStore::open(&path).await.unwrap(),
        )
        .await;
        remove(&path);
    }
}

/// What a [`FaultyWatch`] answers for its token: each is how a token might
/// be read from SQLite, and each misses some commit.
#[derive(Clone, Copy)]
enum TokenFault {
    /// The same token always.
    Never,
    /// `PRAGMA data_version` alone: blind to this connection's own commits.
    DataVersion,
    /// `total_changes()` alone: blind to another connection's commits.
    OwnChanges,
    /// The events table's largest rowid: blind to a purge of any stream but
    /// the newest.
    LargestRowid,
}

/// A SQLite store whose change token is read as `fault` says: the watch
/// checks must fail on it, so they are seen to force their interleaving
/// through a second connection rather than pass by luck.
struct FaultyWatch {
    store: SqliteStore,
    fault: TokenFault,
}

impl AsyncWatch for FaultyWatch {
    async fn change_token(&mut self) -> Result<ChangeToken, StoreError> {
        let sql = match self.fault {
            TokenFault::Never => return Ok(ChangeToken::new([1, 1])),
            TokenFault::DataVersion => "PRAGMA data_version",
            TokenFault::OwnChanges => "SELECT total_changes()",
            TokenFault::LargestRowid => "SELECT coalesce(max(rowid), 0) FROM events",
        };
        let part: i64 = sqlx::query_scalar(sql)
            .fetch_one(&mut self.store.conn)
            .await
            .map_err(unavailable)?;
        Ok(ChangeToken::new([part as u64, 0]))
    }
}

impl AsyncEventStore for FaultyWatch {
    async fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[annals::next::v1::EventPage],
    ) -> Result<u64, StoreError> {
        self.store.append(stream, expected, pages).await
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        self.store.read_from(stream, from).await
    }

    async fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &annals::next::v1::Uuid,
    ) -> Result<Vec<annals::next::v1::EventPage>, StoreError> {
        self.store.find_command(stream, command_id).await
    }

    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.store.covers(domain).await
    }
}

impl AsyncPurge for FaultyWatch {
    async fn purge(&mut self, stream: &Cover) -> Result<u64, StoreError> {
        self.store.purge(stream).await
    }
}

impl AsyncStore for FaultyWatch {
    type Unit<'s> = super::SqliteUnit<'s>;

    async fn begin(&mut self) -> super::SqliteUnit<'_> {
        self.store.begin().await
    }

    async fn commit_position(&mut self) -> Result<u64, StoreError> {
        self.store.commit_position().await
    }
}

/// Whether the pair check fails on a watcher with `fault` and a writer, two
/// connections to one file.
async fn another_fails(fault: TokenFault) -> bool {
    use annals::next::contract as checks;
    let path = database();
    let writer = SqliteStore::open(&path).await.unwrap();
    let store = SqliteStore::open(&path).await.unwrap();
    let watcher = FaultyWatch { store, fault };
    let check = checks::a_change_token_changes_when_another_handle_commits((watcher, writer));
    let failed = tokio::spawn(check).await.is_err();
    remove(&path);
    failed
}

/// Whether the check on one store fails on a store with `fault`.
async fn own_fails(fault: TokenFault) -> bool {
    use annals::next::contract as checks;
    let store = SqliteStore::open_in_memory().await.unwrap();
    let check =
        checks::a_change_token_changes_when_its_own_store_commits(FaultyWatch { store, fault });
    tokio::spawn(check).await.is_err()
}

#[tokio::test]
async fn every_watch_check_fails_a_token_that_never_changes() {
    assert!(another_fails(TokenFault::Never).await, "another handle's");
    assert!(own_fails(TokenFault::Never).await, "its own");
}

#[tokio::test]
async fn the_watch_checks_fail_data_version_alone() {
    // data_version is the connection's own: it changes for another's commit.
    assert!(!another_fails(TokenFault::DataVersion).await);
    assert!(own_fails(TokenFault::DataVersion).await);
}

#[tokio::test]
async fn the_watch_checks_fail_total_changes_alone() {
    assert!(another_fails(TokenFault::OwnChanges).await);
    assert!(!own_fails(TokenFault::OwnChanges).await);
}

#[tokio::test]
async fn the_watch_checks_fail_the_largest_rowid() {
    assert!(another_fails(TokenFault::LargestRowid).await);
    assert!(own_fails(TokenFault::LargestRowid).await);
}

#[tokio::test]
async fn a_change_token_waits_on_no_writer() {
    // The watchers wait on no lock at all: had asking taken the write lock,
    // or waited on the unit holding it, it would fail at once.
    let path = database();
    let stream = cover("tasks", 1);
    let mut writer = SqliteStore::open(&path).await.unwrap();
    let mut hasty = impatient(&path).await;
    let mut read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let hasty_before = hasty.change_token().await.unwrap();
    let read_only_before = read_only.change_token().await.unwrap();
    let mut unit = writer.begin().await;
    unit.append(&stream, 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(hasty.change_token().await, Ok(hasty_before), "uncommitted");
    assert_eq!(read_only.change_token().await, Ok(read_only_before));
    unit.commit().await.unwrap();
    assert_ne!(hasty.change_token().await, Ok(hasty_before), "committed");
    assert_ne!(read_only.change_token().await, Ok(read_only_before));
    drop((writer, hasty, read_only));
    remove(&path);
}

#[tokio::test]
async fn a_change_token_holds_across_a_unit_rolled_back_elsewhere() {
    let path = database();
    let mut writer = SqliteStore::open(&path).await.unwrap();
    let mut watcher = SqliteStore::open(&path).await.unwrap();
    let before = watcher.change_token().await.unwrap();
    let mut unit = writer.begin().await;
    unit.append(&cover("tasks", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    drop(unit);
    assert_eq!(watcher.change_token().await, Ok(before));
    drop((writer, watcher));
    remove(&path);
}

#[tokio::test]
async fn a_change_token_changes_when_a_snapshot_is_written() {
    let stream = cover("tasks", 1);
    let snapshot = Snapshot {
        sequence: 0,
        state: None,
        rules_version: "v1".to_string(),
    };
    let mut store = SqliteStore::open_in_memory().await.unwrap();
    let before = store.change_token().await.unwrap();
    store.write_snapshot(&stream, &snapshot).await.unwrap();
    assert_ne!(store.change_token().await, Ok(before));
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

#[test]
fn every_purge_is_a_send_future() {
    let stream = cover("tasks", 1);
    drop(send(async {
        let mut store = SqliteStore::open_in_memory().await?;
        store.purge(&stream).await?;
        let mut unit = store.begin().await;
        unit.purge(&stream).await?;
        unit.commit().await
    }));
}

#[test]
fn every_read_unit_is_a_send_future() {
    let path = database();
    let stream = cover("tasks", 1);
    drop(send(async {
        let mut store = SqliteStore::open_in_memory().await?;
        let mut reads = store.begin_read().await;
        reads.covers("tasks").await?;
        reads.read_from(&stream, 0).await
    }));
    drop(send(async {
        let Some(mut store) = SqliteStore::open_existing(&path).await? else {
            return Ok(None);
        };
        let mut reads = store.begin_read().await;
        reads.read_from(&stream, 0).await
    }));
}

#[test]
fn every_change_token_is_a_send_future() {
    let path = database();
    drop(send(async {
        let mut store = SqliteStore::open_in_memory().await?;
        store.change_token().await
    }));
    drop(send(async {
        let Some(mut store) = SqliteStore::open_existing(&path).await? else {
            return Ok(None);
        };
        store.change_token().await.map(Some)
    }));
}
