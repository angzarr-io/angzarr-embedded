//! The SQLite store held to angzarr-ports' conformance suite, every section
//! but editions, the same checks `MemoryStore` passes, so annals and angzarr
//! behave alike over either; then what is particular to a store on a file:
//! opening, WAL, migrations (an existing database's included), integrity,
//! damage, purges and reads racing writers, the commit position as stamped,
//! change tokens, and `Send`.
//!
//! **Editions are NOT IMPLEMENTED YET** in this adapter (the owner's ruling,
//! 2026-10-28): the suite's edition checks (three of the base section's,
//! one of the read section's, and the `editions`, `editions_outbox` and
//! `read_forks` sections) are present and ignored, a follow-up;
//! `MemoryStore` covers them meanwhile. Meanwhile no stream is ever forked:
//! an edition stream reads as main's, and a write to one is refused.

use std::borrow::Cow;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use angzarr_ports::angzarr_proto::v1::{Cover, EventBook, EventPage};
use angzarr_ports::contract::{
    append, at, command, correlated, edition_stream, main_stream, page, snapshot,
};
use angzarr_ports::{
    EventStore, Outbox, PositionKey, PositionStore, Purge, ReadByCorrelation, ReadStore, ReadUnit,
    ReadUntilTime, Slice, SnapshotStore, StoreError, StreamRef, Watch,
};
use prost::Message;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};

use super::{into_wal, migrate, pause, read_only, SqliteStore, BUSY_TIMEOUT, MIGRATOR};
use crate::{Damage, Integrity};

/// A path for a database file of this test run's own.
fn database() -> std::path::PathBuf {
    static RUN: AtomicUsize = AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "angzarr-store-sqlite-test-{}-{}.db",
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

/// The `tasks` stream `n`, on the main timeline.
fn stream(n: u8) -> StreamRef<'static> {
    main_stream("tasks", n)
}

/// Every stream named, as the store lists them.
fn covers(streams: &[StreamRef<'_>]) -> Vec<Cover> {
    streams.iter().map(StreamRef::to_cover).collect()
}

/// The code of the cause `error` carries, if it is the store's own failure.
fn code(error: &StoreError) -> Option<&'static str> {
    match error {
        StoreError::Unavailable(cause) => Some(cause.code),
        _ => None,
    }
}

/// The pages of `stream` from 0, as `store` reads them now.
async fn pages(store: &SqliteStore, stream: StreamRef<'_>) -> Vec<EventPage> {
    store.read(stream, 0, None).await.unwrap().pages
}

// ------------------------------------------------------- the ports' suite

/// Two handles on one new file: two connections, as two processes have.
async fn pair() -> (SqliteStore, SqliteStore) {
    let path = database();
    let a = SqliteStore::open(&path).await.unwrap();
    let b = SqliteStore::open(&path).await.unwrap();
    (a, b)
}

/// One `#[tokio::test]` per named check of the suite, each on a fresh
/// store from `$fresh`, a check carrying its own attributes (an edition
/// check's `#[ignore]`). The base and read sections are stamped here, not
/// by `store_contract!`, because each holds edition checks this adapter
/// does not pass yet; their lists are angzarr-ports' at 4be6a68, kept in
/// step by hand when the suite grows.
macro_rules! checks {
    ($fresh:expr; $($(#[$a:meta])* $check:ident,)*) => {
        $(
            $(#[$a])*
            #[tokio::test]
            async fn $check() {
                angzarr_ports::contract::$check($fresh).await;
            }
        )*
    };
}

/// The base section, every check but three of editions (NOT IMPLEMENTED
/// YET in this adapter, owner ruling 2026-10-28).
mod base {
    use super::*;
    checks!(SqliteStore::open_in_memory().await.unwrap();
        append_checks_the_expected_length_and_continuity,
        append_refuses_a_stale_first_sequence,
        an_empty_append_checks_and_creates_nothing,
        read_is_bounded_by_from_and_to,
        a_read_of_no_such_stream_is_empty_at_zero,
        a_read_past_the_end_is_empty_but_knows_the_end,
        head_is_the_next_sequence_and_the_last_recorded_at,
        a_stream_is_domain_edition_and_root,
        covers_orders_main_first_then_by_edition_and_root,
        #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
        an_unforked_edition_stream_reads_as_main,
        #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
        an_edition_stream_takes_only_a_fork_as_its_first_own_event,
        #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
        a_forked_stream_reads_main_below_its_fork_then_its_own,
        find_command_finds_a_commands_events_on_its_stream,
        find_command_finds_a_retried_facts_events_by_its_id,
        find_command_finds_a_refusal_fact_on_the_trigger_stream_by_the_refused_id,
        a_store_keeps_the_newest_snapshot_per_stream,
        a_position_is_zero_until_put_then_the_last_put,
        positions_are_per_handler_and_per_stream,
        the_commit_position_rises_with_every_commit,
        a_write_lands_its_events_and_positions_together,
        a_refused_write_lands_nothing,
        a_position_only_write_is_a_step_with_no_stream,
    );
}

mod pair {
    use super::*;
    angzarr_ports::store_contract!(pair, #[tokio::test], pair().await);
}

mod purge {
    use super::*;
    angzarr_ports::store_contract!(
        purge,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

/// NOT IMPLEMENTED YET: editions are a follow-up in this adapter (owner
/// ruling 2026-10-28). The `editions` section needs the `Editions` port,
/// which this store does not implement, so its checks cannot be stamped
/// here; this placeholder names them, ignored, until they are:
/// `fork_point_is_mains_first_sequence_after_a_commit_position`,
/// `list_editions_derives_each_edition_from_its_fork_events`,
/// `an_edition_deletion_is_recorded_like_any_event`,
/// `a_deleted_stream_is_removed_with_every_row_of_it`,
/// `a_deletion_reaching_an_uncreated_stream_writes_nothing`,
/// `a_deletion_is_recorded_and_removed_stream_by_stream`,
/// `after_deletion_a_later_arrival_in_the_edition_is_refused`,
/// `a_what_if_edition_runs_beside_main_and_leaves_main_untouched`.
mod editions {
    #[test]
    #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
    fn the_editions_section_is_not_implemented_yet() {
        unimplemented!("the SQLite adapter's Editions: a follow-up")
    }
}

/// NOT IMPLEMENTED YET (owner ruling 2026-10-28). The `editions_outbox`
/// section needs `Editions` too:
/// `a_deleted_stream_is_removed_only_after_its_deletion_has_propagated`.
mod editions_outbox {
    #[test]
    #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
    fn the_editions_outbox_section_is_not_implemented_yet() {
        unimplemented!("the SQLite adapter's Editions: a follow-up")
    }
}

/// The read section, every check but its edition one (NOT IMPLEMENTED
/// YET, owner ruling 2026-10-28).
mod read {
    use super::*;
    checks!(pair().await;
        a_read_unit_sees_the_store_as_it_began,
        a_read_unit_heads_and_snapshots_as_it_began,
        #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
        a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
    );
}

mod read_purge {
    use super::*;
    angzarr_ports::store_contract!(read_purge, #[tokio::test], pair().await);
}

/// NOT IMPLEMENTED YET (owner ruling 2026-10-28). The `read_forks`
/// section's one check, `a_read_unit_reads_a_forked_stream_through_to_main`,
/// needs a forked stream, which this store cannot make.
mod read_forks {
    #[test]
    #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
    fn the_read_forks_section_is_not_implemented_yet() {
        unimplemented!("the SQLite adapter's Editions: a follow-up")
    }
}

mod watch {
    use super::*;
    angzarr_ports::store_contract!(
        watch,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod watch_pair {
    use super::*;
    angzarr_ports::store_contract!(watch_pair, #[tokio::test], pair().await);
}

mod until_time {
    use super::*;
    angzarr_ports::store_contract!(
        until_time,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod correlation {
    use super::*;
    angzarr_ports::store_contract!(
        correlation,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod list_streams {
    use super::*;
    angzarr_ports::store_contract!(
        list_streams,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod outbox {
    use super::*;
    angzarr_ports::store_contract!(
        outbox,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod outbox_watch {
    use super::*;
    angzarr_ports::store_contract!(
        outbox_watch,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

// ------------------------------------------------------------ on a file

#[tokio::test]
async fn what_was_written_survives_reopening_the_file() {
    let path = database();
    let s = stream(1);
    {
        let store = SqliteStore::open(&path).await.unwrap();
        append(&store, s, 0, &[page(0, 1), page(1, 2)])
            .await
            .unwrap();
        store.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
    }
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(pages(&store, s).await, [page(0, 1), page(1, 2)]);
    assert_eq!(store.read_snapshot(s).await, Ok(Some(snapshot(1, "v1"))));
    assert_eq!(store.covers("tasks").await, Ok(covers(&[s])));
    drop(store);
    remove(&path);
}

#[tokio::test]
async fn reopening_a_migrated_file_migrates_nothing_twice() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    let store = SqliteStore::open(&path).await.unwrap();
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
        let store = SqliteStore::open(&path).await.unwrap();
        assert_eq!(store.covers("tasks").await, Ok(vec![]));
        drop(store);
        remove(&path);
    }
}

// ------------------------------------------------- writes, on a file

/// A write is one transaction begun and committed inside the call: once
/// it returns, nothing is held open, and another handle on the same file
/// writes at once, waiting on no lock.
#[tokio::test]
async fn a_write_holds_no_transaction_open_after_it_returns() {
    let path = database();
    let s = stream(1);
    let a = SqliteStore::open(&path).await.unwrap();
    let b = SqliteStore::open(&path).await.unwrap();
    append(&a, s, 0, &[page(0, 1)]).await.unwrap();
    let started = Instant::now();
    assert_eq!(append(&b, s, 1, &[page(1, 2)]).await, Ok(2));
    assert!(started.elapsed() < BUSY_TIMEOUT, "it waited on a lock");
    assert_eq!(pages(&a, s).await.len(), 2);
    drop((a, b));
    remove(&path);
}

/// Two handles on one file writing one stream at once: SQLite's write lock
/// takes them in turn, each checked against the length it expected, so
/// one lands and the other is a conflict, nothing lost or doubled.
#[tokio::test]
async fn two_handles_writing_one_stream_at_once_land_one_and_refuse_the_other() {
    let path = database();
    let s = stream(1);
    let a = SqliteStore::open(&path).await.unwrap();
    let b = SqliteStore::open(&path).await.unwrap();
    let (one, two) = ([page(0, 1)], [page(0, 2)]);
    let (first, second) = tokio::join!(append(&a, s, 0, &one), append(&b, s, 0, &two));
    let outcomes = [&first, &second];
    let landed = outcomes.iter().filter(|r| r.is_ok()).count();
    assert_eq!(landed, 1, "{first:?} {second:?}");
    assert!(
        outcomes
            .iter()
            .any(|r| matches!(r, Err(StoreError::Conflict { .. }))),
        "{first:?} {second:?}"
    );
    assert_eq!(pages(&a, s).await.len(), 1);
    drop((a, b));
    remove(&path);
}

// ------------------------------------------------------------- editions

/// Editions are not implemented yet in this adapter (owner ruling
/// 2026-10-28), so no stream is ever forked: a write to an edition stream
/// is outside input to an unforked stream, refused `EditionNotForked`,
/// and nothing is written, in the edition or in main.
#[tokio::test]
async fn a_write_to_an_edition_stream_is_refused_not_forked() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let main = stream(1);
    let what_if = edition_stream("tasks", 1, "what-if");
    assert_eq!(
        append(&store, what_if, 0, &[page(0, 1)]).await,
        Err(StoreError::EditionNotForked {
            stream: Box::new(what_if.to_cover())
        })
    );
    assert_eq!(store.read(main, 0, None).await, Ok(Slice::default()));
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
}

/// An unforked edition stream reads as main's: its events, its head.
#[tokio::test]
async fn an_edition_stream_reads_as_main() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let main = stream(1);
    let what_if = edition_stream("tasks", 1, "what-if");
    append(&store, main, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    let read = store.read(what_if, 0, None).await.unwrap();
    assert_eq!(read.pages, [page(0, 1), page(1, 2)]);
    assert_eq!(read.next_sequence, 2);
    assert_eq!(
        store.covers("tasks").await,
        Ok(covers(&[main])),
        "not listed: it has no event of its own"
    );
}

// ------------------------------------------------- opening, by those who read

/// What a reader that answers empty for a store never written does: `list`
/// on a project with no database.
async fn tasks_in(path: &Path) -> Result<Vec<Cover>, StoreError> {
    match SqliteStore::open_existing(path).await? {
        None => Ok(vec![]),
        Some(store) => store.covers("tasks").await,
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

/// A raw connection to `path` in the middle of a write, as another
/// process's writer would be: it holds the write lock (`BEGIN IMMEDIATE`)
/// and has put `page` on `tasks` stream 1, uncommitted. [`end`] finishes it.
async fn inserting(path: &Path, page: &EventPage) -> SqliteConnection {
    let mut conn = raw(path).await;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO events (domain, edition, root, sequence, command_id, page, position) \
         VALUES ('tasks', '', ?, ?, NULL, ?, 0)",
    )
    .bind(vec![1u8; 16])
    .bind(i64::try_from(page.sequence).unwrap())
    .bind(page.encode_to_vec())
    .execute(&mut conn)
    .await
    .unwrap();
    conn
}

/// A raw connection to `path` in the middle of a purge of `tasks` stream
/// 1: it holds the write lock and has deleted the stream's events,
/// uncommitted. [`end`] finishes it.
async fn deleting(path: &Path) -> SqliteConnection {
    let mut conn = raw(path).await;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("DELETE FROM events WHERE domain = 'tasks' AND root = ?")
        .bind(vec![1u8; 16])
        .execute(&mut conn)
        .await
        .unwrap();
    conn
}

/// Finish a raw connection's write: `COMMIT`, or `ROLLBACK`.
async fn end(mut conn: SqliteConnection, commit: bool) {
    let sql = if commit { "COMMIT" } else { "ROLLBACK" };
    sqlx::query(sql).execute(&mut conn).await.unwrap();
    conn.close().await.unwrap();
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
        Err(Some("angzarr-store-sqlite-unavailable"))
    );
}

#[tokio::test]
async fn reading_finds_what_was_written() {
    let path = database();
    let s = stream(1);
    {
        let store = SqliteStore::open(&path).await.unwrap();
        append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    }
    assert_eq!(tasks_in(&path).await, Ok(covers(&[s])));
    remove(&path);
}

#[tokio::test]
async fn a_read_only_file_in_a_read_only_directory_opens_for_reading() {
    use std::os::unix::fs::PermissionsExt;
    let dir = database();
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.db");
    let s = stream(1);
    // The writer stays open, so its WAL files stay: without them SQLite
    // cannot read a WAL file in a directory it cannot write. Dropped
    // here, it would close on sqlx's worker thread in its own time,
    // deleting them, racing the read.
    let writer = SqliteStore::open(&path).await.unwrap();
    append(&writer, s, 0, &[page(0, 1)]).await.unwrap();
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap()
    };
    mode(&path, 0o444);
    mode(&dir, 0o555);
    let store = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let read = store.read(s, 0, None).await;
    let written = append(&store, s, 1, &[page(1, 2)]).await;
    drop(store);
    writer.conn.close().await.unwrap();
    mode(&dir, 0o755);
    mode(&path, 0o644);
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(read.unwrap().pages, [page(0, 1)]);
    assert_eq!(
        written.as_ref().map_err(code),
        Err(Some("angzarr-store-sqlite-unavailable"))
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
        Some("angzarr-store-sqlite-unmigrated")
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
    let store = read.unwrap().expect("the database its writer made");
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
    let store = read.unwrap().expect("the database its writer made");
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
        Some("angzarr-store-sqlite-unmigrated")
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
            Err(Some("angzarr-store-sqlite-unavailable")),
            "{tamper}"
        );
        assert_eq!(
            read.map_err(|e| code(&e)),
            Err(Some("angzarr-store-sqlite-unavailable")),
            "{tamper}"
        );
    }
}

#[tokio::test]
async fn a_store_opens_and_reads_while_another_holds_the_write_lock() {
    // No open takes the write lock once the file is migrated, so neither
    // waits on a writer; WAL lets both read past its uncommitted write.
    let path = database();
    let s = stream(1);
    drop(SqliteStore::open(&path).await.unwrap());
    let writing = inserting(&path, &page(0, 1)).await;

    let reader = SqliteStore::open(&path).await.unwrap();
    assert_eq!(reader.read(s, 0, None).await, Ok(Slice::default()));
    let read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    assert_eq!(read_only.read(s, 0, None).await, Ok(Slice::default()));

    end(writing, true).await;
    assert_eq!(reader.covers("tasks").await, Ok(covers(&[s])));
    assert_eq!(read_only.covers("tasks").await, Ok(covers(&[s])));
    drop((reader, read_only));
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

// ----------------------------------------- an existing database, migrated
//
// The plan for a database the released adapter (annals-store-sql) made,
// taskloom's above all:
//
// - 0001 and 0002 are kept byte for byte: sqlx records each applied
//   migration's checksum, and refuses a database whose applied migrations
//   differ from the build's (`the_released_migrations_are_unchanged`).
// - A new 0003 adds what the ports need: the positions table (handler,
//   stream, next), the outbox table (id, origin, message, in commit then
//   put order), and nullable `correlation_id` and `recorded_at` columns on
//   `events`, with their indexes (by correlation; by stream and
//   `recorded_at`).
// - A Rust backfill step, run under the same `BEGIN IMMEDIATE` as the
//   migration, decodes each stored page once to fill those two columns
//   (SQL cannot read protobuf), and records that it ran, so a reopen scans
//   nothing again.
// - A store opened read-only never migrates: a released database read that
//   way is refused as unmigrated until a writer has opened it.

/// The released schema: migrations 0001 and 0002, as the released adapter
/// applied them.
fn released() -> Migrator {
    Migrator {
        migrations: Cow::Owned(MIGRATOR.iter().take(2).cloned().collect()),
        ..Migrator::DEFAULT
    }
}

/// A database at `path` exactly as the released adapter left it: migrated
/// to 0002, in WAL, holding `tasks` stream 1 with two events of correlation
/// `0xC1`, committed in one append at commit position 1 (so the position's
/// one row says 2, the next).
async fn released_database(path: &Path) -> Vec<EventPage> {
    let mut conn = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&mut conn)
        .await
        .unwrap();
    released().run(&mut conn).await.unwrap();
    let written = vec![correlated(0, 1, 0xC1), correlated(1, 2, 0xC1)];
    for (n, page) in [1u8, 2].into_iter().zip(&written) {
        sqlx::query(
            "INSERT INTO events (domain, edition, root, sequence, command_id, page, position) \
             VALUES ('tasks', '', ?, ?, ?, ?, 1)",
        )
        .bind(vec![1u8; 16])
        .bind(i64::try_from(page.sequence).unwrap())
        .bind(command(n).as_bytes().to_vec())
        .bind(page.encode_to_vec())
        .execute(&mut conn)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE commit_position SET next = 2")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    written
}

/// What a database already holds keeps opening: 0001 and 0002 as released,
/// checksums and all. (Green at red: a pin, so no edit to them slips by.)
#[test]
fn the_released_migrations_are_unchanged() {
    let checksums: Vec<(i64, String)> = MIGRATOR
        .iter()
        .take(2)
        .map(|m| {
            let hex = m.checksum.iter().map(|b| format!("{b:02x}")).collect();
            (m.version, hex)
        })
        .collect();
    assert_eq!(
        checksums,
        [
            (
                1,
                "33a399da7876fc3cc923efd4afaf686628fa44897cbd8ddffb1412a8687a1fbaf14a1e3abbd2d39e6c2b25b78e6f9ee7"
                    .to_string()
            ),
            (
                2,
                "e3fc061266a9e54e7485243f231037fe0a1cc851c3fa023fb0866f41e881a2f9daf9d7742caa03ddff1c69d838be1afa"
                    .to_string()
            ),
        ]
    );
}

/// A database the released adapter made opens migrated, its events served
/// by every port as if this adapter had written them: the pages back as
/// written, found by correlation and by time (the backfilled columns), no
/// handler positioned, the outbox empty, the commit position as the old row
/// said.
#[tokio::test]
async fn a_database_at_the_released_schema_opens_migrated_and_reads_its_events_by_correlation_and_time(
) {
    let path = database();
    let written = released_database(&path).await;
    let s = stream(1);
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(pages(&store, s).await, written, "the pages as written");
    let books = store
        .read_by_correlation(Some("tasks"), None, command(0xC1))
        .await
        .unwrap();
    assert_eq!(
        books,
        [EventBook {
            cover: Some(s.to_cover()),
            pages: written.clone(),
            snapshot: None,
            next_sequence: 2,
        }],
        "by correlation"
    );
    let until = store.read_until_time(s, &at(0)).await.unwrap();
    assert_eq!(until.pages, written[..1], "by time: the prefix");
    assert_eq!(until.next_sequence, 2);
    let key = PositionKey {
        handler: "any",
        stream: s,
    };
    assert_eq!(store.position(key).await, Ok(0), "no handler positioned");
    assert_eq!(store.pending(10).await, Ok(vec![]), "the outbox empty");
    assert_eq!(store.commit_position().await, Ok(2), "as the old row said");
    drop(store);
    // Reopened, nothing is migrated or filled again, and all of it holds.
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(
        store
            .read_by_correlation(Some("tasks"), None, command(0xC1))
            .await
            .map(|books| books.len()),
        Ok(1)
    );
    drop(store);
    remove(&path);
}

/// A released database opened read-only is not migrated by the read: it is
/// refused as unmigrated, at once, until a writer has opened it. (Red now
/// for want of 0003: with nothing pending, the read opens it.)
#[tokio::test]
async fn a_database_opened_read_only_at_the_released_schema_is_refused_unmigrated() {
    let path = database();
    released_database(&path).await;
    let opened = SqliteStore::open_existing(&path).await.map(drop);
    assert_eq!(
        opened.map_err(|e| code(&e)),
        Err(Some("angzarr-store-sqlite-unmigrated"))
    );
    remove(&path);
}

// --------------------------------------------------------------- integrity

#[tokio::test]
async fn a_healthy_database_is_intact_however_it_is_opened() {
    let path = database();
    let s = stream(1);
    let mut store = SqliteStore::open(&path).await.unwrap();
    append(&store, s, 0, &[page(0, 1), page(1, 2)])
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
    let s = stream(1);
    let writer = SqliteStore::open(&path).await.unwrap();
    append(&writer, s, 0, &[page(0, 1)]).await.unwrap();
    let writing = inserting(&path, &page(1, 2)).await;

    let mut read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let started = Instant::now();
    assert_eq!(read_only.integrity_check().await, Ok(Integrity::Intact));
    assert!(started.elapsed() < BUSY_TIMEOUT, "{:?}", started.elapsed());

    end(writing, true).await;
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
    {
        let store = SqliteStore::open(&path).await.unwrap();
        append(&store, stream(1), 0, &[page(0, 1), page(1, 2)])
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
        assert_eq!(damage.code(), "angzarr-store-sqlite-damaged");
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
        Err(Some("angzarr-store-sqlite-unavailable")),
        "{checked:?}"
    );
    held.rollback().await.unwrap();
    drop(holder);
    remove(&path);
}

// ------------------------------------------------- too damaged to open

/// A copy of a healthy database (one stream of one event), checkpointed so
/// the file holds everything, then changed by `tamper`: the original is
/// removed, and the copy is what each open is given.
async fn damaged_copy(tamper: impl FnOnce(&mut Vec<u8>)) -> std::path::PathBuf {
    let original = database();
    {
        let store = SqliteStore::open(&original).await.unwrap();
        append(&store, stream(1), 0, &[page(0, 1)]).await.unwrap();
    }
    let mut conn = raw(&original).await;
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let copy = database();
    std::fs::copy(&original, &copy).unwrap();
    remove(&original);
    let mut bytes = std::fs::read(&copy).unwrap();
    tamper(&mut bytes);
    std::fs::write(&copy, bytes).unwrap();
    copy
}

/// The English an open-time damage cause leads with; SQLite's message
/// follows it.
const DAMAGED_FILE: &str = "the database file is too damaged to open: ";

/// Each open of the file `tamper` makes of a healthy copy: the code it
/// fails with and SQLite's message, the file left untouched.
async fn open_damaged(tamper: impl FnOnce(&mut Vec<u8>)) -> Vec<(Option<&'static str>, String)> {
    let path = damaged_copy(tamper).await;
    let before = std::fs::read(&path).unwrap();
    let mut found = Vec::new();
    for opened in [
        SqliteStore::open_existing(&path).await.map(drop),
        SqliteStore::open(&path).await.map(drop),
    ] {
        let error = opened.expect_err("a damaged file opened");
        let message = match &error {
            StoreError::Unavailable(cause) => cause
                .message
                .strip_prefix(DAMAGED_FILE)
                .unwrap_or(&cause.message)
                .to_string(),
            other => panic!("not the store's own failure: {other:?}"),
        };
        found.push((code(&error), message));
    }
    assert_eq!(std::fs::read(&path).unwrap(), before, "an open changed it");
    remove(&path);
    found
}

#[tokio::test]
async fn a_file_that_is_no_database_is_damaged_however_it_is_opened() {
    // Something else written over the database: SQLITE_NOTADB.
    let found = open_damaged(|bytes| {
        let len = bytes.len();
        *bytes = b"not a database\n"
            .iter()
            .copied()
            .cycle()
            .take(len)
            .collect();
    })
    .await;
    for (code, message) in found {
        assert_eq!(code, Some("angzarr-store-sqlite-damaged-file"));
        assert_eq!(message, "file is not a database");
    }
}

#[tokio::test]
async fn a_database_whose_header_is_overwritten_is_damaged_however_it_is_opened() {
    // The magic string gone: SQLite no longer takes it for a database.
    for (code, message) in open_damaged(|bytes| bytes[..16].fill(0)).await {
        assert_eq!(code, Some("angzarr-store-sqlite-damaged-file"));
        assert_eq!(message, "file is not a database");
    }
}

#[tokio::test]
async fn a_database_whose_schema_is_beyond_reading_is_damaged_however_it_is_opened() {
    // The header intact, the schema's page after it overwritten: the
    // first read of the schema fails SQLITE_CORRUPT.
    let found = open_damaged(|bytes| {
        let page_size = usize::from(u16::from_be_bytes([bytes[16], bytes[17]]));
        bytes[100..page_size].fill(0xff);
    })
    .await;
    for (code, message) in found {
        assert_eq!(code, Some("angzarr-store-sqlite-damaged-file"));
        assert_eq!(message, "database disk image is malformed");
    }
}

// ------------------------------------------------------------------ purge

/// A store on `path` holding `s` with two events and a snapshot, and a
/// trigger refusing to delete any snapshot: a purge's second statement
/// fails after its first has deleted the events.
async fn refusing_to_delete_snapshots(path: &Path, s: StreamRef<'_>) -> SqliteStore {
    let store = SqliteStore::open(path).await.unwrap();
    append(&store, s, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    store.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
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

/// `s` as `refusing_to_delete_snapshots` left it: nothing deleted.
async fn assert_whole(store: &SqliteStore, s: StreamRef<'_>, what: &str) {
    assert_eq!(pages(store, s).await, [page(0, 1), page(1, 2)], "{what}");
    assert_eq!(
        store.find_command(s, command(2)).await,
        Ok(vec![page(1, 2)]),
        "{what}: its command ids"
    );
    assert_eq!(
        store.read_snapshot(s).await,
        Ok(Some(snapshot(1, "v1"))),
        "{what}"
    );
    assert_eq!(store.covers("tasks").await, Ok(covers(&[s])), "{what}");
}

#[tokio::test]
async fn a_purge_that_fails_part_way_deletes_nothing() {
    let path = database();
    let s = stream(1);
    let store = refusing_to_delete_snapshots(&path, s).await;
    let purged = store.purge(s).await;
    assert_eq!(
        purged.as_ref().map_err(code),
        Err(Some("angzarr-store-sqlite-unavailable")),
        "{purged:?}"
    );
    assert_whole(&store, s, "after the failed purge").await;
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
    // A write in flight holds the write lock: a purge cannot land inside
    // it. Issued while it is in flight, the purge waits, and deletes what
    // the write appended with the rest.
    let path = database();
    let s = stream(1);
    let writer = SqliteStore::open(&path).await.unwrap();
    append(&writer, s, 0, &[page(0, 1)]).await.unwrap();
    let purger = SqliteStore::open(&path).await.unwrap();
    let writing = inserting(&path, &page(1, 2)).await;

    let hasty = impatient(&path).await;
    let refused = hasty.purge(s).await;
    assert_eq!(
        refused.as_ref().map_err(code),
        Err(Some("angzarr-store-sqlite-unavailable")),
        "{refused:?}"
    );

    let (purged, ()) = tokio::join!(purger.purge(s), async {
        pause(Duration::from_millis(20)).await;
        end(writing, true).await
    });
    assert_eq!(purged, Ok(2), "appended, then purged");
    assert_eq!(writer.read(s, 0, None).await, Ok(Slice::default()));
    assert_eq!(writer.covers("tasks").await, Ok(vec![]));
    assert_eq!(
        append(&writer, s, 2, &[page(2, 3)]).await,
        Err(StoreError::Conflict {
            stream: Box::new(s.to_cover()),
            expected: 2,
            actual: 0
        })
    );
    drop((writer, purger, hasty));
    remove(&path);
}

#[tokio::test]
async fn an_append_racing_a_purge_lands_after_it_on_a_new_stream() {
    // A purge in flight holds the write lock: a write cannot land inside
    // it, and a reader still sees the stream whole. Issued while it is in
    // flight, the write waits, then finds no stream, and starts one at
    // sequence 0.
    let path = database();
    let s = stream(1);
    let store = SqliteStore::open(&path).await.unwrap();
    append(&store, s, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    let writer = SqliteStore::open(&path).await.unwrap();
    let purging = deleting(&path).await;

    let hasty = impatient(&path).await;
    let refused = append(&hasty, s, 2, &[page(2, 2)]).await;
    assert_eq!(
        refused.as_ref().map_err(code),
        Err(Some("angzarr-store-sqlite-unavailable")),
        "{refused:?}"
    );
    assert_eq!(
        hasty.read(s, 0, None).await.unwrap().next_sequence,
        2,
        "the purge not yet committed"
    );

    let fresh = [page(0, 2)];
    let (appended, ()) = tokio::join!(append(&writer, s, 0, &fresh), async {
        pause(Duration::from_millis(20)).await;
        end(purging, true).await
    });
    assert_eq!(appended, Ok(1), "purged, then appended at 0");
    assert_eq!(pages(&writer, s).await, [page(0, 2)]);
    assert_eq!(writer.covers("tasks").await, Ok(covers(&[s])));
    drop((store, writer, hasty));
    remove(&path);
}

// ------------------------------------------------------- reading at once

/// `slice`, read from 0, holds together: its pages reach the head it
/// names, and its last `recorded_at` is its last page's.
fn holds_together(slice: &Slice) {
    assert_eq!(slice.pages.len() as u64, slice.next_sequence, "{slice:?}");
    assert_eq!(
        slice.last_recorded_at,
        slice
            .pages
            .last()
            .and_then(|p| p.meta.as_ref())
            .and_then(|m| m.recorded_at),
        "{slice:?}"
    );
}

/// Read `s` from 0 through `reader` over and over while `churn` writes it
/// through another handle on its own task: every read must hold together.
async fn read_while_churning<F>(path: &Path, churn: fn(SqliteStore) -> F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let writer = SqliteStore::open(path).await.unwrap();
    let reader = SqliteStore::open_existing(path).await.unwrap().unwrap();
    let churning = tokio::spawn(churn(writer));
    for _ in 0..300 {
        holds_together(&reader.read(stream(1), 0, None).await.unwrap());
    }
    churning.abort();
    let _ = churning.await;
    drop(reader);
}

/// A read on its own is one moment: a purge committing between its
/// statements is not seen, so it never answers a stream of some length
/// holding fewer pages. (Reworked: the old test forced the interleaving
/// through a hook in a private runner the ports give no way to reach; here
/// another handle purges and refills the stream on its own task throughout
/// the reads.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_outside_a_unit_is_one_moment_across_a_purge() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    read_while_churning(&path, |writer| async move {
        loop {
            append(&writer, stream(1), 0, &[page(0, 1), page(1, 2)])
                .await
                .unwrap();
            writer.purge(stream(1)).await.unwrap();
        }
    })
    .await;
    remove(&path);
}

/// A read on its own is one moment: an append committing between its
/// statements is not seen, so its pages end at its head and its last
/// `recorded_at` is its last page's. (Reworked as the one above.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_outside_a_unit_is_one_moment_across_an_append() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    read_while_churning(&path, |writer| async move {
        for sequence in 0.. {
            append(&writer, stream(1), sequence, &[page(sequence, 1)])
                .await
                .unwrap();
        }
    })
    .await;
    remove(&path);
}

// --------------------------------------------------- reading one moment

/// Run the pair check `check` on a reader and a writer on one new file,
/// the reader from `open_reader`, then remove the file.
async fn on_file<F>(
    open_reader: impl AsyncFnOnce(&Path) -> SqliteStore,
    check: impl FnOnce((SqliteStore, SqliteStore)) -> F,
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

/// The ports' read-unit checks on a file, the reader opened to write or
/// only to read.
mod read_on_file {
    use angzarr_ports::contract as checks;

    use super::{on_file, read_only_store, writable};

    #[tokio::test]
    async fn a_read_unit_sees_the_store_as_it_began() {
        on_file(writable, checks::a_read_unit_sees_the_store_as_it_began).await;
    }

    #[tokio::test]
    #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
    async fn a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main() {
        on_file(
            writable,
            checks::a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
        )
        .await;
    }

    #[tokio::test]
    #[ignore = "editions are not implemented yet in the SQLite adapter (owner ruling 2026-10-28); a follow-up; MemoryStore covers them"]
    async fn a_read_only_stores_read_unit_refuses_an_unknown_edition() {
        on_file(
            read_only_store,
            checks::a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
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

    #[tokio::test]
    async fn a_read_unit_on_a_read_only_store_heads_and_snapshots_as_it_began() {
        on_file(
            read_only_store,
            checks::a_read_unit_heads_and_snapshots_as_it_began,
        )
        .await;
    }
}

#[tokio::test]
async fn a_writer_waits_on_no_open_read_unit() {
    // The writer waits on no lock at all: had the read unit taken the
    // write lock, its write would fail at once.
    let path = database();
    let s = stream(1);
    let writable_reader = SqliteStore::open(&path).await.unwrap();
    let read_only_reader = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let hasty = impatient(&path).await;
    {
        let mut first = writable_reader.begin_read().await.unwrap();
        let mut second = read_only_reader.begin_read().await.unwrap();
        assert_eq!(append(&hasty, s, 0, &[page(0, 1)]).await, Ok(1));
        assert_eq!(hasty.purge(s).await, Ok(1));
        assert_eq!(first.covers("tasks").await, Ok(vec![]));
        assert_eq!(second.read(s, 0, None).await, Ok(Slice::default()));
    }
    drop((writable_reader, read_only_reader, hasty));
    remove(&path);
}

// ------------------------------------------------------ commit position

/// The ports' two-handle commit position check on a file, the second
/// handle opened to write or only to read.
mod position_on_file {
    use angzarr_ports::contract as checks;

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
    append(
        &store,
        main_stream("account", 1),
        0,
        &[page(0, 1), page(1, 1)],
    )
    .await
    .unwrap();
    let first = store.commit_position().await.unwrap();
    append(&store, main_stream("ledger", 1), 0, &[page(0, 2)])
        .await
        .unwrap();
    let second = store.commit_position().await.unwrap();
    let stamped = positions(&mut store).await;
    let at = |i: usize| u64::try_from(stamped[i].2).unwrap();
    assert_eq!(stamped.len(), 3);
    assert_eq!(at(0), at(1), "one write, one position");
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
    store.purge(main_stream("account", 1)).await.unwrap();
    drop(store);
    let reopened = SqliteStore::open(&path).await.unwrap();
    assert_eq!(reopened.commit_position().await, Ok(second));
    drop(reopened);
    remove(&path);
}

/// A refused write lands nothing: no event stamped, no position moved.
#[tokio::test]
async fn a_refused_write_stamps_nothing_and_moves_no_position() {
    let mut store = SqliteStore::open_in_memory().await.unwrap();
    let before = store.commit_position().await.unwrap();
    let refused = append(&store, main_stream("account", 1), 3, &[page(3, 1)]).await;
    assert!(
        matches!(refused, Err(StoreError::Conflict { .. })),
        "{refused:?}"
    );
    assert_eq!(store.commit_position().await, Ok(before));
    assert_eq!(positions(&mut store).await, vec![]);
}

// ------------------------------------------------------------- watching

/// The ports' change-token checks on a file, the watcher opened to write
/// or only to read.
mod watch_on_file {
    use angzarr_ports::contract as checks;

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

#[tokio::test]
async fn a_change_token_waits_on_no_writer() {
    // The watchers wait on no lock at all: had asking taken the write lock,
    // or waited on the write holding it, it would fail at once.
    let path = database();
    let s = stream(1);
    let writer = SqliteStore::open(&path).await.unwrap();
    let hasty = impatient(&path).await;
    let read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let hasty_before = hasty.change_token().await.unwrap();
    let read_only_before = read_only.change_token().await.unwrap();
    let writing = inserting(&path, &page(0, 1)).await;
    assert_eq!(hasty.change_token().await, Ok(hasty_before), "uncommitted");
    assert_eq!(read_only.change_token().await, Ok(read_only_before));
    end(writing, true).await;
    assert_ne!(hasty.change_token().await, Ok(hasty_before), "committed");
    assert_ne!(read_only.change_token().await, Ok(read_only_before));
    assert_eq!(pages(&writer, s).await.len(), 1);
    drop((writer, hasty, read_only));
    remove(&path);
}

#[tokio::test]
async fn a_change_token_holds_across_a_write_rolled_back_elsewhere() {
    let path = database();
    let watcher = SqliteStore::open(&path).await.unwrap();
    let before = watcher.change_token().await.unwrap();
    end(inserting(&path, &page(0, 1)).await, false).await;
    assert_eq!(watcher.change_token().await, Ok(before));
    drop(watcher);
    remove(&path);
}

#[tokio::test]
async fn a_change_token_changes_when_a_snapshot_is_written() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let before = store.change_token().await.unwrap();
    store
        .write_snapshot(stream(1), &snapshot(0, "v1"))
        .await
        .unwrap();
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
    drop(send(async {
        let store = SqliteStore::open_in_memory().await?;
        store.purge(stream(1)).await
    }));
}

#[test]
fn every_read_unit_is_a_send_future() {
    let path = database();
    drop(send(async {
        let store = SqliteStore::open_in_memory().await?;
        let mut reads = store.begin_read().await?;
        reads.covers("tasks").await?;
        reads.read(stream(1), 0, None).await
    }));
    drop(send(async {
        let Some(store) = SqliteStore::open_existing(&path).await? else {
            return Ok(Slice::default());
        };
        let mut reads = store.begin_read().await?;
        reads.read(stream(1), 0, None).await
    }));
}

#[test]
fn every_change_token_is_a_send_future() {
    let path = database();
    drop(send(async {
        let store = SqliteStore::open_in_memory().await?;
        store.change_token().await
    }));
    drop(send(async {
        let Some(store) = SqliteStore::open_existing(&path).await? else {
            return Ok(None);
        };
        store.change_token().await.map(Some)
    }));
}
