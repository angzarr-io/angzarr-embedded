//! SQLite: the shared statements rendered by sea-query's SQLite builder and
//! run on sqlx, over one connection per handle; the schema created by
//! `migrations/sqlite/`. There is no unit of work: a write is one
//! `BEGIN IMMEDIATE` transaction, begun and committed inside the call, so
//! nothing is held open between a step's reads and its write; two handles
//! on one file are two connections, and SQLite's write lock orders their
//! writes. A read of more than one statement runs in a deferred `BEGIN` of
//! its own, so it reads one moment, and a read-only unit is a deferred
//! `BEGIN` held open over many reads.
//! A file is kept in WAL mode, so once it is made a reader waits on no
//! writer. A change token is the connection's `PRAGMA data_version` with
//! its `total_changes()`. The commit position is a one-row table raised once
//! by each commit that appends an event, and every event row is stamped
//! with the position it landed at.

use std::future::Future;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use angzarr_ports::angzarr_proto::v1::{Cover, EventBook, EventPage, Snapshot};
use angzarr_ports::{
    ChangeToken, EventStore, Head, ListStreams, Outbox, OutboxEntry, PositionKey, PositionStore,
    Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice, SnapshotStore, StoreError,
    StreamRef, Uuid, Watch, Write, Written,
};
use prost_types::Timestamp;
use sqlx::migrate::{MigrateError, Migration, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};

use crate::error::{damaged, unavailable, unmigrated, Damage};
use crate::{Dialect, Integrity};

/// SQLite keeps NULLs distinct in a key, so the main timeline is `''`.
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn edition(cover: &Cover) -> String {
        cover.edition.clone().unwrap_or_default()
    }

    fn cover_edition(stored: String) -> Option<String> {
        (!stored.is_empty()).then_some(stored)
    }
}

/// The schema, `migrations/sqlite/`. `open` runs them inside `BEGIN
/// IMMEDIATE`, so none may opt out of a transaction (`-- no-transaction`)
/// or change `PRAGMA journal_mode`: neither can run inside one.
static MIGRATOR: Migrator = sqlx::migrate!("./migrations/sqlite");

/// How long a connection waits on another's lock before failing: SQLite's
/// busy timeout. Readers wait on no one (WAL), so only writers queue, and a
/// unit of work holds the lock for milliseconds; five seconds outlasts a
/// queue of many processes' units, and is short enough that a lock held by
/// something stuck surfaces as an error rather than a hang.
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// How many times `open` tries to move a file into WAL before failing.
/// Each try after the first waits (up to [`BUSY_TIMEOUT`]) until no other
/// connection holds a lock, so only stores making the same move at the same
/// instant take more than two.
const WAL_ATTEMPTS: usize = 20;

/// A store in one SQLite database: one handle, one connection.
pub struct SqliteStore {
    conn: SqliteConnection,
}

impl SqliteStore {
    /// How many statements this handle has sent SQLite (`BEGIN` and
    /// `COMMIT` included): what a command costs in round trips, for its
    /// budget and benchmarks.
    pub fn round_trips(&self) -> u64 {
        todo!("red: count the statements a handle sends")
    }

    /// The database at `path`, created if absent, in WAL mode, and migrated.
    ///
    /// A database whose migrations are all applied is opened without the
    /// write lock, so an open waits on no writer. Otherwise the migrations
    /// run inside `BEGIN IMMEDIATE`: sqlx's migrator takes no lock on
    /// SQLite and reads which migrations are applied before it begins each
    /// one, so stores opening a new file at once (one per process) would
    /// each apply the first migration. Under the lock they take turns, and
    /// each after the first finds nothing to apply. A migration that opts
    /// out of a transaction (`-- no-transaction`) or changes `PRAGMA
    /// journal_mode` therefore cannot be added.
    ///
    /// A file too damaged to open, one SQLite finds malformed or no
    /// database at all, is refused as damage (`angzarr-store-sqlite-damaged-file`),
    /// its one problem SQLite's message, and is left as it was.
    pub async fn open(path: &Path) -> Result<SqliteStore, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .busy_timeout(BUSY_TIMEOUT)
            .create_if_missing(true);
        let mut conn = options.connect().await.map_err(opening)?;
        // A read first, so the connection knows a file already in WAL is,
        // and moving it there is a no-op that waits on no writer.
        let schema = schema(&mut conn).await?;
        into_wal(&mut conn).await?;
        if !matches!(schema, Schema::Current) {
            migrate(&mut conn).await?;
        }
        Ok(SqliteStore { conn })
    }

    /// The database at `path` opened read-only, or `None` if there is no
    /// file there: nothing is created, migrated, or moved into WAL, so a
    /// read-only file opens. Every write to the store fails
    /// (`angzarr-store-sqlite-unavailable`).
    ///
    /// A store's file is in WAL, so reading it needs a directory this
    /// process can write, even though nothing is written: SQLite reads a
    /// WAL file through its `-wal` and `-shm` files beside it, and makes
    /// them if they are not there. In a directory it cannot write, the file
    /// opens only while a writer holds it open, and so keeps those files
    /// there; once the last writer closes it, the open fails
    /// (`angzarr-store-sqlite-unavailable`). WAL is kept because without it a
    /// read of one moment, or a read-only unit of work, would wait on a
    /// writer and hold one up. A store on a read-only mount is the caller's
    /// to arrange with its own tooling (copying the file somewhere
    /// writable, say): this store does not read one.
    ///
    /// A file is opened once it is *usable*: it holds a committed database
    /// with every one of this build's migrations applied. A file that holds
    /// nothing committed (no table, index or anything else in its schema)
    /// is one a writer's [`open`](SqliteStore::open) is still making. From
    /// creating the file until its migration commits, the writer leaves it
    /// zero bytes long, then in WAL with only its header, then migrated
    /// under a lock not yet let go, and a read meets the same empty
    /// database at each step. Such a file is read again until it is
    /// usable, waiting up to [`BUSY_TIMEOUT`] as a lock is waited for. A
    /// file still empty after that has no writer finishing it, and is
    /// refused as unmigrated (`angzarr-store-sqlite-unmigrated`).
    ///
    /// A database that holds something committed but has migrations not
    /// yet applied is older than this build, and is refused as unmigrated
    /// at once, with nothing waited for. So is one whose applied migrations
    /// this build's disagree with (`angzarr-store-sqlite-unavailable`).
    ///
    /// A file too damaged to open, one SQLite finds malformed
    /// (`SQLITE_CORRUPT`) or no database at all (`SQLITE_NOTADB`), is
    /// refused as damage (`angzarr-store-sqlite-damaged`, the code
    /// [`integrity_check`](SqliteStore::integrity_check) reports damage
    /// under), its one problem SQLite's message. It is not waited on, and
    /// is told from every other failure to open.
    pub async fn open_existing(path: &Path) -> Result<Option<SqliteStore>, StoreError> {
        read_only(path, BUSY_TIMEOUT, || {}).await
    }

    /// Whether the database file is sound: SQLite's `PRAGMA
    /// integrity_check`, its problems returned as it reports them (up to
    /// its limit of 100). A read: it takes no write lock, so it runs on a
    /// store opened by [`open_existing`](SqliteStore::open_existing) and
    /// waits on no writer, and it checks the last committed state.
    ///
    /// `integrity_check`, not `quick_check`: only it checks that every
    /// index holds exactly its table's rows, and the store answers a
    /// command's deduplication from an index, so a damaged index would
    /// pass `quick_check` and repeat a command. A store's file is small
    /// enough that the full check's cost does not matter.
    ///
    /// A page too damaged to check stops the check with `SQLITE_CORRUPT`:
    /// that is damage too, its one problem SQLite's message. Any other
    /// failure to run the check is the store's error
    /// (`angzarr-store-sqlite-unavailable`), not damage found.
    pub async fn integrity_check(&mut self) -> Result<Integrity, StoreError> {
        let reported: Vec<String> = match sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_all(&mut self.conn)
            .await
        {
            Ok(reported) => reported,
            Err(error) if primary_code(&error) == Some(SQLITE_CORRUPT) => {
                let message = error.as_database_error().map(|e| e.message().to_string());
                message.into_iter().collect()
            }
            Err(error) => return Err(unavailable(error)),
        };
        Ok(match reported.as_slice() {
            [only] if only == "ok" => Integrity::Intact,
            _ => Integrity::Damaged(Damage { problems: reported }),
        })
    }

    /// A database held only in memory, migrated.
    pub async fn open_in_memory() -> Result<SqliteStore, StoreError> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:").map_err(unavailable)?;
        let mut conn = options.connect().await.map_err(unavailable)?;
        migrate(&mut conn).await?;
        Ok(SqliteStore { conn })
    }
}

/// [`SqliteStore::open_existing`], waiting up to `patience` for a file being
/// made, and calling `waiting` each time it waits.
///
/// Whether the file exists is asked before it is opened, not after an open
/// fails: a writer may create it between the two, and the failed open
/// would then be taken for a failure to open a file that is there. A file
/// found is still there to open, since a database is created at its path
/// and never moved there.
async fn read_only(
    path: &Path,
    patience: Duration,
    mut waiting: impl FnMut(),
) -> Result<Option<SqliteStore>, StoreError> {
    if matches!(path.try_exists(), Ok(false)) {
        return Ok(None);
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(BUSY_TIMEOUT)
        .read_only(true);
    let mut conn = options.connect().await.map_err(opening)?;
    let deadline = Instant::now() + patience;
    let mut wait = FIRST_WAIT;
    loop {
        // Emptiness first: once a file holds something committed it never
        // holds nothing again, so the schema read after sees a committed
        // database, not one a commit lands in the middle of.
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() || !empty(&mut conn).await? {
            return match schema(&mut conn).await? {
                Schema::Current => Ok(Some(SqliteStore { conn })),
                Schema::Pending(pending) => Err(unmigrated(pending)),
                Schema::Refused(error) => Err(unavailable(error)),
            };
        }
        waiting();
        pause(wait.min(left)).await;
        wait = (wait * 2).min(LONGEST_WAIT);
    }
}

/// How long a read waits for a file being made before it looks again,
/// doubling each time it waits, up to [`LONGEST_WAIT`]. A writer makes a
/// file in milliseconds, so most reads look twice or three times.
const FIRST_WAIT: Duration = Duration::from_millis(1);

/// The longest a read waits between looks at a file being made.
const LONGEST_WAIT: Duration = Duration::from_millis(50);

/// Whether the database holds nothing committed: no table, index or
/// anything else in its schema.
async fn empty(conn: &mut SqliteConnection) -> Result<bool, StoreError> {
    let entries: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master")
        .fetch_one(&mut *conn)
        .await
        .map_err(opening)?;
    Ok(entries == 0)
}

/// A future ready once `duration` has passed. The crate drives no runtime
/// and so has no timer to ask for: a thread sleeps the time out and wakes
/// the task.
fn pause(duration: Duration) -> Pause {
    Pause {
        until: Instant::now() + duration,
        waker: None,
    }
}

/// See [`pause`].
struct Pause {
    until: Instant,
    /// The task to wake, shared with the sleeping thread once it is started.
    waker: Option<Arc<Mutex<Option<Waker>>>>,
}

impl Future for Pause {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.until {
            return Poll::Ready(());
        }
        let waker = Some(cx.waker().clone());
        match &self.waker {
            Some(shared) => *shared.lock().unwrap_or_else(PoisonError::into_inner) = waker,
            None => {
                let shared = Arc::new(Mutex::new(waker));
                let woken = Arc::clone(&shared);
                let until = self.until;
                std::thread::spawn(move || {
                    std::thread::sleep(until.saturating_duration_since(Instant::now()));
                    let waker = woken.lock().unwrap_or_else(PoisonError::into_inner).take();
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                });
                self.waker = Some(shared);
            }
        }
        Poll::Pending
    }
}

/// How a database's applied migrations stand against [`MIGRATOR`].
enum Schema {
    /// Every migration is applied, as this build has it.
    Current,
    /// This many migrations are not yet applied; those that are agree.
    Pending(usize),
    /// What sqlx's migrator would refuse: a migration that failed, one this
    /// build does not have, or one changed since it was applied.
    Refused(MigrateError),
}

/// Where the database's migrations stand, read without taking a lock.
///
/// From sqlx's own record, `_sqlx_migrations`, rather than `PRAGMA
/// user_version`: it is what the migrator checks, so the two cannot
/// disagree, and it carries each migration's checksum, so a changed
/// migration is refused here as the migrator would refuse it.
async fn schema(conn: &mut SqliteConnection) -> Result<Schema, StoreError> {
    let recorded: Option<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(opening)?;
    let applied: Vec<(i64, Vec<u8>, bool)> = match recorded {
        Some(_) => sqlx::query_as("SELECT version, checksum, success FROM _sqlx_migrations")
            .fetch_all(&mut *conn)
            .await
            .map_err(opening)?,
        None => Vec::new(),
    };
    let wanted: Vec<&Migration> = MIGRATOR
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .collect();
    for (version, checksum, success) in &applied {
        if !success {
            return Ok(Schema::Refused(MigrateError::Dirty(*version)));
        }
        match wanted.iter().find(|m| m.version == *version) {
            None => return Ok(Schema::Refused(MigrateError::VersionMissing(*version))),
            Some(m) if *m.checksum != **checksum => {
                return Ok(Schema::Refused(MigrateError::VersionMismatch(*version)))
            }
            Some(_) => {}
        }
    }
    let pending = wanted
        .iter()
        .filter(|m| !applied.iter().any(|(version, _, _)| *version == m.version))
        .count();
    Ok(match pending {
        0 => Schema::Current,
        n => Schema::Pending(n),
    })
}

/// Apply what is pending holding the write lock, waiting
/// [`BUSY_TIMEOUT`] for it. The migrator reads again which migrations are
/// applied, under the lock, and refuses a database it disagrees with.
///
/// Through `Migrator::run_direct`, the connection itself, rather than
/// `Migrator::run`, which takes any `Acquire<'a>`: the compiler cannot
/// prove `run`'s future `Send` for every `'a` ("implementation of
/// `Acquire` is not general enough"), so no open calling it would be
/// `Send`, and boxing it does not help, the obligation being on `run`'s own
/// future. `run` acquires the connection and calls `run_direct`, so on a
/// connection the two do the same.
async fn migrate(conn: &mut SqliteConnection) -> Result<(), StoreError> {
    let mut tx = conn
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(unavailable)?;
    MIGRATOR.run_direct(&mut *tx).await.map_err(unavailable)?;
    tx.commit().await.map_err(unavailable)
}

/// Move a file into WAL mode. WAL is the file's own, kept across
/// connections, so this changes a file once and is a no-op after. The move
/// takes the exclusive lock without waiting the busy timeout, failing at
/// once if any connection holds a lock, so after such a failure `BEGIN
/// EXCLUSIVE` waits (the busy timeout) until none does, lets go, and the
/// move is tried again. It cannot run inside a transaction, so it runs
/// before migrating. A file system that cannot hold WAL's shared memory
/// leaves the file in its rollback journal, where the store still works
/// and a reader waits only on a writer's commit.
async fn into_wal(conn: &mut SqliteConnection) -> Result<(), StoreError> {
    for _ in 1..WAL_ATTEMPTS {
        match sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&mut *conn)
            .await
        {
            Err(error) if busy(&error) => {
                conn.begin_with("BEGIN EXCLUSIVE")
                    .await
                    .map_err(unavailable)?
                    .commit()
                    .await
                    .map_err(unavailable)?;
            }
            outcome => return outcome.map(drop).map_err(unavailable),
        }
    }
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&mut *conn)
        .await
        .map(drop)
        .map_err(unavailable)
}

/// Whether `error` is SQLite's `SQLITE_BUSY`, extended codes included.
fn busy(error: &sqlx::Error) -> bool {
    primary_code(error) == Some(SQLITE_BUSY)
}

/// SQLite's primary result code for `error`, its extended code's low byte:
/// `None` for an error not SQLite's own.
fn primary_code(error: &sqlx::Error) -> Option<i32> {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .and_then(|code| code.parse::<i32>().ok())
        .map(|code| code & 0xff)
}

/// An open's failure, as the port's: a file SQLite finds malformed
/// (`SQLITE_CORRUPT`) or no database at all (`SQLITE_NOTADB`) is damage
/// (`angzarr-store-sqlite-damaged-file`), its one problem SQLite's message, so a
/// caller can tell a file to repair or replace from a store that could not
/// say. Any other failure is `angzarr-store-sqlite-unavailable`.
fn opening(error: sqlx::Error) -> StoreError {
    match primary_code(&error) {
        Some(SQLITE_CORRUPT | SQLITE_NOTADB) => damaged(
            error
                .as_database_error()
                .map_or_else(String::new, |e| e.message().to_string()),
        ),
        _ => unavailable(error),
    }
}

/// SQLite's primary result code for a lock another connection holds.
const SQLITE_BUSY: i32 = 5;

/// SQLite's primary result code for a database file found malformed.
const SQLITE_CORRUPT: i32 = 11;

/// SQLite's primary result code for a file that is not a database.
const SQLITE_NOTADB: i32 = 26;

// ------------------------------------------------------------ the ports
//
// Every port and capability but editions (not implemented yet: the owner's
// ruling, 2026-10-28; an edition stream reads as main's and takes no
// write, as a stream never forked does). Each is a stub until the ports'
// suite is green on it.

impl EventStore for SqliteStore {
    /// One `BEGIN IMMEDIATE` transaction: the stream's length checked
    /// against `expected` under the write lock, its pages inserted with the
    /// commit position (raised once), the positions upserted and the outbox
    /// entries kept, then committed. A write to a stream in any edition is
    /// refused (`StoreError::EditionNotForked`): editions are not
    /// implemented, so no stream is ever forked.
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        let _ = write;
        todo!("red: EventStore::write on SQLite")
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        let _ = (stream, from, to);
        todo!("red: EventStore::read on SQLite")
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        let _ = stream;
        todo!("red: EventStore::head on SQLite")
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let _ = (stream, command_id);
        todo!("red: EventStore::find_command on SQLite")
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        let _ = domain;
        todo!("red: EventStore::covers on SQLite")
    }

    async fn commit_position(&self) -> Result<u64, StoreError> {
        todo!("red: EventStore::commit_position on SQLite")
    }
}

impl SnapshotStore for SqliteStore {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        let _ = stream;
        todo!("red: SnapshotStore::read_snapshot on SQLite")
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        let _ = (stream, snapshot);
        todo!("red: SnapshotStore::write_snapshot on SQLite")
    }
}

impl PositionStore for SqliteStore {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        let _ = key;
        todo!("red: PositionStore::position on SQLite")
    }
}

impl Purge for SqliteStore {
    /// One `BEGIN IMMEDIATE` transaction, as a write's: a purge lands wholly
    /// before or wholly after any write.
    async fn purge(&self, stream: StreamRef<'_>) -> Result<u64, StoreError> {
        let _ = stream;
        todo!("red: Purge::purge on SQLite")
    }
}

impl Outbox for SqliteStore {
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, StoreError> {
        let _ = limit;
        todo!("red: Outbox::pending on SQLite")
    }

    async fn clear(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        let _ = ids;
        todo!("red: Outbox::clear on SQLite")
    }
}

impl ReadStore for SqliteStore {
    type Reads<'s> = SqliteRead<'s>;

    /// A deferred `BEGIN`, which takes no lock, and a first read at once:
    /// under WAL a read transaction's moment is its first read's. It runs
    /// on a store opened by [`open_existing`](SqliteStore::open_existing),
    /// and waits on no writer.
    async fn begin_read(&self) -> Result<SqliteRead<'_>, StoreError> {
        todo!("red: ReadStore::begin_read on SQLite")
    }
}

/// A read-only unit: one SQLite read transaction, seeing the database as
/// it began. Dropped, it ends, having written nothing. Editions are not
/// implemented, so an edition stream reads as main's, and no edition
/// exists to be named (`StoreError::UnknownEdition`).
pub struct SqliteRead<'s> {
    _store: PhantomData<&'s SqliteStore>,
}

impl ReadUnit for SqliteRead<'_> {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        let _ = domain;
        todo!("red: ReadUnit::covers on SQLite")
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        let _ = (stream, from, to);
        todo!("red: ReadUnit::read on SQLite")
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        let _ = stream;
        todo!("red: ReadUnit::read_snapshot on SQLite")
    }
}

impl Watch for SqliteStore {
    /// The connection's `PRAGMA data_version` and its `total_changes()`,
    /// read in one statement: `data_version` changes when another
    /// connection commits, `total_changes()` with this one's own. Neither
    /// alone sees every commit; together they do. It reads no table, takes
    /// no write lock, and runs on a store opened by
    /// [`open_existing`](SqliteStore::open_existing).
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        todo!("red: Watch::change_token on SQLite")
    }
}

impl ReadUntilTime for SqliteStore {
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Slice, StoreError> {
        let _ = (stream, until);
        todo!("red: ReadUntilTime on SQLite (recorded_at column)")
    }
}

impl ReadByCorrelation for SqliteStore {
    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: Uuid,
    ) -> Result<Vec<EventBook>, StoreError> {
        let _ = (domain, edition, correlation_id);
        todo!("red: ReadByCorrelation on SQLite (correlation_id column)")
    }
}

impl ListStreams for SqliteStore {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        todo!("red: ListStreams on SQLite")
    }
}

#[cfg(test)]
#[path = "sqlite.test.rs"]
mod tests;
