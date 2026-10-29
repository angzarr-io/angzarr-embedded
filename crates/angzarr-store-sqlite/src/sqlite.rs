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
//! its `total_changes()`. There is no store-wide position (decision 14.35):
//! ordering is per stream, by sequence.

use std::future::Future;
use std::ops::DerefMut;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use angzarr_ports::angzarr_proto::v1::meta::EditionCreated;
use angzarr_ports::angzarr_proto::v1::{Cover, EventBook, EventPage, Snapshot};
use angzarr_ports::{
    Append, ChangeToken, EventStore, Head, ListStreams, Outbox, OutboxEntry, PositionKey,
    PositionStore, Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice,
    SnapshotStore, StoreError, StreamRef, Uuid, Watch, Write, Written, EDITION_CREATED_TYPE_URL,
};
use prost::Message;
use prost_types::Timestamp;
use sea_query::SqliteQueryBuilder;
use sea_query_binder::SqlxBinder;
use sqlx::migrate::{MigrateError, Migration, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteRow};
use sqlx::{ConnectOptions, Connection, Database, Row, Sqlite, TransactionManager};

use crate::error::{damaged, unavailable, undecodable, unmigrated, Damage};
use crate::{rows, statements, Dialect, Integrity};

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
    conn: Conn,
}

/// A handle's one connection, behind an async lock so the ports take
/// `&self`: one call at a time runs on it, each holding it only for its own
/// statements (a write's whole transaction, a read unit's life). With the
/// count of statements sent through it.
pub(crate) struct Conn {
    lock: tokio::sync::Mutex<SqliteConnection>,
    sent: AtomicU64,
}

impl From<SqliteConnection> for Conn {
    fn from(conn: SqliteConnection) -> Conn {
        Conn {
            lock: tokio::sync::Mutex::new(conn),
            sent: AtomicU64::new(0),
        }
    }
}

impl Conn {
    /// The connection, the handle given up.
    #[cfg(test)]
    pub(crate) fn into_inner(self) -> SqliteConnection {
        self.lock.into_inner()
    }

    /// The connection, for a caller holding the handle exclusively.
    pub(crate) fn get_mut(&mut self) -> &mut SqliteConnection {
        self.lock.get_mut()
    }

    /// The connection, once no other call holds it, with the count its
    /// statements are added to.
    async fn exec(&self) -> Exec<'_, tokio::sync::MutexGuard<'_, SqliteConnection>> {
        Exec {
            conn: self.lock.lock().await,
            sent: &self.sent,
        }
    }
}

impl SqliteStore {
    /// How many statements this handle has sent SQLite (`BEGIN` and
    /// `COMMIT` included): what a command costs in round trips, for its
    /// budget and benchmarks.
    pub fn round_trips(&self) -> u64 {
        self.conn.sent.load(Ordering::Relaxed)
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
        Ok(SqliteStore { conn: conn.into() })
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
            .fetch_all(self.conn.get_mut())
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
        Ok(SqliteStore { conn: conn.into() })
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
                Schema::Current => Ok(Some(SqliteStore { conn: conn.into() })),
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
    backfill(&mut tx).await?;
    tx.commit().await.map_err(unavailable)
}

/// An event row as stored: domain, edition, root, sequence, page.
type StoredRow = (String, String, Vec<u8>, i64, Vec<u8>);

/// Fill the events' correlation and time columns from their pages, once:
/// what migration 0003 added, which SQL cannot fill (it decodes a page).
/// Run under the migration's lock, the first time a store migrates the
/// file past 0003, and marked done, so a reopen scans nothing again.
async fn backfill(conn: &mut SqliteConnection) -> Result<(), StoreError> {
    let done: i64 = sqlx::query_scalar(statements::BACKFILL_DONE)
        .bind(statements::EVENTS_META_BACKFILL)
        .fetch_one(&mut *conn)
        .await
        .map_err(unavailable)?;
    if done != 0 {
        return Ok(());
    }
    let unfilled: Vec<StoredRow> = sqlx::query_as(statements::UNFILLED)
        .fetch_all(&mut *conn)
        .await
        .map_err(unavailable)?;
    for (domain, edition, root, sequence, bytes) in unfilled {
        let cover = rows::cover::<SqliteDialect>(&domain, edition.clone(), &root)?;
        let page = rows::page(&cover, sequence, &bytes)?;
        let (correlation_id, recorded_at) = statements::meta_columns(&page);
        sqlx::query(statements::FILL)
            .bind(domain)
            .bind(edition)
            .bind(root)
            .bind(sequence)
            .bind(correlation_id)
            .bind(recorded_at)
            .execute(&mut *conn)
            .await
            .map_err(unavailable)?;
    }
    sqlx::query(statements::BACKFILL_RAN)
        .bind(statements::EVENTS_META_BACKFILL)
        .execute(&mut *conn)
        .await
        .map_err(unavailable)?;
    Ok(())
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
        Some(code @ (SQLITE_CORRUPT | SQLITE_NOTADB)) => damaged(
            code as u64,
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
// Every port and capability. Editions are per-stream forks only as far as
// the base ports need them: an edition stream's first own event is its
// creation (`meta.EditionCreated`), it reads main below its fork, then its
// own; a write of anything else to an unforked one is refused. The
// administration of editions (the `Editions` port:
// listing, deletion and removal) is not implemented yet (the owner's
// ruling, 2026-10-28): a follow-up.

/// A value bound to a statement's parameter.
enum Value {
    Text(String),
    Blob(Vec<u8>),
    Int(i64),
    MaybeText(Option<String>),
}

/// The view's three parameters for `stream`: domain, root, edition column.
fn view_of(stream: StreamRef<'_>) -> [Value; 3] {
    [
        Value::Text(stream.domain.to_string()),
        Value::Blob(stream.root.as_bytes().to_vec()),
        Value::Text(stream.edition.unwrap_or_default().to_string()),
    ]
}

/// Statements run on one connection, each counted.
struct Exec<'a, C> {
    conn: C,
    sent: &'a AtomicU64,
}

impl<C: DerefMut<Target = SqliteConnection>> Exec<'_, C> {
    fn count(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }

    async fn rows(
        &mut self,
        sql: &str,
        values: impl IntoIterator<Item = Value>,
    ) -> Result<Vec<SqliteRow>, StoreError> {
        let mut query = sqlx::query(sql);
        for value in values {
            query = match value {
                Value::Text(text) => query.bind(text),
                Value::Blob(blob) => query.bind(blob),
                Value::Int(int) => query.bind(int),
                Value::MaybeText(text) => query.bind(text),
            };
        }
        self.count();
        query.fetch_all(&mut *self.conn).await.map_err(unavailable)
    }

    async fn execute(
        &mut self,
        sql: &str,
        values: impl IntoIterator<Item = Value>,
    ) -> Result<u64, StoreError> {
        let mut query = sqlx::query(sql);
        for value in values {
            query = match value {
                Value::Text(text) => query.bind(text),
                Value::Blob(blob) => query.bind(blob),
                Value::Int(int) => query.bind(int),
                Value::MaybeText(text) => query.bind(text),
            };
        }
        self.count();
        let done = query.execute(&mut *self.conn).await.map_err(unavailable)?;
        Ok(done.rows_affected())
    }

    async fn built(&mut self, statement: impl SqlxBinder) -> Result<Vec<SqliteRow>, StoreError> {
        let (sql, values) = statement.build_sqlx(SqliteQueryBuilder);
        self.count();
        sqlx::query_with(&sql, values)
            .fetch_all(&mut *self.conn)
            .await
            .map_err(unavailable)
    }

    async fn built_execute(&mut self, statement: impl SqlxBinder) -> Result<u64, StoreError> {
        let (sql, values) = statement.build_sqlx(SqliteQueryBuilder);
        self.count();
        let done = sqlx::query_with(&sql, values)
            .execute(&mut *self.conn)
            .await
            .map_err(unavailable)?;
        Ok(done.rows_affected())
    }

    /// `(sequence, page)` rows as `stream`'s pages.
    async fn pages(
        &mut self,
        stream: StreamRef<'_>,
        sql: &str,
        values: impl IntoIterator<Item = Value>,
    ) -> Result<Vec<EventPage>, StoreError> {
        let cover = stream.to_cover();
        let mut pages = Vec::new();
        for row in self.rows(sql, values).await? {
            let sequence: i64 = row.try_get(0).map_err(unavailable)?;
            let bytes: Vec<u8> = row.try_get(1).map_err(unavailable)?;
            pages.push(rows::page(&cover, sequence, &bytes)?);
        }
        Ok(pages)
    }

    /// The stream's head as read in its edition: one past its last page,
    /// and that page's instant.
    async fn head(&mut self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        let last = self
            .pages(stream, statements::LAST, view_of(stream))
            .await?;
        Ok(head_after(last.last()))
    }

    /// The pages `from <= sequence < to`, with the stream's head.
    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        let [domain, root, edition] = view_of(stream);
        let to = to.map_or(i64::MAX, statements::sequence_column);
        let range = [
            domain,
            root,
            edition,
            Value::Int(statements::sequence_column(from)),
            Value::Int(to),
        ];
        let pages = self.pages(stream, statements::PAGES, range).await?;
        let head = self.head(stream).await?;
        Ok(Slice {
            next_sequence: head.next_sequence,
            last_recorded_at: head.last_recorded_at,
            pages,
        })
    }

    async fn find_command(
        &mut self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let [domain, root, edition] = view_of(stream);
        let id = Value::Blob(command_id.as_bytes().to_vec());
        self.pages(stream, statements::OF_COMMAND, [domain, root, edition, id])
            .await
    }

    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        let mut covers = Vec::new();
        for row in self
            .rows(statements::COVERS, [Value::Text(domain.to_string())])
            .await?
        {
            let edition: String = row.try_get(0).map_err(unavailable)?;
            let root: Vec<u8> = row.try_get(1).map_err(unavailable)?;
            covers.push(rows::cover::<SqliteDialect>(domain, edition, &root)?);
        }
        Ok(covers)
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        let cover = stream.to_cover();
        let rows = self
            .built(statements::snapshot_of::<SqliteDialect>(&cover))
            .await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let bytes: Vec<u8> = row.try_get(0).map_err(unavailable)?;
        rows::snapshot(&cover, &bytes).map(Some)
    }

    /// The events of a write: checked against the stream as read in its
    /// edition, then inserted.
    async fn append(&mut self, append: Append<'_>) -> Result<u64, StoreError> {
        let Append {
            stream,
            expected,
            pages,
        } = append;
        let actual = self.head(stream).await?.next_sequence;
        let creates = pages.first().is_some_and(is_creation);
        match stream.edition {
            None if creates => return Err(StoreError::EditionNameRequired),
            None => {}
            Some(edition) => {
                let own = [
                    Value::Text(stream.domain.to_string()),
                    Value::Text(edition.to_string()),
                    Value::Blob(stream.root.as_bytes().to_vec()),
                ];
                let first: Option<i64> = self
                    .rows(statements::OWN_FIRST, own)
                    .await?
                    .first()
                    .map(|row| row.try_get(0))
                    .transpose()
                    .map_err(unavailable)?
                    .flatten();
                match (first.is_some(), creates) {
                    (true, true) => return Err(StoreError::edition_already_forked(stream)),
                    (false, false) if !pages.is_empty() => {
                        return Err(StoreError::edition_not_forked(stream))
                    }
                    // A fork lands at its fork sequence, as stated
                    // (`EditionCreated.parent_sequence`): main's head or
                    // any sequence below it. A creation stating another
                    // sequence than its own is malformed (14.36).
                    (false, true) => {
                        let page = &pages[0];
                        let stated = parent_sequence(page);
                        if stated != page.sequence {
                            return Err(StoreError::malformed_creation(
                                stream,
                                page.sequence,
                                stated,
                            ));
                        }
                        if expected <= actual {
                            return self.insert(stream, expected, pages).await;
                        }
                    }
                    _ => {}
                }
            }
        }
        if expected != actual {
            return Err(StoreError::conflict(stream, expected, actual));
        }
        self.insert(stream, expected, pages).await
    }

    /// `pages`, continuing the stream from `expected`, inserted: the
    /// stream's new length.
    async fn insert(
        &mut self,
        stream: StreamRef<'_>,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        for (due, page) in (expected..).zip(pages) {
            if page.sequence != due {
                return Err(StoreError::discontinuous(stream, due, page.sequence));
            }
        }
        if pages.is_empty() {
            return Ok(expected);
        }
        let cover = stream.to_cover();
        self.built_execute(statements::insert_pages::<SqliteDialect>(&cover, pages)?)
            .await?;
        Ok(expected + pages.len() as u64)
    }

    async fn put_position(&mut self, key: PositionKey<'_>, next: u64) -> Result<(), StoreError> {
        let [domain, root, edition] = view_of(key.stream);
        let values = [
            domain,
            edition,
            root,
            Value::Text(key.handler.to_string()),
            Value::Int(statements::sequence_column(next)),
        ];
        self.execute(statements::PUT_POSITION, values)
            .await
            .map(drop)
    }

    async fn position(&mut self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        let [domain, root, edition] = view_of(key.stream);
        let values = [domain, edition, root, Value::Text(key.handler.to_string())];
        let rows = self.rows(statements::POSITION, values).await?;
        let next: i64 = match rows.first() {
            Some(row) => row.try_get(0).map_err(unavailable)?,
            None => 0,
        };
        Ok(next.unsigned_abs())
    }

    async fn put_outbox(&mut self, entry: &OutboxEntry) -> Result<(), StoreError> {
        let values = [
            Value::Blob(entry.id.as_bytes().to_vec()),
            Value::Text(entry.origin.clone()),
            Value::Blob(entry.message.encode_to_vec()),
            Value::MaybeText(entry.edition.clone()),
        ];
        self.execute(statements::PUT_OUTBOX, values).await.map(drop)
    }
}

/// The head after `last`, a stream's last page: 0 and no instant for none.
fn head_after(last: Option<&EventPage>) -> Head {
    Head {
        next_sequence: last.map_or(0, |page| page.sequence + 1),
        last_recorded_at: last
            .and_then(|page| page.meta.as_ref())
            .and_then(|meta| meta.recorded_at),
    }
}

/// The sequence a creation page's `EditionCreated` states as its fork
/// point (0 for one that does not decode, which no fork states).
fn parent_sequence(page: &EventPage) -> u64 {
    page.payload
        .as_ref()
        .and_then(|p| EditionCreated::decode(p.value.as_slice()).ok())
        .map_or(0, |created| created.parent_sequence)
}

/// Whether `page` is a stream's creation in its edition.
fn is_creation(page: &EventPage) -> bool {
    page.payload
        .as_ref()
        .is_some_and(|payload| payload.type_url == EDITION_CREATED_TYPE_URL)
}

impl EventStore for SqliteStore {
    /// One `BEGIN IMMEDIATE` transaction, begun and committed inside the
    /// call: the stream's length checked against `expected` under the write
    /// lock, its pages inserted, the
    /// positions upserted and the outbox entries kept. Nothing is held open
    /// after it returns; a failure lands nothing.
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        let Exec { mut conn, sent } = self.conn.exec().await;
        sent.fetch_add(1, Ordering::Relaxed);
        let tx = conn
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        let mut ex = Exec { conn: tx, sent };
        let next_sequence = match write.events {
            Some(append) => Some(ex.append(append).await?),
            None => None,
        };
        for (key, next) in write.positions {
            ex.put_position(*key, *next).await?;
        }
        for entry in write.outbox {
            ex.put_outbox(entry).await?;
        }
        ex.count();
        ex.conn.commit().await.map_err(unavailable)?;
        Ok(Written { next_sequence })
    }

    /// One statement when the read runs to the end from 0, its last page
    /// the head; otherwise the range and the head in one read transaction,
    /// so they are one moment.
    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        let mut ex = self.conn.exec().await;
        if from == 0 && to.is_none() {
            let [domain, root, edition] = view_of(stream);
            let all = [domain, root, edition, Value::Int(0), Value::Int(i64::MAX)];
            let pages = ex.pages(stream, statements::PAGES, all).await?;
            let head = head_after(pages.last());
            return Ok(Slice {
                next_sequence: head.next_sequence,
                last_recorded_at: head.last_recorded_at,
                pages,
            });
        }
        ex.count();
        let tx = ex.conn.begin().await.map_err(unavailable)?;
        let mut moment = Exec {
            conn: tx,
            sent: ex.sent,
        };
        let slice = moment.read(stream, from, to).await?;
        moment.count();
        moment.conn.commit().await.map_err(unavailable)?;
        Ok(slice)
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        self.conn.exec().await.head(stream).await
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.conn
            .exec()
            .await
            .find_command(stream, command_id)
            .await
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.conn.exec().await.covers(domain).await
    }
}

impl SnapshotStore for SqliteStore {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        self.conn.exec().await.read_snapshot(stream).await
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        let cover = stream.to_cover();
        self.conn
            .exec()
            .await
            .built_execute(statements::upsert_snapshot::<SqliteDialect>(
                &cover, snapshot,
            )?)
            .await
            .map(drop)
    }
}

impl PositionStore for SqliteStore {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        self.conn.exec().await.position(key).await
    }
}

impl Purge for SqliteStore {
    /// One `BEGIN IMMEDIATE` transaction, as a write's: a purge lands wholly
    /// before or wholly after any write, or not at all.
    async fn purge(&self, stream: StreamRef<'_>) -> Result<u64, StoreError> {
        let Exec { mut conn, sent } = self.conn.exec().await;
        sent.fetch_add(1, Ordering::Relaxed);
        let tx = conn
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        let mut ex = Exec { conn: tx, sent };
        let cover = stream.to_cover();
        let deleted = ex
            .built_execute(statements::delete_events::<SqliteDialect>(&cover))
            .await?;
        ex.built_execute(statements::delete_snapshot::<SqliteDialect>(&cover))
            .await?;
        let [domain, root, edition] = view_of(stream);
        ex.execute(statements::DELETE_POSITIONS, [domain, edition, root])
            .await?;
        ex.count();
        ex.conn.commit().await.map_err(unavailable)?;
        Ok(deleted)
    }
}

impl Outbox for SqliteStore {
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = self
            .conn
            .exec()
            .await
            .rows(statements::PENDING, [Value::Int(limit)])
            .await?;
        let mut entries = Vec::new();
        for row in rows {
            let id: Vec<u8> = row.try_get(0).map_err(unavailable)?;
            let message: Vec<u8> = row.try_get(2).map_err(unavailable)?;
            entries.push(OutboxEntry {
                id: rows::stream_root(&id)?,
                origin: row.try_get(1).map_err(unavailable)?,
                message: prost_types::Any::decode(message.as_slice())
                    .map_err(|e| StoreError::Unavailable(undecodable(e)))?,
                edition: row.try_get(3).map_err(unavailable)?,
            });
        }
        Ok(entries)
    }

    async fn clear(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        let mut ex = self.conn.exec().await;
        let mut cleared = 0;
        for id in ids {
            cleared += ex
                .execute(statements::CLEAR, [Value::Blob(id.as_bytes().to_vec())])
                .await?;
        }
        Ok(cleared)
    }
}

impl ReadStore for SqliteStore {
    type Reads<'s> = SqliteRead<'s>;

    /// A deferred `BEGIN`, which takes no lock, and a first read at once:
    /// under WAL a read transaction's moment is its first read's. It runs
    /// on a store opened by [`open_existing`](SqliteStore::open_existing),
    /// and waits on no writer. The handle serves nothing else until the
    /// unit is dropped.
    async fn begin_read(&self) -> Result<SqliteRead<'_>, StoreError> {
        let mut ex = self.conn.exec().await;
        ex.count();
        <Sqlite as Database>::TransactionManager::begin(&mut ex.conn, None)
            .await
            .map_err(unavailable)?;
        let unit = SqliteRead { ex };
        // Any read fixes the moment; this one reads only the schema.
        let mut unit = unit;
        unit.ex.count();
        empty(&mut unit.ex.conn).await?;
        Ok(unit)
    }
}

/// A read-only unit: one SQLite read transaction, seeing the database as
/// it began. Dropped, it ends, having written nothing.
pub struct SqliteRead<'s> {
    ex: Exec<'s, tokio::sync::MutexGuard<'s, SqliteConnection>>,
}

impl Drop for SqliteRead<'_> {
    fn drop(&mut self) {
        <Sqlite as Database>::TransactionManager::start_rollback(&mut self.ex.conn);
    }
}

impl ReadUnit for SqliteRead<'_> {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.ex.covers(domain).await
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.ex.read(stream, from, to).await
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        self.ex.read_snapshot(stream).await
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
        let rows = self
            .conn
            .exec()
            .await
            .rows(statements::CHANGE_TOKEN, [])
            .await?;
        let row = rows.first().ok_or_else(|| unavailable("no data version"))?;
        let version: i64 = row.try_get(0).map_err(unavailable)?;
        let changes: i64 = row.try_get(1).map_err(unavailable)?;
        // Their bits as they are: a token is only compared for equality.
        Ok(ChangeToken::new([version as u64, changes as u64]))
    }
}

impl ReadUntilTime for SqliteStore {
    /// The prefix recorded at or before `until`, and the stream's head, in
    /// one read transaction.
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Slice, StoreError> {
        let mut ex = self.conn.exec().await;
        ex.count();
        let tx = ex.conn.begin().await.map_err(unavailable)?;
        let mut moment = Exec {
            conn: tx,
            sent: ex.sent,
        };
        let [domain, root, edition] = view_of(stream);
        let by = Value::Int(statements::instant_column(until));
        let pages = moment
            .pages(stream, statements::UNTIL, [domain, root, edition, by])
            .await?;
        let head = moment.head(stream).await?;
        moment.count();
        moment.conn.commit().await.map_err(unavailable)?;
        Ok(Slice {
            next_sequence: head.next_sequence,
            last_recorded_at: head.last_recorded_at,
            pages,
        })
    }
}

impl ReadByCorrelation for SqliteStore {
    /// Every stream holding an event of the correlation in main or in the
    /// edition, each read as `read` reads it in the edition, in one read
    /// transaction: those with pages of it, one book each.
    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: Uuid,
    ) -> Result<Vec<EventBook>, StoreError> {
        let mut ex = self.conn.exec().await;
        ex.count();
        let tx = ex.conn.begin().await.map_err(unavailable)?;
        let mut moment = Exec {
            conn: tx,
            sent: ex.sent,
        };
        let correlation = || Value::Blob(correlation_id.as_bytes().to_vec());
        let candidates = moment
            .rows(
                statements::CORRELATED_STREAMS,
                [
                    correlation(),
                    Value::Text(edition.unwrap_or_default().to_string()),
                    Value::MaybeText(domain.map(str::to_string)),
                ],
            )
            .await?;
        let mut books = Vec::new();
        for row in candidates {
            let stream_domain: String = row.try_get(0).map_err(unavailable)?;
            let root: Vec<u8> = row.try_get(1).map_err(unavailable)?;
            let stream = StreamRef {
                domain: &stream_domain,
                edition,
                root: rows::stream_root(&root)?,
            };
            let [d, r, e] = view_of(stream);
            let pages = moment
                .pages(stream, statements::OF_CORRELATION, [d, r, e, correlation()])
                .await?;
            if pages.is_empty() {
                continue;
            }
            let head = moment.head(stream).await?;
            books.push(EventBook {
                cover: Some(stream.to_cover()),
                pages,
                snapshot: None,
                next_sequence: head.next_sequence,
            });
        }
        moment.count();
        moment.conn.commit().await.map_err(unavailable)?;
        Ok(books)
    }
}

impl ListStreams for SqliteStore {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        let rows = self
            .conn
            .exec()
            .await
            .rows(statements::ALL_STREAMS, [])
            .await?;
        let mut covers = Vec::new();
        for row in rows {
            let domain: String = row.try_get(0).map_err(unavailable)?;
            let edition: String = row.try_get(1).map_err(unavailable)?;
            let root: Vec<u8> = row.try_get(2).map_err(unavailable)?;
            covers.push(rows::cover::<SqliteDialect>(&domain, edition, &root)?);
        }
        Ok(covers)
    }
}

#[cfg(test)]
#[path = "sqlite.test.rs"]
mod tests;
