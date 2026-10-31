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

use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use angzarr_ports::angzarr_proto::v1::meta::EditionCreated;
use angzarr_ports::angzarr_proto::v1::{Cover, EventBook, EventPage, Snapshot};
use angzarr_ports::{
    Append, AppliedMigration, ChangeToken, EventStore, Head, ListStreams, Outbox, OutboxEntry,
    PositionKey, PositionStore, Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime,
    Slice, SnapshotStore, StoreError, StreamRef, Uuid, Watch, Write, Written,
    EDITION_CREATED_TYPE_URL,
};
use prost::Message;
use prost_types::Timestamp;
use sea_query::SqliteQueryBuilder;
use sea_query_binder::SqlxBinder;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnection, SqliteRow};
use sqlx::{Connection, Database, Row, Sqlite, TransactionManager};

use crate::error::{damaged, no_data_version, unavailable, undecodable, Damage};
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

/// The schema, `migrations/sqlite/`. Every open runs them inside `BEGIN
/// IMMEDIATE`, so none may opt out of a transaction (`-- no-transaction`)
/// or change `PRAGMA journal_mode`: neither can run inside one.
pub(crate) static MIGRATOR: Migrator = sqlx::migrate!("./migrations/sqlite");

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

/// A store in one SQLite database: one handle, one connection; the file
/// it is on (none for one in memory), where a backup before a migration
/// is written beside it; and the build's migrations.
pub struct SqliteStore {
    conn: Conn,
    path: Option<PathBuf>,
    migrator: &'static Migrator,
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

    /// The store on `conn`, at `path` (none in memory), under `migrator`.
    pub(crate) fn on(
        conn: SqliteConnection,
        path: Option<PathBuf>,
        migrator: &'static Migrator,
    ) -> SqliteStore {
        SqliteStore {
            conn: conn.into(),
            path,
            migrator,
        }
    }

    /// The file the store is on, none for one in memory.
    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// The build's migrations.
    pub(crate) fn migrator(&self) -> &'static Migrator {
        self.migrator
    }

    /// The connection, once no other call holds it.
    pub(crate) async fn connection(&self) -> tokio::sync::MutexGuard<'_, SqliteConnection> {
        self.conn.lock.lock().await
    }

    /// The connection, the handle given up.
    pub(crate) fn into_conn(self) -> SqliteConnection {
        self.conn.lock.into_inner()
    }

    /// The database at `path`, created if absent, in WAL mode, and migrated
    /// to the build's latest ([`Migrations`](angzarr_ports::Migrations),
    /// `crate::migrations`).
    ///
    /// A database whose migrations are all applied is opened without the
    /// write lock, so an open waits on no writer. Otherwise the migrations
    /// run inside `BEGIN IMMEDIATE`: sqlx's migrator takes no lock on
    /// SQLite, so stores opening a new file at once (one per process) would
    /// each apply the first migration. Under the lock they take turns, and
    /// each after the first finds nothing to apply. A migration that opts
    /// out of a transaction (`-- no-transaction`) or changes `PRAGMA
    /// journal_mode` therefore cannot be added.
    ///
    /// A store holding anything, with migrations pending, is first backed
    /// up beside it, under the lock and before anything is applied: for
    /// `state.db`, to `state.db.pre-<applied level>-<UTC time>` (the
    /// owner's ruling, 2026-10-31). A store with nothing pending, and a new
    /// one, take no backup.
    ///
    /// Refused, and left as it was: a store a newer build migrated
    /// (`angzarr-ports-store-newer-than-build`), one a migration failed in
    /// (`-migration-failed`), changed in (`-migration-changed`) or that has
    /// a migration this build no longer has (`-migration-missing`); and a
    /// file too damaged to open, one SQLite finds malformed or no database
    /// at all (`angzarr-store-sqlite-damaged-file`), its one problem
    /// SQLite's message.
    pub async fn open(path: &Path) -> Result<SqliteStore, StoreError> {
        crate::migrations::open_writing(path, &MIGRATOR, true).await
    }

    /// The database at `path` opened read-only, or `None` if there is no
    /// file there. Every write to the store fails
    /// (`angzarr-store-sqlite-unavailable`).
    ///
    /// Reading migrates too (the owner, 2026-10-31): a store with
    /// migrations pending is backed up and migrated first, as
    /// [`open`](SqliteStore::open) does, on a connection of its own, and
    /// refused, untouched, as `open` refuses. A store a writer is still
    /// making is one with every migration pending: the read waits on the
    /// writer's lock ([`BUSY_TIMEOUT`]), and then finds nothing to apply. A
    /// store with nothing pending is opened without writing anything, so a
    /// read-only file opens; one with something pending needs a file this
    /// process can write.
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
    /// A file too damaged to open, one SQLite finds malformed
    /// (`SQLITE_CORRUPT`) or no database at all (`SQLITE_NOTADB`), is
    /// refused as damage (`angzarr-store-sqlite-damaged-file`), its one
    /// problem SQLite's message, told from every other failure to open.
    pub async fn open_existing(path: &Path) -> Result<Option<SqliteStore>, StoreError> {
        crate::migrations::open_reading(path, &MIGRATOR).await
    }

    /// The ledger of the store at `path`, sqlx's record of the migrations
    /// applied to it, whole and in version order (empty if it has none),
    /// or `None` if there is no file there: read on a read-only connection
    /// of its own, without opening the store, so nothing is migrated. For a
    /// caller that judges a store before an open migrates it.
    pub async fn ledger_at(path: &Path) -> Result<Option<Vec<AppliedMigration>>, StoreError> {
        if matches!(path.try_exists(), Ok(false)) {
            return Ok(None);
        }
        let mut conn = crate::migrations::reading(path).await?;
        let ledger = crate::migrations::ledger(&mut conn).await?;
        conn.close().await.map_err(unavailable)?;
        Ok(Some(ledger))
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
        crate::migrations::open_in_memory(&MIGRATOR).await
    }
}

#[cfg(test)]
pub(crate) use pausing::pause;

/// A pause, for the tests' racing writers.
#[cfg(test)]
mod pausing {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::task::{Context, Poll, Waker};
    use std::time::{Duration, Instant};

    /// A future ready once `duration` has passed. The crate drives no
    /// runtime and so has no timer to ask for: a thread sleeps the time out
    /// and wakes the task.
    pub(crate) fn pause(duration: Duration) -> Pause {
        Pause {
            until: Instant::now() + duration,
            waker: None,
        }
    }

    /// See [`pause`].
    pub(crate) struct Pause {
        until: Instant,
        /// The task to wake, shared with the sleeping thread once it is
        /// started.
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
}

/// Whether the database holds nothing committed: no table, index or
/// anything else in its schema.
pub(crate) async fn empty(conn: &mut SqliteConnection) -> Result<bool, StoreError> {
    let entries: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master")
        .fetch_one(&mut *conn)
        .await
        .map_err(opening)?;
    Ok(entries == 0)
}

/// An event row as stored: domain, edition, root, sequence, page.
type StoredRow = (String, String, Vec<u8>, i64, Vec<u8>);

/// Fill the events' correlation and time columns from their pages, once:
/// what migration 0003 added, which SQL cannot fill (it decodes a page).
/// Run under the migration's lock, the first time a store migrates the
/// file past 0003, and marked done, so a reopen scans nothing again.
pub(crate) async fn backfill(conn: &mut SqliteConnection) -> Result<(), StoreError> {
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
pub(crate) async fn into_wal(conn: &mut SqliteConnection) -> Result<(), StoreError> {
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
pub(crate) fn opening(error: sqlx::Error) -> StoreError {
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

    /// Keep `entry`, the `place`th of its write, under `at`: the stream it
    /// orders on and the sequence there ([`EventStore::write`] says which),
    /// or no stream at 0 for a write with neither events nor positions
    /// (owner ruling 2026-10-31, Q7: the outbox orders per stream, with no
    /// sequence of its own).
    async fn put_outbox(
        &mut self,
        entry: &OutboxEntry,
        at: Option<(StreamRef<'_>, u64)>,
        place: usize,
    ) -> Result<(), StoreError> {
        let ([domain, root, edition], sequence) = match at {
            Some((stream, sequence)) => (view_of(stream), sequence),
            None => (
                [
                    Value::Text(String::new()),
                    Value::Blob(Vec::new()),
                    Value::Text(String::new()),
                ],
                0,
            ),
        };
        let values = [
            Value::Blob(entry.id.as_bytes().to_vec()),
            Value::Text(entry.origin.clone()),
            Value::Blob(entry.message.encode_to_vec()),
            Value::MaybeText(entry.edition.clone()),
            domain,
            edition,
            root,
            Value::Int(i64::try_from(sequence).unwrap_or(i64::MAX)),
            Value::Int(i64::try_from(place).unwrap_or(i64::MAX)),
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
        // The stream the entries order on: the one the write appends to, at
        // its first sequence; for a write appending none (a policy's step),
        // its trigger's, at the triggering event's sequence, the position it
        // moves to less one (owner ruling 2026-10-31, angzarr-ports' O7).
        let at = match write.events {
            Some(append) => Some((append.stream, append.expected)),
            None => write
                .positions
                .first()
                .map(|(key, next)| (key.stream, next.saturating_sub(1))),
        };
        for (place, entry) in write.outbox.iter().enumerate() {
            ex.put_outbox(entry, at, place).await?;
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
    /// before or wholly after any write, or not at all. The stream's head
    /// is checked against `expected` inside it, under the write lock
    /// (14.43), so a write landing between the caller's read and the purge
    /// is caught; a refusal rolls back, deleting nothing.
    async fn purge(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        let Exec { mut conn, sent } = self.conn.exec().await;
        sent.fetch_add(1, Ordering::Relaxed);
        let tx = conn
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        let mut ex = Exec { conn: tx, sent };
        let actual = ex.head(stream).await?.next_sequence;
        if actual != expected {
            return Err(StoreError::conflict(stream, expected, actual));
        }
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
        let row = rows.first().ok_or_else(no_data_version)?;
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
