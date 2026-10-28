//! SQLite: the shared statements rendered by sea-query's SQLite builder and
//! run on sqlx, over one connection per database; the schema created by
//! `migrations/sqlite/`. A unit of work is a `BEGIN IMMEDIATE` transaction;
//! a read of more than one statement outside one runs in a deferred `BEGIN`
//! of its own, so it reads one moment, and a read-only unit of work is a
//! deferred `BEGIN` held open over many reads. A file is kept in WAL mode, so once
//! it is made a reader waits on no writer. A change token is the
//! connection's `PRAGMA data_version` with its `total_changes()`.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use annals::next::v1::{Cover, EventPage, Snapshot, Uuid};
use annals::next::{
    AsyncEventStore, AsyncPurge, AsyncReadStore, AsyncReadUnit, AsyncStore, AsyncUnitOfWork,
    AsyncWatch, ChangeToken, Slice, StoreError,
};
use sea_query::{SelectStatement, SqliteQueryBuilder};
use sea_query_binder::SqlxBinder;
use sqlx::migrate::{MigrateError, Migration, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteRow};
use sqlx::{ConnectOptions, Connection, Row, Sqlite, Transaction};

use crate::error::{unavailable, unmigrated, Damage};
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

/// A store in one SQLite database.
pub struct SqliteStore {
    conn: SqliteConnection,
}

impl SqliteStore {
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
    pub async fn open(path: &Path) -> Result<SqliteStore, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .busy_timeout(BUSY_TIMEOUT)
            .create_if_missing(true);
        let mut conn = options.connect().await.map_err(unavailable)?;
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
    /// (`annals-store-sql-unavailable`). In a directory this process cannot
    /// write, a file in WAL opens only while its `-wal` and `-shm` files are
    /// there (a writer has it open): SQLite cannot read it without them, and
    /// cannot make them there, so once the last writer closes it the open
    /// fails (`annals-store-sql-unavailable`).
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
    /// refused as unmigrated (`annals-store-sql-unmigrated`).
    ///
    /// A database that holds something committed but has migrations not
    /// yet applied is older than this build, and is refused as unmigrated
    /// at once, with nothing waited for. So is one whose applied migrations
    /// this build's disagree with (`annals-store-sql-unavailable`).
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
    /// (`annals-store-sql-unavailable`), not damage found.
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
    let mut conn = options.connect().await.map_err(unavailable)?;
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
        .map_err(unavailable)?;
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
    .map_err(unavailable)?;
    let applied: Vec<(i64, Vec<u8>, bool)> = match recorded {
        Some(_) => sqlx::query_as("SELECT version, checksum, success FROM _sqlx_migrations")
            .fetch_all(&mut *conn)
            .await
            .map_err(unavailable)?,
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

/// SQLite's primary result code for a lock another connection holds.
const SQLITE_BUSY: i32 = 5;

/// SQLite's primary result code for a database file found malformed.
const SQLITE_CORRUPT: i32 = 11;

impl AsyncEventStore for SqliteStore {
    async fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        // The length check and the inserts are one transaction.
        let mut tx = self.conn.begin().await.map_err(unavailable)?;
        let length = append(&mut tx, stream, expected, pages).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(length)
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        read_alone(&mut self.conn, stream, from, || {}).await
    }

    async fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        select_pages(
            &mut self.conn,
            stream,
            statements::pages_of_command::<SqliteDialect>(stream, command_id),
        )
        .await
    }

    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        covers(&mut self.conn, domain).await
    }

    async fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        read_snapshot(&mut self.conn, stream).await
    }

    async fn write_snapshot(
        &mut self,
        stream: &Cover,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        execute(
            &mut self.conn,
            statements::upsert_snapshot::<SqliteDialect>(stream, snapshot)?,
        )
        .await
        .map(drop)
    }
}

impl AsyncPurge for SqliteStore {
    /// One `BEGIN IMMEDIATE` transaction, as a unit of work's: it waits
    /// (up to [`BUSY_TIMEOUT`]) for a unit in flight to finish, so a purge
    /// lands wholly before or wholly after any append.
    async fn purge(&mut self, stream: &Cover) -> Result<u64, StoreError> {
        let mut tx = self
            .conn
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        let deleted = purge(&mut tx, stream).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(deleted)
    }
}

impl AsyncStore for SqliteStore {
    type Unit<'s> = SqliteUnit<'s>;

    /// `BEGIN IMMEDIATE`, so the unit holds the write lock from its first
    /// read and its length checks cannot go stale. A failure to begin is
    /// answered by every operation on the unit.
    async fn begin(&mut self) -> SqliteUnit<'_> {
        SqliteUnit {
            tx: self
                .conn
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(unavailable),
        }
    }

    async fn commit_position(&mut self) -> Result<u64, StoreError> {
        todo!("SqliteStore::commit_position is a stub")
    }
}

/// A unit of work: one SQLite transaction, committed by
/// [`AsyncUnitOfWork::commit`] and rolled back if dropped.
pub struct SqliteUnit<'s> {
    tx: Result<Transaction<'s, Sqlite>, StoreError>,
}

impl SqliteUnit<'_> {
    /// The transaction's connection, or why the transaction never began.
    fn conn(&mut self) -> Result<&mut SqliteConnection, StoreError> {
        match &mut self.tx {
            Ok(tx) => Ok(&mut **tx),
            Err(error) => Err(error.clone()),
        }
    }
}

impl AsyncEventStore for SqliteUnit<'_> {
    async fn append(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
    ) -> Result<u64, StoreError> {
        append(self.conn()?, stream, expected, pages).await
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        read_from(self.conn()?, stream, from, || {}).await
    }

    async fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        select_pages(
            self.conn()?,
            stream,
            statements::pages_of_command::<SqliteDialect>(stream, command_id),
        )
        .await
    }

    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        covers(self.conn()?, domain).await
    }

    async fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        read_snapshot(self.conn()?, stream).await
    }
}

impl AsyncPurge for SqliteUnit<'_> {
    /// Under a savepoint, so a purge that fails part-way leaves the unit as
    /// it was, and a commit after it deletes nothing.
    async fn purge(&mut self, stream: &Cover) -> Result<u64, StoreError> {
        let mut savepoint = self.conn()?.begin().await.map_err(unavailable)?;
        let deleted = purge(&mut savepoint, stream).await?;
        savepoint.commit().await.map_err(unavailable)?;
        Ok(deleted)
    }
}

impl AsyncUnitOfWork for SqliteUnit<'_> {
    async fn commit(self) -> Result<(), StoreError> {
        self.tx?.commit().await.map_err(unavailable)
    }
}

impl AsyncReadStore for SqliteStore {
    type Reads<'s> = SqliteRead<'s>;

    /// A deferred `BEGIN`, which takes no lock, and a first read at once:
    /// under WAL a read transaction's moment is its first read's, so the
    /// unit's is its beginning, not whenever it is first asked. It runs on
    /// a store opened by [`open_existing`](SqliteStore::open_existing),
    /// and waits on no writer. A failure to begin is answered by every read
    /// on the unit.
    async fn begin_read(&mut self) -> SqliteRead<'_> {
        SqliteRead {
            tx: begin_read(&mut self.conn).await,
        }
    }
}

async fn begin_read(conn: &mut SqliteConnection) -> Result<Transaction<'_, Sqlite>, StoreError> {
    let mut tx = conn.begin().await.map_err(unavailable)?;
    // Any read fixes the moment; this one reads only the schema.
    empty(&mut tx).await?;
    Ok(tx)
}

/// A read-only unit of work: one SQLite read transaction, seeing the
/// database as it began. Dropped, it ends, having written nothing.
pub struct SqliteRead<'s> {
    tx: Result<Transaction<'s, Sqlite>, StoreError>,
}

impl SqliteRead<'_> {
    /// The transaction's connection, or why the transaction never began.
    fn conn(&mut self) -> Result<&mut SqliteConnection, StoreError> {
        match &mut self.tx {
            Ok(tx) => Ok(&mut **tx),
            Err(error) => Err(error.clone()),
        }
    }
}

impl AsyncReadUnit for SqliteRead<'_> {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        covers(self.conn()?, domain).await
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Option<Slice>, StoreError> {
        read_from(self.conn()?, stream, from, || {}).await
    }
}

impl AsyncWatch for SqliteStore {
    /// The connection's `PRAGMA data_version` and its `total_changes()`,
    /// read in one statement. `data_version` changes when another
    /// connection, in this process or another, commits, and never for this
    /// connection's own commits; `total_changes()` counts the rows this
    /// connection's statements changed, so it moves with every commit of
    /// its own (and with a unit it rolled back, a change that is not one).
    /// Neither alone sees every commit; together they do. Not the events
    /// table's largest rowid: a purge of any stream but the newest leaves
    /// it as it was.
    ///
    /// Both come from the connection and the database's header (under WAL,
    /// its index's), not from any table, so asking costs the same at any
    /// size. It is a read: it takes no write lock, runs on a store opened
    /// by [`open_existing`](SqliteStore::open_existing), and under WAL
    /// waits on no writer.
    async fn change_token(&mut self) -> Result<ChangeToken, StoreError> {
        let (version, changes): (i64, i64) =
            sqlx::query_as("SELECT data_version, total_changes() FROM pragma_data_version")
                .fetch_one(&mut self.conn)
                .await
                .map_err(unavailable)?;
        // Their bits as they are: a token is only compared for equality.
        Ok(ChangeToken::new([version as u64, changes as u64]))
    }
}

// ---------------------------------------------------------------- running

async fn fetch_all(
    conn: &mut SqliteConnection,
    select: SelectStatement,
) -> Result<Vec<SqliteRow>, StoreError> {
    let (sql, values) = select.build_sqlx(SqliteQueryBuilder);
    sqlx::query_with(&sql, values)
        .fetch_all(conn)
        .await
        .map_err(unavailable)
}

/// Run `statement`, answering how many rows it changed.
async fn execute(
    conn: &mut SqliteConnection,
    statement: impl SqlxBinder,
) -> Result<u64, StoreError> {
    let (sql, values) = statement.build_sqlx(SqliteQueryBuilder);
    let done = sqlx::query_with(&sql, values)
        .execute(conn)
        .await
        .map_err(unavailable)?;
    Ok(done.rows_affected())
}

async fn length(conn: &mut SqliteConnection, stream: &Cover) -> Result<u64, StoreError> {
    let rows = fetch_all(conn, statements::stream_length::<SqliteDialect>(stream)).await?;
    let count: i64 = match rows.first() {
        Some(row) => row.try_get(0).map_err(unavailable)?,
        None => 0,
    };
    Ok(count.unsigned_abs())
}

async fn append(
    conn: &mut SqliteConnection,
    stream: &Cover,
    expected: u64,
    pages: &[EventPage],
) -> Result<u64, StoreError> {
    let actual = length(conn, stream).await?;
    if expected != actual {
        return Err(StoreError::Conflict {
            stream: stream.clone(),
            expected,
            actual,
        });
    }
    for (due, page) in (expected..).zip(pages) {
        if page.sequence != due {
            return Err(StoreError::Discontinuous {
                stream: stream.clone(),
                expected: due,
                found: page.sequence,
            });
        }
    }
    for page in pages {
        execute(
            &mut *conn,
            statements::insert_page::<SqliteDialect>(stream, page)?,
        )
        .await?;
    }
    Ok(actual + pages.len() as u64)
}

/// Delete the stream's events (their command ids with them), then its
/// snapshot, inside the caller's transaction: how many events it deleted.
async fn purge(conn: &mut SqliteConnection, stream: &Cover) -> Result<u64, StoreError> {
    let deleted = execute(
        &mut *conn,
        statements::delete_events::<SqliteDialect>(stream),
    )
    .await?;
    execute(conn, statements::delete_snapshot::<SqliteDialect>(stream)).await?;
    Ok(deleted)
}

/// [`read_from`] outside a unit of work, in a read transaction of its own
/// (deferred `BEGIN`), so its statements read one moment: under WAL a
/// reader's transaction sees the database as its first statement found it,
/// and waits on no writer. A write or purge that commits in between is not
/// seen, so the read never answers a length its pages do not reach.
async fn read_alone(
    conn: &mut SqliteConnection,
    stream: &Cover,
    from: u64,
    between: impl FnMut(),
) -> Result<Option<Slice>, StoreError> {
    let mut tx = conn.begin().await.map_err(unavailable)?;
    let slice = read_from(&mut tx, stream, from, between).await?;
    tx.commit().await.map_err(unavailable)?;
    Ok(slice)
}

/// The stream's slice from `from`, in three statements: its length, its
/// pages, and its last page's `recorded_at`. Consistent only inside a
/// transaction. `between` runs after the length is read, before the pages
/// are: a test's hook for writing in between, a no-op otherwise.
async fn read_from(
    conn: &mut SqliteConnection,
    stream: &Cover,
    from: u64,
    mut between: impl FnMut(),
) -> Result<Option<Slice>, StoreError> {
    let next_sequence = length(conn, stream).await?;
    if next_sequence == 0 {
        return Ok(None);
    }
    between();
    let pages = select_pages(
        conn,
        stream,
        statements::pages_from::<SqliteDialect>(stream, from),
    )
    .await?;
    let last = select_pages(
        conn,
        stream,
        statements::page_at::<SqliteDialect>(stream, next_sequence - 1),
    )
    .await?;
    Ok(Some(Slice {
        stream: stream.clone(),
        next_sequence,
        last_recorded_at: last
            .first()
            .and_then(|p| p.meta.as_ref())
            .and_then(|m| m.recorded_at),
        pages,
    }))
}

/// The pages `select` returns, as `(sequence, page)` rows, decoded.
async fn select_pages(
    conn: &mut SqliteConnection,
    stream: &Cover,
    select: SelectStatement,
) -> Result<Vec<EventPage>, StoreError> {
    let mut pages = Vec::new();
    for row in fetch_all(conn, select).await? {
        let sequence: i64 = row.try_get(0).map_err(unavailable)?;
        let bytes: Vec<u8> = row.try_get(1).map_err(unavailable)?;
        pages.push(rows::page(stream, sequence, &bytes)?);
    }
    Ok(pages)
}

async fn covers(conn: &mut SqliteConnection, domain: &str) -> Result<Vec<Cover>, StoreError> {
    let mut covers = Vec::new();
    for row in fetch_all(conn, statements::domain_covers(domain)).await? {
        let edition: String = row.try_get(0).map_err(unavailable)?;
        let root: Vec<u8> = row.try_get(1).map_err(unavailable)?;
        covers.push(rows::cover::<SqliteDialect>(domain, edition, &root)?);
    }
    Ok(covers)
}

async fn read_snapshot(
    conn: &mut SqliteConnection,
    stream: &Cover,
) -> Result<Option<Snapshot>, StoreError> {
    let rows = fetch_all(conn, statements::snapshot_of::<SqliteDialect>(stream)).await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let bytes: Vec<u8> = row.try_get(0).map_err(unavailable)?;
    rows::snapshot(stream, &bytes).map(Some)
}

#[cfg(test)]
#[path = "sqlite.test.rs"]
mod tests;
