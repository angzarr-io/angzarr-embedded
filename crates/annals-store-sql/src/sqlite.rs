//! SQLite: the shared statements rendered by sea-query's SQLite builder and
//! run on sqlx, over one connection per database; the schema created by
//! `migrations/sqlite/`. A unit of work is a `BEGIN IMMEDIATE` transaction.
//! A file is kept in WAL mode, so readers never wait on a writer.

use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use annals::next::v1::{Cover, EventPage, Snapshot, Uuid};
use annals::next::{AsyncEventStore, AsyncStore, AsyncUnitOfWork, Slice, StoreError};
use sea_query::{InsertStatement, SelectStatement, SqliteQueryBuilder};
use sea_query_binder::SqlxBinder;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteRow};
use sqlx::{ConnectOptions, Connection, Row, Sqlite, Transaction};

use crate::error::unavailable;
use crate::{rows, statements, Dialect};

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

/// The schema, `migrations/sqlite/`.
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
    pub async fn open(path: &Path) -> Result<SqliteStore, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .busy_timeout(BUSY_TIMEOUT)
            .create_if_missing(true);
        let mut conn = options.connect().await.map_err(unavailable)?;
        into_wal(&mut conn).await?;
        SqliteStore::migrated(conn).await
    }

    /// A database held only in memory, migrated.
    pub async fn open_in_memory() -> Result<SqliteStore, StoreError> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:").map_err(unavailable)?;
        SqliteStore::migrated(options.connect().await.map_err(unavailable)?).await
    }

    /// Migrate holding the database's write lock: sqlx's
    /// migrator takes no lock on SQLite and reads which migrations are
    /// applied before it begins each one, so stores opening a new file at
    /// once (one per process) would each apply the first migration, and
    /// all but one fail on tables that already exist. Under `BEGIN
    /// IMMEDIATE` they take turns (waiting the connection's busy timeout),
    /// and each after the first finds nothing to apply.
    async fn migrated(mut conn: SqliteConnection) -> Result<SqliteStore, StoreError> {
        let mut tx = conn
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        MIGRATOR.run(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(SqliteStore { conn })
    }
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
    error
        .as_database_error()
        .and_then(|e| e.code())
        .and_then(|code| code.parse::<i32>().ok())
        .is_some_and(|code| code & 0xff == SQLITE_BUSY)
}

/// SQLite's primary result code for a lock another connection holds.
const SQLITE_BUSY: i32 = 5;

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
        read_from(&mut self.conn, stream, from).await
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
        read_from(self.conn()?, stream, from).await
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

impl AsyncUnitOfWork for SqliteUnit<'_> {
    async fn commit(self) -> Result<(), StoreError> {
        self.tx?.commit().await.map_err(unavailable)
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

async fn execute(conn: &mut SqliteConnection, insert: InsertStatement) -> Result<(), StoreError> {
    let (sql, values) = insert.build_sqlx(SqliteQueryBuilder);
    sqlx::query_with(&sql, values)
        .execute(conn)
        .await
        .map_err(unavailable)?;
    Ok(())
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

async fn read_from(
    conn: &mut SqliteConnection,
    stream: &Cover,
    from: u64,
) -> Result<Option<Slice>, StoreError> {
    let next_sequence = length(conn, stream).await?;
    if next_sequence == 0 {
        return Ok(None);
    }
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
