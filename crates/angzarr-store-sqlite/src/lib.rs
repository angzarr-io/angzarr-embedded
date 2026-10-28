//! angzarr-ports on SQLite: the one SQLite adapter annals and angzarr both
//! use (plan ruling 10). A leaf: it depends on angzarr-ports and SQL, and
//! on nothing of annals.
//!
//! [`SqliteStore`] implements every base port and every opt-in capability
//! but the edition administration: [`EventStore`](angzarr_ports::EventStore),
//! [`SnapshotStore`](angzarr_ports::SnapshotStore),
//! [`PositionStore`](angzarr_ports::PositionStore) (a step's one
//! [`write`](angzarr_ports::EventStore::write) is a `BEGIN IMMEDIATE`
//! transaction, begun and committed inside the call),
//! [`Purge`](angzarr_ports::Purge),
//! [`ReadStore`](angzarr_ports::ReadStore), [`Watch`](angzarr_ports::Watch),
//! [`ReadUntilTime`](angzarr_ports::ReadUntilTime),
//! [`ReadByCorrelation`](angzarr_ports::ReadByCorrelation),
//! [`ListStreams`](angzarr_ports::ListStreams) and
//! [`Outbox`](angzarr_ports::Outbox). The handle's one connection is held
//! behind an async lock, so every port takes `&self` and one call at a time
//! runs on it.
//!
//! Editions are per-stream forks as far as the base ports need them: an
//! edition stream's first own event is its creation (`meta.EditionCreated`,
//! at main's head or below it), and it reads main's events below that, then
//! its own; an edition stream with none of its own reads as main's, and
//! anything else written to it first is refused
//! (`StoreError::EditionNotForked`). The edition administration, the
//! [`Editions`](angzarr_ports::Editions) port (listing, time-based forks,
//! deletion and removal), is NOT IMPLEMENTED YET (the owner's ruling,
//! 2026-10-28: a follow-up; `MemoryStore` covers it meanwhile), and the
//! suite's checks of it are ignored here.
//!
//! The rows are written with sea-query from the schema's identifiers
//! (`schema`, `statements`) and read with SQL over one condition, a stream
//! as read in its edition; stored rows decode in `rows`. Pages are stored
//! as written and never decoded except to read them back (and, once, to
//! fill the columns migration 0003 added: `migrations/sqlite/`).

// clippy::result_large_err: angzarr-ports' StoreError is over clippy's
// 128-byte default, and the ports fix it, unboxed, as every method's error;
// the helpers here exist to be returned through those methods.
#![allow(clippy::result_large_err)]

mod error;
mod rows;
mod schema;
mod statements;

pub mod sqlite;

use angzarr_ports::angzarr_proto::v1::Cover;

pub use error::{causes, damage_sample, Damage, Integrity, SqlError};
pub use sqlite::{SqliteRead, SqliteStore, BUSY_TIMEOUT};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's errors. The text of
/// `crates/angzarr-store-sqlite/locales/en-US/angzarr-store-sqlite.ftl`.
pub const EN_US: (&str, &str) = (
    "angzarr-store-sqlite",
    include_str!("../locales/en-US/angzarr-store-sqlite.ftl"),
);

/// What differs between SQL backends in how a stream is keyed.
pub(crate) trait Dialect {
    /// The edition column's value for `cover`. A backend whose keys treat
    /// NULLs as distinct (SQLite) stores the main timeline as `''`, which
    /// the ports' refusal of an empty edition keeps from naming a real one.
    fn edition(cover: &Cover) -> String;

    /// The edition a stored value names: `None` for the main timeline.
    fn cover_edition(stored: String) -> Option<String>;
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
