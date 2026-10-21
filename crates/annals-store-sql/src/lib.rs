//! annals' store port on SQL.
//!
//! What every SQL backend shares is here and backend-neutral: the schema as
//! sea-query identifiers (`schema`), every statement built from them
//! (`statements`), and decoding stored rows (`rows`). A backend differs only
//! in how it keys the main timeline's edition ([`Dialect`]), how it renders
//! and runs a statement, its migrations, and how a unit of work begins; each
//! is a module behind a feature:
//!
//! - `sqlite` ([`sqlite`]): sqlx on SQLite.
//!
//! Every backend's store is held to annals' store contract
//! (`annals::async_store_contract!`), so the kernel behaves alike over any of
//! them and over `InMemoryStore`. Every backend can purge a stream
//! (`annals::next::AsyncPurge`, on the store and on its unit of work), and is
//! held to the contract's purge section too. Every backend opens read-only
//! units of work (`annals::next::AsyncReadStore`), one read transaction
//! each, held to the contract's read section, and answers each read outside
//! a unit as one moment. Pages are stored as written and never
//! decoded except to read them back; the command id is copied into its own
//! indexed column for the deduplication lookup.

// clippy::result_large_err: annals' StoreError is about 152 bytes, over
// clippy's 128-byte default, so every function here returning it is flagged.
// The port's trait methods fix StoreError, unboxed, as their error (annals
// keeps its errors unboxed so callers can match them, and allows this lint in
// its own kernel for that reason), and these helpers exist to be returned
// through those methods, so boxing them would only be undone at each method.
#![allow(clippy::result_large_err)]

mod error;
mod rows;
mod schema;
mod statements;

#[cfg(feature = "sqlite")]
pub mod sqlite;

use annals::next::v1::Cover;

pub use error::{diagnostic_samples, Damage, Integrity, SqlError};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `crates/annals-store-sql/locales/en-US/annals-store-sql.ftl`.
pub const EN_US: (&str, &str) = (
    "annals-store-sql",
    include_str!("../locales/en-US/annals-store-sql.ftl"),
);

/// What differs between SQL backends in how a stream is keyed.
pub trait Dialect {
    /// The edition column's value for `cover`. A backend whose keys treat
    /// NULLs as distinct (SQLite) stores the main timeline as `''`, which
    /// annals' refusal of an empty edition keeps from naming a real one.
    fn edition(cover: &Cover) -> String;

    /// The edition a stored value names: `None` for the main timeline.
    fn cover_edition(stored: String) -> Option<String>;
}
