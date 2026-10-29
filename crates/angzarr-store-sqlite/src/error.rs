//! Why SQLite could not answer, and how that reaches the ports.
//!
//! In angzarr-ports' style, depending on no diagnostics crate: every error
//! has a stable kebab-case `code` starting with this crate's name, named
//! `args` that carry data and never English, and an English `Display`.
//! What the store answers through the ports is angzarr-ports'
//! [`StoreError`], whose `Unavailable` and `Unreadable` carry this crate's
//! failure as a [`Cause`]: its code and its English, no args. So a cause's
//! message in `locales/en-US/angzarr-store-sqlite.ftl` takes no variables;
//! the detail is in the English.

use std::fmt;

use angzarr_ports::{Arg, Cause, StoreError};

/// The database's own failures, as causes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SqlError {
    /// The database itself failed: the store could not say.
    Unavailable { detail: String },
    /// A stored value's bytes do not decode as what was written.
    Undecodable { detail: String },
    /// A database opened read-only has `pending` migrations this build
    /// would apply: it is older than the build, or empty with no writer
    /// finishing it, and only a store opened to write migrates it.
    Unmigrated { pending: usize },
    /// A file too damaged to open: SQLite finds it malformed or no database
    /// at all. `message` is SQLite's.
    DamagedFile { message: String },
}

impl SqlError {
    pub fn code(&self) -> &'static str {
        match self {
            SqlError::Unavailable { .. } => "angzarr-store-sqlite-unavailable",
            SqlError::Undecodable { .. } => "angzarr-store-sqlite-undecodable",
            SqlError::Unmigrated { .. } => "angzarr-store-sqlite-unmigrated",
            SqlError::DamagedFile { .. } => "angzarr-store-sqlite-damaged-file",
        }
    }

    /// As a [`Cause`]: the code and the English.
    pub fn cause(&self) -> Cause {
        Cause::new(self.code(), [])
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::Unavailable { detail } => write!(f, "the database failed: {detail}"),
            SqlError::Undecodable { detail } => {
                write!(f, "a stored value does not decode: {detail}")
            }
            SqlError::Unmigrated { pending: 1 } => {
                write!(
                    f,
                    "the database, opened to read, has a migration not yet applied"
                )
            }
            SqlError::Unmigrated { pending } => write!(
                f,
                "the database, opened to read, has {pending} migrations not yet applied"
            ),
            SqlError::DamagedFile { message } => {
                write!(f, "the database file is too damaged to open: {message}")
            }
        }
    }
}

impl std::error::Error for SqlError {}

/// What an integrity check found: the database file is sound, or it is
/// damaged.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Integrity {
    /// The database reported no problem.
    Intact,
    /// The database reported at least one problem.
    Damaged(Damage),
}

/// The problems the database reported in its own file, as it worded them:
/// at least one. A database keeps its own wording, so each problem is text
/// that was read, not a message to translate.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Damage {
    pub(crate) problems: Vec<String>,
}

impl Damage {
    /// Every problem reported, in the database's order; never empty.
    pub fn problems(&self) -> &[String] {
        &self.problems
    }

    fn first(&self) -> &str {
        self.problems.first().map_or("", String::as_str)
    }

    fn more(&self) -> usize {
        self.problems.len().saturating_sub(1)
    }

    pub fn code(&self) -> &'static str {
        "angzarr-store-sqlite-damaged"
    }

    pub fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![
            ("first", Arg::Str(self.first().to_string())),
            ("more", Arg::Uint(self.more() as u64)),
        ]
    }
}

impl fmt::Display for Damage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the database file is damaged: {}", self.first())?;
        match self.more() {
            0 => Ok(()),
            1 => write!(f, " (and 1 more problem)"),
            more => write!(f, " (and {more} more problems)"),
        }
    }
}

/// Every cause this crate can put in a [`StoreError`], one each, for the
/// catalog test; and a [`Damage`].
pub fn causes() -> Vec<Cause> {
    let detail = || "x".to_string();
    [
        SqlError::Unavailable { detail: detail() },
        SqlError::Undecodable { detail: detail() },
        SqlError::Unmigrated { pending: 1 },
        SqlError::DamagedFile { message: detail() },
    ]
    .iter()
    .map(SqlError::cause)
    .collect()
}

/// A damage report, for the catalog test.
pub fn damage_sample() -> Damage {
    Damage {
        problems: vec!["x".to_string(), "y".to_string()],
    }
}

/// The database's failure, as the port's: the store could not say
/// (`Recovery::Retry`).
pub(crate) fn unavailable(detail: impl fmt::Display) -> StoreError {
    StoreError::Unavailable(
        SqlError::Unavailable {
            detail: detail.to_string(),
        }
        .cause(),
    )
}

/// A database opened read-only that is not migrated: only a store opened
/// to write can migrate it.
pub(crate) fn unmigrated(pending: usize) -> StoreError {
    StoreError::Unavailable(SqlError::Unmigrated { pending }.cause())
}

/// A database file too damaged to open, as the port's failure
/// (`angzarr-store-sqlite-damaged-file`), SQLite's message its detail.
pub(crate) fn damaged(message: String) -> StoreError {
    StoreError::Unavailable(SqlError::DamagedFile { message }.cause())
}

/// A stored value that does not decode (`Recovery::Bug`, as `Unreadable`).
pub(crate) fn undecodable(error: prost::DecodeError) -> Cause {
    SqlError::Undecodable {
        detail: error.to_string(),
    }
    .cause()
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
