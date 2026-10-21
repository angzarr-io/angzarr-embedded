//! Why a SQL backend could not answer, and how that reaches the port.

use std::fmt;

#[cfg(feature = "sqlite")]
use annals::next::StoreError;
#[cfg(feature = "sqlite")]
use annals_diagnostic::Report;
use annals_diagnostic::{Arg, Diagnostic};

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
        }
    }
}

impl Diagnostic for SqlError {
    fn code(&self) -> &'static str {
        match self {
            SqlError::Unavailable { .. } => "annals-store-sql-unavailable",
            SqlError::Undecodable { .. } => "annals-store-sql-undecodable",
            SqlError::Unmigrated { .. } => "annals-store-sql-unmigrated",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            SqlError::Unavailable { detail } | SqlError::Undecodable { detail } => {
                vec![("detail", detail.as_str().into())]
            }
            SqlError::Unmigrated { pending } => vec![("pending", (*pending).into())],
        }
    }
}

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

impl Diagnostic for Damage {
    fn code(&self) -> &'static str {
        "annals-store-sql-damaged"
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![("first", self.first().into()), ("more", self.more().into())]
    }
}

/// One value of every error this crate defines, for the catalog test.
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let detail = || "x".to_string();
    vec![
        Box::new(SqlError::Unavailable { detail: detail() }),
        Box::new(SqlError::Undecodable { detail: detail() }),
        Box::new(SqlError::Unmigrated { pending: 1 }),
        Box::new(Damage {
            problems: vec![detail()],
        }),
    ]
}

/// The database's failure, as the port's: the store could not say
/// (`Recovery::Retry`).
#[cfg(feature = "sqlite")]
pub(crate) fn unavailable(detail: impl fmt::Display) -> StoreError {
    StoreError::Unavailable(Report::of(&SqlError::Unavailable {
        detail: detail.to_string(),
    }))
}

/// A database opened read-only that is not migrated: only a store opened
/// to write can migrate it.
#[cfg(feature = "sqlite")]
pub(crate) fn unmigrated(pending: usize) -> StoreError {
    StoreError::Unavailable(Report::of(&SqlError::Unmigrated { pending }))
}

/// A database file too damaged to open, as the port's failure: damage,
/// its one problem the database's own `message`
/// (`annals-store-sql-damaged`, as `integrity_check` reports it).
#[cfg(feature = "sqlite")]
pub(crate) fn damaged(message: String) -> StoreError {
    StoreError::Unavailable(Report::of(&Damage {
        problems: vec![message],
    }))
}

/// A stored value that does not decode (`Recovery::Bug`, as `Unreadable`).
#[cfg(feature = "sqlite")]
pub(crate) fn undecodable(error: prost::DecodeError) -> Report {
    Report::of(&SqlError::Undecodable {
        detail: error.to_string(),
    })
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
