//! Why a SQL backend could not answer, and how that reaches the port.

use std::fmt;

use annals::next::StoreError;
use annals_diagnostic::{Arg, Diagnostic, Report};

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

/// One value of every error this crate defines, for the catalog test.
pub fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let detail = || "x".to_string();
    vec![
        Box::new(SqlError::Unavailable { detail: detail() }),
        Box::new(SqlError::Undecodable { detail: detail() }),
        Box::new(SqlError::Unmigrated { pending: 1 }),
    ]
}

/// The database's failure, as the port's: the store could not say
/// (`Recovery::Retry`).
pub(crate) fn unavailable(detail: impl fmt::Display) -> StoreError {
    StoreError::Unavailable(Report::of(&SqlError::Unavailable {
        detail: detail.to_string(),
    }))
}

/// A database opened read-only that is not migrated: only a store opened
/// to write can migrate it.
pub(crate) fn unmigrated(pending: usize) -> StoreError {
    StoreError::Unavailable(Report::of(&SqlError::Unmigrated { pending }))
}

/// A stored value that does not decode (`Recovery::Bug`, as `Unreadable`).
pub(crate) fn undecodable(error: prost::DecodeError) -> Report {
    Report::of(&SqlError::Undecodable {
        detail: error.to_string(),
    })
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
