//! Why a wire message is not a valid domain value: what a generated
//! conversion from a prost message to its domain type returns
//! (`angzarr_build`, backlog CR-4). The payload or snapshot it came from is
//! then refused as [`PayloadError::Invalid`](super::PayloadError).

use std::fmt;

use annals_diagnostic::{Arg, Diagnostic};

/// A field of a decoded message that its domain type cannot hold.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum WireError {
    /// A field the domain type requires is absent: a message field, which
    /// the domain layer holds without an `Option`.
    Missing {
        message: &'static str,
        field: &'static str,
    },
    /// An enum field holds a value the domain does not define. The zero
    /// value is one: `*_UNSPECIFIED` is the wire's absence, never a domain
    /// value.
    UnknownEnum {
        message: &'static str,
        field: &'static str,
        value: i32,
    },
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::Missing { message, field } => {
                write!(f, "{message}.{field} is required and absent")
            }
            WireError::UnknownEnum {
                message,
                field,
                value,
            } => write!(
                f,
                "{message}.{field} holds {value}, which no variant defines"
            ),
        }
    }
}

impl Diagnostic for WireError {
    fn code(&self) -> &'static str {
        match self {
            WireError::Missing { .. } => "annals-next-wire-missing",
            WireError::UnknownEnum { .. } => "annals-next-wire-unknown-enum",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            WireError::Missing { message, field } => {
                vec![("message", (*message).into()), ("field", (*field).into())]
            }
            WireError::UnknownEnum {
                message,
                field,
                value,
            } => vec![
                ("message", (*message).into()),
                ("field", (*field).into()),
                ("value", (*value).into()),
            ],
        }
    }
}
