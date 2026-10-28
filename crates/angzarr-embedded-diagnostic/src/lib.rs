//! Localizable errors.
//!
//! Every error in angzarr-embedded implements [`Diagnostic`]: a stable [`code`] that
//! names the message, and the [`args`] the message fills in. The game client
//! renders a diagnostic in the player's language by looking the code up in a
//! Fluent catalog and passing the args by name. angzarr-embedded renders nothing; it
//! ships an en-US catalog per crate (`crates/<crate>/locales/en-US/<crate>.ftl`),
//! which each crate with codes exports, embedded, as `EN_US`: the crate's
//! name and the catalog's text (`angzarr_embedded::EN_US`, `angzarr_embedded_yaml::EN_US`, ...).
//! A dependent adds those to its own catalog to render angzarr-embedded's diagnostics.
//! The English `Display` every error keeps is the fallback, for logs and for
//! a code no catalog has.
//!
//! The rules:
//!
//! - **A code is stable and never reused.** It is kebab-case, starts with the
//!   crate's name (`angzarr-embedded-yaml-too-new`), and is a valid Fluent message id.
//!   A code whose meaning changes gets a new code.
//! - **Args carry data, never English.** Numbers, ids, paths and the text
//!   that was read are args; a phrase a player reads is the catalog's. A
//!   wrapped cause is [`Arg::Nested`], rendered by the renderer in its turn.
//! - **Every code has a message,** in every shipped locale, using exactly the
//!   args the error gives.
//!
//! A game's own errors implement [`Diagnostic`] the same way, with codes
//! starting with the game's name. Where an angzarr-embedded error carries a game's
//! error (a migration step's, a decoder's) it holds it as a [`Report`]: the
//! code, args and English captured, so the error stays `Clone` and `Eq`.
//!
//! [`code`]: Diagnostic::code
//! [`args`]: Diagnostic::args

use std::fmt;

/// An error a player may read: a stable code and its named data. `Display`
/// is the English fallback.
pub trait Diagnostic: fmt::Debug + fmt::Display {
    /// The message id: kebab-case, starting with the crate's name, never
    /// reused.
    fn code(&self) -> &'static str;

    /// The message's arguments, by name. The same names every time for a
    /// given code.
    fn args(&self) -> Vec<(&'static str, Arg)>;
}

/// What cannot fail has nothing to say: for a domain that never rejects,
/// say.
impl Diagnostic for std::convert::Infallible {
    fn code(&self) -> &'static str {
        match *self {}
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match *self {}
    }
}

/// One argument to a message.
#[derive(Debug)]
pub enum Arg {
    Int(i64),
    /// A count, a length, a sequence number: anything that may exceed
    /// `i64`.
    Uint(u64),
    /// An id, a path, a key or the text that was read: never prose to
    /// translate.
    Str(String),
    /// A wrapped cause, rendered in its own right.
    Nested(Box<dyn Diagnostic>),
}

impl Arg {
    /// A wrapped cause, cloned into the arg.
    pub fn nested<D: Diagnostic + Clone + 'static>(cause: &D) -> Arg {
        Arg::Nested(Box::new(cause.clone()))
    }
}

macro_rules! from_int {
    ($variant:ident, $wide:ty: $($t:ty),*) => {
        $(impl From<$t> for Arg {
            fn from(n: $t) -> Arg {
                Arg::$variant(<$wide>::from(n))
            }
        })*
    };
}

from_int!(Int, i64: i8, i16, i32, i64);
from_int!(Uint, u64: u8, u16, u32, u64);

impl From<usize> for Arg {
    fn from(n: usize) -> Arg {
        Arg::Uint(n as u64)
    }
}

impl From<String> for Arg {
    fn from(s: String) -> Arg {
        Arg::Str(s)
    }
}

impl From<&str> for Arg {
    fn from(s: &str) -> Arg {
        Arg::Str(s.to_string())
    }
}

impl From<char> for Arg {
    fn from(c: char) -> Arg {
        Arg::Str(c.to_string())
    }
}

/// A diagnostic captured as data: its code, its args and its English. How
/// an angzarr-embedded error holds a game's error, keeping the holder `Clone` and
/// `Eq`. Also a quick way for a game to raise one without an error type of
/// its own:
///
/// ```
/// use angzarr_embedded_diagnostic::{Diagnostic, Report};
///
/// let e = Report::new("my-game-unknown-unit", "no unit \"ogre\"").arg("unit", "ogre");
/// assert_eq!(e.code(), "my-game-unknown-unit");
/// assert_eq!(e.to_string(), "no unit \"ogre\"");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    code: &'static str,
    args: Vec<(&'static str, Held)>,
    english: String,
}

/// An [`Arg`], owned and comparable.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Held {
    Int(i64),
    Uint(u64),
    Str(String),
    Nested(Box<Report>),
}

impl Held {
    fn of(arg: Arg) -> Held {
        match arg {
            Arg::Int(n) => Held::Int(n),
            Arg::Uint(n) => Held::Uint(n),
            Arg::Str(s) => Held::Str(s),
            Arg::Nested(d) => Held::Nested(Box::new(Report::of(&*d))),
        }
    }

    fn arg(&self) -> Arg {
        match self {
            Held::Int(n) => Arg::Int(*n),
            Held::Uint(n) => Arg::Uint(*n),
            Held::Str(s) => Arg::Str(s.clone()),
            Held::Nested(r) => Arg::Nested(r.clone()),
        }
    }
}

impl Report {
    /// A diagnostic with no args yet, and its English.
    pub fn new(code: &'static str, english: impl Into<String>) -> Report {
        Report {
            code,
            args: Vec::new(),
            english: english.into(),
        }
    }

    /// Add an arg.
    pub fn arg(mut self, name: &'static str, value: impl Into<Arg>) -> Report {
        self.args.push((name, Held::of(value.into())));
        self
    }

    /// Capture any diagnostic, nested causes and all.
    pub fn of(d: &dyn Diagnostic) -> Report {
        Report {
            code: d.code(),
            args: d
                .args()
                .into_iter()
                .map(|(name, arg)| (name, Held::of(arg)))
                .collect(),
            english: d.to_string(),
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.english)
    }
}

impl std::error::Error for Report {}

impl Diagnostic for Report {
    fn code(&self) -> &'static str {
        self.code
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        self.args
            .iter()
            .map(|(name, held)| (*name, held.arg()))
            .collect()
    }
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
