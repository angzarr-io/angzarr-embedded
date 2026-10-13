//! Why a document does not parse.

use std::fmt;

use annals_diagnostic::{Arg, Diagnostic};

/// What went wrong, and on which line (1-based; 0 for the end of input).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub line: usize,
    pub problem: ParseProblem,
}

/// What is wrong with a line. Text from the document (`found`, `key`,
/// `marker`, `escape`) is kept as read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseProblem {
    /// A `\r` not followed by `\n`.
    LoneCarriageReturn,
    /// A tab where indentation goes.
    TabIndent,
    /// A non-ASCII space character where indentation goes.
    NonAsciiIndent { character: char },
    /// `---` or `...`: a second document.
    DocumentMarker { marker: String },
    /// `&`: an anchor.
    Anchor,
    /// `*`: an alias.
    Alias,
    /// `!`: a tag.
    Tag,
    /// `|` or `>`: a block scalar.
    BlockScalar,
    /// A line indented deeper than its block.
    UnexpectedIndent,
    /// A line in a mapping that is not `key: value`.
    ExpectedKeyValue { found: String },
    /// A key that is a collection.
    KeyNotScalar,
    /// A key twice in one mapping.
    DuplicateKey { key: String },
    /// Text left over after a value.
    Unexpected { found: String },
    /// No `,` or `]` after an item of a flow sequence.
    ExpectedSeqSeparator,
    /// No `:` after a key in a flow mapping.
    ExpectedColon,
    /// No `,` or `}` after an entry of a flow mapping.
    ExpectedMapSeparator,
    /// A `\u` or `\U` escape that does not name a Unicode scalar value.
    BadUnicodeEscape { escape: String },
    /// A backslash escape this reader does not take.
    UnknownEscape { escape: String },
    /// A quoted scalar with no closing quote.
    UnterminatedQuote,
}

impl fmt::Display for ParseProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseProblem::LoneCarriageReturn => {
                f.write_str("a carriage return without a line feed: lines end in \\n or \\r\\n")
            }
            ParseProblem::TabIndent => f.write_str("tabs can't indent"),
            ParseProblem::NonAsciiIndent { character } => {
                write!(f, "indentation is ASCII spaces only, not {}", code_point(*character))
            }
            ParseProblem::DocumentMarker { marker } => write!(f, "one document per file: no {marker}"),
            ParseProblem::Anchor => f.write_str("anchors (&) aren't supported"),
            ParseProblem::Alias => f.write_str("aliases (*) aren't supported"),
            ParseProblem::Tag => f.write_str("tags (!) aren't supported"),
            ParseProblem::BlockScalar => f.write_str("block scalars (| and >) aren't supported"),
            ParseProblem::UnexpectedIndent => f.write_str("unexpected indentation"),
            ParseProblem::ExpectedKeyValue { found } => write!(f, "expected `key: value`, found {found:?}"),
            ParseProblem::KeyNotScalar => f.write_str("keys must be scalars"),
            ParseProblem::DuplicateKey { key } => write!(f, "{key:?} twice"),
            ParseProblem::Unexpected { found } => write!(f, "unexpected {found:?}"),
            ParseProblem::ExpectedSeqSeparator => f.write_str("expected , or ] in a flow sequence"),
            ParseProblem::ExpectedColon => f.write_str("expected : in a flow mapping"),
            ParseProblem::ExpectedMapSeparator => f.write_str("expected , or } in a flow mapping"),
            ParseProblem::BadUnicodeEscape { escape } => write!(
                f,
                "bad escape {escape}: \\u takes 4 hex digits and \\U 8, naming a Unicode scalar value; \
                 surrogates are refused (write \\U0001F5E1, not \\uD83D\\uDDE1)"
            ),
            ParseProblem::UnknownEscape { escape } => write!(f, "unknown escape {escape}"),
            ParseProblem::UnterminatedQuote => f.write_str("unterminated quote"),
        }
    }
}

/// `U+00A0`: how a message names a character that may not show.
fn code_point(c: char) -> String {
    format!("U+{:04X}", c as u32)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.problem)
    }
}

impl std::error::Error for Error {}

impl Diagnostic for Error {
    fn code(&self) -> &'static str {
        match self.problem {
            ParseProblem::LoneCarriageReturn => "annals-yaml-lone-carriage-return",
            ParseProblem::TabIndent => "annals-yaml-tab-indent",
            ParseProblem::NonAsciiIndent { .. } => "annals-yaml-non-ascii-indent",
            ParseProblem::DocumentMarker { .. } => "annals-yaml-document-marker",
            ParseProblem::Anchor => "annals-yaml-anchor",
            ParseProblem::Alias => "annals-yaml-alias",
            ParseProblem::Tag => "annals-yaml-tag",
            ParseProblem::BlockScalar => "annals-yaml-block-scalar",
            ParseProblem::UnexpectedIndent => "annals-yaml-unexpected-indent",
            ParseProblem::ExpectedKeyValue { .. } => "annals-yaml-expected-key-value",
            ParseProblem::KeyNotScalar => "annals-yaml-key-not-scalar",
            ParseProblem::DuplicateKey { .. } => "annals-yaml-duplicate-key",
            ParseProblem::Unexpected { .. } => "annals-yaml-unexpected",
            ParseProblem::ExpectedSeqSeparator => "annals-yaml-expected-seq-separator",
            ParseProblem::ExpectedColon => "annals-yaml-expected-colon",
            ParseProblem::ExpectedMapSeparator => "annals-yaml-expected-map-separator",
            ParseProblem::BadUnicodeEscape { .. } => "annals-yaml-bad-unicode-escape",
            ParseProblem::UnknownEscape { .. } => "annals-yaml-unknown-escape",
            ParseProblem::UnterminatedQuote => "annals-yaml-unterminated-quote",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        let mut args = vec![("line", Arg::from(self.line))];
        match &self.problem {
            ParseProblem::NonAsciiIndent { character } => args.push(("character", code_point(*character).into())),
            ParseProblem::DocumentMarker { marker } => args.push(("marker", marker.as_str().into())),
            ParseProblem::ExpectedKeyValue { found } | ParseProblem::Unexpected { found } => {
                args.push(("found", found.as_str().into()))
            }
            ParseProblem::DuplicateKey { key } => args.push(("key", key.as_str().into())),
            ParseProblem::BadUnicodeEscape { escape } | ParseProblem::UnknownEscape { escape } => {
                args.push(("escape", escape.as_str().into()))
            }
            ParseProblem::LoneCarriageReturn
            | ParseProblem::TabIndent
            | ParseProblem::Anchor
            | ParseProblem::Alias
            | ParseProblem::Tag
            | ParseProblem::BlockScalar
            | ParseProblem::UnexpectedIndent
            | ParseProblem::KeyNotScalar
            | ParseProblem::ExpectedSeqSeparator
            | ParseProblem::ExpectedColon
            | ParseProblem::ExpectedMapSeparator
            | ParseProblem::UnterminatedQuote => {}
        }
        args
    }
}
