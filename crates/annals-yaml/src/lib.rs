//! A strict subset of YAML, read and written: content files a person edits by
//! hand and a tool rewrites.
//!
//! Supported: block mappings (`key: value`, `key:` then an indented block),
//! block sequences (`- item`, including `- key: value` starting a mapping),
//! flow sequences `[a, b]` and flow mappings `{a: 1, b: 2}`, plain and quoted
//! scalars, `#` comments, and an empty value as null. Not supported, and
//! refused rather than misread: anchors and aliases, tags, multi-line
//! strings, multiple documents, tabs for indentation.
//!
//! Text: whitespace in the grammar is ASCII space and tab, as YAML defines
//! it. Indentation is spaces; a `#` starts a comment at the start of a line
//! or after a space or tab, outside quotes; a quote opens a quoted scalar
//! only where a scalar starts, so `l'épée` is plain. Any other space
//! character (U+00A0, U+3000, ...) is content, and one at the start of a line
//! is refused rather than taken as indentation. Lines end in `\n` or `\r\n`;
//! a lone `\r` is refused, not read as a line break. One leading byte-order
//! mark is skipped.
//!
//! Double-quoted scalars take the escapes `\\` `\"` `\n` `\t` `\r` `\0`,
//! `\uXXXX` and `\UXXXXXXXX`; any other escape is an error. A `\u` or `\U`
//! must name a Unicode scalar value: UTF-16 surrogates, paired or not, are
//! refused, because YAML's `\u` is one character, not a UTF-16 unit (an
//! astral character is `\U0001F5E1`, or just the character). Single quotes
//! take `''` for a quote.
//!
//! The writer writes a scalar plain only when the reader reads it back
//! unchanged, and otherwise double-quotes it, escaping the control characters
//! (C0, DEL, C1), U+2028, U+2029 and U+FEFF. Every written document parses
//! back to the value written.
//!
//! Every scalar is kept as its text: there is no type coercion, so `no`
//! stays `"no"` and `1.10` stays `"1.10"`. The reader of a value decides what
//! it means. Mappings keep their order.
//!
//! Keys compare by bytes, with no Unicode normalization: `é` (U+00E9) and
//! `e` + U+0301 look the same and are two different keys. Normalizing would
//! need Unicode's tables, a dependency this crate does not take. Games should
//! keep ids (keys and the names other content refers to) ASCII, which
//! [`is_ascii_id`] checks, as annals-graph's tags already are; display text
//! in values can be any Unicode.
//!
//! A document that does not parse gives an [`Error`]: the line and a
//! [`ParseProblem`]. Every error here is an [`annals_diagnostic::Diagnostic`],
//! with its en-US messages in `locales/en-US/annals-yaml.ftl`.
//!
//! [`versioned`] loads documents stamped with a root `version`, running
//! registered migrations up to the version the code reads. [`edit`] changes
//! one sequence item of a document in place and leaves every other byte,
//! comments included, as written.

pub mod edit;
mod error;
pub mod versioned;

pub use error::{Error, ParseProblem};

/// A YAML value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Scalar(String),
    Seq(Vec<Value>),
    Map(Vec<(String, Value)>),
}

/// Whether `s` is an ASCII id: non-empty, and only ASCII letters, digits,
/// `_`, `-` and `.`. Such ids have one spelling, so they compare as the
/// reader expects without Unicode normalization.
pub fn is_ascii_id(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

fn err<T>(line: usize, problem: ParseProblem) -> Result<T, Error> {
    Err(Error { line, problem })
}

impl Value {
    pub fn scalar(s: impl Into<String>) -> Value {
        Value::Scalar(s.into())
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Scalar(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_seq(&self) -> Option<&[Value]> {
        match self {
            Value::Seq(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// The value at `key`, if this is a mapping that has it.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_map()?.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// The value at `key`, mutably, if this is a mapping that has it.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        match self {
            Value::Map(m) => m.iter_mut().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// Take `key` out of this mapping, keeping the rest in order. None if
    /// this is not a mapping or has no such key.
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let Value::Map(m) = self else { return None };
        let i = m.iter().position(|(k, _)| k == key)?;
        Some(m.remove(i).1)
    }

    /// The value at `key`, inserted as null if missing. Panics on a
    /// non-mapping.
    pub fn entry(&mut self, key: &str) -> &mut Value {
        let Value::Map(m) = self else {
            panic!("entry({key:?}) on a value that is not a mapping")
        };
        let i = match m.iter().position(|(k, _)| k == key) {
            Some(i) => i,
            None => {
                m.push((key.to_string(), Value::Null));
                m.len() - 1
            }
        };
        &mut m[i].1
    }

    /// Lay `patch` over this value: mappings merge key by key (recursively),
    /// sequences append, anything else is replaced.
    pub fn merge(&mut self, patch: &Value) {
        match (self, patch) {
            (Value::Map(mine), Value::Map(theirs)) => {
                for (k, v) in theirs {
                    match mine.iter_mut().find(|(mk, _)| mk == k) {
                        Some((_, mv)) => mv.merge(v),
                        None => mine.push((k.clone(), v.clone())),
                    }
                }
            }
            (Value::Seq(mine), Value::Seq(theirs)) => mine.extend(theirs.iter().cloned()),
            (me, other) => *me = other.clone(),
        }
    }
}

// ---------------------------------------------------------------- reading

/// One meaningful line: its number, indentation, and text without the
/// comment or trailing space.
struct Line<'a> {
    no: usize,
    indent: usize,
    text: &'a str,
}

/// Whitespace as the grammar means it: ASCII space and tab, as in YAML. Any
/// other space character (U+00A0, U+3000, ...) is content.
const SPACE: [char; 2] = [' ', '\t'];

fn trim(s: &str) -> &str {
    s.trim_matches(SPACE)
}

/// Cut a `#` comment off: a `#` at the start or after a space or tab, outside
/// quotes. A quote opens a quoted scalar only where a scalar starts: at the
/// start, after a space or tab, or after `[` `{` `,` inside a flow
/// collection. Elsewhere (`l'épée`) it is a plain character.
fn strip_comment(s: &str) -> &str {
    let b = s.as_bytes();
    let (mut i, mut prev, mut depth) = (0, b' ', 0u32);
    let starts = |prev: u8, depth: u32| matches!(prev, b' ' | b'\t') || depth > 0 && matches!(prev, b'[' | b'{' | b',');
    while i < b.len() {
        let c = b[i];
        match c {
            b'"' | b'\'' if starts(prev, depth) => i = closing_quote(b, i),
            b'#' if matches!(prev, b' ' | b'\t') => return &s[..i],
            b'[' | b'{' if starts(prev, depth) => depth += 1,
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        prev = c;
        i += 1;
    }
    s
}

/// The index of the quote closing the quoted scalar that opens at `b[open]`,
/// or the length if it never closes. Inside `"`, a backslash escapes the
/// next character; inside `'`, `''` is a quote.
fn closing_quote(b: &[u8], open: usize) -> usize {
    let q = b[open];
    let mut i = open + 1;
    while i < b.len() {
        match b[i] {
            b'\\' if q == b'"' => i += 2,
            b'\'' if q == b'\'' && b.get(i + 1) == Some(&b'\'') => i += 2,
            c if c == q => return i,
            _ => i += 1,
        }
    }
    b.len()
}

/// The lines of `src`, split at `\n`, each without a `\r` before its `\n`. A
/// carriage return anywhere else is refused, not read as a line break: no
/// editor still writes CR-only files, and inside a value it is written `\r`.
fn raw_lines(src: &str) -> impl Iterator<Item = Result<(usize, &str), Error>> {
    let last = src.split('\n').count() - 1;
    src.split('\n').enumerate().map(move |(i, raw)| {
        let line = if i < last { raw.strip_suffix('\r').unwrap_or(raw) } else { raw };
        if line.contains('\r') {
            return err(i + 1, ParseProblem::LoneCarriageReturn);
        }
        Ok((i + 1, line))
    })
}

fn lines(src: &str) -> Result<Vec<Line<'_>>, Error> {
    let mut out = Vec::new();
    for raw in raw_lines(src) {
        let (no, raw) = raw?;
        let body = strip_comment(raw).trim_end_matches(SPACE);
        if trim(body).is_empty() {
            continue;
        }
        let indent = body.len() - body.trim_start_matches(' ').len();
        let text = &body[indent..];
        if text.starts_with('\t') {
            return err(no, ParseProblem::TabIndent);
        }
        if let Some(character) = text.chars().next().filter(|c| c.is_whitespace()) {
            return err(no, ParseProblem::NonAsciiIndent { character });
        }
        if text == "---" || text == "..." {
            return err(no, ParseProblem::DocumentMarker { marker: text.to_string() });
        }
        if let Some(problem) = unsupported(text) {
            return err(no, problem);
        }
        out.push(Line { no, indent, text });
    }
    Ok(out)
}

/// Parse a document. One leading byte-order mark is skipped: editors add it,
/// and it is never part of the first key.
pub fn parse(src: &str) -> Result<Value, Error> {
    let src = src.strip_prefix('\u{feff}').unwrap_or(src);
    let lines = lines(src)?;
    if lines.is_empty() {
        return Ok(Value::Null);
    }
    let mut p = Parser { lines, at: 0 };
    let indent = p.lines[0].indent;
    let v = p.block(indent)?;
    if let Some(l) = p.lines.get(p.at) {
        return err(l.no, ParseProblem::UnexpectedIndent);
    }
    Ok(v)
}

struct Parser<'a> {
    lines: Vec<Line<'a>>,
    at: usize,
}

/// Split `key: rest` at the first `:` followed by a space, a tab or the end
/// of the line. A quoted key is the whole quoted scalar, then the `:`. A line
/// starting `[` or `{` is a flow collection, never a key.
fn split_key(s: &str) -> Option<(&str, &str)> {
    let b = s.as_bytes();
    let separator = |i: usize| b[i] == b':' && (i + 1 == b.len() || matches!(b[i + 1], b' ' | b'\t'));
    let split = |i: usize| Some((s[..i].trim_end_matches(SPACE), s[i + 1..].trim_start_matches(SPACE)));
    match b.first()? {
        b'[' | b'{' => None,
        b'"' | b'\'' => {
            let mut i = closing_quote(b, 0) + 1;
            while i < b.len() && matches!(b[i], b' ' | b'\t') {
                i += 1;
            }
            (i < b.len() && separator(i)).then(|| split(i)).flatten()
        }
        _ => (0..b.len()).find(|&i| separator(i)).and_then(split),
    }
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Line<'a>> {
        self.lines.get(self.at)
    }

    /// A block at exactly `indent`: a sequence if it starts with `- `, else a
    /// mapping, else a single scalar line.
    fn block(&mut self, indent: usize) -> Result<Value, Error> {
        let first = self.peek().expect("a line");
        if is_item(first.text) {
            self.seq(indent)
        } else if split_key(first.text).is_some() {
            self.map(indent)
        } else {
            let (no, text) = (first.no, first.text);
            self.at += 1;
            inline(text, no)
        }
    }

    fn seq(&mut self, indent: usize) -> Result<Value, Error> {
        let mut items = Vec::new();
        while let Some(l) = self.peek() {
            if l.indent < indent {
                break;
            }
            if l.indent > indent {
                return err(l.no, ParseProblem::UnexpectedIndent);
            }
            if !is_item(l.text) {
                break;
            }
            let (no, rest) = (l.no, l.text[1..].trim_start_matches(SPACE));
            let inner = indent + (l.text.len() - rest.len());
            self.at += 1;
            if rest.is_empty() {
                items.push(self.nested(indent, no)?);
            } else if is_item(rest) || split_key(rest).is_some() && !rest.starts_with('{') && !rest.starts_with('[') {
                // `- key: value` starts a mapping (or `- - x` a sequence)
                // whose further lines sit at the item's own indent.
                self.lines.insert(self.at, Line { no, indent: inner, text: rest });
                items.push(self.block(inner)?);
            } else {
                items.push(inline(rest, no)?);
            }
        }
        Ok(Value::Seq(items))
    }

    fn map(&mut self, indent: usize) -> Result<Value, Error> {
        let mut entries: Vec<(String, Value)> = Vec::new();
        while let Some(l) = self.peek() {
            if l.indent < indent {
                break;
            }
            if l.indent > indent {
                return err(l.no, ParseProblem::UnexpectedIndent);
            }
            if is_item(l.text) {
                break;
            }
            let no = l.no;
            let Some((key, rest)) = split_key(l.text) else {
                return err(no, ParseProblem::ExpectedKeyValue { found: l.text.to_string() });
            };
            let key = match inline(key, no)? {
                Value::Scalar(k) => k,
                _ => return err(no, ParseProblem::KeyNotScalar),
            };
            if entries.iter().any(|(k, _)| *k == key) {
                return err(no, ParseProblem::DuplicateKey { key });
            }
            self.at += 1;
            let value = if rest.is_empty() {
                // A sequence may sit at the key's own indent.
                match self.peek() {
                    Some(n) if n.indent == indent && is_item(n.text) => self.seq(indent)?,
                    _ => self.nested(indent, no)?,
                }
            } else {
                inline(rest, no)?
            };
            entries.push((key, value));
        }
        Ok(Value::Map(entries))
    }

    /// The block under a line at `indent`, or null if nothing is deeper.
    fn nested(&mut self, indent: usize, _no: usize) -> Result<Value, Error> {
        match self.peek() {
            Some(n) if n.indent > indent => {
                let i = n.indent;
                self.block(i)
            }
            _ => Ok(Value::Null),
        }
    }
}

fn is_item(text: &str) -> bool {
    text == "-" || text.starts_with("- ")
}

/// The feature a line or value starts with that this reader refuses.
fn unsupported(text: &str) -> Option<ParseProblem> {
    match text.as_bytes().first()? {
        b'&' => Some(ParseProblem::Anchor),
        b'*' => Some(ParseProblem::Alias),
        b'!' => Some(ParseProblem::Tag),
        b'|' | b'>' => Some(ParseProblem::BlockScalar),
        _ => None,
    }
}

/// A value written on one line: a flow collection or a scalar.
fn inline(text: &str, no: usize) -> Result<Value, Error> {
    if let Some(problem) = unsupported(text) {
        return err(no, problem);
    }
    let mut f = Flow { s: text, at: 0, no };
    let v = f.value()?;
    f.space();
    if f.at != f.s.len() {
        return err(no, ParseProblem::Unexpected { found: f.s[f.at..].to_string() });
    }
    Ok(v)
}

struct Flow<'a> {
    s: &'a str,
    at: usize,
    no: usize,
}

impl<'a> Flow<'a> {
    fn rest(&self) -> &'a str {
        &self.s[self.at..]
    }

    fn space(&mut self) {
        self.at += self.rest().len() - self.rest().trim_start_matches(SPACE).len();
    }

    fn eat(&mut self, c: char) -> bool {
        self.space();
        if self.rest().starts_with(c) {
            self.at += c.len_utf8();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Result<Value, Error> {
        self.space();
        if self.eat('[') {
            let mut items = Vec::new();
            if self.eat(']') {
                return Ok(Value::Seq(items));
            }
            loop {
                items.push(self.value()?);
                if self.eat(']') {
                    return Ok(Value::Seq(items));
                }
                if !self.eat(',') {
                    return err(self.no, ParseProblem::ExpectedSeqSeparator);
                }
            }
        }
        if self.eat('{') {
            let mut entries: Vec<(String, Value)> = Vec::new();
            if self.eat('}') {
                return Ok(Value::Map(entries));
            }
            loop {
                let key = match self.scalar(true)? {
                    Value::Scalar(k) => k,
                    _ => return err(self.no, ParseProblem::KeyNotScalar),
                };
                if !self.eat(':') {
                    return err(self.no, ParseProblem::ExpectedColon);
                }
                let v = self.value()?;
                if entries.iter().any(|(k, _)| *k == key) {
                    return err(self.no, ParseProblem::DuplicateKey { key });
                }
                entries.push((key, v));
                if self.eat('}') {
                    return Ok(Value::Map(entries));
                }
                if !self.eat(',') {
                    return err(self.no, ParseProblem::ExpectedMapSeparator);
                }
            }
        }
        self.scalar(false)
    }

    /// A scalar: quoted, or plain up to the end — or, inside a flow
    /// collection, up to `,` `]` `}` (and `:` for a key).
    fn scalar(&mut self, key: bool) -> Result<Value, Error> {
        self.space();
        let rest = self.rest();
        let nested = self.s.starts_with('[') || self.s.starts_with('{');
        match rest.chars().next() {
            Some(q @ ('"' | '\'')) => {
                let mut out = String::new();
                let mut chars = rest.char_indices().skip(1).peekable();
                while let Some((i, c)) = chars.next() {
                    if c == q {
                        if q == '\'' && matches!(chars.peek(), Some((_, '\''))) {
                            chars.next();
                            out.push('\'');
                            continue;
                        }
                        self.at += i + 1;
                        return Ok(Value::Scalar(out));
                    }
                    if q == '"' && c == '\\' {
                        match chars.next() {
                            Some((_, 'n')) => out.push('\n'),
                            Some((_, 't')) => out.push('\t'),
                            Some((_, 'r')) => out.push('\r'),
                            Some((_, '0')) => out.push('\0'),
                            Some((_, e @ ('"' | '\\'))) => out.push(e),
                            Some((_, u @ ('u' | 'U'))) => {
                                let n = if u == 'u' { 4 } else { 8 };
                                let digits: String =
                                    (0..n).map_while(|_| chars.next_if(|(_, c)| c.is_ascii_hexdigit()).map(|(_, c)| c)).collect();
                                let c = u32::from_str_radix(&digits, 16).ok().filter(|_| digits.len() == n).and_then(char::from_u32);
                                match c {
                                    Some(c) => out.push(c),
                                    None => {
                                        return err(self.no, ParseProblem::BadUnicodeEscape { escape: format!("\\{u}{digits}") })
                                    }
                                }
                            }
                            other => {
                                let escape = other.map_or(String::from("\\"), |(_, c)| format!("\\{c}"));
                                return err(self.no, ParseProblem::UnknownEscape { escape });
                            }
                        }
                        continue;
                    }
                    out.push(c);
                }
                err(self.no, ParseProblem::UnterminatedQuote)
            }
            None => Ok(Value::Null),
            Some(_) => {
                let end = if nested {
                    rest.find(|c| c == ',' || c == ']' || c == '}' || (key && c == ':')).unwrap_or(rest.len())
                } else {
                    rest.len()
                };
                let text = rest[..end].trim_end_matches(SPACE);
                self.at += end;
                if text.is_empty() {
                    return Ok(Value::Null);
                }
                if text == "~" || text == "null" {
                    return Ok(Value::Null);
                }
                Ok(Value::Scalar(text.to_string()))
            }
        }
    }
}

// ---------------------------------------------------------------- writing

/// Where a scalar is written.
#[derive(Clone, Copy, PartialEq, Eq)]
enum At {
    Key,
    Value,
    FlowKey,
    FlowValue,
}

/// A character written as an escape: the control characters (C0, DEL, C1,
/// tab and line feed included), the Unicode line and paragraph separators,
/// and the byte-order mark.
fn escaped(c: char) -> bool {
    c.is_control() || matches!(c, '\u{2028}' | '\u{2029}' | '\u{feff}')
}

/// Whether a scalar can be written plain at `at` and read back the same.
fn plain_ok(s: &str, at: At) -> bool {
    let (Some(first), Some(last)) = (s.chars().next(), s.chars().next_back()) else {
        return false;
    };
    if matches!(s, "~" | "null" | "---" | "...") || first.is_whitespace() || last.is_whitespace() || s.chars().any(escaped) {
        return false;
    }
    if "-?:,[]{}#&*!|>'\"%@`".contains(first) && !(first == '-' && s.len() > 1 && !s.starts_with("- ")) {
        return false;
    }
    // `: ` would end a key, ` #` start a comment, and a quote after a space
    // or `[` `{` `,` open a quoted scalar.
    if s.contains(": ") || s.ends_with(':') || s.contains(" #") {
        return false;
    }
    if s.as_bytes().windows(2).any(|w| matches!(w[0], b' ' | b'[' | b'{' | b',') && matches!(w[1], b'"' | b'\'')) {
        return false;
    }
    match at {
        At::Value => true,
        At::Key => !s.contains(['[', ']', '{', '}']),
        At::FlowValue => !s.contains([',', '[', ']', '{', '}']),
        At::FlowKey => !s.contains([',', '[', ']', '{', '}', ':']),
    }
}

fn scalar_text(s: &str, at: At) -> String {
    if plain_ok(s, at) {
        return s.to_string();
    }
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            c if escaped(c) => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A value on one line, if it is small enough: scalars, and collections of
/// scalars.
fn flow(v: &Value) -> Option<String> {
    match v {
        Value::Null => Some(String::new()),
        Value::Scalar(s) => Some(scalar_text(s, At::Value)),
        Value::Seq(items) => {
            let parts: Option<Vec<String>> = items
                .iter()
                .map(|i| match i {
                    Value::Scalar(s) => Some(scalar_text(s, At::FlowValue)),
                    _ => None,
                })
                .collect();
            Some(format!("[{}]", parts?.join(", ")))
        }
        Value::Map(entries) => {
            let parts: Option<Vec<String>> = entries
                .iter()
                .map(|(k, v)| match v {
                    Value::Scalar(s) => Some(format!("{}: {}", scalar_text(k, At::FlowKey), scalar_text(s, At::FlowValue))),
                    _ => None,
                })
                .collect();
            Some(format!("{{{}}}", parts?.join(", ")))
        }
    }
}

/// Write a value as a document: block style, with small collections of
/// scalars on one line once they are nested (`flow_depth` levels deep).
pub fn write(v: &Value, flow_depth: usize) -> String {
    match v {
        // Nothing in block style would read back as null.
        Value::Map(m) if m.is_empty() => "{}\n".to_string(),
        Value::Seq(s) if s.is_empty() => "[]\n".to_string(),
        _ => {
            let mut out = String::new();
            block(v, 0, 0, flow_depth, &mut out);
            out
        }
    }
}

fn block(v: &Value, indent: usize, depth: usize, flow_depth: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    match v {
        Value::Map(entries) => {
            for (k, val) in entries {
                let key = scalar_text(k, At::Key);
                match val {
                    Value::Map(m) if !(depth + 1 >= flow_depth && flow(val).is_some()) && !m.is_empty() => {
                        out.push_str(&format!("{pad}{key}:\n"));
                        block(val, indent + 2, depth + 1, flow_depth, out);
                    }
                    Value::Seq(s) if !(depth + 1 >= flow_depth && flow(val).is_some()) && !s.is_empty() => {
                        out.push_str(&format!("{pad}{key}:\n"));
                        block(val, indent + 2, depth + 1, flow_depth, out);
                    }
                    _ => {
                        let text = flow(val).unwrap_or_default();
                        if text.is_empty() {
                            out.push_str(&format!("{pad}{key}:\n"));
                        } else {
                            out.push_str(&format!("{pad}{key}: {text}\n"));
                        }
                    }
                }
            }
        }
        Value::Seq(items) => {
            for item in items {
                match item {
                    Value::Map(m) if !m.is_empty() && !(depth + 1 >= flow_depth && flow(item).is_some()) => {
                        let mut inner = String::new();
                        block(item, indent + 2, depth + 1, flow_depth, &mut inner);
                        // The first line moves up beside the dash.
                        let first = inner.trim_start_matches(' ');
                        out.push_str(&format!("{pad}- {first}"));
                    }
                    Value::Seq(s) if !s.is_empty() && !(depth + 1 >= flow_depth && flow(item).is_some()) => {
                        out.push_str(&format!("{pad}-\n"));
                        block(item, indent + 2, depth + 1, flow_depth, out);
                    }
                    _ => {
                        let text = flow(item).unwrap_or_default();
                        out.push_str(&format!("{pad}- {text}\n").replace("- \n", "-\n"));
                    }
                }
            }
        }
        other => out.push_str(&format!("{pad}{}\n", flow(other).unwrap_or_default())),
    }
}

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test: each one's code must have a message in every locale,
/// using exactly its args. Its messages are in
/// `crates/annals-yaml/locales/en-US/annals-yaml.ftl`.
pub fn diagnostic_samples() -> Vec<Box<dyn annals_diagnostic::Diagnostic>> {
    let text = |s: &str| s.to_string();
    let parse = [
        ParseProblem::LoneCarriageReturn,
        ParseProblem::TabIndent,
        ParseProblem::NonAsciiIndent { character: '\u{a0}' },
        ParseProblem::DocumentMarker { marker: text("---") },
        ParseProblem::Anchor,
        ParseProblem::Alias,
        ParseProblem::Tag,
        ParseProblem::BlockScalar,
        ParseProblem::UnexpectedIndent,
        ParseProblem::ExpectedKeyValue { found: text("b") },
        ParseProblem::KeyNotScalar,
        ParseProblem::DuplicateKey { key: text("a") },
        ParseProblem::Unexpected { found: text("x") },
        ParseProblem::ExpectedSeqSeparator,
        ParseProblem::ExpectedColon,
        ParseProblem::ExpectedMapSeparator,
        ParseProblem::BadUnicodeEscape { escape: text(r"\uD83D") },
        ParseProblem::UnknownEscape { escape: text(r"\x") },
        ParseProblem::UnterminatedQuote,
    ];
    let mut samples: Vec<Box<dyn annals_diagnostic::Diagnostic>> = Vec::new();
    samples.extend(parse.into_iter().map(|problem| Box::new(Error { line: 3, problem }) as _));
    samples.extend(versioned::diagnostic_samples());
    samples.extend(edit::diagnostic_samples());
    samples
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(x: &str) -> Value {
        Value::scalar(x)
    }

    #[test]
    fn reads_block_mappings_and_sequences() {
        let v = parse(
            "# a comment\nname: helmet   # trailing\nwards:\n  - Concussion\n  - \"Cracked: Rib\"\nwounds:\n- absorb: Helmet Deflects\n  depth: 250\n- flesh: Helmet Rings\n  depth: 200\nempty:\n",
        )
        .unwrap();
        assert_eq!(v.get("name"), Some(&s("helmet")));
        assert_eq!(v.get("wards"), Some(&Value::Seq(vec![s("Concussion"), s("Cracked: Rib")])));
        let wounds = v.get("wounds").unwrap().as_seq().unwrap();
        assert_eq!(wounds.len(), 2);
        assert_eq!(wounds[0].get("depth"), Some(&s("250")));
        assert_eq!(wounds[1].get("flesh"), Some(&s("Helmet Rings")));
        assert_eq!(v.get("empty"), Some(&Value::Null));
    }

    #[test]
    fn reads_flow_collections() {
        let v = parse("modes: [Attack (Snap), Attack (Aimed)]\nstat: {reflex: 3, will: 1}\nnone: []\n").unwrap();
        assert_eq!(v.get("modes"), Some(&Value::Seq(vec![s("Attack (Snap)"), s("Attack (Aimed)")])));
        assert_eq!(v.get("stat").unwrap().get("will"), Some(&s("1")));
        assert_eq!(v.get("none"), Some(&Value::Seq(vec![])));
    }

    #[test]
    fn scalars_keep_their_text() {
        let v = parse("a: no\nb: 1.10\nc: 'it''s'\nd: .45 ACP\ne: -1/4\n").unwrap();
        assert_eq!(v.get("a"), Some(&s("no")));
        assert_eq!(v.get("b"), Some(&s("1.10")));
        assert_eq!(v.get("c"), Some(&s("it's")));
        assert_eq!(v.get("d"), Some(&s(".45 ACP")));
        assert_eq!(v.get("e"), Some(&s("-1/4")));
    }

    #[test]
    fn refuses_what_it_does_not_support() {
        let found = |s: &str| s.to_string();
        for (src, line, problem) in [
            ("a: &x 1\n", 1, ParseProblem::Anchor),
            ("a: *x\n", 1, ParseProblem::Alias),
            ("- !tag x\n", 1, ParseProblem::Tag),
            ("a: |\n  text\n", 1, ParseProblem::BlockScalar),
            ("a:\n\t- x\n", 2, ParseProblem::TabIndent),
            ("---\na: 1\n", 1, ParseProblem::DocumentMarker { marker: found("---") }),
            ("a: 1\na: 2\n", 2, ParseProblem::DuplicateKey { key: found("a") }),
            ("a: {b: 1, b: 2}\n", 1, ParseProblem::DuplicateKey { key: found("b") }),
            ("a: [1, 2\n", 1, ParseProblem::ExpectedSeqSeparator),
            ("a: {b 1}\n", 1, ParseProblem::ExpectedColon),
            ("a: {b: 1\n", 1, ParseProblem::ExpectedMapSeparator),
            ("a: {: 1}\n", 1, ParseProblem::KeyNotScalar),
            ("a: 1\n   b: 2\n", 2, ParseProblem::UnexpectedIndent),
            ("a: 1\nb\n", 2, ParseProblem::ExpectedKeyValue { found: found("b") }),
            ("a: [1] x\n", 1, ParseProblem::Unexpected { found: found("x") }),
            ("a: \"x\n", 1, ParseProblem::UnterminatedQuote),
        ] {
            assert_eq!(parse(src), Err(Error { line, problem }), "{src:?}");
        }
        assert_eq!(parse("a: 1\na: 2\n").unwrap_err().to_string(), "line 2: \"a\" twice");
        assert_eq!(parse("a: &x 1\n").unwrap_err().to_string(), "line 1: anchors (&) aren't supported");
    }

    #[test]
    fn writes_what_it_reads() {
        let src = "items:\n  - name: helmet\n    wards: [Concussion, \"Cracked, Rib\"]\n    wounds:\n      - {absorb: Helmet Deflects, depth: 250, soak: 3}\n  - name: \"key: value\"\n    deltas:\n      stat: {reflex: 3}\n";
        let v = parse(src).unwrap();
        for depth in 0..5 {
            let written = write(&v, depth);
            assert_eq!(parse(&written).unwrap(), v, "flow depth {depth}:\n{written}");
        }
    }

    #[test]
    fn a_leading_bom_is_not_part_of_the_first_key() {
        let v = parse("\u{feff}version: 3\nname: x\n").unwrap();
        assert_eq!(v.get("version"), Some(&s("3")));
        let seq = parse("\u{feff}- a\n- b\n").unwrap();
        assert_eq!(seq, Value::Seq(vec![s("a"), s("b")]));
        // Only one: a second mark is content.
        let two = parse("\u{feff}\u{feff}x: 1\n").unwrap();
        assert_eq!(two.as_map().unwrap()[0].0, "\u{feff}x");
    }

    #[test]
    fn only_a_space_or_tab_before_hash_starts_a_comment() {
        assert_eq!(parse("name: a\u{a0}#b\n").unwrap().get("name"), Some(&s("a\u{a0}#b")));
        assert_eq!(parse("name: a\u{3000}#b\n").unwrap().get("name"), Some(&s("a\u{3000}#b")));
        assert_eq!(parse("name: a\t#b\n").unwrap().get("name"), Some(&s("a")));
        assert_eq!(parse("name: a #b\n").unwrap().get("name"), Some(&s("a")));
    }

    #[test]
    fn a_quote_opens_only_at_the_start_of_a_scalar() {
        assert_eq!(parse("name: l'\u{e9}p\u{e9}e # the sword\n").unwrap().get("name"), Some(&s("l'\u{e9}p\u{e9}e")));
        assert_eq!(parse("name: 5\"2 # tall\n").unwrap().get("name"), Some(&s("5\"2")));
        assert_eq!(parse("name: 'it''s # in' # out\n").unwrap().get("name"), Some(&s("it's # in")));
        assert_eq!(parse("name: \"a\\\" # in\" # out\n").unwrap().get("name"), Some(&s("a\" # in")));
        assert_eq!(parse("v: [l'a, 'b # c'] # d\n").unwrap().get("v"), Some(&Value::Seq(vec![s("l'a"), s("b # c")])));
        // A quoted key ends at its closing quote, escapes and all.
        let keys = parse("\"a\\\": b\": 1\n'it''s: x' : 2\na]b: 3\n").unwrap();
        assert_eq!(keys.get("a\": b"), Some(&s("1")));
        assert_eq!(keys.get("it's: x"), Some(&s("2")));
        assert_eq!(keys.get("a]b"), Some(&s("3")));
    }

    #[test]
    fn indentation_is_ascii_spaces_only() {
        for src in ["a:\n\u{a0}\u{a0}b: 1\n", "a:\n\u{3000}b: 1\n", "a:\n  \u{a0}b: 1\n", "\u{a0}\n"] {
            let e = parse(src).unwrap_err();
            assert!(matches!(e.problem, ParseProblem::NonAsciiIndent { .. }), "{src:?}: {e}");
            assert_eq!(e.line, if src.starts_with('\u{a0}') { 1 } else { 2 }, "{src:?}");
        }
        // Not whitespace to the grammar: content, kept.
        assert_eq!(parse("a: \u{a0}x\u{a0}\n").unwrap().get("a"), Some(&s("\u{a0}x\u{a0}")));
        assert_eq!(parse("a: {k: \u{3000}x}\n").unwrap().get("a").unwrap().get("k"), Some(&s("\u{3000}x")));
        assert_eq!(parse("a:\u{a0}b: 1\n").unwrap().get("a:\u{a0}b"), Some(&s("1")));
        assert_eq!(parse("a:\tb\n").unwrap().get("a"), Some(&s("b")));
    }

    #[test]
    fn line_breaks_are_lf_or_crlf_and_a_lone_cr_is_refused() {
        let crlf = parse("a: 1\r\nb:\r\n  - x\r\n  - \"y\"\r\n").unwrap();
        assert_eq!(crlf, parse("a: 1\nb:\n  - x\n  - \"y\"\n").unwrap());
        for (src, line) in [("a: 1\rb: 2\r", 1), ("a: 1\nb: x\ry\n", 2), ("a: \"x\ry\"", 1), ("a: 1\r", 1)] {
            let e = parse(src).unwrap_err();
            assert_eq!(e.line, line, "{src:?}");
            assert_eq!(e.problem, ParseProblem::LoneCarriageReturn, "{src:?}: {e}");
        }
    }

    /// `^` stands for a backslash, so the escapes under test stay readable.
    fn bs(s: &str) -> String {
        s.replace('^', "\\")
    }

    #[test]
    fn double_quotes_take_unicode_cr_and_nul_escapes() {
        let v = parse(&bs(r#"a: "^u00e9^r^0^t^n^^^"^U0001F5E1""#)).unwrap();
        assert_eq!(v.get("a"), Some(&s("\u{e9}\r\0\t\n\\\"\u{1f5e1}")));
        assert_eq!(parse(&bs(r#"a: "^u00E9""#)).unwrap().get("a"), Some(&s("\u{e9}")));
        for bad in ["^uD83D^uDDE1", "^uDDE1", "^u00e", "^U00110000", "^x41", "^q", "^u+0e9"] {
            let e = parse(&bs(&format!("a: \"{bad}\"\n"))).unwrap_err();
            assert!(matches!(e.problem, ParseProblem::BadUnicodeEscape { .. } | ParseProblem::UnknownEscape { .. }), "{bad}: {e}");
        }
        let e = parse(&bs(r#"a: "^q""#)).unwrap_err();
        assert_eq!(e.problem, ParseProblem::UnknownEscape { escape: bs("^q") });
        assert_eq!(e.to_string(), bs("line 1: unknown escape ^q"));
        let e = parse(&bs(r#"a: "^uDDE1""#)).unwrap_err();
        assert_eq!(e.problem, ParseProblem::BadUnicodeEscape { escape: bs("^uDDE1") });
    }

    fn round_trips(v: &Value) {
        for depth in 0..4 {
            let written = write(v, depth);
            assert_eq!(parse(&written).as_ref(), Ok(v), "flow depth {depth}:\n{written}");
        }
    }

    #[test]
    fn keys_with_brackets_or_colons_are_quoted() {
        for key in ["a[1", "a{1", "a]b", "a}b", "x:y", "x:", "a,b", "a\t#b", "a #b", "a\u{a0}#b"] {
            let flat = Value::Map(vec![(key.to_string(), s("v"))]);
            round_trips(&flat);
            round_trips(&Value::Map(vec![("k".into(), flat.clone())]));
            round_trips(&Value::Seq(vec![flat]));
        }
    }

    #[test]
    fn control_characters_are_escaped() {
        for text in ["a\u{0}b", "\u{1b}[31m", "a\rb", "a\u{85}b", "a\u{2028}b", "\u{feff}bom", "tab\there", "a\u{7f}", "a\u{9c}"] {
            let v = Value::Map(vec![(text.to_string(), s(text)), ("seq".into(), Value::Seq(vec![s(text)]))]);
            round_trips(&v);
            let written = write(&v, 0);
            assert!(!written.chars().any(|c| c.is_control() && c != '\n'), "{written:?}");
        }
    }

    #[test]
    fn keys_compare_by_bytes_and_ascii_ids_have_one_spelling() {
        let v = parse("\u{e9}: 1\ne\u{301}: 2\n").unwrap();
        assert_eq!(v.get("\u{e9}"), Some(&s("1")));
        assert_eq!(v.get("e\u{301}"), Some(&s("2")));
        for id in ["sword", "Iron_Sword-2", "units.club", "a"] {
            assert!(is_ascii_id(id), "{id}");
        }
        for id in ["", "\u{e9}", "e\u{301}", "\u{e9}p\u{e9}e", "a b", "a:b", "\u{ff53}word", "a\u{200b}"] {
            assert!(!is_ascii_id(id), "{id:?}");
        }
    }

    #[test]
    fn merge_lays_a_patch_over() {
        let mut v = parse("profile: {power: 4, aim_ms: 600}\nwards: [A]\n").unwrap();
        v.merge(&parse("profile: {power: 8}\nwards: [B]\nweight: 2\n").unwrap());
        assert_eq!(v.get("profile").unwrap().get("power"), Some(&s("8")));
        assert_eq!(v.get("profile").unwrap().get("aim_ms"), Some(&s("600")));
        assert_eq!(v.get("wards"), Some(&Value::Seq(vec![s("A"), s("B")])));
        assert_eq!(v.get("weight"), Some(&s("2")));
    }
}
