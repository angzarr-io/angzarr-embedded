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
//! Every scalar is kept as its text: there is no type coercion, so `no`
//! stays `"no"` and `1.10` stays `"1.10"`. The reader of a value decides what
//! it means. Mappings keep their order.

use std::fmt;

/// A YAML value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Scalar(String),
    Seq(Vec<Value>),
    Map(Vec<(String, Value)>),
}

/// What went wrong, and on which line (1-based; 0 for the end of input).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub line: usize,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for Error {}

fn err<T>(line: usize, message: impl Into<String>) -> Result<T, Error> {
    Err(Error {
        line,
        message: message.into(),
    })
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

/// Cut a `#` comment off, unless it is inside quotes or not preceded by
/// space.
fn strip_comment(s: &str) -> &str {
    let mut quote: Option<char> = None;
    let mut prev = ' ';
    for (i, c) in s.char_indices() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == '"' || c == '\'' => quote = Some(c),
            None if c == '#' && prev.is_whitespace() => return &s[..i],
            None => {}
        }
        prev = c;
    }
    s
}

fn lines(src: &str) -> Result<Vec<Line<'_>>, Error> {
    let mut out = Vec::new();
    for (i, raw) in src.lines().enumerate() {
        let no = i + 1;
        let body = strip_comment(raw).trim_end();
        if body.trim().is_empty() {
            continue;
        }
        let indent = body.len() - body.trim_start_matches(' ').len();
        if body[indent..].starts_with('\t') {
            return err(no, "tabs can't indent");
        }
        let text = &body[indent..];
        if text == "---" || text == "..." {
            return err(no, "one document per file: no --- or ...");
        }
        if text.starts_with('&') || text.starts_with('*') || text.starts_with('!') || text.starts_with('|') || text.starts_with('>') {
            return err(no, "anchors, aliases, tags and block scalars aren't supported");
        }
        out.push(Line { no, indent, text });
    }
    Ok(out)
}

/// Parse a document.
pub fn parse(src: &str) -> Result<Value, Error> {
    let lines = lines(src)?;
    if lines.is_empty() {
        return Ok(Value::Null);
    }
    let mut p = Parser { lines, at: 0 };
    let indent = p.lines[0].indent;
    let v = p.block(indent)?;
    if let Some(l) = p.lines.get(p.at) {
        return err(l.no, "unexpected indentation");
    }
    Ok(v)
}

struct Parser<'a> {
    lines: Vec<Line<'a>>,
    at: usize,
}

/// Split `key: rest` at the first `: ` (or a trailing `:`) outside quotes
/// and brackets.
fn split_key(s: &str) -> Option<(&str, &str)> {
    let mut quote: Option<char> = None;
    let mut depth = 0i32;
    let b = s.as_bytes();
    for (i, c) in s.char_indices() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                '"' | '\'' if i == 0 => quote = Some(c),
                '[' | '{' => depth += 1,
                ']' | '}' => depth -= 1,
                ':' if depth == 0 && (i + 1 == b.len() || b[i + 1] == b' ') => {
                    return Some((s[..i].trim_end(), s[i + 1..].trim_start()));
                }
                _ => {}
            },
        }
    }
    None
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
                return err(l.no, "unexpected indentation");
            }
            if !is_item(l.text) {
                break;
            }
            let (no, rest) = (l.no, l.text[1..].trim_start());
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
                return err(l.no, "unexpected indentation");
            }
            if is_item(l.text) {
                break;
            }
            let no = l.no;
            let Some((key, rest)) = split_key(l.text) else {
                return err(no, format!("expected `key: value`, found {:?}", l.text));
            };
            let key = match inline(key, no)? {
                Value::Scalar(k) => k,
                _ => return err(no, "keys must be scalars"),
            };
            if entries.iter().any(|(k, _)| *k == key) {
                return err(no, format!("{key:?} twice"));
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

/// A value written on one line: a flow collection or a scalar.
fn inline(text: &str, no: usize) -> Result<Value, Error> {
    if text.starts_with(['&', '*', '!', '|', '>']) {
        return err(no, "anchors, aliases, tags and block scalars aren't supported");
    }
    let mut f = Flow { s: text, at: 0, no };
    let v = f.value()?;
    f.space();
    if f.at != f.s.len() {
        return err(no, format!("unexpected {:?}", &f.s[f.at..]));
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
        self.at += self.rest().len() - self.rest().trim_start().len();
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
                    return err(self.no, "expected , or ] in a flow sequence");
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
                    _ => return err(self.no, "keys must be scalars"),
                };
                if !self.eat(':') {
                    return err(self.no, "expected : in a flow mapping");
                }
                let v = self.value()?;
                if entries.iter().any(|(k, _)| *k == key) {
                    return err(self.no, format!("{key:?} twice"));
                }
                entries.push((key, v));
                if self.eat('}') {
                    return Ok(Value::Map(entries));
                }
                if !self.eat(',') {
                    return err(self.no, "expected , or } in a flow mapping");
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
                            Some((_, e @ ('"' | '\\'))) => out.push(e),
                            _ => return err(self.no, "unknown escape"),
                        }
                        continue;
                    }
                    out.push(c);
                }
                err(self.no, "unterminated quote")
            }
            None => Ok(Value::Null),
            Some(_) => {
                let end = if nested {
                    rest.find(|c| c == ',' || c == ']' || c == '}' || (key && c == ':')).unwrap_or(rest.len())
                } else {
                    rest.len()
                };
                let text = rest[..end].trim_end();
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

/// Whether a scalar can be written plain and read back the same.
fn plain_ok(s: &str, in_flow: bool) -> bool {
    if s.is_empty() || s == "~" || s == "null" || s != s.trim() {
        return false;
    }
    let first = s.chars().next().unwrap();
    if "-?:,[]{}#&*!|>'\"%@`".contains(first) && !(first == '-' && s.len() > 1 && !s.starts_with("- ")) {
        return false;
    }
    if s.contains(": ") || s.ends_with(':') || s.contains(" #") || s.contains('\n') {
        return false;
    }
    !(in_flow && s.contains([',', '[', ']', '{', '}']))
}

fn scalar_text(s: &str, in_flow: bool) -> String {
    if plain_ok(s, in_flow) {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n"))
    }
}

/// A value on one line, if it is small enough: scalars, and collections of
/// scalars.
fn flow(v: &Value) -> Option<String> {
    match v {
        Value::Null => Some(String::new()),
        Value::Scalar(s) => Some(scalar_text(s, false)),
        Value::Seq(items) => {
            let parts: Option<Vec<String>> = items
                .iter()
                .map(|i| match i {
                    Value::Scalar(s) => Some(scalar_text(s, true)),
                    _ => None,
                })
                .collect();
            Some(format!("[{}]", parts?.join(", ")))
        }
        Value::Map(entries) => {
            let parts: Option<Vec<String>> = entries
                .iter()
                .map(|(k, v)| match v {
                    Value::Scalar(s) => Some(format!("{}: {}", scalar_text(k, true), scalar_text(s, true))),
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
    let mut out = String::new();
    block(v, 0, 0, flow_depth, &mut out);
    out
}

fn block(v: &Value, indent: usize, depth: usize, flow_depth: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    match v {
        Value::Map(entries) => {
            for (k, val) in entries {
                let key = scalar_text(k, false);
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
        assert!(parse("a: &x 1\n").is_err());
        assert!(parse("a: *x\n").is_err());
        assert!(parse("- !tag x\n").is_err());
        assert!(parse("a: |\n  text\n").is_err());
        assert!(parse("a:\n\t- x\n").is_err());
        assert!(parse("---\na: 1\n").is_err());
        assert!(parse("a: 1\na: 2\n").is_err());
        assert!(parse("a: [1, 2\n").is_err());
        assert!(parse("a: 1\n   b: 2\n").is_err());
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
    fn merge_lays_a_patch_over() {
        let mut v = parse("profile: {power: 4, aim_ms: 600}\nwards: [A]\n").unwrap();
        v.merge(&parse("profile: {power: 8}\nwards: [B]\nweight: 2\n").unwrap());
        assert_eq!(v.get("profile").unwrap().get("power"), Some(&s("8")));
        assert_eq!(v.get("profile").unwrap().get("aim_ms"), Some(&s("600")));
        assert_eq!(v.get("wards"), Some(&Value::Seq(vec![s("A"), s("B")])));
        assert_eq!(v.get("weight"), Some(&s("2")));
    }
}
