//! Editing a document in place, for tools that rewrite files people also edit
//! by hand: push, remove or replace one item of a sequence, and every byte
//! outside that item stays as written, comments and blank lines included.
//!
//! A [`Document`] keeps the text. Each edit finds the sequence by a path of
//! mapping keys, splices the item's lines, and parses the result: the edit
//! applies only if the new text reads back as the old value with exactly that
//! change, so an edit never leaves a document that means something else.
//!
//! What belongs to an item: its lines, the comment lines directly above it
//! (no blank line between) indented no deeper than its dash, and comment lines
//! directly below it indented deeper than its dash. Removing an item also
//! removes one blank line where two would otherwise meet, or where one would
//! be left between a key and its first item.
//!
//! A new item is written with [`crate::write`]'s layout at the sequence's own
//! indent, `flow_depth` levels counted from the item. A flow sequence
//! (`[a, b]`) stays on its line while every item is a scalar, and becomes a
//! block otherwise. Removing the last item of a block sequence writes `[]`,
//! so the value stays an empty sequence rather than becoming null.

use std::fmt;

use annals_diagnostic::{Arg, Diagnostic};

use crate::{flow, inline, is_item, lines, parse, scalar_text, split_key, strip_comment, write, At, Error, Value};

/// A document's text and the value it reads as.
#[derive(Clone, Debug)]
pub struct Document {
    bom: bool,
    lines: Vec<Raw>,
    value: Value,
}

/// One line of the text and the line ending after it (`\n`, `\r\n`, or empty
/// for a last line without one).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Raw {
    text: String,
    eol: String,
}

/// An edit that could not be made, and the path it was given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EditError {
    pub path: Vec<String>,
    pub problem: EditProblem,
}

/// Why an edit could not be made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EditProblem {
    /// A key on the path is not there.
    NotFound,
    /// The path goes through a value that is not a mapping.
    NotAMapping,
    /// The path ends at a value that is not a sequence.
    NotASequence,
    /// The path goes into a flow collection (`{a: [b]}`), which is edited as a
    /// whole line, never inside.
    InsideFlow,
    /// The sequence has no item at `index`.
    NoSuchItem { index: usize, len: usize },
    /// The edited text would not read back as the value intended. A bug in
    /// this module, refused rather than written.
    Unfaithful,
}

impl Document {
    /// Read `src`. It must parse; the text is kept exactly, a byte-order mark
    /// and line endings included.
    pub fn parse(src: &str) -> Result<Document, Error> {
        let value = parse(src)?;
        let (bom, body) = match src.strip_prefix('\u{feff}') {
            Some(body) => (true, body),
            None => (false, src),
        };
        Ok(Document { bom, lines: split(body), value })
    }

    /// The value the text reads as.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// The text.
    pub fn text(&self) -> String {
        let mut out = String::from(if self.bom { "\u{feff}" } else { "" });
        for l in &self.lines {
            out.push_str(&l.text);
            out.push_str(&l.eol);
        }
        out
    }

    /// The text, consuming the document.
    pub fn into_string(self) -> String {
        self.text()
    }

    /// Append `item` to the sequence at `path`. A missing last key is added,
    /// at the end of its mapping; a key with no value gets a sequence.
    pub fn push(&mut self, path: &[&str], item: &Value, flow_depth: usize) -> Result<(), EditError> {
        self.edit(path, Op::Push(item, flow_depth))
    }

    /// Remove item `index` of the sequence at `path`.
    pub fn remove(&mut self, path: &[&str], index: usize) -> Result<(), EditError> {
        self.edit(path, Op::Remove(index))
    }

    /// Replace item `index` of the sequence at `path` with `item`, keeping the
    /// comment lines above it.
    pub fn replace(&mut self, path: &[&str], index: usize, item: &Value, flow_depth: usize) -> Result<(), EditError> {
        self.edit(path, Op::Replace(index, item, flow_depth))
    }

    fn edit(&mut self, path: &[&str], op: Op) -> Result<(), EditError> {
        let fail = |problem| EditError { path: path.iter().map(|k| k.to_string()).collect(), problem };
        let body = self.body();
        let meaningful = Meaningful::of(&body);
        let place = meaningful.locate(path, matches!(op, Op::Push(..))).map_err(fail)?;

        let mut expected = self.value.clone();
        let seq = expected_seq(&mut expected, path);
        let len = seq.len();
        match op {
            Op::Push(item, _) => seq.push(item.clone()),
            Op::Remove(index) | Op::Replace(index, ..) if index >= len => return Err(fail(EditProblem::NoSuchItem { index, len })),
            Op::Remove(index) => {
                seq.remove(index);
            }
            Op::Replace(index, item, _) => seq[index] = item.clone(),
        }
        let items = seq.clone();

        let mut lines = self.lines.clone();
        splice(&mut lines, &meaningful, &place, &op, &items);
        let edited = Document { bom: self.bom, lines: normalize_eol(lines, &self.lines), value: expected };
        match parse(&edited.body()) {
            Ok(back) if back == edited.value => {
                *self = edited;
                Ok(())
            }
            _ => Err(fail(EditProblem::Unfaithful)),
        }
    }

    fn body(&self) -> String {
        self.lines.iter().map(|l| format!("{}{}", l.text, l.eol)).collect()
    }
}

#[derive(Clone, Copy)]
enum Op<'a> {
    Push(&'a Value, usize),
    Remove(usize),
    Replace(usize, &'a Value, usize),
}

/// The sequence at `path` in `v`, made one if the last key is missing or
/// null. `locate` has already checked the path, so this does not fail.
fn expected_seq<'v>(v: &'v mut Value, path: &[&str]) -> &'v mut Vec<Value> {
    let mut at = v;
    for key in path {
        at = at.entry(key);
    }
    if !matches!(at, Value::Seq(_)) {
        *at = Value::Seq(Vec::new());
    }
    match at {
        Value::Seq(items) => items,
        _ => unreachable!("just made a sequence"),
    }
}

fn split(body: &str) -> Vec<Raw> {
    let pieces: Vec<&str> = body.split('\n').collect();
    let last = pieces.len() - 1;
    let mut out = Vec::new();
    for (i, piece) in pieces.iter().enumerate() {
        if i < last {
            match piece.strip_suffix('\r') {
                Some(text) => out.push(Raw { text: text.to_string(), eol: "\r\n".to_string() }),
                None => out.push(Raw { text: piece.to_string(), eol: "\n".to_string() }),
            }
        } else if !piece.is_empty() {
            out.push(Raw { text: piece.to_string(), eol: String::new() });
        }
    }
    out
}

/// Give every line but the last an ending, the one `old` mostly uses, and the
/// last one an ending only if `old`'s last line had one.
fn normalize_eol(mut lines: Vec<Raw>, old: &[Raw]) -> Vec<Raw> {
    let eol = old.iter().find(|l| !l.eol.is_empty()).map_or("\n", |l| l.eol.as_str()).to_string();
    let final_newline = old.last().map_or(true, |l| !l.eol.is_empty());
    let n = lines.len();
    for (i, l) in lines.iter_mut().enumerate() {
        if i + 1 < n {
            if l.eol.is_empty() {
                l.eol = eol.clone();
            }
        } else if !final_newline {
            l.eol.clear();
        } else if l.eol.is_empty() {
            l.eol = eol.clone();
        }
    }
    lines
}

fn blank(text: &str) -> bool {
    text.trim_matches([' ', '\t']).is_empty()
}

fn indent_of(text: &str) -> usize {
    text.len() - text.trim_start_matches(' ').len()
}

/// A line holding only a comment, and its indent.
fn comment_only(text: &str) -> Option<usize> {
    text.trim_start_matches(' ').starts_with('#').then(|| indent_of(text))
}

/// The lines that carry content (not blank, not comment-only): where each is
/// in the text, its indent, and its text without the comment.
struct Meaningful {
    lines: Vec<(usize, usize, String)>,
}

/// Where the sequence an edit works on is.
enum Place {
    /// A block sequence: its items' indent, and each item's first and last
    /// meaningful line. `key` is the line of the key it is the value of.
    Block { key: Option<usize>, indent: usize, items: Vec<(usize, usize)> },
    /// A value on the key's own line (a flow sequence, or null written `~`).
    Inline { key: usize },
    /// A key with nothing under it.
    Empty { key: usize, indent: usize },
    /// A key that is not there yet: it goes after line `after`, at `indent`.
    Missing { key: String, after: usize, indent: usize },
}

impl Meaningful {
    fn of(body: &str) -> Meaningful {
        let lines = lines(body).expect("the document parsed when it was read");
        Meaningful { lines: lines.into_iter().map(|l| (l.no - 1, l.indent, l.text.to_string())).collect() }
    }

    fn raw(&self, m: usize) -> usize {
        self.lines[m].0
    }

    fn indent(&self, m: usize) -> usize {
        self.lines[m].1
    }

    fn text(&self, m: usize) -> &str {
        &self.lines[m].2
    }

    /// The meaningful lines `lo..hi` hold a mapping at their first line's
    /// indent; find `key` in it, giving the key's line and the lines of its
    /// value under it.
    fn entry(&self, lo: usize, hi: usize, key: &str) -> Option<(usize, usize, usize)> {
        let indent = self.indent(lo);
        let at = (lo..hi).find(|&i| {
            self.indent(i) == indent
                && !is_item(self.text(i))
                && split_key(self.text(i)).is_some_and(|(k, _)| inline(k, 0).ok().as_ref().and_then(Value::as_str) == Some(key))
        })?;
        let mut end = at + 1;
        let same_indent_seq = end < hi && self.indent(end) == indent && is_item(self.text(end));
        while end < hi && (self.indent(end) > indent || same_indent_seq && self.indent(end) == indent && is_item(self.text(end))) {
            end += 1;
        }
        Some((at, at + 1, end))
    }

    fn locate(&self, path: &[&str], push: bool) -> Result<Place, EditProblem> {
        let (mut lo, mut hi, mut key) = (0, self.lines.len(), None);
        for (depth, k) in path.iter().enumerate() {
            if let Some(at) = key {
                let (_, rest) = split_key(self.text(at)).expect("a key line");
                if !rest.is_empty() {
                    return Err(if rest.starts_with('{') { EditProblem::InsideFlow } else { EditProblem::NotAMapping });
                }
            }
            if lo == hi {
                return Err(EditProblem::NotAMapping);
            }
            if is_item(self.text(lo)) || split_key(self.text(lo)).is_none() {
                return Err(EditProblem::NotAMapping);
            }
            match self.entry(lo, hi, k) {
                Some((at, vlo, vhi)) => (key, lo, hi) = (Some(at), vlo, vhi),
                None if push && depth + 1 == path.len() => {
                    return Ok(Place::Missing { key: k.to_string(), after: self.raw(hi - 1), indent: self.indent(lo) })
                }
                None => return Err(EditProblem::NotFound),
            }
        }
        if let Some(at) = key {
            let (_, rest) = split_key(self.text(at)).expect("a key line");
            if !rest.is_empty() {
                return match inline(rest, 0) {
                    Ok(Value::Seq(_) | Value::Null) => Ok(Place::Inline { key: at }),
                    _ => Err(EditProblem::NotASequence),
                };
            }
            if lo == hi {
                return Ok(Place::Empty { key: at, indent: self.indent(at) });
            }
        }
        if lo == hi || !is_item(self.text(lo)) {
            return Err(EditProblem::NotASequence);
        }
        let indent = self.indent(lo);
        let starts: Vec<usize> = (lo..hi).filter(|&i| self.indent(i) == indent && is_item(self.text(i))).collect();
        let items = starts.iter().enumerate().map(|(n, &s)| (s, starts.get(n + 1).map_or(hi, |&e| e) - 1)).collect();
        Ok(Place::Block { key, indent, items })
    }
}

/// The first line of item `(first, last)` with the comments above it, and its
/// last line with the deeper comments below it.
fn item_span(lines: &[Raw], m: &Meaningful, (first, last): (usize, usize), indent: usize) -> (usize, usize) {
    let mut start = m.raw(first);
    while start > 0 && comment_only(&lines[start - 1].text).is_some_and(|i| i <= indent) {
        start -= 1;
    }
    let mut end = m.raw(last);
    while end + 1 < lines.len() && comment_only(&lines[end + 1].text).is_some_and(|i| i > indent) {
        end += 1;
    }
    (start, end)
}

/// `item` written as a block sequence item at `indent`.
fn item_lines(item: &Value, indent: usize, flow_depth: usize) -> Vec<Raw> {
    let pad = " ".repeat(indent);
    write(&Value::Seq(vec![item.clone()]), flow_depth)
        .lines()
        .map(|l| Raw { text: format!("{pad}{l}"), eol: String::new() })
        .collect()
}

/// The key line `text` with its value replaced by `value` (empty for none),
/// keeping the key and any comment as written.
fn rekey(text: &str, value: &str) -> String {
    let body = strip_comment(text).trim_end_matches([' ', '\t']);
    let comment = &text[body.len()..];
    let indent = indent_of(body);
    let (_, rest) = split_key(&body[indent..]).expect("a key line");
    let key = body[..body.len() - rest.len()].trim_end_matches([' ', '\t']);
    if value.is_empty() {
        format!("{key}{comment}")
    } else {
        format!("{key} {value}{comment}")
    }
}

fn line(text: String) -> Raw {
    Raw { text, eol: String::new() }
}

fn splice(lines: &mut Vec<Raw>, m: &Meaningful, place: &Place, op: &Op, items: &[Value]) {
    match place {
        Place::Block { key, indent, items: spans } => match *op {
            Op::Push(item, flow_depth) => {
                let (_, end) = item_span(lines, m, *spans.last().expect("a block sequence has an item"), *indent);
                let mut new = Vec::new();
                if spans.len() >= 2 {
                    let (_, prev_end) = item_span(lines, m, spans[spans.len() - 2], *indent);
                    let (start, _) = item_span(lines, m, spans[spans.len() - 1], *indent);
                    if lines[prev_end + 1..start].iter().any(|l| blank(&l.text)) {
                        new.push(line(String::new()));
                    }
                }
                new.extend(item_lines(item, *indent, flow_depth));
                lines.splice(end + 1..end + 1, new);
            }
            Op::Replace(index, item, flow_depth) => {
                let (_, end) = item_span(lines, m, spans[index], *indent);
                let first = m.raw(spans[index].0);
                lines.splice(first..=end, item_lines(item, *indent, flow_depth));
            }
            Op::Remove(index) => {
                let (start, end) = item_span(lines, m, spans[index], *indent);
                lines.drain(start..=end);
                if items.is_empty() {
                    match key {
                        Some(k) => lines[m.raw(*k)].text = rekey(&lines[m.raw(*k)].text, "[]"),
                        None => lines.insert(start, line(format!("{}[]", " ".repeat(*indent)))),
                    }
                }
                let before_blank = start > 0 && blank(&lines[start - 1].text);
                let after_blank = start < lines.len() && blank(&lines[start].text);
                if after_blank && (before_blank || index == 0) {
                    lines.remove(start);
                } else if before_blank && index + 1 == spans.len() {
                    lines.remove(start - 1);
                }
            }
        },
        Place::Inline { key } => {
            let k = m.raw(*key);
            let indent = m.indent(*key);
            let whole = Value::Seq(items.to_vec());
            match flow(&whole).filter(|t| !t.is_empty()) {
                Some(text) => lines[k].text = rekey(&lines[k].text, &text),
                None => {
                    lines[k].text = rekey(&lines[k].text, "");
                    let new: Vec<Raw> = items.iter().flat_map(|i| item_lines(i, indent + 2, depth_of(op))).collect();
                    lines.splice(k + 1..k + 1, new);
                }
            }
        }
        Place::Empty { key, indent } => {
            let k = m.raw(*key);
            let new: Vec<Raw> = items.iter().flat_map(|i| item_lines(i, indent + 2, depth_of(op))).collect();
            lines.splice(k + 1..k + 1, new);
        }
        Place::Missing { key, after, indent } => {
            let mut new = vec![line(format!("{}{}:", " ".repeat(*indent), scalar_text(key, At::Key)))];
            new.extend(items.iter().flat_map(|i| item_lines(i, indent + 2, depth_of(op))));
            lines.splice(after + 1..after + 1, new);
        }
    }
}

fn depth_of(op: &Op) -> usize {
    match *op {
        Op::Push(_, d) | Op::Replace(_, _, d) => d,
        Op::Remove(_) => 2,
    }
}

// ------------------------------------------------------------------ errors

impl fmt::Display for EditProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EditProblem::NotFound => f.write_str("not found"),
            EditProblem::NotAMapping => f.write_str("not a mapping"),
            EditProblem::NotASequence => f.write_str("not a sequence"),
            EditProblem::InsideFlow => f.write_str("inside a flow collection, which is edited only as a whole"),
            EditProblem::NoSuchItem { index, len } => write!(f, "no item {index}: the sequence has {len}"),
            EditProblem::Unfaithful => f.write_str("the edit would not read back as written; nothing was changed"),
        }
    }
}

fn path_text(path: &[String]) -> String {
    if path.is_empty() {
        "the root".to_string()
    } else {
        path.join(".")
    }
}

impl fmt::Display for EditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", path_text(&self.path), self.problem)
    }
}

impl std::error::Error for EditError {}

impl Diagnostic for EditError {
    fn code(&self) -> &'static str {
        match self.problem {
            EditProblem::NotFound => "annals-yaml-edit-not-found",
            EditProblem::NotAMapping => "annals-yaml-edit-not-a-mapping",
            EditProblem::NotASequence => "annals-yaml-edit-not-a-sequence",
            EditProblem::InsideFlow => "annals-yaml-edit-inside-flow",
            EditProblem::NoSuchItem { .. } => "annals-yaml-edit-no-such-item",
            EditProblem::Unfaithful => "annals-yaml-edit-unfaithful",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        let mut args = vec![("path", Arg::from(path_text(&self.path)))];
        if let EditProblem::NoSuchItem { index, len } = self.problem {
            args.push(("index", Arg::from(index)));
            args.push(("len", Arg::from(len)));
        }
        args
    }
}

/// One value of every edit error, for the catalog parity test.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let path = vec!["packages".to_string()];
    [
        EditProblem::NotFound,
        EditProblem::NotAMapping,
        EditProblem::NotASequence,
        EditProblem::InsideFlow,
        EditProblem::NoSuchItem { index: 3, len: 2 },
        EditProblem::Unfaithful,
    ]
    .into_iter()
    .map(|problem| Box::new(EditError { path: path.clone(), problem }) as Box<dyn Diagnostic>)
    .collect()
}
