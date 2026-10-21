//! A small Gherkin runner.
//!
//! Hand-rolled rather than `cucumber`: annals-sim has no dependencies, cucumber
//! brings an async runtime a deterministic simulation has no use for, and the
//! subset needed — Feature, Background, Scenario, Given/When/Then/And/But and
//! data tables — is short.
//!
//! Use it from a test target with `harness = false`, and give it a world type
//! and one function that runs a step:
//!
//! ```no_run
//! use annals_sim::gherkin::{self, Step};
//!
//! #[derive(Default)]
//! struct World { total: i64 }
//!
//! fn step(w: &mut World, s: &Step) {
//!     let n = s.numbers();
//!     match s.text.as_str() {
//!         t if t.starts_with("I add") => w.total += n[0],
//!         t if t.starts_with("the total is") => assert_eq!(w.total, n[0]),
//!         t => panic!("unknown step {t:?}"),
//!     }
//! }
//!
//! fn main() {
//!     let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("features");
//!     gherkin::main(&dir, World::default, step);
//! }
//! ```
//!
//! Scope rule: **named things get behavioural scenarios; invariants do not.**
//! A universally quantified claim — determinism, conservation — is a property
//! test, because a single example is a weak way to assert it.

use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;

/// One step, with its keyword resolved: an `And` or `But` takes the keyword
/// before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// `Given`, `When` or `Then`.
    pub keyword: String,
    pub text: String,
    /// The data table under the step, row by row, cells trimmed and with
    /// `\|`, `\\` and `\n` resolved. The header row, if any, is the first.
    pub table: Vec<Vec<String>>,
}

impl Step {
    /// The step is a `Then`.
    pub fn is_assertion(&self) -> bool {
        self.keyword == "Then"
    }

    /// The step's quoted strings, in order.
    pub fn quoted(&self) -> Vec<String> {
        quoted(&self.text)
    }

    /// The step's unquoted integers, in order.
    pub fn numbers(&self) -> Vec<i64> {
        numbers(&self.text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scenario {
    pub name: String,
    /// The background's steps, then the scenario's own.
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Feature {
    pub name: String,
    pub scenarios: Vec<Scenario>,
}

/// Parse one feature file. Comments (`#`) and blank lines are skipped, and so
/// is description prose under `Feature:`. A leading byte-order mark is
/// skipped.
pub fn parse_feature(src: &str) -> Feature {
    let src = src.strip_prefix('\u{feff}').unwrap_or(src);
    let mut feature = String::new();
    let mut background: Vec<Step> = Vec::new();
    let mut scenarios: Vec<Scenario> = Vec::new();
    let mut current: Option<Scenario> = None;
    let mut in_background = false;
    let mut last_keyword = String::from("Given");

    for raw in src.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(rest) = line.strip_prefix("Feature:") {
            feature = rest.trim().to_string();
            continue;
        }
        if line.starts_with("Background:") {
            in_background = true;
            continue;
        }
        if let Some(rest) = line.strip_prefix("Scenario:") {
            if let Some(s) = current.take() {
                scenarios.push(s);
            }
            in_background = false;
            current = Some(Scenario {
                name: rest.trim().to_string(),
                steps: background.clone(),
            });
            continue;
        }

        if line.starts_with('|') {
            let cells = table_row(line);
            let target = if in_background {
                background.last_mut()
            } else {
                current.as_mut().and_then(|s| s.steps.last_mut())
            };
            if let Some(step) = target {
                step.table.push(cells);
            }
            continue;
        }

        if !in_background && current.is_none() {
            continue;
        }

        let (keyword, text) = split_keyword(line, &mut last_keyword);
        let step = Step {
            keyword,
            text,
            table: Vec::new(),
        };
        if in_background {
            background.push(step);
        } else if let Some(s) = current.as_mut() {
            s.steps.push(step);
        }
    }
    if let Some(s) = current.take() {
        scenarios.push(s);
    }
    Feature {
        name: feature,
        scenarios,
    }
}

/// The cells of a table row `| a | b |`, trimmed. As in Gherkin, `\|` is a
/// pipe inside a cell, `\\` a backslash and `\n` a line break; any other
/// backslash is kept.
fn table_row(line: &str) -> Vec<String> {
    let row = line.trim();
    let row = row.strip_prefix('|').unwrap_or(row);
    // Split at the pipes no backslash escapes, then trim, then unescape, so
    // an escaped character at a cell's edge stays.
    let mut raw = Vec::new();
    let (mut start, mut escaped) = (0, false);
    for (i, c) in row.char_indices() {
        match c {
            _ if escaped => escaped = false,
            '\\' => escaped = true,
            '|' => {
                raw.push(&row[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if !row[start..].trim().is_empty() {
        raw.push(&row[start..]);
    }
    raw.into_iter()
        .map(|cell| unescape_cell(cell.trim()))
        .collect()
}

fn unescape_cell(cell: &str) -> String {
    let mut out = String::with_capacity(cell.len());
    let mut chars = cell.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('|') => out.push('|'),
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

fn split_keyword(line: &str, last: &mut String) -> (String, String) {
    for kw in ["Given ", "When ", "Then "] {
        if let Some(rest) = line.strip_prefix(kw) {
            *last = kw.trim().to_string();
            return (last.clone(), rest.trim().to_string());
        }
    }
    for kw in ["And ", "But "] {
        if let Some(rest) = line.strip_prefix(kw) {
            return (last.clone(), rest.trim().to_string());
        }
    }
    (last.clone(), line.to_string())
}

/// The strings between double quotes, in order.
pub fn quoted(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '"' {
            let mut buf = String::new();
            for c2 in chars.by_ref() {
                if c2 == '"' {
                    break;
                }
                buf.push(c2);
            }
            out.push(buf);
        }
    }
    out
}

/// Runs of digits outside quotes, in order. Digits only: a sign is the
/// step's to read.
pub fn numbers(s: &str) -> Vec<i64> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_quotes = false;
    let flush = |buf: &mut String, out: &mut Vec<i64>| {
        if !buf.is_empty() {
            out.push(
                buf.parse()
                    .unwrap_or_else(|_| panic!("number too large: {buf}")),
            );
            buf.clear();
        }
    };
    for c in s.chars() {
        if c == '"' {
            in_quotes = !in_quotes;
            flush(&mut buf, &mut out);
            continue;
        }
        if !in_quotes && c.is_ascii_digit() {
            buf.push(c);
        } else {
            flush(&mut buf, &mut out);
        }
    }
    flush(&mut buf, &mut out);
    out
}

/// How a run went.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub passed: usize,
    /// `feature: scenario` for every scenario that panicked.
    pub failed: Vec<String>,
}

/// Run one feature: a fresh world per scenario, its steps in order. A step
/// fails by panicking; the scenario stops there and the next one runs.
pub fn run_feature<W>(
    feature: &Feature,
    new: impl Fn() -> W,
    step: impl Fn(&mut W, &Step),
    report: &mut Report,
) {
    println!("\nFeature: {}", feature.name);
    for sc in &feature.scenarios {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut w = new();
            for s in &sc.steps {
                step(&mut w, s);
            }
        }));
        match result {
            Ok(()) => {
                println!("  ok    {}", sc.name);
                report.passed += 1;
            }
            Err(_) => {
                println!("  FAIL  {}", sc.name);
                report.failed.push(format!("{}: {}", feature.name, sc.name));
            }
        }
    }
}

/// Run every `.feature` file in `dir`, in file-name order.
pub fn run_dir<W>(dir: &Path, new: impl Fn() -> W, step: impl Fn(&mut W, &Step)) -> Report {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read features in {}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "feature").unwrap_or(false))
        .collect();
    files.sort();

    let mut report = Report::default();
    for path in files {
        let src =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        run_feature(&parse_feature(&src), &new, &step, &mut report);
    }
    report
}

/// A test binary's `main`: run every feature in `dir`, print a summary, and
/// exit non-zero if any scenario failed.
pub fn main<W>(dir: &Path, new: impl Fn() -> W, step: impl Fn(&mut W, &Step)) {
    let report = run_dir(dir, new, step);
    println!("\n{} passed, {} failed", report.passed, report.failed.len());
    if !report.failed.is_empty() {
        for f in &report.failed {
            println!("  failed: {f}");
        }
        std::process::exit(1);
    }
}

#[cfg(test)]
#[path = "gherkin.test.rs"]
mod tests;
