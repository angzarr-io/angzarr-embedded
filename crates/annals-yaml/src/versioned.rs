//! Versioned documents: a root mapping stamped `version: N`, read through a
//! chain of registered migrations up to the version the code reads.
//!
//! - **The stamp.** Every versioned document is a mapping with a root
//!   `version` whose value is a non-negative integer (decimal digits). A
//!   missing `version` is an error, not version 0: an unstamped file is more
//!   likely the wrong file than an ancient one, and guessing would migrate it
//!   silently.
//! - **The chain.** [`Migrations::new`] names the document kind and the
//!   current version; [`Migrations::step`] registers the step from `N` to
//!   `N + 1`. [`Migrations::build`] checks the chain and gives a [`Loader`]:
//!   no duplicate steps, no step at or past the current version, and no gap
//!   between the oldest step and the current version. The oldest supported
//!   version is the lowest registered step's (the current version with no
//!   steps). Dropping support for old files is deleting their oldest steps.
//! - **A step** edits the tree in place and leaves the stamp to the loader,
//!   which writes `version: N + 1` after the step returns. The helpers here
//!   ([`rename_key`], [`move_key`], [`set_default`], [`remove_key`],
//!   [`map_seq`]) cover the usual edits.
//! - **Loading** is string in, tree out: parse, read the version, run the
//!   steps from it to the current version, hand the tree to the caller's
//!   decoder. It never writes anything back; [`Loader::upgrade`] gives the
//!   upgraded text for a tool that means to rewrite a file. No I/O, no clock:
//!   the same input gives the same tree and the same bytes.
//! - **Fixtures.** [`Loader::check_fixtures`] proves old files still load:
//!   each fixture, at any supported version, must migrate to its expected
//!   current-version tree.

use std::fmt;

use crate::{parse, write, Error, Value};

/// A migration step: edits a document at version `N` into version `N + 1`.
/// The loader stamps the new version; the step need not.
pub type Step = fn(&mut Value) -> Result<(), MigrationError>;

/// Why a migration step could not migrate a document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationError(pub String);

impl MigrationError {
    pub fn new(message: impl Into<String>) -> MigrationError {
        MigrationError(message.into())
    }
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MigrationError {}

impl From<String> for MigrationError {
    fn from(s: String) -> MigrationError {
        MigrationError(s)
    }
}

impl From<&str> for MigrationError {
    fn from(s: &str) -> MigrationError {
        MigrationError(s.to_string())
    }
}

// ---------------------------------------------------------------- registry

/// A document kind's migration steps, not yet checked. [`Migrations::build`]
/// checks them.
#[derive(Clone, Debug)]
pub struct Migrations {
    kind: String,
    current: u32,
    steps: Vec<(u32, Step)>,
}

/// A chain of steps that cannot load documents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainError {
    /// The document kind.
    pub kind: String,
    pub problem: ChainProblem,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChainProblem {
    /// Two steps from the same version.
    Duplicate { from: u32 },
    /// A step from a version at or past the current one.
    BeyondCurrent { from: u32, current: u32 },
    /// No step from this version, though older versions have one.
    Gap { from: u32 },
}

impl fmt::Display for ChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} migrations: ", self.kind)?;
        match self.problem {
            ChainProblem::Duplicate { from } => write!(f, "two steps from version {from}"),
            ChainProblem::BeyondCurrent { from, current } => {
                write!(f, "a step from version {from}, but the current version is {current}")
            }
            ChainProblem::Gap { from } => write!(f, "no step from version {from} to {}", from + 1),
        }
    }
}

impl std::error::Error for ChainError {}

impl Migrations {
    /// No steps yet, for documents of `kind` (named in every error) whose
    /// current version is `current`.
    pub fn new(kind: &str, current: u32) -> Migrations {
        Migrations { kind: kind.to_string(), current, steps: Vec::new() }
    }

    /// Register the step from version `from` to `from + 1`.
    pub fn step(mut self, from: u32, step: Step) -> Migrations {
        self.steps.push((from, step));
        self
    }

    /// Check the chain. Not a panic at [`Migrations::step`], because a gap
    /// shows only once every step is in, and a typed error lets a test say
    /// which chain is wrong; a game calls this once, where it defines its
    /// steps, and expects it to succeed.
    pub fn build(self) -> Result<Loader, ChainError> {
        let Migrations { kind, current, mut steps } = self;
        let fail = |problem| Err(ChainError { kind: kind.clone(), problem });
        steps.sort_by_key(|(from, _)| *from);
        for pair in steps.windows(2) {
            if pair[0].0 == pair[1].0 {
                return fail(ChainProblem::Duplicate { from: pair[0].0 });
            }
        }
        if let Some(&(from, _)) = steps.iter().find(|(from, _)| *from >= current) {
            return fail(ChainProblem::BeyondCurrent { from, current });
        }
        let oldest = steps.first().map_or(current, |(from, _)| *from);
        for (i, (from, _)) in steps.iter().enumerate() {
            let want = oldest + i as u32;
            if *from != want {
                return fail(ChainProblem::Gap { from: want });
            }
        }
        if oldest + steps.len() as u32 != current {
            return fail(ChainProblem::Gap { from: oldest + steps.len() as u32 });
        }
        Ok(Loader { kind, current, oldest, steps: steps.into_iter().map(|(_, s)| s).collect() })
    }
}

// ---------------------------------------------------------------- loading

/// Loads one kind of versioned document, migrating it to the current
/// version. Made by [`Migrations::build`], so its chain is whole: every
/// version from [`Loader::oldest`] up to [`Loader::current`] has its step.
#[derive(Clone, Debug)]
pub struct Loader {
    kind: String,
    current: u32,
    oldest: u32,
    /// The step from `oldest + i` at index `i`.
    steps: Vec<Step>,
}

/// A document that does not load.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadError {
    /// The document kind.
    pub kind: String,
    /// Where the document came from, if the caller said.
    pub source: Option<String>,
    pub problem: LoadProblem,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadProblem {
    /// Not YAML this crate reads.
    Yaml(Error),
    /// The root is not a mapping.
    NotAMapping,
    /// The root mapping has no `version`.
    MissingVersion,
    /// `version` is not a non-negative integer; what it is instead.
    BadVersion(String),
    /// Written by a newer build than this one.
    TooNew { found: u32, current: u32 },
    /// Older than the oldest version this build migrates.
    TooOld { found: u32, oldest: u32 },
    /// The step from `from` to `from + 1` failed.
    Step { from: u32, error: MigrationError },
    /// The current-version tree did not decode.
    Decode(String),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} document", self.kind)?;
        if let Some(source) = &self.source {
            write!(f, " {source}")?;
        }
        f.write_str(": ")?;
        match &self.problem {
            LoadProblem::Yaml(e) => write!(f, "{e}"),
            LoadProblem::NotAMapping => f.write_str("the root must be a mapping with a `version`"),
            LoadProblem::MissingVersion => f.write_str("no root `version`"),
            LoadProblem::BadVersion(found) => {
                write!(f, "`version` must be a non-negative integer, not {found}")
            }
            LoadProblem::TooNew { found, current } => write!(
                f,
                "version {found} was written by a newer build; this build reads up to version {current}"
            ),
            LoadProblem::TooOld { found, oldest } => write!(
                f,
                "version {found} is older than this build migrates; the oldest it reads is version {oldest}"
            ),
            LoadProblem::Step { from, error } => {
                write!(f, "migrating version {from} to {}: {error}", from + 1)
            }
            LoadProblem::Decode(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for LoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.problem {
            LoadProblem::Yaml(e) => Some(e),
            LoadProblem::Step { error, .. } => Some(error),
            _ => None,
        }
    }
}

/// A document's root `version`.
pub fn version_of(doc: &Value) -> Result<u32, LoadProblem> {
    if doc.as_map().is_none() {
        return Err(LoadProblem::NotAMapping);
    }
    let found = match doc.get("version") {
        None => return Err(LoadProblem::MissingVersion),
        Some(Value::Scalar(s)) => s,
        Some(Value::Null) => return Err(LoadProblem::BadVersion("an empty value".into())),
        Some(Value::Seq(_)) => return Err(LoadProblem::BadVersion("a sequence".into())),
        Some(Value::Map(_)) => return Err(LoadProblem::BadVersion("a mapping".into())),
    };
    if found.is_empty() || !found.bytes().all(|b| b.is_ascii_digit()) {
        return Err(LoadProblem::BadVersion(format!("{found:?}")));
    }
    found.parse().map_err(|_| LoadProblem::BadVersion(format!("{found:?} (too large)")))
}

impl Loader {
    /// The document kind errors name.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The version documents migrate to.
    pub fn current(&self) -> u32 {
        self.current
    }

    /// The oldest version this loader migrates.
    pub fn oldest(&self) -> u32 {
        self.oldest
    }

    fn fail<T>(&self, source: Option<&str>, problem: LoadProblem) -> Result<T, LoadError> {
        Err(LoadError { kind: self.kind.clone(), source: source.map(str::to_string), problem })
    }

    /// Parse `text` and migrate it to the current version. `source` (a path,
    /// say) is only named in errors.
    pub fn load(&self, source: Option<&str>, text: &str) -> Result<Value, LoadError> {
        match parse(text) {
            Ok(doc) => self.migrate(source, doc),
            Err(e) => self.fail(source, LoadProblem::Yaml(e)),
        }
    }

    /// Migrate an already-parsed document to the current version. A
    /// current-version document comes back untouched.
    pub fn migrate(&self, source: Option<&str>, mut doc: Value) -> Result<Value, LoadError> {
        let found = match version_of(&doc) {
            Ok(v) => v,
            Err(problem) => return self.fail(source, problem),
        };
        if found > self.current {
            return self.fail(source, LoadProblem::TooNew { found, current: self.current });
        }
        if found < self.oldest {
            return self.fail(source, LoadProblem::TooOld { found, oldest: self.oldest });
        }
        for from in found..self.current {
            let step = self.steps[(from - self.oldest) as usize];
            let outcome = step(&mut doc).and_then(|()| match doc {
                Value::Map(_) => Ok(()),
                _ => Err(MigrationError::new("the step left the root not a mapping")),
            });
            if let Err(error) = outcome {
                return self.fail(source, LoadProblem::Step { from, error });
            }
            *doc.entry("version") = Value::Scalar((from + 1).to_string());
        }
        Ok(doc)
    }

    /// Load `text` and decode the current-version tree with `decode`, whose
    /// error is reported as [`LoadProblem::Decode`].
    pub fn load_as<T, E: fmt::Display>(
        &self,
        source: Option<&str>,
        text: &str,
        decode: impl FnOnce(&Value) -> Result<T, E>,
    ) -> Result<T, LoadError> {
        let doc = self.load(source, text)?;
        decode(&doc).or_else(|e| self.fail(source, LoadProblem::Decode(e.to_string())))
    }

    /// The text of `text` migrated to the current version, written with
    /// [`crate::write`] at `flow_depth`, or None if it is already current.
    /// For tools that upgrade files on purpose: the written text has no
    /// comments, so a current file is left as it is.
    pub fn upgrade(&self, source: Option<&str>, text: &str, flow_depth: usize) -> Result<Option<String>, LoadError> {
        let doc = match parse(text) {
            Ok(doc) => doc,
            Err(e) => return self.fail(source, LoadProblem::Yaml(e)),
        };
        if version_of(&doc) == Ok(self.current) {
            return Ok(None);
        }
        Ok(Some(write(&self.migrate(source, doc)?, flow_depth)))
    }

    /// Check that every fixture loads to its expected tree. Every failure is
    /// listed, one line each.
    pub fn check_fixtures(&self, fixtures: &[Fixture<'_>]) -> Result<(), Vec<String>> {
        let mut failures = Vec::new();
        if fixtures.is_empty() {
            failures.push(format!("{}: no fixtures", self.kind));
        }
        for fx in fixtures {
            let got = match self.load(Some(fx.name), fx.input) {
                Ok(doc) => doc,
                Err(e) => {
                    failures.push(e.to_string());
                    continue;
                }
            };
            let want = match fx.expected {
                Some(text) => match parse(text) {
                    Ok(doc) => doc,
                    Err(e) => {
                        failures.push(format!("{}: expected output: {e}", fx.name));
                        continue;
                    }
                },
                None => match parse(fx.input) {
                    Ok(doc) => doc,
                    Err(e) => {
                        failures.push(format!("{}: {e}", fx.name));
                        continue;
                    }
                },
            };
            match version_of(&want) {
                Ok(v) if v == self.current => {}
                Ok(v) if fx.expected.is_none() => failures.push(format!(
                    "{}: at version {v}, not the current {}: give it its expected current-version output",
                    fx.name, self.current
                )),
                _ => failures.push(format!(
                    "{}: the expected output must be at the current version, {}",
                    fx.name, self.current
                )),
            }
            if got != want {
                failures.push(format!(
                    "{}: migrated to\n{}which is not the expected\n{}",
                    fx.name,
                    write(&got, 2),
                    write(&want, 2)
                ));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }

    /// [`Loader::check_fixtures`], panicking with every failure: for tests.
    #[track_caller]
    pub fn assert_fixtures(&self, fixtures: &[Fixture<'_>]) {
        if let Err(failures) = self.check_fixtures(fixtures) {
            panic!("{} fixtures do not migrate:\n{}", self.kind, failures.join("\n"));
        }
    }
}

/// A document at some supported version, and what it must migrate to.
#[derive(Clone, Copy, Debug)]
pub struct Fixture<'a> {
    /// Named in failures: its path, say.
    pub name: &'a str,
    /// The document as stored.
    pub input: &'a str,
    /// The current-version document it must migrate to. None for a document
    /// already at the current version, which must load unchanged; when the
    /// version moves on, such a fixture fails until it is given its expected
    /// output.
    pub expected: Option<&'a str>,
}

// ---------------------------------------------------------------- helpers

fn not_a_mapping(what: &str) -> MigrationError {
    MigrationError(format!("{what}: not a mapping"))
}

/// Rename `from` to `to` in `map`, keeping its place. Nothing to do if `from`
/// is missing; an error if `to` is already there.
pub fn rename_key(map: &mut Value, from: &str, to: &str) -> Result<(), MigrationError> {
    let Value::Map(entries) = map else {
        return Err(not_a_mapping(&format!("rename {from:?}")));
    };
    if from == to || !entries.iter().any(|(k, _)| k == from) {
        return Ok(());
    }
    if entries.iter().any(|(k, _)| k == to) {
        return Err(MigrationError(format!("rename {from:?} to {to:?}: {to:?} is already there")));
    }
    for (k, _) in entries.iter_mut() {
        if k == from {
            *k = to.to_string();
        }
    }
    Ok(())
}

/// Move the value at key path `from` to key path `to`, both from `root`.
/// Missing mappings on the way to `to` are made (a null counts as an empty
/// mapping). Nothing to do if `from` is missing; an error if `to` is already
/// there or a key on either path is not a mapping, and then `root` is left
/// as it was.
pub fn move_key(root: &mut Value, from: &[&str], to: &[&str]) -> Result<(), MigrationError> {
    let before = root.clone();
    let moved = try_move(root, from, to);
    if moved.is_err() {
        *root = before;
    }
    moved
}

fn try_move(root: &mut Value, from: &[&str], to: &[&str]) -> Result<(), MigrationError> {
    let what = || format!("move {} to {}", from.join("."), to.join("."));
    let (Some((from_key, from_parents)), Some((to_key, to_parents))) = (from.split_last(), to.split_last()) else {
        return Err(MigrationError(format!("{}: empty key path", what())));
    };
    let mut parent = &mut *root;
    for key in from_parents {
        match parent.get_mut(key) {
            Some(next @ Value::Map(_)) => parent = next,
            Some(Value::Null) | None => return Ok(()),
            Some(_) => return Err(not_a_mapping(&format!("{}: {key:?}", what()))),
        }
    }
    if parent.as_map().is_none() {
        return Err(not_a_mapping(&what()));
    }
    let Some(value) = parent.remove(from_key) else {
        return Ok(());
    };
    let mut parent = &mut *root;
    for key in to_parents {
        if parent.as_map().is_none() {
            return Err(not_a_mapping(&format!("{}: {key:?}", what())));
        }
        let next = parent.entry(key);
        if *next == Value::Null {
            *next = Value::Map(Vec::new());
        }
        parent = next;
    }
    if parent.as_map().is_none() {
        return Err(not_a_mapping(&what()));
    }
    if parent.get(to_key).is_some() {
        return Err(MigrationError(format!("{}: {to_key:?} is already there", what())));
    }
    *parent.entry(to_key) = value;
    Ok(())
}

/// Set `key` in `map` to `value` if it is missing or null.
pub fn set_default(map: &mut Value, key: &str, value: Value) -> Result<(), MigrationError> {
    if map.as_map().is_none() {
        return Err(not_a_mapping(&format!("default {key:?}")));
    }
    let slot = map.entry(key);
    if *slot == Value::Null {
        *slot = value;
    }
    Ok(())
}

/// Take `key` out of `map`, giving its value if it was there.
pub fn remove_key(map: &mut Value, key: &str) -> Result<Option<Value>, MigrationError> {
    if map.as_map().is_none() {
        return Err(not_a_mapping(&format!("remove {key:?}")));
    }
    Ok(map.remove(key))
}

/// Run `f` over each item of the sequence at `key` in `map`. Nothing to do if
/// `key` is missing or null; an error names the failing item's index.
pub fn map_seq(
    map: &mut Value,
    key: &str,
    mut f: impl FnMut(&mut Value) -> Result<(), MigrationError>,
) -> Result<(), MigrationError> {
    if map.as_map().is_none() {
        return Err(not_a_mapping(&format!("map over {key:?}")));
    }
    match map.get_mut(key) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Seq(items)) => {
            for (i, item) in items.iter_mut().enumerate() {
                f(item).map_err(|e| MigrationError(format!("{key}[{i}]: {e}")))?;
            }
            Ok(())
        }
        Some(_) => Err(MigrationError(format!("map over {key:?}: not a sequence"))),
    }
}
