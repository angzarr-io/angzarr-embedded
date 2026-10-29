//! Semantic tags: how a query names a *kind* of node.
//!
//! A tag is written `namespace:name` or `namespace:name=value`:
//!
//! - `terrain:rooftop` — a flag;
//! - `terrain:elevation=600` — a number (here, centimetres);
//! - `faction:owner=crimson` — a symbol.
//!
//! The namespace is optional — `rooftop` lands in the default namespace — but
//! discouraged: [`TagRegistry::unnamespaced`] lists every such key so content
//! checks can flag them.
//!
//! Open — content defines them — and interned: every key and every symbol
//! becomes a small id, and the registry remembers which namespace each key
//! belongs to, so a query can ask for anything in `hazard:`.

use std::fmt;

use angzarr_embedded_diagnostic::{Arg, Diagnostic};

/// An interned `namespace:name`.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TagId(pub u16);

/// An interned namespace.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NamespaceId(pub u16);

/// An interned symbol value.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SymbolId(pub u16);

/// What a tag carries.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum TagValue {
    /// Present, nothing more.
    Flag,
    Int(i32),
    Symbol(SymbolId),
}

impl TagValue {
    pub fn int(self) -> Option<i32> {
        match self {
            TagValue::Int(i) => Some(i),
            _ => None,
        }
    }
}

/// Why a tag could not be read: the text, and what is wrong with it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BadTag {
    pub text: String,
    pub problem: TagProblem,
}

/// What is wrong with a tag.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum TagProblem {
    /// The namespace is not lowercase letters, digits, `_` and `-`.
    BadNamespace,
    /// The name is not lowercase letters, digits, `_` and `-`.
    BadName,
    /// The symbol value is not lowercase letters, digits, `_` and `-`.
    BadSymbol,
    /// `:name`: a colon with nothing before it.
    EmptyNamespace,
}

impl fmt::Display for BadTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_embedded_diagnostic::display(self, f)
    }
}

impl std::error::Error for BadTag {}

impl Diagnostic for BadTag {
    fn code(&self) -> &'static str {
        match self.problem {
            TagProblem::BadNamespace => "angzarr-embedded-graph-bad-namespace",
            TagProblem::BadName => "angzarr-embedded-graph-bad-name",
            TagProblem::BadSymbol => "angzarr-embedded-graph-bad-symbol",
            TagProblem::EmptyNamespace => "angzarr-embedded-graph-empty-namespace",
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        vec![("text", self.text.as_str().into())]
    }
}

/// Every namespace, key and symbol a game uses, interned in the order first
/// seen. Ids are stable for as long as the registry is built the same way.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct TagRegistry {
    namespaces: Vec<String>,
    /// Each key's namespace and name.
    keys: Vec<(NamespaceId, String)>,
    symbols: Vec<String>,
}

fn word_ok(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

fn intern<T>(list: &mut Vec<String>, s: &str, make: impl Fn(u16) -> T) -> T {
    let i = match list.iter().position(|x| x == s) {
        Some(i) => i,
        None => {
            list.push(s.to_string());
            list.len() - 1
        }
    };
    make(u16::try_from(i).expect("too many tags"))
}

impl TagRegistry {
    pub fn new() -> TagRegistry {
        TagRegistry::default()
    }

    /// The id for `namespace:name`, adding it if it is new. An empty
    /// namespace is the default one.
    pub fn key(&mut self, namespace: &str, name: &str) -> Result<TagId, BadTag> {
        let bad = |problem| BadTag {
            text: format!("{namespace}:{name}"),
            problem,
        };
        if !namespace.is_empty() && !word_ok(namespace) {
            return Err(bad(TagProblem::BadNamespace));
        }
        if !word_ok(name) {
            return Err(bad(TagProblem::BadName));
        }
        let ns = intern(&mut self.namespaces, namespace, NamespaceId);
        if let Some(i) = self.keys.iter().position(|(n, k)| *n == ns && k == name) {
            return Ok(TagId(i as u16));
        }
        self.keys.push((ns, name.to_string()));
        Ok(TagId(
            u16::try_from(self.keys.len() - 1).expect("too many tags"),
        ))
    }

    pub fn symbol(&mut self, value: &str) -> Result<SymbolId, BadTag> {
        if !word_ok(value) {
            return Err(BadTag {
                text: value.to_string(),
                problem: TagProblem::BadSymbol,
            });
        }
        Ok(intern(&mut self.symbols, value, SymbolId))
    }

    /// Read `[namespace:]name[=value]`, interning as needed.
    pub fn parse(&mut self, text: &str) -> Result<(TagId, TagValue), BadTag> {
        let bad = |problem| BadTag {
            text: text.to_string(),
            problem,
        };
        let (key, value) = match text.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (text, None),
        };
        let (ns, name) = match key.split_once(':') {
            Some(("", _)) => return Err(bad(TagProblem::EmptyNamespace)),
            Some((ns, name)) => (ns, name),
            None => ("", key),
        };
        let id = self.key(ns, name).map_err(|e| bad(e.problem))?;
        let value = match value {
            None => TagValue::Flag,
            Some(v) if v.parse::<i32>().is_ok() => TagValue::Int(v.parse().unwrap()),
            Some(v) => TagValue::Symbol(self.symbol(v).map_err(|e| bad(e.problem))?),
        };
        Ok((id, value))
    }

    /// Look up an existing key without adding it.
    pub fn get(&self, tag: &str) -> Option<TagId> {
        let (ns, name) = tag.split_once(':').unwrap_or(("", tag));
        let ns = self.namespaces.iter().position(|n| n == ns)?;
        self.keys
            .iter()
            .position(|(n, k)| n.0 as usize == ns && k == name)
            .map(|i| TagId(i as u16))
    }

    pub fn namespace_of(&self, tag: TagId) -> NamespaceId {
        self.keys[tag.0 as usize].0
    }

    pub fn namespace(&self, name: &str) -> Option<NamespaceId> {
        self.namespaces
            .iter()
            .position(|n| n == name)
            .map(|i| NamespaceId(i as u16))
    }

    /// `namespace:name`, or just `name` in the default namespace.
    pub fn name(&self, tag: TagId) -> String {
        let (ns, name) = &self.keys[tag.0 as usize];
        match self.namespaces[ns.0 as usize].as_str() {
            "" => name.clone(),
            ns => format!("{ns}:{name}"),
        }
    }

    /// Every key in the default namespace. Allowed, discouraged: content
    /// checks list these.
    pub fn unnamespaced(&self) -> Vec<TagId> {
        (0..self.keys.len())
            .map(|i| TagId(i as u16))
            .filter(|t| self.namespaces[self.namespace_of(*t).0 as usize].is_empty())
            .collect()
    }

    /// `namespace:name` or `namespace:name=value`, as it would be written.
    pub fn show(&self, tag: TagId, value: TagValue) -> String {
        match value {
            TagValue::Flag => self.name(tag),
            TagValue::Int(i) => format!("{}={}", self.name(tag), i),
            TagValue::Symbol(s) => format!("{}={}", self.name(tag), self.symbols[s.0 as usize]),
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// A node's tags, sorted by key, one value each.
#[derive(Clone, Default, PartialEq, Eq, Hash, Debug)]
pub struct TagSet {
    tags: Vec<(TagId, TagValue)>,
}

impl TagSet {
    pub fn new() -> TagSet {
        TagSet::default()
    }

    /// Add a tag, or change its value.
    pub fn set(&mut self, tag: TagId, value: TagValue) {
        match self.tags.binary_search_by_key(&tag, |(t, _)| *t) {
            Ok(i) => self.tags[i].1 = value,
            Err(i) => self.tags.insert(i, (tag, value)),
        }
    }

    pub fn with(mut self, tag: TagId, value: TagValue) -> TagSet {
        self.set(tag, value);
        self
    }

    /// Remove a tag. Returns whether it was there.
    pub fn remove(&mut self, tag: TagId) -> bool {
        match self.tags.binary_search_by_key(&tag, |(t, _)| *t) {
            Ok(i) => {
                self.tags.remove(i);
                true
            }
            Err(_) => false,
        }
    }

    pub fn has(&self, tag: TagId) -> bool {
        self.value(tag).is_some()
    }

    pub fn value(&self, tag: TagId) -> Option<TagValue> {
        self.tags
            .binary_search_by_key(&tag, |(t, _)| *t)
            .ok()
            .map(|i| self.tags[i].1)
    }

    /// Every tag and its value, in key order.
    pub fn iter(&self) -> impl Iterator<Item = (TagId, TagValue)> + '_ {
        self.tags.iter().copied()
    }

    pub fn len(&self) -> usize {
        self.tags.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }
}

/// Anything that carries tags — what the tag queries read.
pub trait Tagged {
    fn tags(&self) -> &TagSet;
}

impl Tagged for TagSet {
    fn tags(&self) -> &TagSet {
        self
    }
}

#[cfg(test)]
#[path = "tags.test.rs"]
mod tests;
