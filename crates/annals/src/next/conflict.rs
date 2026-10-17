//! Conflict units: what COMMUTATIVE compares, exactly as angzarr defines
//! them (`options.proto`, the `conflict_units` region).
//!
//! A state message opts in with `option (io.angzarr.v1.commutative_state)
//! = true` and marks each field with `(io.angzarr.v1.conflict)`. From those
//! markers a generator (angzarr's `protoc-gen-angzarr-conflict`, one plugin
//! for every language, backlog CR-1) emits, per marked state message, the
//! pure function
//!
//! ```text
//! changed(before, after) -> the unit paths whose values differ
//! ```
//!
//! For Rust, and so for annals, that function is an
//! `impl CommutativeState for State` ([`CommutativeState::changed`]), and a
//! domain whose state has one names it in [`Domain::COMMUTATIVE`]. Business
//! code never writes it: the generated code shares angzarr's semantics
//! table and is held to the same golden parity suite as every client
//! library, so a state resolves identically under both runtimes. The only
//! hand-written implementations are test fixtures standing in for
//! generated code.
//!
//! This module is the generated code's interface: the path type it builds
//! ([`UnitPath`], with [`Key`]), the set it returns ([`Units`]), and the
//! overlap rule the kernel applies to two sets.
//!
//! [`Domain::COMMUTATIVE`]: super::Domain::COMMUTATIVE

use std::collections::BTreeSet;
use std::fmt;

/// One conflict unit's path (`options.proto`, *Unit path syntax*): proto
/// field names (never JSON names) joined by `.`, a map or keyed-repeated
/// key in brackets after its field (`items[42]`, `items[42].weight`), the
/// order unit `field[#order]`, and the **wildcard**: `*` followed by the
/// path of an unmarked field that changed (`*notes`), or `*` alone for
/// unknown fields at the top level.
///
/// Held as bytes, because a bytes key is written raw; compared as byte
/// strings, as the proto says. Built only through the constructors, so the
/// escaping is the kernel's, once.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct UnitPath(Vec<u8>);

/// A map or element key, as a path writes it: integers in decimal, bools
/// as `true` / `false`, enums (element keys only) by number, strings as
/// their UTF-8 and bytes raw, each `\`, `]` and `#` in a string or bytes
/// key preceded by a `\` (so the string key `"#order"` is `[\#order]` and
/// never collides with the order unit).
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Key<'a> {
    Int(i64),
    Uint(u64),
    Bool(bool),
    Enum(i32),
    Str(&'a str),
    Bytes(&'a [u8]),
}

impl UnitPath {
    /// The top-level field `name`.
    pub fn field(name: &str) -> UnitPath {
        todo!("proposal: the name's bytes")
    }

    /// The inner field `name` of this (descended) message: `self.name`.
    pub fn dot(&self, name: &str) -> UnitPath {
        todo!("proposal: self, '.', name")
    }

    /// This map's or keyed list's entry `key`: `self[key]`, escaped.
    pub fn key(&self, key: Key<'_>) -> UnitPath {
        todo!("proposal: self, '[', the key written and escaped as Key says, ']'")
    }

    /// This keyed, ordered list's order unit: `self[#order]`.
    pub fn order(&self) -> UnitPath {
        todo!("proposal: self, \"[#order]\"")
    }

    /// The wildcard for this unmarked field: `*` then its path. Overlaps
    /// every unit of a non-empty other set.
    pub fn wildcard(&self) -> UnitPath {
        todo!("proposal: '*', self")
    }

    /// The wildcard for unknown fields at the top level: `*` alone.
    pub fn unknown() -> UnitPath {
        UnitPath(b"*".to_vec())
    }

    pub fn is_wildcard(&self) -> bool {
        self.0.first() == Some(&b'*')
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Whether two units name overlapping state: they are equal, or one is
    /// a prefix of the other ending at a `.` or `[` boundary
    /// (`items[42]` overlaps `items[42].weight`; `items[4]` does not
    /// overlap `items[42]`). A coarser unit always overlaps the finer
    /// units inside it, which makes every fallback sound. Wildcards are
    /// [`Units::overlaps`]'s concern.
    pub fn overlaps(&self, other: &UnitPath) -> bool {
        todo!("proposal: equal, or the longer continues the shorter with '.' or '['")
    }
}

/// For messages and logs: the path as text, any bytes key shown lossily.
impl fmt::Display for UnitPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&String::from_utf8_lossy(&self.0))
    }
}

/// The units one change touched: what [`CommutativeState::changed`]
/// returns. Ordered, so the kernel stays deterministic.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Units(BTreeSet<UnitPath>);

impl Units {
    pub fn new() -> Units {
        Units::default()
    }

    pub fn insert(&mut self, unit: UnitPath) {
        self.0.insert(unit);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &UnitPath> {
        self.0.iter()
    }

    /// Whether two sets overlap (`options.proto`, *Overlap*): one holds a
    /// wildcard and the other is non-empty, or a unit of one
    /// [overlaps](UnitPath::overlaps) a unit of the other. An empty set
    /// overlaps nothing, so an empty window or a command that changes
    /// nothing always commutes.
    pub fn overlaps(&self, other: &Units) -> bool {
        todo!("proposal: wildcard against non-empty, else any pair of UnitPath::overlaps")
    }
}

impl FromIterator<UnitPath> for Units {
    fn from_iter<I: IntoIterator<Item = UnitPath>>(iter: I) -> Units {
        Units(iter.into_iter().collect())
    }
}

/// A state message marked `(commutative_state)`: implemented by generated
/// code only, from the `(conflict)` markers (see the module docs).
pub trait CommutativeState {
    /// The units whose values differ between `before` and `after`, by the
    /// `conflict_units` table: per field kind (a scalar, an optional with
    /// presence, a whole or descended message, a map per key, a keyed list
    /// per element plus `[#order]` if ordered, a list or a oneof whole),
    /// floats compared bitwise, the non-unique-key fallback to the whole
    /// list, the descend depth bound of 32, and the **fail-safe wildcard**
    /// for every unmarked or unknown field that differs. Pure.
    fn changed(before: &Self, after: &Self) -> Units;
}

/// The diff a domain names in [`Domain::COMMUTATIVE`](super::Domain::COMMUTATIVE).
pub type Changed<S> = fn(&S, &S) -> Units;

/// `S`'s generated diff, for [`Domain::COMMUTATIVE`](super::Domain::COMMUTATIVE):
/// `const COMMUTATIVE: Option<Changed<ListState>> = commutative::<ListState>();`.
pub const fn commutative<S: CommutativeState>() -> Option<Changed<S>> {
    Some(S::changed)
}
