//! A small, deterministic graph.
//!
//! One shape for every graph a game has — positions on a map, stages in a
//! scene, scenes in a run, runs in a campaign:
//!
//! - **Typed ids** ([`id!`]), so an id from one graph can never index another.
//! - **Nodes and edges carry the caller's data.** The graph knows nothing about
//!   what they mean.
//! - **Semantic tags** ([`TagRegistry`], [`TagSet`]): `namespace:name=value`,
//!   open and interned, each value a flag, an integer or a symbol, kept sorted.
//! - **Queries** ([`Where`], [`Score`], [`best`]): filter and rank nodes. The
//!   same machinery asks "an elevated spot in range of the target" on a map and
//!   "a run offered by a friendly faction" on a campaign.
//! - **Search** ([`Graph::shortest_path`], [`Graph::reachable`]) over a cost
//!   the caller supplies, so a path's cost can depend on who is travelling.
//! - **Deterministic.** Storage is `Vec`s in id order; every tie breaks on id;
//!   nothing iterates a hash map. Ids are only ever appended, so an id, once
//!   given, never means anything else — nodes can be added mid-game.

mod graph;
mod query;
mod tags;

pub use graph::{Edge, EdgeId, Graph, Id, Path};
pub use query::{best, rank, All, Any, HasTag, InNamespace, Not, Score, TagAtLeast, TagIs, Where};
pub use tags::{BadTag, NamespaceId, SymbolId, TagId, TagRegistry, TagSet, TagValue, Tagged};
