//! Queries: filter nodes with [`Where`]s, rank them with a [`Score`], ties
//! broken on id. Generic over the node type and over a context `C` — whatever
//! else the question needs (the live fight and who is asking, the campaign
//! and its reputations).
//!
//! Tag filters work on any node that is [`Tagged`]. Anything the tags can't
//! answer — "in range of the target", "out of their sight" — is the caller's
//! own `Where`, implemented once next to the mechanic that needs it.

use crate::tags::{NamespaceId, TagId, TagRegistry, TagValue, Tagged};

/// A filter: does this node qualify?
pub trait Where<I, N, C: ?Sized> {
    fn holds(&self, id: I, node: &N, ctx: &C) -> bool;
}

/// A ranking: lower is better.
pub trait Score<I, N, C: ?Sized> {
    fn score(&self, id: I, node: &N, ctx: &C) -> i64;
}

/// The node carries this tag.
#[derive(Copy, Clone, Debug)]
pub struct HasTag(pub TagId);

impl<I, N: Tagged, C: ?Sized> Where<I, N, C> for HasTag {
    fn holds(&self, _: I, node: &N, _: &C) -> bool {
        node.tags().has(self.0)
    }
}

/// The node carries this tag with a number at least this — elevation of 3 m
/// or more, cover at least Low.
#[derive(Copy, Clone, Debug)]
pub struct TagAtLeast(pub TagId, pub i32);

impl<I, N: Tagged, C: ?Sized> Where<I, N, C> for TagAtLeast {
    fn holds(&self, _: I, node: &N, _: &C) -> bool {
        node.tags()
            .value(self.0)
            .and_then(TagValue::int)
            .map(|v| v >= self.1)
            .unwrap_or(false)
    }
}

/// The node carries this tag with exactly this value — `faction:owner=crimson`.
#[derive(Copy, Clone, Debug)]
pub struct TagIs(pub TagId, pub TagValue);

impl<I, N: Tagged, C: ?Sized> Where<I, N, C> for TagIs {
    fn holds(&self, _: I, node: &N, _: &C) -> bool {
        node.tags().value(self.0) == Some(self.1)
    }
}

/// The node carries any tag in this namespace — anything `hazard:`.
#[derive(Clone, Debug)]
pub struct InNamespace(Vec<TagId>);

impl InNamespace {
    /// Every key the registry has in `ns`, as of now.
    pub fn new(registry: &TagRegistry, ns: NamespaceId) -> InNamespace {
        InNamespace(
            (0..registry.len())
                .map(|i| TagId(i as u16))
                .filter(|t| registry.namespace_of(*t) == ns)
                .collect(),
        )
    }
}

impl<I, N: Tagged, C: ?Sized> Where<I, N, C> for InNamespace {
    fn holds(&self, _: I, node: &N, _: &C) -> bool {
        self.0.iter().any(|t| node.tags().has(*t))
    }
}

/// The filter does not hold.
pub struct Not<W>(pub W);

impl<I, N, C: ?Sized, W: Where<I, N, C>> Where<I, N, C> for Not<W> {
    fn holds(&self, id: I, node: &N, ctx: &C) -> bool {
        !self.0.holds(id, node, ctx)
    }
}

/// Every filter holds.
pub struct All<I, N, C: ?Sized>(pub Vec<Box<dyn Where<I, N, C>>>);

impl<I: Copy, N, C: ?Sized> Where<I, N, C> for All<I, N, C> {
    fn holds(&self, id: I, node: &N, ctx: &C) -> bool {
        self.0.iter().all(|w| w.holds(id, node, ctx))
    }
}

/// At least one filter holds.
pub struct Any<I, N, C: ?Sized>(pub Vec<Box<dyn Where<I, N, C>>>);

impl<I: Copy, N, C: ?Sized> Where<I, N, C> for Any<I, N, C> {
    fn holds(&self, id: I, node: &N, ctx: &C) -> bool {
        self.0.iter().any(|w| w.holds(id, node, ctx))
    }
}

/// Every candidate that passes `wants`, best first: by score, then by id.
pub fn rank<'n, I, N: 'n, C: ?Sized>(
    candidates: impl IntoIterator<Item = (I, &'n N)>,
    wants: &dyn Where<I, N, C>,
    prefer: &dyn Score<I, N, C>,
    ctx: &C,
) -> Vec<I>
where
    I: Copy + Ord,
{
    let mut scored: Vec<(i64, I)> = candidates
        .into_iter()
        .filter(|(id, n)| wants.holds(*id, n, ctx))
        .map(|(id, n)| (prefer.score(id, n, ctx), id))
        .collect();
    scored.sort();
    scored.into_iter().map(|(_, id)| id).collect()
}

/// The best candidate that passes `wants`, if any.
pub fn best<'n, I, N: 'n, C: ?Sized>(
    candidates: impl IntoIterator<Item = (I, &'n N)>,
    wants: &dyn Where<I, N, C>,
    prefer: &dyn Score<I, N, C>,
    ctx: &C,
) -> Option<I>
where
    I: Copy + Ord,
{
    candidates
        .into_iter()
        .filter(|(id, n)| wants.holds(*id, n, ctx))
        .map(|(id, n)| (prefer.score(id, n, ctx), id))
        .min()
        .map(|(_, id)| id)
}

#[cfg(test)]
#[path = "query.test.rs"]
mod tests;
