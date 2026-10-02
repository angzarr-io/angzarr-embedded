//! Queries: filter nodes with [`Where`]s, rank them with a [`Score`], ties
//! broken on id. Generic over the node type and over a context `C` — whatever
//! else the question needs (the live fight and who is asking, the campaign
//! and its reputations).
//!
//! Tag filters work on any node that is [`Tagged`]. Anything the tags can't
//! answer — "in range of the target", "out of their sight" — is the caller's
//! own `Where`, implemented once next to the mechanic that needs it.

use crate::tags::{TagId, Tagged};

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

/// The node carries this tag at this value or more — cover at least Low.
#[derive(Copy, Clone, Debug)]
pub struct TagAtLeast(pub TagId, pub i32);

impl<I, N: Tagged, C: ?Sized> Where<I, N, C> for TagAtLeast {
    fn holds(&self, _: I, node: &N, _: &C) -> bool {
        node.tags().value(self.0).map(|v| v >= self.1).unwrap_or(false)
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
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::tags::{TagRegistry, TagSet};

    crate::id!(Spot);

    struct Place {
        tags: TagSet,
        metres_to_target: i64,
    }

    impl Tagged for Place {
        fn tags(&self) -> &TagSet {
            &self.tags
        }
    }

    /// A caller's own filter: within the weapon's reach, read from context.
    struct InReach;
    impl Where<Spot, Place, i64> for InReach {
        fn holds(&self, _: Spot, p: &Place, reach: &i64) -> bool {
            p.metres_to_target <= *reach
        }
    }

    /// Prefer the farthest spot still in reach: a sniper's taste.
    struct Farthest;
    impl Score<Spot, Place, i64> for Farthest {
        fn score(&self, _: Spot, p: &Place, _: &i64) -> i64 {
            -p.metres_to_target
        }
    }

    #[test]
    fn find_a_perch() {
        let mut tags = TagRegistry::new();
        let high = tags.intern("Elevated");
        let cover = tags.intern("Cover");
        let mut g: Graph<Spot, Place, ()> = Graph::new();
        let place = |t: TagSet, m| Place {
            tags: t,
            metres_to_target: m,
        };
        g.add_node(place(TagSet::new(), 30));
        g.add_node(place(TagSet::new().with(high, 0), 900)); // too far
        g.add_node(place(TagSet::new().with(high, 0).with(cover, 1), 400));
        g.add_node(place(TagSet::new().with(high, 0), 400)); // a tie on score
        g.add_node(place(TagSet::new().with(cover, 2), 600));

        let perch = All(vec![Box::new(HasTag(high)), Box::new(InReach)]);
        let reach = 500i64;
        assert_eq!(best(g.nodes(), &perch, &Farthest, &reach), Some(Spot(2)));
        assert_eq!(rank(g.nodes(), &perch, &Farthest, &reach), vec![Spot(2), Spot(3)]);

        let anywhere = Any(vec![Box::new(TagAtLeast(cover, 1)), Box::new(Not(HasTag(high)))]);
        assert_eq!(
            rank(g.nodes(), &anywhere, &Farthest, &10_000),
            vec![Spot(4), Spot(2), Spot(0)]
        );
    }
}
