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
    let (high, flag) = tags.parse("terrain:elevated").unwrap();
    let (cover, _) = tags.parse("cover:grade=1").unwrap();
    let grade = TagValue::Int;
    let mut g: Graph<Spot, Place, ()> = Graph::new();
    let place = |t: TagSet, m| Place {
        tags: t,
        metres_to_target: m,
    };
    g.add_node(place(TagSet::new(), 30));
    g.add_node(place(TagSet::new().with(high, flag), 900)); // too far
    g.add_node(place(
        TagSet::new().with(high, flag).with(cover, grade(1)),
        400,
    ));
    g.add_node(place(TagSet::new().with(high, flag), 400)); // a tie on score
    g.add_node(place(TagSet::new().with(cover, grade(2)), 600));

    let perch = All(vec![Box::new(HasTag(high)), Box::new(InReach)]);
    let reach = 500i64;
    assert_eq!(best(g.nodes(), &perch, &Farthest, &reach), Some(Spot(2)));
    assert_eq!(
        rank(g.nodes(), &perch, &Farthest, &reach),
        vec![Spot(2), Spot(3)]
    );

    let anywhere = Any(vec![
        Box::new(TagAtLeast(cover, 1)),
        Box::new(Not(HasTag(high))),
    ]);
    assert_eq!(
        rank(g.nodes(), &anywhere, &Farthest, &10_000),
        vec![Spot(4), Spot(2), Spot(0)]
    );
}

#[test]
fn exact_values_and_namespaces() {
    let mut tags = TagRegistry::new();
    let (owner, crimson) = tags.parse("faction:owner=crimson").unwrap();
    let (_, azure) = tags.parse("faction:owner=azure").unwrap();
    let (smoke, thick) = tags.parse("hazard:smoke=3").unwrap();
    let (fire, flag) = tags.parse("hazard:fire").unwrap();
    let mut g: Graph<Spot, Place, ()> = Graph::new();
    let place = |t: TagSet| Place {
        tags: t,
        metres_to_target: 0,
    };
    g.add_node(place(TagSet::new().with(owner, crimson)));
    g.add_node(place(TagSet::new().with(owner, azure).with(smoke, thick)));
    g.add_node(place(TagSet::new().with(fire, flag)));

    let ours = TagIs(owner, crimson);
    assert_eq!(rank(g.nodes(), &ours, &Farthest, &0), vec![Spot(0)]);
    let hazard = tags.namespace("hazard").unwrap();
    let danger = InNamespace::new(&tags, hazard);
    assert_eq!(
        rank(g.nodes(), &danger, &Farthest, &0),
        vec![Spot(1), Spot(2)]
    );
}
