//! Nodes, edges, and search over them.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;

/// A typed node id: a `u32` index that knows which graph it belongs to.
/// Declare one with [`id!`](crate::id).
pub trait Id: Copy + Ord + Debug {
    fn from_index(i: usize) -> Self;
    fn index(self) -> usize;
}

/// Declare a typed id.
///
/// ```
/// annals_graph::id!(NodeId);
/// annals_graph::id!(RunId);
/// // A `RunId` can never be used to index a graph of `NodeId`s.
/// ```
#[macro_export]
macro_rules! id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub struct $name(pub u32);

        impl $crate::Id for $name {
            fn from_index(i: usize) -> Self {
                $name(u32::try_from(i).expect("graph too large for a u32 id"))
            }
            fn index(self) -> usize {
                self.0 as usize
            }
        }
    };
}

/// An edge's id, in the order edges were added.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EdgeId(pub u32);

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Edge<I, E> {
    pub from: I,
    pub to: I,
    /// A directed edge is only walked `from` → `to`.
    pub directed: bool,
    pub data: E,
}

/// A route through a graph: the nodes in order (both ends included), the
/// edges between them, and the total cost.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Path<I> {
    pub nodes: Vec<I>,
    pub edges: Vec<EdgeId>,
    pub cost: u64,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Graph<I, N, E> {
    nodes: Vec<N>,
    edges: Vec<Edge<I, E>>,
    /// Per node, the edges touching it, in the order they were added.
    touching: Vec<Vec<EdgeId>>,
}

impl<I: Id, N, E> Default for Graph<I, N, E> {
    fn default() -> Self {
        Graph {
            nodes: Vec::new(),
            edges: Vec::new(),
            touching: Vec::new(),
        }
    }
}

impl<I: Id, N, E> Graph<I, N, E> {
    pub fn new() -> Self {
        Self::default()
    }

    // ------------------------------------------------------------ building

    /// Append a node. Ids are handed out in order and never reused.
    pub fn add_node(&mut self, node: N) -> I {
        let id = I::from_index(self.nodes.len());
        self.nodes.push(node);
        self.touching.push(Vec::new());
        id
    }

    /// An edge walkable both ways.
    pub fn add_edge(&mut self, a: I, b: I, data: E) -> EdgeId {
        self.push_edge(a, b, false, data)
    }

    /// An edge walkable only `from` → `to`.
    pub fn add_arc(&mut self, from: I, to: I, data: E) -> EdgeId {
        self.push_edge(from, to, true, data)
    }

    fn push_edge(&mut self, from: I, to: I, directed: bool, data: E) -> EdgeId {
        assert!(from.index() < self.nodes.len(), "edge from unknown node {from:?}");
        assert!(to.index() < self.nodes.len(), "edge to unknown node {to:?}");
        let id = EdgeId(u32::try_from(self.edges.len()).expect("too many edges"));
        self.edges.push(Edge {
            from,
            to,
            directed,
            data,
        });
        self.touching[from.index()].push(id);
        if to != from {
            self.touching[to.index()].push(id);
        }
        id
    }

    // ------------------------------------------------------------ reading

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn node(&self, id: I) -> &N {
        &self.nodes[id.index()]
    }

    pub fn node_mut(&mut self, id: I) -> &mut N {
        &mut self.nodes[id.index()]
    }

    /// Every node, in id order.
    pub fn nodes(&self) -> impl Iterator<Item = (I, &N)> {
        self.nodes.iter().enumerate().map(|(i, n)| (I::from_index(i), n))
    }

    pub fn ids(&self) -> impl Iterator<Item = I> {
        (0..self.nodes.len()).map(I::from_index)
    }

    pub fn edge(&self, id: EdgeId) -> &Edge<I, E> {
        &self.edges[id.0 as usize]
    }

    pub fn edge_mut(&mut self, id: EdgeId) -> &mut Edge<I, E> {
        &mut self.edges[id.0 as usize]
    }

    /// Every edge, in id order.
    pub fn edges(&self) -> impl Iterator<Item = (EdgeId, &Edge<I, E>)> {
        self.edges.iter().enumerate().map(|(i, e)| (EdgeId(i as u32), e))
    }

    /// Edges that can be walked out of `from`, with where each leads, in the
    /// order they were added.
    pub fn exits(&self, from: I) -> impl Iterator<Item = (EdgeId, &Edge<I, E>, I)> {
        self.touching[from.index()].iter().filter_map(move |&id| {
            let e = &self.edges[id.0 as usize];
            if e.from == from {
                Some((id, e, e.to))
            } else if !e.directed {
                Some((id, e, e.from))
            } else {
                None
            }
        })
    }

    // ------------------------------------------------------------ search

    /// The cheapest route from `from` to `to`. `cost` prices each edge for this
    /// traveller; `None` means it cannot be taken at all. Equal-cost routes
    /// break toward lower ids, so the answer never depends on anything but the
    /// graph and the costs.
    pub fn shortest_path<C>(&self, from: I, to: I, cost: C) -> Option<Path<I>>
    where
        C: Fn(&Edge<I, E>) -> Option<u64>,
    {
        let (dist, via) = self.dijkstra(from, &cost);
        let total = dist[to.index()]?;
        let mut nodes = vec![to];
        let mut edges = Vec::new();
        let mut at = to;
        while at != from {
            let (prev, edge) = via[at.index()].expect("a reached node has a predecessor");
            edges.push(edge);
            nodes.push(prev);
            at = prev;
        }
        nodes.reverse();
        edges.reverse();
        Some(Path {
            nodes,
            edges,
            cost: total,
        })
    }

    /// The cost of the cheapest route from `from` to every node, or `None`
    /// where there is none.
    pub fn costs_from<C>(&self, from: I, cost: C) -> Vec<Option<u64>>
    where
        C: Fn(&Edge<I, E>) -> Option<u64>,
    {
        self.dijkstra(from, &cost).0
    }

    /// Every node reachable from `from` along edges that `pass` allows, in id
    /// order, `from` included.
    pub fn reachable<P>(&self, from: I, pass: P) -> Vec<I>
    where
        P: Fn(&Edge<I, E>) -> bool,
    {
        let mut seen = vec![false; self.nodes.len()];
        let mut stack = vec![from];
        seen[from.index()] = true;
        while let Some(at) = stack.pop() {
            for (_, e, next) in self.exits(at) {
                if !seen[next.index()] && pass(e) {
                    seen[next.index()] = true;
                    stack.push(next);
                }
            }
        }
        self.ids().filter(|i| seen[i.index()]).collect()
    }

    /// Whether every node can reach every other along edges `pass` allows,
    /// treating directed edges as directed.
    pub fn strongly_connected<P>(&self, pass: P) -> bool
    where
        P: Fn(&Edge<I, E>) -> bool,
    {
        self.ids()
            .all(|i| self.reachable(i, &pass).len() == self.nodes.len())
    }

    #[allow(clippy::type_complexity)]
    fn dijkstra<C>(&self, from: I, cost: &C) -> (Vec<Option<u64>>, Vec<Option<(I, EdgeId)>>)
    where
        C: Fn(&Edge<I, E>) -> Option<u64>,
    {
        let n = self.nodes.len();
        let mut dist: Vec<Option<u64>> = vec![None; n];
        let mut via: Vec<Option<(I, EdgeId)>> = vec![None; n];
        let mut done = vec![false; n];
        let mut heap = BinaryHeap::new();
        dist[from.index()] = Some(0);
        heap.push(Reverse((0u64, from)));
        while let Some(Reverse((d, at))) = heap.pop() {
            if done[at.index()] {
                continue;
            }
            done[at.index()] = true;
            for (edge, e, next) in self.exits(at) {
                if done[next.index()] {
                    continue;
                }
                let Some(c) = cost(e) else {
                    continue;
                };
                let nd = d.saturating_add(c);
                let better = match (dist[next.index()], via[next.index()]) {
                    (None, _) => true,
                    (Some(old), _) if nd < old => true,
                    // Equal cost: prefer the lower predecessor, then the
                    // lower edge — ties break on id, never on order.
                    (Some(old), Some((p, pe))) if nd == old => (at, edge) < (p, pe),
                    _ => false,
                };
                if better {
                    dist[next.index()] = Some(nd);
                    via[next.index()] = Some((at, edge));
                    heap.push(Reverse((nd, next)));
                }
            }
        }
        (dist, via)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::id!(N);

    /// a — b — d, a — c — d, with a costlier detour through c.
    fn diamond() -> Graph<N, &'static str, u64> {
        let mut g = Graph::new();
        let a = g.add_node("a");
        let b = g.add_node("b");
        let c = g.add_node("c");
        let d = g.add_node("d");
        g.add_edge(a, b, 2);
        g.add_edge(b, d, 2);
        g.add_edge(a, c, 1);
        g.add_edge(c, d, 5);
        g
    }

    #[test]
    fn the_cheapest_route_wins() {
        let g = diamond();
        let p = g.shortest_path(N(0), N(3), |e| Some(e.data)).unwrap();
        assert_eq!(p.nodes, vec![N(0), N(1), N(3)]);
        assert_eq!(p.cost, 4);
        assert_eq!(p.edges.len(), 2);
    }

    #[test]
    fn the_cost_belongs_to_the_traveller() {
        let g = diamond();
        // Someone who cannot take edges costing more than 3.
        let p = g
            .shortest_path(N(0), N(3), |e| (e.data <= 3).then_some(e.data))
            .unwrap();
        assert_eq!(p.nodes, vec![N(0), N(1), N(3)]);
        // Someone who cannot pass b at all.
        let p = g.shortest_path(N(0), N(3), |e| (e.to != N(1)).then_some(e.data));
        assert_eq!(p.unwrap().nodes, vec![N(0), N(2), N(3)]);
    }

    #[test]
    fn equal_routes_break_toward_lower_ids() {
        let mut g: Graph<N, (), u64> = Graph::new();
        let a = g.add_node(());
        let hi = g.add_node(());
        let lo = g.add_node(());
        let d = g.add_node(());
        // Added in the "wrong" order on purpose.
        g.add_edge(a, lo, 1);
        g.add_edge(lo, d, 1);
        g.add_edge(a, hi, 1);
        g.add_edge(hi, d, 1);
        let p = g.shortest_path(a, d, |e| Some(e.data)).unwrap();
        assert_eq!(p.nodes, vec![a, hi, d], "N(1) is the lower id");
    }

    #[test]
    fn arcs_go_one_way() {
        let mut g: Graph<N, (), ()> = Graph::new();
        let roof = g.add_node(());
        let street = g.add_node(());
        g.add_arc(roof, street, ()); // a drop
        assert!(g.shortest_path(roof, street, |_| Some(1)).is_some());
        assert!(g.shortest_path(street, roof, |_| Some(1)).is_none());
        assert_eq!(g.reachable(street, |_| true), vec![street]);
        assert!(!g.strongly_connected(|_| true));
    }

    #[test]
    fn nodes_added_later_keep_every_earlier_id() {
        let mut g = diamond();
        let e = g.add_node("e");
        g.add_edge(N(3), e, 1);
        assert_eq!(e, N(4));
        assert_eq!(*g.node(N(0)), "a");
        assert_eq!(g.costs_from(N(0), |e| Some(e.data))[4], Some(5));
    }
}
