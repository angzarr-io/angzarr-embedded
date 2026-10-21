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

#[test]
fn a_graph_is_empty_until_it_has_a_node() {
    let mut g: Graph<N, &str, u64> = Graph::new();
    assert!(g.is_empty());
    g.add_node("a");
    assert!(!g.is_empty());
}

#[test]
fn a_graph_counts_its_nodes_and_lists_its_edges() {
    let g = diamond();
    assert_eq!(g.len(), 4);
    let edges: Vec<_> = g
        .edges()
        .map(|(id, e)| (id, e.from, e.to, e.data))
        .collect();
    assert_eq!(
        edges,
        vec![
            (EdgeId(0), N(0), N(1), 2),
            (EdgeId(1), N(1), N(3), 2),
            (EdgeId(2), N(0), N(2), 1),
            (EdgeId(3), N(2), N(3), 5),
        ]
    );
}

#[test]
fn an_edge_is_walked_either_way_and_a_loop_once() {
    let mut g = diamond();
    // Back along every edge, from the end each was added at.
    let p = g.shortest_path(N(3), N(0), |e| Some(e.data)).unwrap();
    assert_eq!(p.nodes, vec![N(3), N(1), N(0)]);
    let ring = g.add_edge(N(2), N(2), 1);
    let exits: Vec<_> = g.exits(N(2)).map(|(id, _, to)| (id, to)).collect();
    assert_eq!(
        exits,
        vec![(EdgeId(2), N(0)), (EdgeId(3), N(3)), (ring, N(2))]
    );
}

#[test]
fn reachable_follows_only_the_edges_that_pass() {
    let g = diamond();
    assert_eq!(g.reachable(N(0), |_| true), vec![N(0), N(1), N(2), N(3)]);
    assert_eq!(g.reachable(N(0), |_| false), vec![N(0)]);
    assert_eq!(
        g.reachable(N(0), |e| e.data < 5),
        vec![N(0), N(1), N(2), N(3)]
    );
    assert_eq!(g.reachable(N(2), |e| e.data < 2), vec![N(0), N(2)]);
}

#[test]
fn an_undirected_graph_whose_edges_all_pass_is_strongly_connected() {
    let g = diamond();
    assert!(g.strongly_connected(|_| true));
    assert!(!g.strongly_connected(|e| e.data < 2));
}

#[test]
fn an_equal_route_through_a_lower_node_found_later_still_wins() {
    let mut g: Graph<N, (), u64> = Graph::new();
    let a = g.add_node(());
    let lo = g.add_node(());
    let hi = g.add_node(());
    let d = g.add_node(());
    // hi is reached first and reaches d first; lo ties it later.
    g.add_edge(a, lo, 3);
    g.add_edge(lo, d, 1);
    g.add_edge(a, hi, 1);
    g.add_edge(hi, d, 3);
    let p = g.shortest_path(a, d, |e| Some(e.data)).unwrap();
    assert_eq!((p.nodes, p.cost), (vec![a, lo, d], 4));
}

#[test]
fn a_dearer_route_through_a_lower_node_never_wins() {
    let mut g: Graph<N, (), u64> = Graph::new();
    let a = g.add_node(());
    let lo = g.add_node(());
    let hi = g.add_node(());
    let d = g.add_node(());
    // lo is done before d, and offers d a dearer route than hi's.
    g.add_edge(a, hi, 1);
    g.add_edge(hi, d, 10);
    g.add_edge(a, lo, 2);
    g.add_edge(lo, d, 10);
    let p = g.shortest_path(a, d, |e| Some(e.data)).unwrap();
    assert_eq!((p.nodes, p.cost), (vec![a, hi, d], 11));
}
