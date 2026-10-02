# graph

A small, deterministic graph. Zero dependencies, nothing specific to any game,
meant to be reused, like `es`.

One shape for every graph a game has: positions on a map, stages in a scene,
scenes in a run, runs in a campaign.

- **Typed ids.** `graph::id!(NodeId);` declares one. An id from one graph can
  never index another.
- **The caller's data on nodes and edges.** The graph knows nothing about what
  they mean. Edges are undirected (`add_edge`) or one-way (`add_arc`).
- **Ids are only appended,** so nodes can be added mid-game (a spot minted in
  a crowd) and every earlier id keeps its meaning.
- **Tags.** `TagRegistry` interns open, content-defined names. `TagSet` keeps a
  node's tags sorted, each with a small value (a grade, a count), and they can
  change.
- **Queries.** `Where` filters and `Score` ranks, generic over the node type and
  a context the caller chooses. `HasTag`, `TagAtLeast`, `Not`, `All` and `Any`
  come built in. Anything the tags can't answer ("in range of the target") is
  the caller's own `Where`. `best` and `rank` break ties on id.
- **Search.** `shortest_path` and `costs_from` (Dijkstra) take a cost function,
  so a route's price can depend on who is travelling; `None` means an edge is
  impassable for them. `reachable` and `strongly_connected` cover validation.
  Equal-cost routes break toward lower ids.
- **Deterministic.** `Vec` storage in id order, every tie on id, no hash-map
  iteration.

## Not yet

- **Topology builders** (irregular layouts with chokepoints, dead ends, branches
  and merges) arrive with map generation.
- **Nested graphs** need nothing from the crate: a node's data may own a graph.
