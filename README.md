# gamekit

Small, deterministic, zero-dependency Rust crates shared by card-game and
tdautobattle.

| Crate | What it is |
|---|---|
| [`es`](crates/es) | event-sourcing kernel: decide, emit, apply, replay |
| [`graph`](crates/graph) | typed-id graph with tags, queries and Dijkstra search |
| [`yaml`](crates/yaml) | a strict YAML subset, read and written |
| [`simkit`](crates/simkit) | simulation parts on `es`: per-entity dice, a folded tick scheduler, jobs as data, a parallel map, a Gherkin runner |

```
cargo test --release
```
