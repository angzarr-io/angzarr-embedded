# gamekit

Small, deterministic, zero-dependency Rust crates shared by card-game and
tdautobattle.

| Crate | What it is |
|---|---|
| [`es`](crates/es) | event-sourcing kernel: decide, emit, apply, replay |
| [`graph`](crates/graph) | typed-id graph with tags, queries and Dijkstra search |
| [`yaml`](crates/yaml) | a strict YAML subset, read and written |

```
cargo test --release
```
