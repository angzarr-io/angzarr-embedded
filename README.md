# Annals

Annals is an event-sourced, deterministic game engine: small Rust crates
shared by card-game and tdautobattle. All but annals-log have zero
third-party dependencies.

| Crate | What it is |
|---|---|
| [`annals`](crates/annals) | event-sourcing kernel: decide, emit, apply, replay |
| [`annals-graph`](crates/annals-graph) | typed-id graph with tags, queries and Dijkstra search |
| [`annals-yaml`](crates/annals-yaml) | a strict YAML subset, read and written |
| [`annals-sim`](crates/annals-sim) | simulation parts on `annals`: per-entity dice, a folded tick scheduler, jobs as data, a parallel map, a Gherkin runner |
| [`annals-log`](crates/annals-log) | a binary event-log format for `annals` streams: length-delimited protobuf (prost) |

```
just test       # cargo test --release, plus annals-log's codegen cross-check
just lint       # buf lint + clippy
just breaking   # buf breaking against main
```

Plain `cargo build` and `cargo test` need nothing beyond Rust, so every crate
works as a git dependency. The `just` recipes also need
[`just`](https://just.systems), [`buf`](https://buf.build) and
`protoc-gen-prost` 0.5.0 (`cargo install protoc-gen-prost --version 0.5.0 --locked`)
on `PATH`: they generate annals-log's protobuf types to check its
hand-written ones against.
