# gamekit

Small, deterministic Rust crates shared by card-game and tdautobattle. All but
eslog have zero third-party dependencies.

| Crate | What it is |
|---|---|
| [`es`](crates/es) | event-sourcing kernel: decide, emit, apply, replay |
| [`graph`](crates/graph) | typed-id graph with tags, queries and Dijkstra search |
| [`yaml`](crates/yaml) | a strict YAML subset, read and written |
| [`simkit`](crates/simkit) | simulation parts on `es`: per-entity dice, a folded tick scheduler, jobs as data, a parallel map, a Gherkin runner |
| [`eslog`](crates/eslog) | a binary event-log format for `es` streams: length-delimited protobuf (prost) |

```
just test       # generate eslog's protobuf types, then cargo test --release
just lint       # buf lint + clippy
just breaking   # buf breaking against main
```

Needs [`just`](https://just.systems), [`buf`](https://buf.build) and
`protoc-gen-prost` 0.5.0 (`cargo install protoc-gen-prost --version 0.5.0 --locked`)
on `PATH`.
