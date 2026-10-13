# Annals

Annals is an event-sourced, deterministic game engine: small Rust crates
shared by card-game and tdautobattle. All but annals-log (and the
test-only annals-catalog-check) have zero third-party dependencies.
Every error is localizable: a stable code and named args (see
`annals-diagnostic`), with an en-US Fluent catalog per crate under
`crates/<crate>/locales/`.

| Crate | What it is |
|---|---|
| [`annals`](crates/annals) | event-sourcing kernel: decide, emit, apply, replay |
| [`annals-diagnostic`](crates/annals-diagnostic) | localizable errors: every error has a stable code and named args for a Fluent catalog |
| [`annals-graph`](crates/annals-graph) | typed-id graph with tags, queries and Dijkstra search |
| [`annals-yaml`](crates/annals-yaml) | a strict YAML subset, read and written; versioned documents with registered migrations |
| [`annals-sim`](crates/annals-sim) | simulation parts on `annals`: per-entity dice, a folded tick scheduler, jobs as data, a parallel map, a Gherkin runner |
| [`annals-log`](crates/annals-log) | a binary event-log format for `annals` streams: length-delimited protobuf (prost) |
| [`annals-catalog-check`](crates/annals-catalog-check) | test-only: every error code has a message, with exactly its args, in every shipped Fluent catalog |

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

## Licence

Annals is licensed under the [European Union Public Licence v. 1.2](LICENSE)
(EUPL-1.2).
