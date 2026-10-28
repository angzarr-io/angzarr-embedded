# Annals

Annals is a deterministic application engine on CQRS/ES: small Rust
crates, knowing nothing of the applications that use them. All but annals,
annals-store-sql, annals-macros (and the test-only annals-catalog-check)
have zero third-party dependencies; annals depends on angzarr's wire types
(angzarr-proto, prost, prost-types), which its kernel operates on directly,
and annals-store-sql on sea-query and sqlx.
Every error is localizable: a stable code and named args (see
`annals-diagnostic`), with an en-US Fluent catalog per crate under
`crates/<crate>/locales/`.

| Crate | What it is |
|---|---|
| [`annals`](crates/annals) | event-sourcing kernel on angzarr's proto types (`annals::next`): decide, emit, apply, replay; angzarr's local runtime |
| [`annals-diagnostic`](crates/annals-diagnostic) | localizable errors: every error has a stable code and named args for a Fluent catalog |
| [`annals-graph`](crates/annals-graph) | typed-id graph with tags, queries and Dijkstra search |
| [`annals-yaml`](crates/annals-yaml) | a strict YAML subset, read and written; versioned documents with registered migrations |
| [`annals-store-sql`](crates/annals-store-sql) | `annals::next`'s store port on SQL: backend-neutral sea-query statements, and each backend (`sqlite`) behind a feature, on sqlx with its migrations |
| [`annals-macros`](crates/annals-macros) | markup over hand-written aggregates, process managers, policies and projectors, generating their `annals::next` glue |
| [`annals-catalog-check`](crates/annals-catalog-check) | test-only: every error code has a message, with exactly its args, in every shipped Fluent catalog |

```
just test           # cargo-nextest over the workspace
just lint           # clippy, and the licence lists
just lint-deps      # cargo-deny: licences, sources, bans, advisories (deny.toml)
just fmt            # cargo fmt over the workspace's crates (just fmt-check changes nothing)
just mutants        # cargo-mutants over the workspace; `just mutants annals` for one crate
just wasm           # build annals for wasm32-unknown-unknown
just hooks-install  # install the pre-commit hooks (gates/)
```

annals depends on angzarr-proto, a git dependency whose build runs
`protoc`, so building annals (and every crate that depends on it, and so
`cargo build --workspace`) needs `protoc` on `PATH` and access to
github.com. The crates that do not depend on annals need nothing beyond
Rust. The recipes need [`just`](https://just.systems).
`just mutants` also needs
[`cargo-mutants`](https://mutants.rs) (`cargo install cargo-mutants --locked`);
its config is `.cargo/mutants.toml`.

## Licence

Annals is licensed under the [BSD 3-Clause License](LICENSE) (BSD-3-Clause).

Some of annals' crates depend on `angzarr-proto`, angzarr's wire types,
which is AGPL-3.0-only. In a normal build (dev-dependencies, which only
tests build, aside):

- **Carry angzarr-proto:** `annals`, which depends on it directly, and
  `annals-store-sql`, which depends on `annals`. A binary that links any of these is effectively AGPL-3.0-only:
  their source stays BSD-3-Clause, but the linked whole carries
  angzarr-proto's terms.
- **BSD-3-Clause only:** `annals-diagnostic`, `annals-graph`,
  `annals-yaml`, `annals-catalog-check` and `annals-macros`.
  `annals-catalog-check` and `annals-macros` reach angzarr-proto only
  through dev-dependencies, so their tests link it but
  a binary built on them does not.

`just licence-check`, part of `just lint`, fails when these two lists stop
matching `cargo tree`.
