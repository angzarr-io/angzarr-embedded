# angzarr-embedded

angzarr-embedded is angzarr's **in-app runtime**: angzarr run inside your
application's own process, single-process and in memory or on SQLite, with
no servers to deploy. "Embedded" means embedded in your application, not
microcontroller firmware. For the distributed system (separate services,
brokers, mTLS between them), see angzarr core,
<https://github.com/angzarr-io/angzarr>; both share angzarr's wire contract.

It is a deterministic application engine on CQRS/ES: small Rust
crates, knowing nothing of the applications that use them. All but angzarr-embedded,
angzarr-store-sqlite, angzarr-embedded-macros (and the test-only angzarr-embedded-catalog-check)
have zero third-party dependencies; angzarr-embedded depends on angzarr's wire types
(angzarr-proto, prost, prost-types), which its kernel operates on directly,
and on angzarr's shared store contract (angzarr-ports), and
angzarr-store-sqlite on angzarr-ports, sea-query and sqlx.
Every error is localizable: a stable code and named args (see
`angzarr-embedded-diagnostic`), with an en-US Fluent catalog per crate under
`crates/<crate>/locales/`.

| Crate | What it is |
|---|---|
| [`angzarr-embedded`](crates/angzarr-embedded) | event-sourcing kernel on angzarr's proto types (`angzarr_embedded::next`): decide, emit, apply, replay; angzarr's local runtime |
| [`angzarr-embedded-diagnostic`](crates/angzarr-embedded-diagnostic) | localizable errors: every error has a stable code and named args for a Fluent catalog |
| [`angzarr-embedded-host`](crates/angzarr-embedded-host) | remote domains as child processes (the go-plugin model): launched from a `[[domain]]` config, handshaken, reached by gRPC on a Unix socket, supervised, and handed back as angzarr-router `CommandHandler`s |
| [`angzarr-embedded-graph`](crates/angzarr-embedded-graph) | typed-id graph with tags, queries and Dijkstra search |
| [`angzarr-embedded-yaml`](crates/angzarr-embedded-yaml) | a strict YAML subset, read and written; versioned documents with registered migrations |
| [`angzarr-store-sqlite`](crates/angzarr-store-sqlite) | angzarr-ports on SQLite, the one SQLite adapter angzarr-embedded and angzarr share: a leaf (nothing of angzarr-embedded), sea-query statements on sqlx, with its migrations; held to the ports' conformance suite |
| [`angzarr-embedded-macros`](crates/angzarr-embedded-macros) | markup over hand-written aggregates, process managers, policies and projectors, generating their `angzarr_embedded::next` glue |
| [`angzarr-embedded-catalog-check`](crates/angzarr-embedded-catalog-check) | test-only: every error code has a message, with exactly its args, in every shipped Fluent catalog |

```
just test           # cargo-nextest over the workspace
just lint           # clippy, and the licence lists
just lint-deps      # cargo-deny: licences, sources, bans, advisories (deny.toml)
just fmt            # cargo fmt over the workspace's crates (just fmt-check changes nothing)
just mutants        # cargo-mutants over the workspace; `just mutants angzarr-embedded` for one crate
just wasm           # build angzarr-embedded for wasm32-unknown-unknown
just hooks-install  # install the pre-commit hooks (gates/)
```

angzarr-embedded depends on angzarr-proto, a git dependency whose build runs
`protoc`, so building angzarr-embedded (and every crate that depends on it, and so
`cargo build --workspace`) needs `protoc` on `PATH` and access to
github.com. The crates that do not depend on angzarr-embedded need nothing beyond
Rust. The recipes need [`just`](https://just.systems).
`just mutants` also needs
[`cargo-mutants`](https://mutants.rs) (`cargo install cargo-mutants --locked`);
its config is `.cargo/mutants.toml`.

## Licence

angzarr-embedded is licensed under the [BSD 3-Clause License](LICENSE) (BSD-3-Clause).

Some of angzarr-embedded's crates depend on `angzarr-proto`, angzarr's wire types,
which is AGPL-3.0-only. In a normal build (dev-dependencies, which only
tests build, aside):

- **Carry angzarr-proto:** `angzarr-embedded` and `angzarr-embedded-host`,
  which depend on it directly, and `angzarr-store-sqlite`, which depends on
  angzarr-ports (AGPL-3.0-only too). A binary that links any of these is effectively AGPL-3.0-only:
  their source stays BSD-3-Clause, but the linked whole carries
  angzarr-proto's terms.
- **BSD-3-Clause only:** `angzarr-embedded-diagnostic`, `angzarr-embedded-graph`,
  `angzarr-embedded-yaml`, `angzarr-embedded-catalog-check` and `angzarr-embedded-macros`.
  `angzarr-embedded-catalog-check` and `angzarr-embedded-macros` reach angzarr-proto only
  through dev-dependencies, so their tests link it but
  a binary built on them does not.

`just licence-check`, part of `just lint`, fails when these two lists stop
matching `cargo tree`.
