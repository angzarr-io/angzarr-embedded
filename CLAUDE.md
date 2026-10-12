# Working rules

## Docs and code state the current design only

Version control holds the history; the docs and code do not. When a decision
changes, rewrite the text in place to say what is true now.

- No strikethroughs, "revised", "superseded", "originally", "previously",
  "moved to", "was X, now Y", or dated decision notes.
- No sections kept "for reference" after the design moved on — delete them.
- No code comments about what the code used to do.
- A rejected alternative may stay only as rationale for the current design
  ("not X, because …"), never as a story of how we got here.

## Zero dependencies, no domain

Every crate here knows nothing about any particular game. They are shared by
card-game and tdautobattle, so anything specific to one of them belongs in
that repository, not here.

Every crate has zero third-party dependencies, except eslog. A crate may
depend on another crate in this workspace, as simkit does on es.

eslog depends on `prost` because it is a protobuf format: the wire encoding
needs a protobuf runtime, and hand-rolling the encoding would be a second,
unchecked implementation of a published format. All protobuf code stays in
eslog, so es and everything else stay dependency-free.

## eslog's wire types are hand-written; the `.proto` is the schema of record

eslog's messages are `#[derive(prost::Message)]` structs written by hand,
matching `crates/eslog/proto/` field for field, so eslog builds with plain `cargo`
and works as a git dependency with no codegen step. Change the `.proto`
first, then the Rust to match. `buf lint` and `buf breaking` guard the
`.proto`.

Codegen is only a test-time cross-check: `buf generate` writes into
`crates/eslog/gen/`, which is gitignored and never committed, and
`crates/eslog/tests/codegen.rs` (the `codegen-check` feature) proves the
hand-written and generated types are wire-identical. Never edit or include
`gen/` from library code.

## Not published

Every crate has `publish = false` and `license = "LicenseRef-Proprietary"`,
both inherited from `[workspace.package]`. A new crate sets
`license.workspace = true` and `publish.workspace = true`.

## Use `just`

Plain `cargo build` and `cargo test` work from a clean checkout and skip the
codegen check. `just test`, `just lint` and `just build` run `just gen` first
and turn on `eslog/codegen-check`, so they need `buf` and `protoc-gen-prost`.
`just breaking` checks the schema against main for wire-breaking changes.
