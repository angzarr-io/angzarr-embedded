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

Annals is an event-sourced, deterministic game engine. Every crate here knows
nothing about any particular game. They are shared by card-game and
tdautobattle, so anything specific to one of them belongs in that
repository, not here.

Every crate has zero third-party dependencies, except annals-log. A crate may
depend on another crate in this workspace, as annals-sim does on annals.

annals-log depends on `prost` because it is a protobuf format: the wire
encoding needs a protobuf runtime, and hand-rolling the encoding would be a
second, unchecked implementation of a published format. All protobuf code
stays in annals-log, so annals and everything else stay dependency-free.

## annals-log's wire types are hand-written; the `.proto` is the schema of record

annals-log's messages are `#[derive(prost::Message)]` structs written by
hand, matching `crates/annals-log/proto/` field for field, so annals-log
builds with plain `cargo` and works as a git dependency with no codegen step.
Change the `.proto` first, then the Rust to match. `buf lint` and `buf
breaking` guard the `.proto`.

Codegen is only a test-time cross-check: `buf generate` writes into
`crates/annals-log/gen/`, which is gitignored and never committed, and
`crates/annals-log/tests/codegen.rs` (the `codegen-check` feature) proves the
hand-written and generated types are wire-identical. Never edit or include
`gen/` from library code.

## Licence: EUPL-1.2; not published

Annals is licensed under the EUPL-1.2 (`LICENSE`, the official English text).
Every crate has `license = "EUPL-1.2"` and `publish = false`, both inherited
from `[workspace.package]`. A new crate sets `license.workspace = true` and
`publish.workspace = true`.

## Errors are localizable: a code, args, and an en-US message

Every public error type implements `annals_diagnostic::Diagnostic` and keeps
an English `Display` as the fallback. The game client renders the localized
text from Fluent catalogs; Annals renders nothing. The trait lives in its own
crate, annals-diagnostic, not in annals, so annals-yaml and annals-graph get
it without depending on the event-sourcing kernel.

- **Codes are stable and never reused.** Kebab-case, starting with the
  crate's name (`annals-yaml-too-new`), and a valid Fluent message id. A code
  whose meaning changes gets a new code; a retired code is not given to
  anything else.
- **Args carry data, never pre-formatted English.** Numbers, ids, paths and
  the text that was read are args; any phrase a player reads belongs in the
  catalog. A wrapped cause is `Arg::Nested`. An error does not hold a
  `String` of prose: a new failure is a new variant with data fields.
- **A new variant needs a code and an en-US message** in
  `crates/<crate>/locales/en-US/<crate>.ftl`, using exactly the variant's
  args, with a Fluent selector wherever a count is shown.
- A game's error that an Annals error carries (a migration step's, a content
  decoder's) is held as an `annals_diagnostic::Report`, keeping the game's
  own code.

## Use `just`

Plain `cargo build` and `cargo test` work from a clean checkout and skip the
codegen check. `just test`, `just lint` and `just build` run `just gen` first
and turn on `annals-log/codegen-check`, so they need `buf` and
`protoc-gen-prost`.
`just breaking` checks the schema against main for wire-breaking changes.

## Versioned documents: bump, register a step, keep an old fixture

`annals_yaml::versioned` loads content stamped with a root `version: N` (a
non-negative integer; a mapping with no `version` is version 0). It
is a module of annals-yaml rather than its own crate because it works only on
`annals_yaml::Value` and every user of it already depends on annals-yaml.

- `Migrations::new(kind, current).step(from, fn).build()` checks the chain
  (no gaps, no duplicates, no step at or past `current`) and returns a
  `ChainError` rather than panicking, so a test can name what is wrong.
- A step edits version `from` into `from + 1`; the loader stamps the new
  version. Use the helpers (`rename_key`, `move_key`, `set_default`,
  `remove_key`, `map_seq`).
- Loading is string in, tree out, and never rewrites a file.
  `Loader::upgrade` gives the upgraded text for a tool that rewrites on
  purpose (the written text has no comments).
- Changing a document's schema means: bump `current`, register the step from
  the old version, and keep a fixture at the old version with its expected
  current-version output (`Loader::assert_fixtures`).
