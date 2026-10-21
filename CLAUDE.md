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

## Few dependencies, no domain

Annals is a deterministic application engine on CQRS/ES. Every crate here
knows nothing about any application that uses it, so anything specific to
one belongs in that application's repository, not here.

Every crate has zero third-party dependencies, except annals,
annals-log, annals-store-sql and annals-catalog-check. A crate may depend
on another crate in this workspace, as annals-sim does on annals.

annals-catalog-check is test-only: its one dev-dependency, `fluent-syntax`
(Apache-2.0 OR MIT), parses the Fluent catalogs for the parity test.
Nothing depends on it and it has no library to speak of.

annals-log depends on `prost` because it is a protobuf format: the wire
encoding needs a protobuf runtime, and hand-rolling the encoding would be a
second, unchecked implementation of a published format.

annals depends on `angzarr-proto` (a git dependency pinned by rev:
angzarr's own prost types for `io.angzarr.v1`, whose build needs `protoc`),
`prost` and `prost-types`, because its event-sourcing kernel (`annals::next`)
operates on angzarr's wire types directly rather than on parallel annals
types: angzarr owns that contract, and annals is its in-process runtime.
Every user of annals therefore builds angzarr-proto; this is accepted.

annals-store-sql depends on `sea-query` and, behind each backend's feature,
`sqlx` and `sea-query-binder`, because it is a SQL store: hand-rolling a
query builder or a driver would be a second, unchecked implementation of each
database's dialect. Its statements are backend-neutral; only rendering,
running, migrations and the dialect differ per backend. It lives here for
now: angzarr core's own SQL storage is older, and this crate moves to angzarr
core when that storage is brought onto it.

Protobuf code lives in annals (angzarr's types) and annals-log (annals' own
log format); every other crate stays dependency-free except as above.

## annals-log's wire types are hand-written; the `.proto` is the schema of record

annals-log's messages are `#[derive(prost::Message)]` structs written by
hand, matching `crates/annals-log/proto/` field for field, so annals-log
builds with plain `cargo` and works as a git dependency with no codegen step.
Change the `.proto` first, then the Rust to match. `buf lint` and `buf
breaking` guard the `.proto`.

Codegen is only a test-time cross-check: `buf generate` writes into
`crates/annals-log/gen/`, which is gitignored and never committed, and
`crates/annals-log/src/codegen.test.rs` (the `codegen-check` feature) proves
the hand-written and generated types are wire-identical. Never edit `gen/`, or
include it from anything but that test.

## Licence: BSD-3-Clause; not published

Annals is licensed under the BSD 3-Clause License (`LICENSE`).
Every crate has `license = "BSD-3-Clause"` and `publish = false`, both inherited
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
- Where a game's error crosses into the engine, the trait demands it:
  `Domain::Rejection: Diagnostic`, so a domain whose rejections are not
  localizable does not compile.

## Unicode in source is visible

The workspace denies three clippy lints, and every crate opts in with
`[lints] workspace = true`: `invisible_characters`, `unicode_not_nfc` and
`non_ascii_literal`. A non-ASCII character in a literal is written as an
escape (`"\u{e9}"`), so what the code holds is plain. The one allow is the
`roundtrip` module of `crates/annals-yaml/src/lib.test.rs`, whose corpus is
readable text by design; a new allow needs a comment saying why.

## Tests live beside the code, in `*.test.rs`

A source file `foo.rs` keeps its tests in `foo.test.rs` in the same
directory, included at the end of `foo.rs`:

```rust
#[cfg(test)]
#[path = "foo.test.rs"]
mod tests;
```

Not an inline `mod tests { ... }`, and no `tests/` directory: the tests stay
next to what they test, and out of the source file, so reading the code does
not put its tests in the reader's (or an LLM's) context.

- Shared fixtures are `*.test.rs` files too, declared
  `#[cfg(test)] #[path = "toys.test.rs"] pub(crate) mod toys;`
  (`annals::next::toys`, `annals_log::toy`, `annals_sim::brawl`).
- A source file with several test files includes each,
  `#[cfg(test)] #[path = "edit_map.test.rs"] mod edit_map_tests;`, or keeps
  one `foo.test.rs` with a module per subject.
- A test reaches the crate as `crate::`, its private items included.
- Library-source scans (the catalog parity's code-literal check) skip
  `*.test.rs`.

## Use `just`

Plain `cargo build` and `cargo test` work from a clean checkout and skip the
codegen check; annals' build also needs `protoc` and github.com
(for angzarr-proto). `just test`, `just lint` and `just build` run `just gen` first
and turn on `annals-log/codegen-check`, so they need `buf` and
`protoc-gen-prost`.
`just breaking` checks the schema against main for wire-breaking changes.
`just wasm` builds annals for `wasm32-unknown-unknown`; it must keep
building there.

Every gate prints one line on a pass, `ok   <gate> (<scope>) <duration>`;
on a failure, a `FAIL` line, the extract worth reading, and the full log in
`target/logs/<gate>.log`. `VERBOSE=1 just <gate>` shows the full output.
`just test` runs cargo-nextest.

## The pre-commit gates are in gates/; the justfile gives their parameters

The gates and their recipes are `gates/gates.just` and
`gates/lefthook.yml`: the justfile imports the one and lefthook.yml extends
the other. The justfile keeps annals' parameters for them
(`mutation_thresholds`, `workspace_wide`, `staged_tree`, `feature_rules`,
`features_prepare`) and its own `lint`; their tool configs are
`deny.toml`, `.gitleaks.toml` and `.cargo/mutants.toml`. `just
hooks-install` installs the hooks, once per clone (every worktree shares
them), and again after a change to the gates names a new hook
(`gates/MIGRATING.md`). They came from the shared gates repository that
taskloom and ctxloom-rs also copied; a change here does not reach them.

lefthook runs the gates one at a time, stopping at the first failure. A
failing gate is a finding: fix the cause and commit again; never
`--no-verify`, and never weaken a gate to get through.

1. **fmt** formats the staged `*.rs` files in the index (`just
   fmt-staged`); it does not refuse. A fully staged file's working copy gets
   the same content; a partially staged file's working copy is left exactly
   as it was, and named. `just fmt-check` is CI's check.
2. **secrets**: gitleaks over the staged changes (`.gitleaks.toml`). A fake
   secret in a test ends its line with `gitleaks:allow`.
3. **clippy**: `just lint`: buf lint, clippy over the workspace with the
   codegen check, and annals-store-sql's library with no backend, which the
   workspace run cannot see (its own dev-dependency turns `sqlite` on).
4. **deps**: `just lint-deps`. A new licence, git source or advisory ignore
   is a change to `deny.toml`, with its reason.
5. **tests**: `just test-staged` tests the crates with staged changes and
   their workspace dependents (normal, dev and build), on a throwaway
   worktree of HEAD plus the staged diff (`target/staged-tree`), so unstaged
   edits cannot pass or fail it.
6. **mutants**: `just mutants-staged` runs `cargo mutants --in-diff` over
   the staged diff in that tree and holds every crate's new code to a 90%
   kill rate (`mutation_thresholds`; a new crate gets a threshold there
   before it gets code). The last run's mutants.out is kept in
   `target/logs/mutants-staged/`. A mutant judged equivalent is excluded by
   an `exclude_re` in `.cargo/mutants.toml` matching that one mutant, with a
   comment saying why: not `#[mutants::skip]`, which needs the `mutants`
   crate as a dependency, and never a lowered threshold.

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
  `remove_key`, `map_seq`). A step that fails for the game's own reason
  returns the game's diagnostic: `MigrationError::game(&e)`, or
  `Report::new(code, english).arg(..).into()`.
- Loading is string in, tree out, and never rewrites a file.
  `Loader::upgrade` gives the upgraded text for a tool that rewrites on
  purpose (the written text has no comments).
- Changing a document's schema means: bump `current`, register the step from
  the old version, and keep a fixture at the old version with its expected
  current-version output (`Loader::assert_fixtures`).
