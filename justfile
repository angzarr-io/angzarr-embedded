# Build, test and lint the workspace. Plain cargo needs only protoc here; these
# recipes also run `gen` so the annals-log codegen check (the `codegen-check`
# feature) can compare the hand-written wire types with buf's output.
#
# Needs: buf (~/go/bin/buf) and protoc-gen-prost 0.5.0 on PATH, and protoc
# (angzarr-proto, under annals, runs it in its build script)
#   cargo install protoc-gen-prost --version 0.5.0 --locked
#
# The pre-commit gates and their recipes (fmt, fmt-check, test, test-staged,
# mutants-staged, lint-deps, secrets-scan-staged and the `_run` machinery)
# are the shared gates repository's gates.just, imported through `.gates`;
# this file gives them annals' parameters. `just gates-link` makes the link.

set windows-shell := ["bash", "-cu"]

# Where the shared gates repository is checked out, relative to this
# repository's main checkout (so every worktree of it finds the same one):
# the one place its path is stated.
gates_dir := "../gates"

import? '.gates/gates.just'

# Mutation kill-rate thresholds, percent, by crate path. A mutant is judged
# by the longest prefix its file matches; one matching none fails the gate,
# so a new crate gets a threshold before it gets code. Every crate is
# hand-written library code whose behaviour its tests reach in-process
# (annals-store-sql's SQLite backend included), so every crate is held to the
# same bar. annals-log's generated protobuf (gen/) is not a tier: it is
# reached only from a `#[cfg(test)]` module, which cargo-mutants never
# mutates (.cargo/mutants.toml).
mutation_thresholds := "crates/annals=90 crates/annals-catalog-check=90 crates/annals-diagnostic=90 crates/annals-graph=90 crates/annals-log=90 crates/annals-sim=90 crates/annals-store-sql=90 crates/annals-yaml=90"

# Staged paths outside any crate that test the whole workspace. buf's files
# decide what the codegen check compares against.
workspace_wide := "Cargo.toml Cargo.lock rust-toolchain.toml .cargo/config.toml .cargo/config buf.yaml buf.gen.yaml"

# annals has no path dependency outside its tree, so the staged tree can live
# under target/.
staged_tree := "target/staged-tree"

# The codegen check, whenever annals-log is tested or mutated, after `gen`.
feature_rules := "annals-log=annals-log/codegen-check"
features_prepare := "gen"

# List the recipes.
default:
    @just --list

# Generate annals-log's protobuf types into crates/annals-log/gen (never
# committed), for the codegen check only.
gen:
    @buf generate

# Build everything, tests and the codegen check included.
[no-exit-message]
build: gen
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(-q --message-format=short)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just _run build workspace cargo \
        cargo build "${terse[@]}" --release --workspace --all-targets --features annals-log/codegen-check

# Lint the protobuf schema and the Rust: the pre-commit clippy gate.
[no-exit-message]
lint: gen
    @just _run lint workspace cargo just _lint

# Clippy, warnings denied, over the workspace with the codegen check; then
# annals-store-sql's library alone, with no backend: in the workspace run its
# own dev-dependency turns `sqlite` on, which would hide what the default
# build leaves unused.
[no-exit-message]
_lint:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(-q --message-format=short)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    buf lint
    cargo clippy "${terse[@]}" --workspace --all-targets --features annals-log/codegen-check -- -D warnings
    cargo clippy "${terse[@]}" --package annals-store-sql --lib -- -D warnings

# Check the schema for wire-breaking changes against main.
breaking:
    buf breaking --against '.git#branch=main'

# Build the crates that must run in a browser for wasm32-unknown-unknown.
#   rustup target add wasm32-unknown-unknown
wasm:
    cargo build --release --target wasm32-unknown-unknown --package annals

# Mutation-test with cargo-mutants (config in .cargo/mutants.toml): the whole
# workspace, or only the named crates' code (`just mutants annals`). Each
# mutant runs every crate's tests, annals-log's codegen check included
# whenever annals-log is mutated. Results land in mutants.out/.
#   cargo install cargo-mutants --locked
mutants *crates: gen
    cargo mutants \
        {{ if crates == "" { "--workspace" } else { "--package " + replace(crates, " ", " --package ") } }} \
        {{ if crates == "" { "--features annals-log/codegen-check" } else if crates =~ '(^| )annals-log( |$)' { "--features annals-log/codegen-check" } else { "" } }}

# Mutation-test only the lines changed since `base` (default main): the
# working tree, uncommitted and untracked files included, against the merge
# base, so it matches the source cargo-mutants mutates. cargo-mutants builds
# only the changed crates' packages, so the codegen check is on only when
# annals-log changed (cargo refuses the feature otherwise). `mutants-staged`
# is the commit's gate; this is the branch's.
mutants-diff base="main": gen
    #!/usr/bin/env bash
    set -euo pipefail
    patch=target/mutants-diff.patch
    mkdir -p target
    git diff "$(git merge-base '{{base}}' HEAD)" -- '*.rs' > "$patch"
    git ls-files --others --exclude-standard -z -- '*.rs' \
        | xargs -0 -r -n1 git diff --no-index -- /dev/null >> "$patch" || true
    if [ ! -s "$patch" ]; then
        echo "mutants-diff: no Rust changes since '{{base}}'; nothing to mutate."
        exit 0
    fi
    features=()
    if grep -q '^+++ b/crates/annals-log/' "$patch"; then
        features=(--features annals-log/codegen-check)
    fi
    cargo mutants --workspace "${features[@]}" --in-diff "$patch"

# The bootstrap, which must run without the shared gates, so it uses nothing
# from them.

# Link .gates to the shared gates checkout at `gates_dir`, failing with where
# to clone it when it is not there.
gates-link:
    #!/usr/bin/env bash
    set -euo pipefail
    main=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")
    target=$(realpath -m "$main/{{gates_dir}}")
    if [ ! -f "$target/gates.just" ] || [ ! -f "$target/lefthook.yml" ]; then
        echo "FAIL gates: no shared gates checkout at $target (gates_dir, justfile)." >&2
        echo "Clone the gates repository there: git clone <gates repository> $target" >&2
        exit 1
    fi
    ln -sfn "$target" .gates

# lefthook's first pre-commit command: fail while .gates does not reach the
# shared gates, because lefthook then runs no gate at all. A missing link is
# made, and the commit still refused: the gates were not loaded for it.
[no-exit-message]
gates-check:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f .gates/gates.just ] && [ -f .gates/lefthook.yml ]; then exit 0; fi
    just gates-link
    echo "FAIL gates: .gates was missing, so no gate ran; it is linked now: commit again." >&2
    exit 1

# Wire lefthook's git hooks into the repository's .git/hooks, after linking
# the shared gates. Worktrees share .git/hooks, so lefthook install is once
# per clone; `just gates-link` is once per checkout (`gates-check` does it
# when a commit finds the link missing). Needs lefthook (1.13.6) and
# gitleaks (v8), both by `go install`; cargo-nextest, cargo-deny and
# cargo-mutants by `cargo install <name> --locked`; jq and flock.
hooks-install: gates-link
    lefthook install
