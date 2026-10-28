# Build, test and lint the workspace. Plain cargo needs only protoc here; these
# recipes also run `gen` so the annals-log codegen check (the `codegen-check`
# feature) can compare the hand-written wire types with buf's output.
#
# Needs: buf (~/go/bin/buf) and protoc-gen-prost 0.5.0 on PATH, and protoc
# (angzarr-proto, under annals, runs it in its build script)
#   cargo install protoc-gen-prost --version 0.5.0 --locked
#
# The pre-commit gates and their recipes (fmt, fmt-check, test, test-staged,
# test-commit, mutants-staged, lint-deps, secrets-scan-staged and the `_run`
# machinery) are gates/gates.just, imported below; this file gives them annals'
# parameters.

set windows-shell := ["bash", "-cu"]

import 'gates/gates.just'

# Mutation kill-rate thresholds, percent, by crate path. A mutant is judged
# by the longest prefix its file matches; one matching none fails the gate,
# so a new crate gets a threshold before it gets code. Every crate is
# hand-written library code whose behaviour its tests reach in-process
# (annals-store-sql's SQLite backend included), so every crate is held to the
# same bar. annals-log's generated protobuf (gen/) is not a tier: it is
# reached only from a test file, and .cargo/mutants.toml mutates no test
# file.
mutation_thresholds := "crates/annals=90 crates/annals-catalog-check=90 crates/annals-diagnostic=90 crates/annals-graph=90 crates/annals-log=90 crates/annals-macros=90 crates/annals-sim=90 crates/annals-store-sql=90 crates/annals-yaml=90"

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

# Lint the protobuf schema and the Rust, and check the licence lists: the
# pre-commit clippy gate.
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
    just licence-check

# Hold the README's *Licence* lists and deny.toml's comment to `cargo tree`:
# the crates that carry angzarr-proto (AGPL-3.0-only) in a normal build, with
# every feature on, and the rest of the workspace as BSD-3-Clause only.
[no-exit-message]
licence-check:
    #!/usr/bin/env bash
    set -euo pipefail
    names() { grep -o '`annals[a-z-]*`' | tr -d '`' | sort -u; }
    # A README bullet: from the line starting it to the next bullet or blank.
    bullet() { awk -v h="$1" 'index($0, h) == 1 { f = 1; print; next } f && /^(- |$)/ { exit } f' README.md; }
    actual=$(cargo tree -q --workspace --all-features -e normal,build -i angzarr-proto --prefix none --format '{p}' \
        | awk '{ print $1 }' | grep -v '^angzarr-proto$' | sort -u)
    members=$(cargo metadata -q --no-deps --format-version 1 | jq -r '.packages[].name' | sort -u)
    bsd=$(comm -23 <(echo "$members") <(echo "$actual"))
    fail=0
    check() {
        if [ "$2" != "$3" ]; then
            echo "FAIL licence-check: $1 lists" $2 "but cargo tree says" $3 >&2
            fail=1
        fi
    }
    check "README's carry-angzarr-proto bullet" "$(bullet '- **Carry angzarr-proto:**' | names)" "$actual"
    check "README's BSD-3-Clause-only bullet" "$(bullet '- **BSD-3-Clause only:**' | names)" "$bsd"
    check "deny.toml's comment" "$(grep -A1 'In normal builds those crates are:' deny.toml | tail -n1 | names)" "$actual"
    [ "$fail" = 0 ] && echo "licence-check: angzarr-proto in" $actual"; BSD-3-Clause only:" $bsd
    exit "$fail"

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

# Wire lefthook's git hooks into the repository's .git/hooks. Worktrees share
# .git/hooks, so this is once per clone, and again whenever lefthook.yml or
# gates/lefthook.yml names a hook not installed yet. Needs lefthook (1.13.6)
# and gitleaks (v8), both by `go install`; cargo-nextest, cargo-deny and
# cargo-mutants by `cargo install <name> --locked`; jq, flock, setsid, ps
# and pkill.
hooks-install:
    lefthook install
