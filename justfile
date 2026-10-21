# Build, test and lint the workspace. Plain cargo needs only protoc here; these
# recipes also run `gen` so the annals-log codegen check (the `codegen-check`
# feature) can compare the hand-written wire types with buf's output.
#
# Needs: buf (~/go/bin/buf) and protoc-gen-prost 0.5.0 on PATH, and protoc
# (angzarr-proto, under annals, runs it in its build script)
#   cargo install protoc-gen-prost --version 0.5.0 --locked
# The gates also need cargo-nextest, cargo-deny, cargo-mutants, gitleaks,
# lefthook, jq and flock (`just hooks-install` says where each comes from).

set windows-shell := ["bash", "-cu"]

root := justfile_directory()

# Mutation kill-rate thresholds, percent, by crate path: the one place they
# are stated. A mutant is judged by the longest prefix its file matches; one
# matching none fails the gate, so a new crate gets a threshold before it gets
# code. Every crate is hand-written library code whose behaviour its tests
# reach in-process (annals-store-sql's SQLite backend included), so every
# crate is held to the same bar. annals-log's generated protobuf (gen/) is
# not a tier: it is reached only from a `#[cfg(test)]` module, which
# cargo-mutants never mutates (.cargo/mutants.toml).
mutation_thresholds := "crates/annals=90 crates/annals-catalog-check=90 crates/annals-diagnostic=90 crates/annals-graph=90 crates/annals-log=90 crates/annals-sim=90 crates/annals-store-sql=90 crates/annals-yaml=90"

# Staged paths outside any crate that can change what every crate builds or
# tests (bash globs, `*` crossing `/`): one of these staged tests the whole
# workspace. buf's files decide what the codegen check compares against.
workspace_wide := "Cargo.toml Cargo.lock rust-toolchain.toml .cargo/config.toml .cargo/config buf.yaml buf.gen.yaml"

# Gate output is terse, because it lands in agents' context windows. Every
# gate runs through `_run`: a pass prints one line,
#     ok   <gate> (<scope>) <duration>
# and a failure prints
#     FAIL <gate>: <summary> (full log: target/logs/<gate>.log)
# then only the extract that matters, taken from the tool's own terse or
# structured output (nextest's failure output, cargo's short messages,
# rustfmt's file list, mutants.out). The full output is always in the log, and
# the exit code is the tool's. VERBOSE=1 runs each tool with its full output,
# shown as well as logged.

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
    exec just --justfile '{{root}}/justfile' _run build workspace cargo \
        cargo build "${terse[@]}" --manifest-path '{{root}}/Cargo.toml' --release --workspace --all-targets \
        --features annals-log/codegen-check

# Run every test, the codegen check included, by cargo-nextest (`_tests`). A
# missing nextest (`cargo install cargo-nextest --locked`) fails rather than
# skipping.
[no-exit-message]
test: (_require-nextest "test")
    @just --justfile '{{root}}/justfile' _run test workspace tests just --justfile '{{root}}/justfile' _tests --workspace

# Lint the protobuf schema and the Rust (clippy, warnings denied).
[no-exit-message]
lint: gen
    @just --justfile '{{root}}/justfile' _run lint workspace cargo just --justfile '{{root}}/justfile' _lint

[no-exit-message]
_lint:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(-q --message-format=short)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    cd '{{root}}'
    buf lint
    cargo clippy "${terse[@]}" --workspace --all-targets --features annals-log/codegen-check -- -D warnings

# Check the schema for wire-breaking changes against main.
breaking:
    buf breaking --against '.git#branch=main'

# Build the crates that must run in a browser for wasm32-unknown-unknown.
#   rustup target add wasm32-unknown-unknown
wasm:
    cargo build --release --target wasm32-unknown-unknown --package annals

# Format the Rust: this workspace's crates only (`_members`), never a path
# dependency outside it, which `cargo fmt --all` would also format.
fmt:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t packages < <(just --justfile '{{root}}/justfile' _members)
    cargo fmt --manifest-path '{{root}}/Cargo.toml' "${packages[@]}"

# Check the Rust is formatted, changing nothing: CI's check, scoped like
# `fmt`. Lists the files rustfmt would change (`-l`); VERBOSE=1 shows the
# diffs.
[no-exit-message]
fmt-check:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t packages < <(just --justfile '{{root}}/justfile' _members)
    terse=(-l)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile '{{root}}/justfile' _run fmt-check workspace fmt \
        cargo fmt --manifest-path '{{root}}/Cargo.toml' "${packages[@]}" -- --check "${terse[@]}"

# This workspace's packages, as `--package <name>` arguments, one per line.
_members:
    @cargo metadata --format-version 1 --no-deps --offline --manifest-path '{{root}}/Cargo.toml' \
        | jq -r '.packages[] | "--package\n\(.name)"'

# Format the named Rust files in place: lefthook's pre-commit step, which
# passes the staged `*.rs` files and re-stages what this changes
# (`stage_fixed`). Each file is formatted alone, read from stdin, so rustfmt
# does not follow its `mod` declarations into files that were not staged;
# the edition is that of the package owning the file.
[positional-arguments]
[no-exit-message]
fmt-staged +files:
    @just --justfile '{{root}}/justfile' _run fmt-staged "$# staged files" fmt-staged \
        just --justfile '{{root}}/justfile' _fmt-files "$@"

[positional-arguments]
[no-exit-message]
_fmt-files +files:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{root}}'
    declare -A edition_of=()
    while IFS=$'\t' read -r dir edition; do edition_of[$dir]=$edition; done < <(cargo metadata --format-version 1 \
        --no-deps --offline --manifest-path "$root/Cargo.toml" | jq -r --arg root "$root/" \
        '.packages[] | "\(.manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml"))\t\(.edition)"')
    scratch=$(mktemp)
    trap 'rm -f "$scratch"' EXIT
    rc=0
    for file in "$@"; do
        owner=
        for dir in "${!edition_of[@]}"; do
            if [[ $file == "$dir"/* && ${#dir} -gt ${#owner} ]]; then owner=$dir; fi
        done
        if [ -z "$owner" ]; then
            echo "$file: no package of this workspace owns it, so its edition is unknown" >&2
            rc=1
            continue
        fi
        if ! (cd "$root" && rustfmt --edition "${edition_of[$owner]}" --emit stdout < "$file" > "$scratch"); then
            echo "$file: rustfmt failed (above)" >&2
            rc=1
            continue
        fi
        if ! cmp -s "$scratch" "$root/$file"; then
            cat "$scratch" > "$root/$file"
            echo "reformatted $file"
        fi
    done
    exit "$rc"

# Dependency policy: licenses, sources, bans and RustSec advisories (deny.toml).
# Fetches the advisory database, so it needs the network. The inclusion graphs
# are hidden (VERBOSE=1 shows them). bans' duplicate-version warnings do not
# fail the gate; they stay in the log.
[no-exit-message]
lint-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(--hide-inclusion-graph)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile '{{root}}/justfile' _run lint-deps workspace deny \
        cargo deny --color never --manifest-path '{{root}}/Cargo.toml' check "${terse[@]}"

# Scan the STAGED changes for secrets: lefthook's pre-commit step. Rules and
# allowlists are .gitleaks.toml; output is redacted. A missing binary fails
# rather than skipping: a scan that passes by not running looked at nothing.
[no-exit-message]
secrets-scan-staged:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v gitleaks >/dev/null 2>&1; then
        echo "FAIL secrets-scan-staged: gitleaks is not on PATH; the secret scan cannot pass by not running." >&2
        exit 1
    fi
    exec just --justfile '{{root}}/justfile' _run secrets-scan-staged staged secrets \
        gitleaks git --pre-commit --staged --redact --verbose --no-banner --no-color \
        --config '{{root}}/.gitleaks.toml' '{{root}}'

# Test what the commit changes: lefthook's pre-commit step. Runs the tests of
# every crate with a staged change and of the workspace crates depending on
# them (a change can break a dependent's tests; annals-catalog-check depends
# on nearly every crate); a staged workspace-wide file (`workspace_wide`)
# tests the whole workspace. It judges the STAGED tree, built by
# `_staged-tree`, so unstaged edits cannot pass or fail it.
[no-exit-message]
test-staged: (_require-nextest "test-staged")
    #!/usr/bin/env bash
    set -euo pipefail
    read -ra wide <<< "{{workspace_wide}}"
    while IFS= read -r -d '' path; do
        [[ $path == crates/* ]] && exec just --justfile '{{root}}/justfile' _staged-tree _test-affected
        for pattern in "${wide[@]}"; do
            [[ $path == $pattern ]] && exec just --justfile '{{root}}/justfile' _staged-tree _test-affected
        done
    done < <(git -C '{{root}}' diff --cached --name-only --no-renames -z HEAD)
    echo "ok   test-staged (nothing to test: no staged change is in a crate or a workspace-wide file)"

# Inside the staged tree: map the staged paths to crates through cargo
# metadata, close over workspace dependents, and test them.
[no-exit-message]
_test-affected:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{root}}'
    read -ra wide <<< "{{workspace_wide}}"
    meta=$(cargo metadata --format-version 1 --no-deps --offline --manifest-path "$root/Cargo.toml")
    declare -A dir_of=() selected=()
    while IFS=$'\t' read -r name dir; do dir_of[$name]=$dir; done < <(jq -r --arg root "$root/" \
        '.packages[] | "\(.name)\t\(.manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml"))"' <<< "$meta")
    whole=
    while IFS= read -r -d '' path; do
        for pattern in "${wide[@]}"; do [[ $path == $pattern ]] && whole="$path"; done
        owner=
        for name in "${!dir_of[@]}"; do
            [[ $path == "${dir_of[$name]}"/* ]] && owner=$name
        done
        if [ -n "$owner" ]; then
            selected[$owner]=1
        elif [[ $path == crates/* && ( $path == *.rs || $path == */Cargo.toml ) ]]; then
            # Crate source no current crate owns: a crate moved or removed.
            whole="$path"
        fi
    done < <(git diff --cached --name-only --no-renames -z HEAD)
    if [ -n "$whole" ]; then
        exec just --justfile "$root/justfile" _run test-staged "workspace: $whole is staged" tests \
            just --justfile "$root/justfile" _tests --workspace
    fi
    if [ "${#selected[@]}" -eq 0 ]; then
        echo "ok   test-staged (nothing to test: the staged change is in no crate's sources or manifest)"
        exit 0
    fi
    changed=$(printf '%s ' "${!selected[@]}")
    # Dependents, transitively: every workspace crate depending (normal, dev or
    # build) on a selected one is selected too, until nothing changes.
    mapfile -t edges < <(jq -r '[.packages[].name] as $ws | .packages[] as $p
        | $p.dependencies[] | select(.path != null and (.name as $n | any($ws[]; . == $n)))
        | "\(.name)\t\($p.name)"' <<< "$meta")
    grew=1
    while [ -n "$grew" ]; do
        grew=
        for edge in "${edges[@]}"; do
            IFS=$'\t' read -r dep dependent <<< "$edge"
            if [ -n "${selected[$dep]:-}" ] && [ -z "${selected[$dependent]:-}" ]; then
                selected[$dependent]=1
                grew=1
            fi
        done
    done
    mapfile -t packages < <(printf '%s\n' "${!selected[@]}" | sort)
    exec just --justfile "$root/justfile" _run test-staged "changed: ${changed% }; with dependents: ${packages[*]}" tests \
        just --justfile "$root/justfile" _tests "${packages[@]}"

# The test run of `test` and `test-staged`: cargo-nextest over the packages
# named (or --workspace), each test in its own process, without stopping at
# the first failure; then the doc tests of those with a library, which nextest
# cannot run, once the rest pass. Whenever annals-log is tested, `gen` runs
# first and the codegen check is on (cargo refuses the feature when
# annals-log is not selected). The tests are built first by cargo, whose
# short messages nextest has no form of. Exits 101 if they did not build, else
# with nextest's code if it failed (100: a test failed), else the doc tests'.
[positional-arguments]
[no-exit-message]
_tests +packages:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{root}}'
    terse=(--status-level fail --final-status-level fail --success-output never --failure-output final
        --hide-progress-bar --cargo-quiet)
    cargo_terse=(-q --message-format=short)
    if [ "${VERBOSE:-}" = 1 ]; then terse=(); cargo_terse=(); fi
    features=()
    if [ "$1" = --workspace ]; then
        selection=(--workspace)
        doc=(--workspace)
        features=(--features annals-log/codegen-check)
    else
        selection=() doc=()
        for name in "$@"; do
            selection+=(--package "$name")
            [ "$name" = annals-log ] && features=(--features annals-log/codegen-check)
        done
        # `cargo test --doc` refuses a package without a library.
        while IFS= read -r name; do doc+=(--package "$name"); done < <(cargo metadata --format-version 1 --no-deps \
            --offline --manifest-path "$root/Cargo.toml" | jq -r --args '.packages[] | select(.name as $n | $ARGS.positional
            | index($n)) | select(any(.targets[]; .kind | index("lib"))) | .name' "$@")
    fi
    if [ "${#features[@]}" -gt 0 ]; then
        (cd "$root" && buf generate)
    fi
    cargo test --manifest-path "$root/Cargo.toml" --no-run "${cargo_terse[@]}" "${selection[@]}" "${features[@]}"
    rc=0
    cargo nextest run --manifest-path "$root/Cargo.toml" --no-fail-fast --no-tests=pass "${terse[@]}" \
        "${selection[@]}" "${features[@]}" || rc=$?
    # After a failed test, the doc tests would only bury it.
    if [ "$rc" -eq 0 ] && [ "${#doc[@]}" -gt 0 ]; then
        cargo test --manifest-path "$root/Cargo.toml" --doc "${cargo_terse[@]}" "${doc[@]}" "${features[@]}" || rc=$?
    fi
    exit "$rc"

# Fail, never skip, when cargo-nextest is missing: tests that pass by not
# running tested nothing.
[no-exit-message]
_require-nextest gate:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! cargo nextest --version >/dev/null 2>&1; then
        echo "FAIL {{gate}}: cargo-nextest is not installed; the tests cannot pass by not running." >&2
        echo "Install it: cargo install cargo-nextest --locked" >&2
        exit 1
    fi

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

# Mutation-test what the commit changes: lefthook's pre-commit step, last
# because it is the slowest. `cargo mutants --in-diff` over the staged diff,
# in the STAGED tree (`_staged-tree`), judged by `mutation_thresholds`; the
# commit is refused with the per-crate verdict and the surviving mutants
# listed. cargo-mutants' own output is the log, and its mutants.out is kept
# in target/logs/mutants-staged/. A missing cargo-mutants (`cargo install
# cargo-mutants --locked`) fails rather than skipping: a mutation run that
# passes by not running tested nothing.
[no-exit-message]
mutants-staged:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! cargo mutants --version >/dev/null 2>&1; then
        echo "FAIL mutants-staged: cargo-mutants is not installed; the mutation gate cannot pass by not running." >&2
        echo "Install it: cargo install cargo-mutants --locked" >&2
        exit 1
    fi
    if [ -z "$(git -C '{{root}}' diff --cached --name-only HEAD -- 'crates/*.rs' ':(exclude)*.test.rs')" ]; then
        echo "ok   mutants-staged (no mutants: the staged change touches no Rust source under crates/)"
        exit 0
    fi
    exec just --justfile '{{root}}/justfile' _staged-tree _mutants-affected

# Inside the staged tree: `_mutants-judge`, run as the gate.
[no-exit-message]
_mutants-affected:
    @just --justfile '{{root}}/justfile' _run mutants-staged "staged diff" mutants just --justfile '{{root}}/justfile' _mutants-judge

# Mutate the staged diff in place (the tree is throwaway) and judge each
# crate's kill rate. As `mutants-diff`, the codegen check is on (after `gen`)
# only when the diff touches annals-log. Killed = caught or timed out (a
# mutant the tests could not finish under changed behaviour); unviable
# mutants (did not build) count neither way. A failing verdict goes to
# verdict.txt beside mutants.out: its summary line, then the kill rate by
# crate, for `_extract-mutants`. Mutants run under `cargo test`, not nextest:
# a process per test only slows a run that rebuilds per mutant.
[no-exit-message]
_mutants-judge:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{root}}'
    out=${GATE_LOG_DIR:-$root/target/logs}/mutants-staged
    rm -rf "$out"
    mkdir -p "$out"
    verdict() { printf '%s\n' "$@" | tee -a "$out/verdict.txt"; }
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
    git diff --cached --no-color --no-ext-diff --no-renames --src-prefix=a/ --dst-prefix=b/ HEAD -- '*.rs' > "$work/staged.patch"
    features=()
    if grep -q '^+++ b/crates/annals-log/' "$work/staged.patch"; then
        (cd "$root" && buf generate)
        features=(--features annals-log/codegen-check)
    fi
    rc=0
    cargo mutants --dir "$root" --in-place --workspace "${features[@]}" --in-diff "$work/staged.patch" \
        --output "$out" --colors never || rc=$?
    case "$rc" in
        0|2|3) ;;
        4)
            verdict "the staged tree's tests fail unmutated (cargo-mutants baseline); fix them first" \
                "baseline log: target/logs/mutants-staged/mutants.out/log/baseline.log"
            exit 1 ;;
        *)
            verdict "cargo mutants failed before a verdict (exit $rc)"
            exit 1 ;;
    esac
    outcomes="$out/mutants.out/outcomes.json"
    judged='[.outcomes[] | select(.scenario | type == "object") | {name: .scenario.Mutant.name, file: .scenario.Mutant.file, summary}]'
    if [ ! -f "$outcomes" ] || [ "$(jq "$judged | length" "$outcomes")" -eq 0 ]; then
        echo "mutants-staged: no mutants: the staged diff changes no mutable code."
        exit 0
    fi
    read -ra tiers <<< "{{mutation_thresholds}}"
    # Each mutant's tier: the longest threshold prefix its file is under.
    tiered=$(jq --arg tiers "${tiers[*]}" "$judged"' | ($tiers | split(" ") | map(split("=") | {prefix: .[0], min: (.[1] | tonumber)})) as $t
        | map(. as $m | . + {tier: ([$t[] | . as $tier | select($m.file | startswith($tier.prefix + "/"))] | max_by(.prefix | length))})' "$outcomes")
    untiered=$(jq -r '.[] | select(.tier == null) | .file' <<< "$tiered" | sort -u)
    if [ -n "$untiered" ]; then
        verdict "no threshold in mutation_thresholds (justfile) for: $(paste -sd ' ' <<< "$untiered")"
        exit 1
    fi
    report=$(jq -r 'group_by(.tier.prefix)[] | (map(select(.summary == "CaughtMutant" or .summary == "Timeout")) | length) as $killed
        | (map(select(.summary == "MissedMutant")) | length) as $missed | ($killed + $missed) as $viable
        | .[0].tier as $tier | (if $viable == 0 then 100 else ($killed * 100 / $viable | floor) end) as $rate
        | "\(if $killed * 100 >= $tier.min * $viable then "pass" else "FAIL" end)  \($tier.prefix): \($killed)/\($viable) killed (\($rate)%), threshold \($tier.min)%"
            + (map(select(.summary == "Unviable")) | length | if . > 0 then "; \(.) unviable, not counted" else "" end)
            + (map(select(.summary == "Timeout")) | length | if . > 0 then "; \(.) timed out, counted killed" else "" end)' <<< "$tiered")
    echo "mutants-staged: kill rate by crate:"
    sed 's/^/  /' <<< "$report"
    if grep -q '^FAIL' <<< "$report"; then
        verdict "below the kill-rate threshold: $(grep '^FAIL' <<< "$report" | sed 's/^FAIL  //; s/;.*//' | paste -sd ';' | sed 's/;/; /g')" \
            "kill rate by crate:" "$(sed 's/^/  /' <<< "$report")"
        exit 1
    fi

# Run a recipe of the STAGED tree: HEAD checked out into a throwaway git
# worktree with `git diff --cached` applied on top, so the recipe sees what the
# commit will contain and nothing else (no unstaged edit, no untracked file),
# and nothing here writes the working tree, the index or the stash.
#
# The worktree is target/staged-tree, inside this checkout: annals has no
# path dependency outside its own tree, so it builds from anywhere. Its name
# is fixed and the run holds a lock, so builds reuse one target dir
# (target/staged) across commits. Gate logs go to this checkout's
# target/logs (GATE_LOG_DIR), not the throwaway tree's.
[positional-arguments]
[no-exit-message]
_staged-tree +recipe:
    #!/usr/bin/env bash
    set -euo pipefail
    top='{{root}}'
    # git exports GIT_INDEX_FILE (and, under some commits, GIT_DIR and friends)
    # into a hook. Reading the staged change needs it: under `git commit -a` it
    # is git's temporary index, holding exactly what will be committed. Every
    # git touching the throwaway worktree must not inherit it, or it writes the
    # real index. git names its own repository-locating variables.
    scrub=(env)
    while IFS= read -r var; do scrub+=(-u "$var"); done < <(git rev-parse --local-env-vars)
    mkdir -p "$top/target"
    exec 9> "$top/target/.staged-tree.lock"
    flock 9
    tree="$top/target/staged-tree"
    scratch=$(mktemp -d)
    remove_tree() {
        if [ -e "$tree" ]; then
            "${scrub[@]}" git -C "$top" worktree remove --force "$tree" >/dev/null 2>&1 || rm -rf "$tree"
        fi
        "${scrub[@]}" git -C "$top" worktree prune
    }
    trap 'remove_tree; rm -rf "$scratch"' EXIT
    remove_tree
    git diff --cached --binary --full-index --no-color --no-ext-diff --no-textconv \
        --no-renames --src-prefix=a/ --dst-prefix=b/ HEAD > "$scratch/staged.patch"
    # --no-checkout, then read-tree: a checkout would run post-checkout hooks
    # for a tree nobody works in.
    "${scrub[@]}" git -C "$top" worktree add --quiet --detach --no-checkout "$tree" HEAD
    "${scrub[@]}" git -C "$tree" read-tree -u --reset HEAD
    # --index, so the recipe's `git diff --cached HEAD` is the staged change.
    if [ -s "$scratch/staged.patch" ]; then
        "${scrub[@]}" git -C "$tree" apply --index --binary --whitespace=nowarn "$scratch/staged.patch"
    fi
    cd "$tree"
    "${scrub[@]}" "CARGO_TARGET_DIR=$top/target/staged" "GATE_LOG_DIR=$top/target/logs" just --justfile "$tree/justfile" "$@"

# Run one gate: `command` with all its output to <GATE_LOG_DIR>/<gate>.log
# (default target/logs), timed. A pass prints `ok   <gate> (<scope>)
# <duration>`; a failure prints `FAIL <gate>: <summary> (full log: ...)` and
# the extract, both from `_extract-<extractor> <log> <exit code>`, whose first
# line is the summary. Exits with the command's code. VERBOSE=1 shows the
# output as it is logged, and the summary line after it.
[positional-arguments]
[no-exit-message]
_run gate scope extractor +command:
    #!/usr/bin/env bash
    set -euo pipefail
    gate=$1 scope=$2 extractor=$3
    shift 3
    logs=${GATE_LOG_DIR:-{{root}}/target/logs}
    mkdir -p "$logs"
    log="$logs/$gate.log"
    start=$(date +%s%N)
    rc=0
    if [ "${VERBOSE:-}" = 1 ]; then
        set +e
        "$@" 2>&1 | tee "$log"
        rc=${PIPESTATUS[0]}
        set -e
    else
        "$@" > "$log" 2>&1 || rc=$?
    fi
    ms=$((($(date +%s%N) - start) / 1000000))
    if [ "$rc" -eq 0 ]; then
        printf 'ok   %s (%s) %d.%ds\n' "$gate" "$scope" $((ms / 1000)) $((ms % 1000 / 100))
        exit 0
    fi
    extract=$(just --justfile '{{root}}/justfile' _extract-"$extractor" "$log" "$rc" 2>&1) || true
    printf 'FAIL %s: %s (full log: target/logs/%s.log)\n' "$gate" "${extract%%$'\n'*}" "$gate" >&2
    if [ "${VERBOSE:-}" != 1 ] && [[ $extract == *$'\n'* ]]; then
        printf '%s\n' "${extract#*$'\n'}" >&2
    fi
    exit "$rc"

# The extractors: given a gate's log and exit code, print a one-line summary,
# then the part of the log worth reading. Each gate runs its tool in a terse
# or structured mode, so an extract is that output, not a search of prose.

# cargo build/clippy with --message-format=short (a line per diagnostic), and
# buf lint (a line per finding).
[positional-arguments]
_extract-cargo log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    errors=$(grep -cE '^[^ ]+:[0-9]+:[0-9]+: error' "$1" || true)
    case "$errors" in
        0) echo "failed (exit $2)" ;;
        1) echo "1 error" ;;
        *) echo "$errors errors" ;;
    esac
    cat "$1"

# rustfmt -l: a line per file it would change.
[positional-arguments]
_extract-fmt log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "files rustfmt would change: $(grep -c . "$1" || true); run just fmt"
    sed 's|^{{root}}/||' "$1"

# fmt-staged: the files rustfmt could not format, and why.
[positional-arguments]
_extract-fmt-staged log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "rustfmt could not format a staged file"
    cat "$1"

# nextest's terse run: the failing tests' output, then its summary.
[positional-arguments]
_extract-tests log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    case "$2" in
        100) echo "tests failed: $(grep -m1 -E '^ *Summary \[' "$1" | sed 's/^.*\] *//')" ;;
        101) echo "the tests did not build ($(grep -cE '^[^ ]+:[0-9]+:[0-9]+: error' "$1" || true) errors), or a doc test failed" ;;
        *) echo "the test run failed (exit $2)" ;;
    esac
    # The tests built, so the build's warnings are not why they failed; they
    # stay in the log.
    if [ "$2" = 100 ]; then
        grep -vE '^[^ ]+:[0-9]+:[0-9]+: warning' "$1" || true
    else
        cat "$1"
    fi

# cargo-deny: its own rendering at error level, without the inclusion graphs,
# offline, from the database the run just fetched; its last line is deny's
# summary.
[positional-arguments]
_extract-deny log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    errors=$(cargo deny --offline --color never --log-level error --manifest-path '{{root}}/Cargo.toml' \
        check --hide-inclusion-graph 2>&1 || true)
    grep . <<< "$errors" | tail -1
    printf '%s\n' "$errors"

# gitleaks --verbose --redact: the redacted findings.
[positional-arguments]
_extract-secrets log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "gitleaks found a secret in the staged changes, or could not scan (redacted below)"
    cat "$1"
    echo "A real credential: unstage it and rotate it. A test fixture: end its line with 'gitleaks:allow'."

# The mutants verdict (`_mutants-judge`), then the survivors and timeouts from
# mutants.out's missed.txt and timeout.txt.
[positional-arguments]
_extract-mutants log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    dir=${1%.log}
    if [ -s "$dir/verdict.txt" ]; then cat "$dir/verdict.txt"; else echo "cargo mutants failed (exit $2)"; fi
    missed=$dir/mutants.out/missed.txt timeout=$dir/mutants.out/timeout.txt
    if [ -s "$missed" ]; then
        echo "surviving mutants (no test failed with the change applied):"
        sed 's/^/  /' "$missed"
    fi
    if [ -s "$timeout" ]; then
        echo "timed out (counted killed):"
        sed 's/^/  /' "$timeout"
    fi
    if [ -s "$missed" ]; then
        echo "Add or sharpen the test that would kill each survivor, stage it, and commit again."
    fi

# Wire lefthook's git hooks into the repository's .git/hooks. Worktrees share
# that directory, so once per clone covers every checkout of it. Needs
# lefthook (1.13.6) and gitleaks (v8), both by `go install`; cargo-nextest,
# cargo-deny and cargo-mutants by `cargo install <name> --locked`; jq and
# flock.
hooks-install:
    lefthook install
