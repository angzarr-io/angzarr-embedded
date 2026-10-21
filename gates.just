# Pre-commit gates for a Rust cargo workspace, shared by every repository
# that uses them. Each gate prints one line on a pass and, on a failure, the
# extract worth reading plus its full log in target/logs/<gate>.log.
#
# A consumer links its checkout to this repository's (`.gates`, gitignored,
# made by its `gates-link` recipe from its one `gates_dir` variable) and
# imports this file, optionally, so its bootstrap recipes still run without
# the link:
#
#     import? '.gates/gates.just'
#
# and its lefthook.yml extends lefthook.yml here. The consumer defines these
# variables, which parameterize every recipe below (none is optional; "" means
# none):
#
#   mutation_thresholds  "<path>=<percent> ..." kill-rate thresholds by path
#                        prefix; a mutant is judged by the longest prefix its
#                        file is under, and one under none fails the gate.
#   workspace_wide       bash globs (`*` crossing `/`) of staged paths outside
#                        any crate that test the whole workspace.
#   staged_tree          where `_staged-tree` puts its throwaway worktree,
#                        relative to the checkout; `{name}` is replaced by the
#                        checkout directory's name. Inside target/ unless a
#                        relative path dependency needs the same depth.
#   feature_rules        "<package>=<feature> ...": a feature turned on whenever
#                        that package is tested, or its sources are mutated.
#   features_prepare     a recipe of the consumer, run first whenever a
#                        feature rule turns a feature on.
#
# and these recipes, which the hooks call: `lint` (clippy, through `_run`),
# and `gates-check`, a command of the consumer's own lefthook.yml, which fails
# when the link is missing, saying where to clone this repository. Those and
# `gates-link` must not use anything from this file: they run without it.
#
# Needs bash, git, jq, flock, setsid, ps and pkill (util-linux, procps),
# cargo-nextest, cargo-deny, cargo-mutants, gitleaks and lefthook. A missing
# tool fails its gate; nothing skips.

gates_root := justfile_directory()

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

# Format the Rust: this workspace's crates only (`_members`), never a path
# dependency outside it, which `cargo fmt --all` would also format.
fmt:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t packages < <(just --justfile '{{gates_root}}/justfile' _members)
    cargo fmt --manifest-path '{{gates_root}}/Cargo.toml' "${packages[@]}"

# Check the Rust is formatted, changing nothing: CI's check, scoped like
# `fmt`. Lists the files rustfmt would change (`-l`); VERBOSE=1 shows the
# diffs.
[no-exit-message]
fmt-check:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t packages < <(just --justfile '{{gates_root}}/justfile' _members)
    terse=(-l)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile '{{gates_root}}/justfile' _run fmt-check workspace fmt \
        cargo fmt --manifest-path '{{gates_root}}/Cargo.toml' "${packages[@]}" -- --check "${terse[@]}"

# This workspace's packages, as `--package <name>` arguments, one per line.
_members:
    @cargo metadata --format-version 1 --no-deps --offline --manifest-path '{{gates_root}}/Cargo.toml' \
        | jq -r '.packages[] | "--package\n\(.name)"'

# Format the staged Rust files: lefthook's pre-commit step, given the staged
# `*.rs` paths. What is formatted is each file's STAGED content, written back
# to the index (its mode kept), so the commit is formatted whatever the
# working tree holds. A file with no unstaged change gets the same content in
# its working copy. A partially staged file's working copy is left exactly as
# it was, and named: rewriting it would move its unstaged hunks, because
# lefthook (1.13.6) sets them aside as a context-free patch and re-applies it
# by line number after the hook. While the hook runs, lefthook has reset such
# a file to its staged content, so it is recognized by that patch
# (.git/info/lefthook-unstaged.patch) as well as by an unstaged diff.
[positional-arguments]
[no-exit-message]
fmt-staged +files:
    #!/usr/bin/env bash
    set -euo pipefail
    logs=${GATE_LOG_DIR:-{{gates_root}}/target/logs}
    just --justfile '{{gates_root}}/justfile' _run fmt-staged "$# staged files" fmt-staged \
        just --justfile '{{gates_root}}/justfile' _fmt-files "$@"
    grep '^partially staged: ' "$logs/fmt-staged.log" || true

# Each file is formatted alone, read from stdin, so rustfmt does not follow
# its `mod` declarations into files that were not staged; the edition is that
# of the package owning the file.
[positional-arguments]
[no-exit-message]
_fmt-files +files:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    cd "$root"
    declare -A edition_of=()
    while IFS=$'\t' read -r dir edition; do edition_of[$dir]=$edition; done < <(cargo metadata --format-version 1 \
        --no-deps --offline --manifest-path "$root/Cargo.toml" | jq -r --arg root "$root/" \
        '.packages[] | "\(.manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml"))\t\(.edition)"')
    lefthook_patch=$(git rev-parse --path-format=absolute --git-path info/lefthook-unstaged.patch)
    scratch=$(mktemp)
    trap 'rm -f "$scratch"' EXIT
    rc=0
    for file in "$@"; do
        read -r mode blob _ < <(git ls-files --stage -- "$file") || true
        case "${mode:-}" in
            100644|100755) ;;
            *) continue ;;
        esac
        owner=
        for dir in "${!edition_of[@]}"; do
            if [[ $file == "$dir"/* && ${#dir} -gt ${#owner} ]]; then owner=$dir; fi
        done
        if [ -z "$owner" ]; then
            echo "$file: no package of this workspace owns it, so its edition is unknown"
            rc=1
            continue
        fi
        if ! git cat-file blob "$blob" | rustfmt --edition "${edition_of[$owner]}" --emit stdout > "$scratch"; then
            echo "$file: rustfmt failed (above)"
            rc=1
            continue
        fi
        formatted=$(git hash-object -w --stdin < "$scratch")
        [ "$formatted" = "$blob" ] && continue
        partial=
        if ! git diff --quiet -- "$file" \
            || { [ -s "$lefthook_patch" ] && grep -qxF "diff --git a/$file b/$file" "$lefthook_patch"; }; then
            partial=1
        fi
        git update-index --cacheinfo "$mode,$formatted,$file"
        if [ -n "$partial" ]; then
            echo "partially staged: $file is committed formatted; its working copy is left as it was"
        else
            cat "$scratch" > "$file"
            echo "reformatted $file"
        fi
    done
    exit "$rc"

# Dependency policy: licenses, sources, bans and RustSec advisories (the
# consumer's deny.toml). Fetches the advisory database, so it needs the
# network. The inclusion graphs are hidden (VERBOSE=1 shows them). bans'
# duplicate-version warnings do not fail the gate; they stay in the log.
[no-exit-message]
lint-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(--hide-inclusion-graph)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile '{{gates_root}}/justfile' _run lint-deps workspace deny \
        cargo deny --color never --manifest-path '{{gates_root}}/Cargo.toml' check "${terse[@]}"

# Scan the STAGED changes for secrets: lefthook's pre-commit step. Rules and
# allowlists are the consumer's .gitleaks.toml; output is redacted. A missing
# binary fails rather than skipping: a scan that passes by not running looked
# at nothing.
[no-exit-message]
secrets-scan-staged:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v gitleaks >/dev/null 2>&1; then
        echo "FAIL secrets-scan-staged: gitleaks is not on PATH; the secret scan cannot pass by not running." >&2
        exit 1
    fi
    exec just --justfile '{{gates_root}}/justfile' _run secrets-scan-staged staged secrets \
        gitleaks git --pre-commit --staged --redact --verbose --no-banner --no-color \
        --config '{{gates_root}}/.gitleaks.toml' '{{gates_root}}'

# Every test in the workspace, by cargo-nextest (`_tests`), every feature of
# `feature_rules` on.
[no-exit-message]
test: (_require-nextest "test")
    @just --justfile '{{gates_root}}/justfile' _run test workspace tests just --justfile '{{gates_root}}/justfile' _tests --workspace

# Test what the commit changes: lefthook's pre-commit step. Runs the tests of
# every crate with a staged change and of the workspace crates depending on
# them (a change can break a dependent's tests); a staged workspace-wide file
# (`workspace_wide`), or crate source no crate owns, tests the whole
# workspace. It judges the STAGED tree, built by `_staged-tree`, so unstaged
# edits cannot pass or fail it.
[no-exit-message]
test-staged: (_require-nextest "test-staged")
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    read -ra wide <<< "{{workspace_wide}}"
    mapfile -t dirs < <(cargo metadata --format-version 1 --no-deps --offline --manifest-path "$root/Cargo.toml" \
        | jq -r --arg root "$root/" '.packages[] | .manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml")')
    while IFS= read -r -d '' path; do
        [[ $path == *.rs || $path == */Cargo.toml ]] && exec just --justfile "$root/justfile" _staged-tree _test-affected
        for dir in "${dirs[@]}"; do
            [[ $path == "$dir"/* ]] && exec just --justfile "$root/justfile" _staged-tree _test-affected
        done
        for pattern in "${wide[@]}"; do
            [[ $path == $pattern ]] && exec just --justfile "$root/justfile" _staged-tree _test-affected
        done
    done < <(git -C "$root" diff --cached --name-only --no-renames -z HEAD)
    echo "ok   test-staged (nothing to test: no staged change is in a crate or a workspace-wide file)"

# Inside the staged tree: map the staged paths to crates through cargo
# metadata, close over workspace dependents, and test them.
[no-exit-message]
_test-affected:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    read -ra wide <<< "{{workspace_wide}}"
    meta=$(cargo metadata --format-version 1 --no-deps --offline --manifest-path "$root/Cargo.toml")
    declare -A dir_of=() selected=()
    while IFS=$'\t' read -r name dir; do dir_of[$name]=$dir; done < <(jq -r --arg root "$root/" \
        '.packages[] | "\(.name)\t\(.manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml"))"' <<< "$meta")
    whole=
    while IFS= read -r -d '' path; do
        for pattern in "${wide[@]}"; do [[ $path == $pattern ]] && whole="$path"; done
        owner= owner_dir=
        for name in "${!dir_of[@]}"; do
            dir=${dir_of[$name]}
            if [[ $path == "$dir"/* && ${#dir} -gt ${#owner_dir} ]]; then owner=$name owner_dir=$dir; fi
        done
        if [ -n "$owner" ]; then
            selected[$owner]=1
        elif [[ $path == *.rs || $path == */Cargo.toml ]]; then
            # Rust source no current crate owns: a crate moved or removed.
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
# cannot run, once the rest pass. `feature_rules` turns on the features of the
# packages selected (all of them for --workspace), after `features_prepare`.
# The tests are built first by cargo, whose short messages nextest has no
# form of. Exits 101 if they did not build, else with nextest's code if it
# failed (100: a test failed), else the doc tests'.
[positional-arguments]
[no-exit-message]
_tests +packages:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    terse=(--status-level fail --final-status-level fail --success-output never --failure-output final
        --hide-progress-bar --cargo-quiet)
    cargo_terse=(-q --message-format=short)
    if [ "${VERBOSE:-}" = 1 ]; then terse=(); cargo_terse=(); fi
    read -ra rules <<< "{{feature_rules}}"
    features=()
    if [ "$1" = --workspace ]; then
        selection=(--workspace)
        doc=(--workspace)
        for rule in "${rules[@]}"; do features+=(--features "${rule#*=}"); done
    else
        selection=() doc=()
        for name in "$@"; do
            selection+=(--package "$name")
            for rule in "${rules[@]}"; do
                [ "${rule%%=*}" = "$name" ] && features+=(--features "${rule#*=}")
            done
        done
        # `cargo test --doc` refuses a package without a library.
        while IFS= read -r name; do doc+=(--package "$name"); done < <(cargo metadata --format-version 1 --no-deps \
            --offline --manifest-path "$root/Cargo.toml" | jq -r --args '.packages[] | select(.name as $n | $ARGS.positional
            | index($n)) | select(any(.targets[]; .kind | index("lib"))) | .name' "$@")
    fi
    if [ "${#features[@]}" -gt 0 ] && [ -n "{{features_prepare}}" ]; then
        just --justfile "$root/justfile" {{features_prepare}}
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

# Mutation-test what the commit changes: lefthook's pre-commit step, last
# because it is the slowest. `cargo mutants --in-diff` over the staged diff,
# in the STAGED tree (`_staged-tree`), judged by `mutation_thresholds`; the
# commit is refused with the per-tier verdict and the surviving mutants
# listed. cargo-mutants' own output is the log, and its mutants.out is kept
# in target/logs/mutants-staged/; its config (excludes, test_workspace) is
# the consumer's .cargo/mutants.toml. A missing cargo-mutants (`cargo install
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
    if [ -z "$(git -C '{{gates_root}}' diff --cached --name-only HEAD -- '*.rs' ':(exclude)*.test.rs')" ]; then
        echo "ok   mutants-staged (no mutants: the staged change touches no Rust source)"
        exit 0
    fi
    exec just --justfile '{{gates_root}}/justfile' _staged-tree _mutants-affected

# Inside the staged tree: `_mutants-judge`, run as the gate.
[no-exit-message]
_mutants-affected:
    @just --justfile '{{gates_root}}/justfile' _run mutants-staged "staged diff" mutants just --justfile '{{gates_root}}/justfile' _mutants-judge

# Mutate the staged diff in place (the tree is throwaway) and judge each
# tier's kill rate. `feature_rules` turns on the features of the packages
# whose sources the diff touches, after `features_prepare`. Killed = caught
# or timed out (a mutant the tests could not finish under changed behaviour);
# unviable mutants (did not build) count neither way. A failing verdict goes
# to verdict.txt beside mutants.out: its summary line, then the kill rate by
# tier, for `_extract-mutants`. Mutants run under `cargo test`, not nextest:
# a process per test only slows a run that rebuilds for each mutant.
#
# cargo mutants runs in a session of its own (setsid), and whatever is left
# in that session when it ends, or when the gate is interrupted, is killed
# (`reap`) and named in the log. A test that times out under a mutant is
# killed by cargo-mutants (SIGTERM to the test's process group), but a
# process the test spawned outlives it when the mutant broke that process's
# own way to stop (a server with its SIGTERM handler installed and its stop
# condition mutated): re-parented, it would run until the machine
# restarts. No such process can hold the staged tree's lock either: fd 9 is
# closed for the run. Only the session's processes are touched; nothing
# started outside the run is in it.
[no-exit-message]
_mutants-judge:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    out=${GATE_LOG_DIR:-$root/target/logs}/mutants-staged
    rm -rf "$out"
    mkdir -p "$out"
    verdict() { printf '%s\n' "$@" | tee -a "$out/verdict.txt"; }
    work=$(mktemp -d)
    # The run's session id: its leader's pid, written by the leader.
    session="$work/session"
    reap() {
        local sid left
        sid=$(cat "$session" 2>/dev/null) || return 0
        [ -n "$sid" ] || return 0
        left=$(ps -o pid=,args= -s "$sid" 2>/dev/null) || true
        if [ -n "$left" ]; then
            echo "mutants-staged: killed what the run left running:"
            sed 's/^ */  /' <<< "$left"
            pkill -KILL -s "$sid" || true
        fi
    }
    trap 'reap; rm -rf "$work"' EXIT
    trap 'exit 130' INT TERM HUP
    git diff --cached --no-color --no-ext-diff --no-renames --src-prefix=a/ --dst-prefix=b/ HEAD -- '*.rs' > "$work/staged.patch"
    read -ra rules <<< "{{feature_rules}}"
    features=()
    if [ "${#rules[@]}" -gt 0 ]; then
        declare -A dir_of=()
        while IFS=$'\t' read -r name dir; do dir_of[$name]=$dir; done < <(cargo metadata --format-version 1 --no-deps \
            --offline --manifest-path "$root/Cargo.toml" | jq -r --arg root "$root/" \
            '.packages[] | "\(.name)\t\(.manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml"))"')
        for rule in "${rules[@]}"; do
            dir=${dir_of[${rule%%=*}]:-}
            if [ -n "$dir" ] && grep -qF "+++ b/$dir/" "$work/staged.patch"; then
                features+=(--features "${rule#*=}")
            fi
        done
    fi
    if [ "${#features[@]}" -gt 0 ] && [ -n "{{features_prepare}}" ]; then
        just --justfile "$root/justfile" {{features_prepare}}
    fi
    rc=0
    # In the background, so an interrupt is handled at once rather than once
    # the run ends: the trap's exit reaps the session, cargo mutants too.
    setsid --wait bash -c 'echo $$ > "$1"; shift; exec "$@"' mutants-session "$session" \
        cargo mutants --dir "$root" --in-place --workspace "${features[@]}" --in-diff "$work/staged.patch" \
        --output "$out" --colors never 9>&- &
    wait "$!" || rc=$?
    reap
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
    echo "mutants-staged: kill rate by tier:"
    sed 's/^/  /' <<< "$report"
    if grep -q '^FAIL' <<< "$report"; then
        verdict "below the kill-rate threshold: $(grep '^FAIL' <<< "$report" | sed 's/^FAIL  //; s/;.*//' | paste -sd ';' | sed 's/;/; /g')" \
            "kill rate by tier:" "$(sed 's/^/  /' <<< "$report")"
        exit 1
    fi

# Run a recipe of the STAGED tree: HEAD checked out into a throwaway git
# worktree with `git diff --cached` applied on top, so the recipe sees what the
# commit will contain and nothing else (no unstaged edit, no untracked file),
# and nothing here writes the working tree, the index or the stash.
#
# The worktree is at `staged_tree`. Its name is fixed per checkout and the run
# holds a lock, so builds reuse one target dir (target/staged) across
# commits. It gets its own `.gates` link, to the same gates checkout. Gate
# logs go to this checkout's target/logs (GATE_LOG_DIR), not the throwaway
# tree's.
[positional-arguments]
[no-exit-message]
_staged-tree +recipe:
    #!/usr/bin/env bash
    set -euo pipefail
    top='{{gates_root}}'
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
    relative='{{staged_tree}}'
    tree=$(realpath -m "$top/${relative//\{name\}/$(basename "$top")}")
    gates=$(readlink -f "$top/.gates")
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
    ln -sfn "$gates" "$tree/.gates"
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
    logs=${GATE_LOG_DIR:-{{gates_root}}/target/logs}
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
    extract=$(just --justfile '{{gates_root}}/justfile' _extract-"$extractor" "$log" "$rc" 2>&1) || true
    printf 'FAIL %s: %s (full log: target/logs/%s.log)\n' "$gate" "${extract%%$'\n'*}" "$gate" >&2
    if [ "${VERBOSE:-}" != 1 ] && [[ $extract == *$'\n'* ]]; then
        printf '%s\n' "${extract#*$'\n'}" >&2
    fi
    exit "$rc"

# The extractors: given a gate's log and exit code, print a one-line summary,
# then the part of the log worth reading. Each gate runs its tool in a terse
# or structured mode, so an extract is that output, not a search of prose.

# cargo build/clippy with --message-format=short: a line per diagnostic.
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
    sed 's|^{{gates_root}}/||' "$1"

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
    cat "$1"

# cargo-deny: its own rendering at error level, without the inclusion graphs,
# offline, from the database the run just fetched; its last line is deny's
# summary.
[positional-arguments]
_extract-deny log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    errors=$(cargo deny --offline --color never --log-level error --manifest-path '{{gates_root}}/Cargo.toml' \
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
