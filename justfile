# Build, test and lint the workspace. Plain cargo needs nothing here; these
# recipes also run `gen` so the annals-log codegen check (the `codegen-check`
# feature) can compare the hand-written wire types with buf's output.
#
# Needs: buf (~/go/bin/buf) and protoc-gen-prost 0.5.0 on PATH
#   cargo install protoc-gen-prost --version 0.5.0 --locked

set windows-shell := ["bash", "-cu"]

# List the recipes.
default:
    @just --list

# Generate annals-log's protobuf types into crates/annals-log/gen (never
# committed), for the codegen check only.
gen:
    buf generate

# Build everything, tests and the codegen check included.
build: gen
    cargo build --release --workspace --all-targets --features annals-log/codegen-check

# Run every test, the codegen check included.
test: gen
    cargo test --release --workspace --features annals-log/codegen-check

# Lint the protobuf schema and the Rust.
lint: gen
    buf lint
    cargo clippy --workspace --all-targets --features annals-log/codegen-check -- -D warnings

# Check the schema for wire-breaking changes against main.
breaking:
    buf breaking --against '.git#branch=main'

# Format the Rust.
fmt:
    cargo fmt --all

# Check the Rust is formatted, changing nothing.
fmt-check:
    cargo fmt --all -- --check

# Mutation-test with cargo-mutants (config in .cargo/mutants.toml): the whole
# workspace, or only the named crates' code (`just mutants annals`). Each
# mutant runs every crate's tests, annals-log's codegen check included
# whenever annals-log is mutated. Results land in mutants.out/.
#   cargo install cargo-mutants --locked
mutants *crates: gen
    cargo mutants \
        {{ if crates == "" { "--workspace" } else { "--package " + replace(crates, " ", " --package ") } }} \
        {{ if crates == "" { "--features annals-log/codegen-check" } else if crates =~ '(^| )annals-log( |$)' { "--features annals-log/codegen-check" } else { "" } }}
