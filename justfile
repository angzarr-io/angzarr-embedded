# Build, test and lint the workspace. eslog's Rust types are generated into
# crates/eslog/gen by `gen`, which build, test and lint run first, so these
# work from a clean checkout.
#
# Needs: buf (~/go/bin/buf) and protoc-gen-prost 0.5.0 on PATH
#   cargo install protoc-gen-prost --version 0.5.0 --locked

set windows-shell := ["bash", "-cu"]

# List the recipes.
default:
    @just --list

# Generate eslog's protobuf types into crates/eslog/gen (never committed).
gen:
    buf generate

# Build everything, tests included.
build: gen
    cargo build --release --workspace --all-targets

# Run every test.
test: gen
    cargo test --release --workspace

# Lint the protobuf schema and the Rust.
lint: gen
    buf lint
    cargo clippy --workspace --all-targets -- -D warnings

# Check the schema for wire-breaking changes against main.
breaking:
    buf breaking --against '.git#branch=main'
