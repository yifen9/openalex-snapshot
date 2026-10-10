set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

lint:
    cargo fmt --check
    cargo clippy --locked --all-targets -- -D warnings

test:
    cargo test --locked

check: lint test

extract *args:
    cargo run --release --locked --quiet -- {{args}}
