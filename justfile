# Recipes for the athena-udf workspace.
#
#   just                 list recipes
#   just ci              what CI runs (format, lint, tests, docs)
#   just lambda-build    build simple-udf for AWS Lambda
#   just deploy          deploy simple-udf to AWS Lambda
#   just publish patch   bump, tag and push; then publish a GitHub Release
#                        to run the crates.io workflow

default:
    @just --list

# --- development -------------------------------------------------------------

# Type-check the crate and both examples.
check:
    cargo check --workspace --all-targets --all-features

# Build release artifacts (the Lambda binary is built by lambda-build).
build:
    cargo build --workspace --release

# Unit and integration tests. Use test-doc for doctests.
test:
    cargo nextest run --workspace --all-features --nocapture

# Doctests, which nextest does not run.
test-doc:
    cargo test --workspace --all-features --doc

# Lint with the same settings as CI.
lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

# Build the API docs and fail on warnings.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc -p athena-udf --all-features --no-deps

# Check the declared minimum supported Rust version.
msrv:
    rustup run 1.88.0 cargo check --workspace --all-targets --all-features

# Rebuild and test on every file change.
work:
    cargo watch -x "check" -s "just test" -s "just lint"

# Everything CI runs, except coverage and the MSRV job.
ci: fmt-check lint test test-doc doc

# ci plus the MSRV check.
ci-full: ci msrv

coverage:
    cargo llvm-cov --workspace --all-features --html

clean:
    cargo clean

# --- AWS Lambda --------------------------------------------------------------

# Build one example for Lambda. Needs Zig. TARGET is --arm64 or --x86-64.
lambda-build NAME="simple-udf" TARGET="--arm64":
    cargo lambda build --release {{TARGET}} -p {{NAME}}

# Run one example on the local emulator.
lambda-watch NAME="simple-udf":
    cargo lambda watch -p {{NAME}}

# Create or update one example Lambda. Extra flags pass through, e.g. --iam-role.
deploy NAME="simple-udf" *FLAGS:
    cargo lambda deploy {{NAME}} --manifest-path examples/{{NAME}}/Cargo.toml {{FLAGS}}

# Send a PingRequest to the local emulator. Start lambda-watch first.
ping:
    cargo lambda invoke --data-ascii '{"@type":"PingRequest","identity":{}}'

# --- release -----------------------------------------------------------------

# Bump, sign, tag and push. The pushed tag creates the GitHub release and publishes.
publish LEVEL="patch" *FLAGS:
    cargo release {{LEVEL}} --package athena-udf --exclude simple-udf --exclude manual-udf {{FLAGS}}

# Verify the package on crates.io without uploading it.
publish-dry:
    cargo publish -p athena-udf --dry-run
