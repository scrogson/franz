# Franz development commands

# Default recipe shows available commands
default:
    @just --list

# Install dependencies
deps:
    mix deps.get
    cd native/franz && cargo fetch

# Format Elixir code
format-elixir:
    mix format

# Format Rust code
format-rust:
    cd native/franz && cargo fmt

# Format all code (Elixir + Rust)
format: format-elixir format-rust

# Check Elixir formatting
check-format-elixir:
    mix format --check-formatted

# Check Rust formatting
check-format-rust:
    cd native/franz && cargo fmt --check

# Check all formatting
check-format: check-format-elixir check-format-rust

# Lint Elixir with Credo
lint-elixir:
    mix credo

# Lint Elixir with Credo (strict)
lint-elixir-strict:
    mix credo --strict

# Lint Rust with Clippy
lint-rust:
    cd native/franz && cargo clippy

# Lint Rust with Clippy (warnings as errors)
lint-rust-strict:
    cd native/franz && cargo clippy -- -D warnings

# Lint all code
lint: lint-elixir lint-rust

# Lint all code (strict)
lint-strict: lint-elixir-strict lint-rust-strict

# Run Elixir tests
test:
    mix test

# Run Elixir tests with coverage
test-coverage:
    mix coveralls

# Run Elixir tests with HTML coverage report
test-coverage-html:
    mix coveralls.html

# Compile project
compile:
    mix compile

# Clean build artifacts
clean:
    mix clean
    cd native/franz && cargo clean

# Full CI check (format, lint, test)
ci: check-format lint test

# Full CI check (strict)
ci-strict: check-format lint-strict test

# Start Kafka/Redpanda
kafka-start:
    docker compose up -d

# Stop Kafka/Redpanda
kafka-stop:
    docker compose down

# View Kafka/Redpanda logs
kafka-logs:
    docker compose logs -f redpanda
