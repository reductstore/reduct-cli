# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs` is the CLI entry point.
- `src/cmd/` holds command implementations (e.g., `bucket`, `alias`, `server`, `cp`, `rm`, `token`, `replica`).
- `src/parse/` contains argument and value parsing helpers (byte sizes, paths, quota types, etc.).
- `src/io/` provides I/O backends and ReductStore client wiring.
- Cross-cutting helpers live in `src/config.rs`, `src/context.rs`, and `src/helpers.rs`.
- Tests are embedded in modules using `#[cfg(test)]` blocks (no top-level `tests/` directory).

## Build, Test, and Development Commands
- `cargo build` — compile the CLI in debug mode.
- `cargo run -- <args>` — run the CLI locally (example: `cargo run -- server status <alias>`).
- `cargo test` — run unit tests embedded in modules.
- `cargo fmt` — format code with rustfmt (standard Rust formatting).
- `cargo clippy` — optional linting for common Rust issues.

## Coding Style & Naming Conventions
- Rust 2021 edition; keep code formatted with rustfmt.
- Use idiomatic Rust module naming (snake_case for files/modules, CamelCase for types).
- CLI commands and subcommands map to modules under `src/cmd/`; keep names consistent with the CLI surface (e.g., `bucket/ls.rs`).
- Prefer small, focused functions and explicit error contexts using `anyhow`.

## Testing Guidelines
- Unit tests live next to the code they exercise under `#[cfg(test)]`.
- Use `rstest`, `mockall`, and `tempfile` (see `Cargo.toml`) where appropriate.
- Name tests descriptively (e.g., `parses_quota_type`, `rejects_invalid_alias`).

## Commit & Pull Request Guidelines
- Commit subjects are short and imperative; dependency updates follow the pattern `Bump <crate> from <old> to <new> (#NN)`.
- PRs should include: a clear description, rationale, and test coverage notes (e.g., `cargo test`).
- Link related issues or discussions when applicable.

## Configuration Tips
- The CLI connects to ReductStore servers via aliases; see `README.md` for example usage.
- When changing API-facing behavior, update or add CLI examples to keep user docs accurate.
