---
name: fix-tests
description: Fix Rust test failures and compilation errors. Use when tests do not compile, warnings break CI, or unit/integration tests fail (especially when a ReductStore container must be running). Includes guidance to run tests, start ReductStore via Docker, improve assertions, and reduce test duplication.
---

# Fix Tests

## Overview

Fix compilation errors, warnings, and failing tests in Rust repos, including integration tests that require a running ReductStore instance.

## Workflow

### 1. Establish baseline

- Run `cargo test` first to capture compile errors, warnings, and failing tests.
- If tests are integration-heavy or flaky, re-run with `-- --test-threads=1` to reduce concurrency.

### 2. Fix compilation errors and warnings

- Address compiler errors first; they block everything else.
- Remove unused imports, fix type mismatches, and update signatures.
- Re-run `cargo test` or a focused test module to confirm the compile fix.

### 3. Start ReductStore when tests need it

If failures show connection errors to `localhost:8383` or ReductStore endpoints:

- Use CI as the source of truth for how to run the service.
- Start a local container (examples mirror CI):
  - `docker run --network=host -v ${PWD}/misc:/misc --env RS_API_TOKEN=TOKEN -d reduct/store:latest`
  - `docker run --network=host -v ${PWD}/misc:/misc --env RS_API_TOKEN=TOKEN -d reduct/store:main`
- Run tests with the token set:
  - `RS_API_TOKEN=TOKEN cargo test -- --test-threads=1`

### 4. Fix failing tests

- Update test logic to match current behavior or expected outputs.
- Prefer small, deterministic fixtures. Avoid network or timing dependencies where possible.
- Re-run the smallest relevant test(s) before full suite.

### 5. Improve test readability

- Reduce duplication by extracting helper functions/fixtures inside `#[cfg(test)]` modules.
- Add assertion messages that explain intent and expected values.
- Keep tests short and focused on a single behavior.

### 6. Verify and report

- Re-run full test suite (or CI-equivalent subset) and confirm passes.
- Summarize changes and note any remaining gaps (e.g., requires running container).
