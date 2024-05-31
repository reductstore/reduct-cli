# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2024-04-29

### Added:

- RS-55: `--only-entries` option to delete only entries in `bucket rm` cmd, [PR-8](https://github.com/reductstore/reduct-cli/pull/8)

### Fixed:

- Wrong file extension for application/octet-stream content type in `reduct-cli cp` command, [PR-4](https://github.com/reductstore/reduct-cli/pull/4)
- Wrong progress calculation in `reduct-cli cp`, [PR-7](https://github.com/reductstore/reduct-cli/pull/7)

### Changed:

- RS-298: update command documentation, [PR-9](https://github.com/reductstore/reduct-cli/pull/9)

### Security:

- Bump `rustls` from 0.21.10 to 0.21.12, [PR-5](https://github.com/reductstore/reduct-cli/pull/5)
- Bump `h2` from 0.3.25 to 0.3.26, [PR-6](https://github.com/reductstore/reduct-cli/pull/6)

## [0.1.0] - 2024-04-03

- Moved from https://github.com/reductstore/reductstore

[Unreleased]: https://github.com/reductstore/reduct-cli/compare/0.2.0...HEAD

[0.2.0]: https://github.com/reductstore/reduct-cli/compare/v0.1.0...v0.2.0

[0.1.0]: https://github.com/reductstore/reduct-cli/releases/tag/v0.1.0
