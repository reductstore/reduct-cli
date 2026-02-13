# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Add Linux ARM32 CI/CD artifacts, [PR-177](https://github.com/reductstore/reduct-cli/pull/177)

## 0.10.1 - 2026-02-04

### Fixed

- Fix cp parsing for base-path URLs, [PR-176](https://github.com/reductstore/reduct-cli/pull/176)

## 0.10.0 - 2026-02-04

### Added

- Add batching in reduct-cli for cp command, [PR-148](https://github.com/reductstore/reduct-cli/pull/148)
- Show bucket status and update copy/removal handling, [PR-167](https://github.com/reductstore/reduct-cli/pull/167)
- Add replica mode commands and mode-aware outputs, [PR-168](https://github.com/reductstore/reduct-cli/pull/168)
- Add --from-last option for incremental cp, [PR-169](https://github.com/reductstore/reduct-cli/pull/169)
- Add wildcard cp and improved progress reporting, [PR-170](https://github.com/reductstore/reduct-cli/pull/170)

### Changed

- Improve cp URL parsing and CLI status tables, [PR-172](https://github.com/reductstore/reduct-cli/pull/172)

## 0.9.4 - 2025-12-17

### Changed

- Update reduct-rs to 1.17.2 with cookie jar support

## [0.9.3] - 2025-10-20

### Changed

- Update reduct-rs to 1.17.0 and other dependencies

## [0.9.2] - 2025-10-08

## Fixed

- Ignore 401,403 and 503 errors during license check, [PR-146](https://github.com/reductstore/reduct-cli/pull/146)

## [0.9.1] - 2025-10-07

### Fixed

- Fix compatibility break in replication settings v1.17, [PR-145](https://github.com/reductstore/reduct-cli/pull/145)

## [0.9.0] - 2025-08-26

### Added

- Add attempts to `reduct-cli cp` command to recover after
  errors, [PR-132](https://github.com/reductstore/reduct-cli/pull/132)

## [0.8.0] - 2025-07-31

### Breaking changes:

- Remove include/exclude options from `reduct-cli cp` and `reduct-cli replica`
  commands, [PR-122](https://github.com/reductstore/reduct-cli/pull/122)

## [0.7.0] - 2025-06-11

### Breaking changes:

- Use `-t` instead of `-T` for the `--time` flag in `reduct-cli rm`
  command, [PR-108](https://github.com/reductstore/reduct-cli/pull/108)

### Added:

- Add `--ext-params` flag to copy command, [PR-108](https://github.com/reductstore/reduct-cli/pull/108)
- Add MCAP and other well know types, [PR-115](https://github.com/reductstore/reduct-cli/pull/115)

### Fixed:

- Update reduct-rs up to 1.15.2, [PR-114](https://github.com/reductstore/reduct-cli/pull/114)

## [0.6.1] - 2025-05-07

### Breaking changes:

- RS-661: use `-T` instead of `-t` argument for timeout, [PR-96](https://github.com/reductstore/reduct-cli/pull/96)

### Changed:

- RS-647: Build binaries for Linux and Macos ARM64, [PR-92](https://github.com/reductstore/reduct-cli/pull/92)

## [0.6.0] - 2025-03-18

### Added:

- RS-624: Add option --when for replication settings. [PR-78](https://github.com/reductstore/reduct-cli/pull/78)

### Changed:

RS-633: Link runtime libraries statically, [PR-85](https://github.com/reductstore/reduct-cli/pull/85)

## [0.5.0] - 2024-12-14

### Added:

- RS-552: Add when and strict arguments to cp and rm
  commands, [PR-51](https://github.com/reductstore/reduct-cli/pull/51)

## [0.4.0] - 2024-10-22

### Added:

- RS-507: `bucket rename` command, [PR-21](https://github.com/reductstore/reduct-cli/pull/21)
- RS-508: `--only-entry` option to rename an entry, [PR-22](https://github.com/reductstore/reduct-cli/pull/22)
- RS-451: `reduct-cli rm` command for removing records`, [PR-25](https://github.com/reductstore/reduct-cli/pull/25)

### Fixed:

- RS-357: Print an error message if SSL certificate is
  invalid, [PR-19](https://github.com/reductstore/reduct-cli/pull/19)
- RS-518: Progress bar status for limited query, [PR-20](https://github.com/reductstore/reduct-cli/pull/20)

## [0.3.1] - 2024-07-19

### Fixed:

- Print all errors in progress of reduct-cli cp command, [PR-12](https://github.com/reductstore/reduct-cli/pull/12)

## [0.3.0] - 2024-06-25

### Added:

- RS-318: Downsampling options for `reduct-cli cp` and `reduct-cli replica`
  commands, [PR-10](https://github.com/reductstore/reduct-cli/pull/10)

## [0.2.0] - 2024-04-29

### Added:

- RS-55: `--only-entries` option to delete only entries in `bucket rm`
  cmd, [PR-8](https://github.com/reductstore/reduct-cli/pull/8)

### Fixed:

- Wrong file extension for application/octet-stream content type in `reduct-cli cp`
  command, [PR-4](https://github.com/reductstore/reduct-cli/pull/4)
- Wrong progress calculation in `reduct-cli cp`, [PR-7](https://github.com/reductstore/reduct-cli/pull/7)

### Changed:

- RS-298: update command documentation, [PR-9](https://github.com/reductstore/reduct-cli/pull/9)

### Security:

- Bump `rustls` from 0.21.10 to 0.21.12, [PR-5](https://github.com/reductstore/reduct-cli/pull/5)
- Bump `h2` from 0.3.25 to 0.3.26, [PR-6](https://github.com/reductstore/reduct-cli/pull/6)

## [0.1.0] - 2024-04-03

- Moved from https://github.com/reductstore/reductstore

[Unreleased]: https://github.com/reductstore/reduct-cli/compare/0.9.3...HEAD

[0.9.3]: https://github.com/reductstore/reduct-cli/compare/v0.9.2...v0.9.3

[0.9.2]: https://github.com/reductstore/reduct-cli/compare/v0.9.1...v0.9.2

[0.9.1]: https://github.com/reductstore/reduct-cli/compare/v0.9.0...v0.9.1

[0.9.0]: https://github.com/reductstore/reduct-cli/compare/v0.8.0...v0.9.0

[0.8.0]: https://github.com/reductstore/reduct-cli/compare/v0.7.0...v0.8.0

[0.7.0]: https://github.com/reductstore/reduct-cli/compare/v0.6.1...v0.7.0

[0.6.1]: https://github.com/reductstore/reduct-cli/compare/v0.6.0...v0.6.1

[0.6.0]: https://github.com/reductstore/reduct-cli/compare/v0.5.0...v0.6.0

[0.5.0]: https://github.com/reductstore/reduct-cli/compare/v0.4.0...v0.5.0

[0.4.0]: https://github.com/reductstore/reduct-cli/compare/v0.3.1...v0.4.0

[0.3.1]: https://github.com/reductstore/reduct-cli/compare/v0.3.0...v0.3.1

[0.3.0]: https://github.com/reductstore/reduct-cli/compare/v0.2.0...v0.3.0

[0.2.0]: https://github.com/reductstore/reduct-cli/compare/v0.1.0...v0.2.0

[0.1.0]: https://github.com/reductstore/reduct-cli/releases/tag/v0.1.0
