# Changelog

All notable changes are documented here using [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Unreleased changes are assigned a version and date when a release is prepared.
For the 0.x series, breaking Python API or supported-platform changes increment
MINOR; backward-compatible fixes increment PATCH. No release is implied by an
Unreleased entry.

## [Unreleased]

### Added
- CI for relevant pull requests, pushes to `master`, and manual runs, with path filters and cancellation of superseded runs.
- Rust checks, regression tests, pedantic Clippy, and Python API tests on Python 3.10 and 3.15.

### Changed
- Update the Rust `rsloop` dependency to 0.1.56 and `wreq` to the stable 0.16.1 series.
- Update PyO3 to 0.29.3 for compatibility with `rsloop`.
- Require Python 3.10 or newer and Rust 1.98 or newer for the updated dependencies.
- Adapt proxy parsing, idle connection configuration, and body streaming to the current `wreq` API.

### Fixed
- Drain response bodies without accumulating them when `return_response=false`, preserving body-read failures and full body return when enabled (PR #1).

[Unreleased]: https://github.com/RustedBytes/proxychecker/compare/a82dce87b87247cf33dd6ec676b485d4f157c7c1...HEAD
