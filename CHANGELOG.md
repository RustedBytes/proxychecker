# Changelog

All notable changes are documented here using [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Unreleased changes are assigned a version and date when a release is prepared.
For the 0.x series, breaking Python API or supported-platform changes increment
MINOR; backward-compatible fixes increment PATCH. No release is implied by an
Unreleased entry.

## [Unreleased]

### Added
- Internal Rust module documentation and a rustdoc warning check.
- Unit and local Python integration tests for stream messages, error propagation, body handling, and bounded concurrency.
- A combined Rust/Python coverage check requiring at least 90% production Rust line coverage, with a CI progress bar and downloadable reports/badge.
- CI for relevant pull requests, pushes to `master`, and manual runs, with path filters and cancellation of superseded runs.
- Rust checks, regression tests, pedantic Clippy, and Python API tests on Python 3.10 and 3.15.

### Changed
- Use a current-thread Tokio runtime per batch and move Python result delivery to the blocking pool. Bound queued results by the batch concurrency.
- **Breaking:** Rename the Python distribution, import module, and Rust crate to `proxyprobe`; update dependencies and imports from `rsloop-rust-proxychecker` / `rsloop_rust_proxychecker`. Function signatures and result dictionaries remain unchanged.
- Split the extension implementation into API, configuration, checker, worker, and stream modules without changing the public Python API.
- Enable PyO3 extension-module mode through Maturin so Rust unit tests can embed Python normally.
- Update the Rust `rsloop` dependency to 0.1.56 and `wreq` to the stable 0.16.1 series.
- Update PyO3 to 0.29.3 for compatibility with `rsloop`.
- Require Python 3.10 or newer and Rust 1.98 or newer for the updated dependencies.
- Adapt proxy parsing, idle connection configuration, and body streaming to the current `wreq` API.

### Fixed
- Cancel in-flight checks when the stream is dropped or a pending iteration is cancelled; propagate worker panics and thread startup failures instead of leaving consumers waiting.
- Reject unsupported proxy schemes before sending requests, preventing silent direct-request fallback with the current `wreq`; preserve bare `host:port` proxy inputs.
- Drain response bodies without accumulating them when `return_response=false`, preserving body-read failures and full body return when enabled (PR #1).

[Unreleased]: https://github.com/RustedBytes/proxychecker/compare/a82dce87b87247cf33dd6ec676b485d4f157c7c1...HEAD
