# Changelog

## [0.2.0] - 2025-10-27

### Changed

- impl of `AsyncDrop` now uses `drop`

### Fixed

- use `UpdateOne` operation for delete event to avoid duplicate `_id_` index error

## [0.1.1] - 2025-06-06

### Fixed

- Updated toolchain version to post: https://github.com/rust-lang/rust/issues/140906

## [0.1.0] - 2025-05-10

### Added

- Initial release of the project.
- `tracing` feature for logging support.
- `MongoDrop` struct - why this exists.
