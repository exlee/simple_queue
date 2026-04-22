# Changelog

## Unreleased

## 0.2.3

### Added

- Add `completed_at` timestamp tracking on jobs
- Add v2 migration (`0002_queue_completed_at`) for `completed_at` column on main, DLQ, and archive tables

### Changed

- Queue-specific configuration builders (`with_queue_semaphore`, `with_queue_strategy`, `with_queue_backoff_strategy`) can be used and modified on references (direct or Arc)
- Reaper now sets `completed_at` when marking jobs as permanently failed
- Job completion now sets `completed_at` timestamp

## 0.2.1, 0.2.2

### Maintenance

- Set SQLX_OFFLINE=true in build.rs in hopes to have docs.rs build go through
- Makefile has `--all-features` for `sqlx prepare` so that tests are picked up too

## 0.2.0

### Added

- wait-for-job feature (oneshot rx for job insertion)

### Maintenance

- TestContext is RAII guard now, guaranteed to drop stale schemas even on crash
- Makefile for checks

## 0.1.4

### Maintanance

- SQLX prepare files
- Include test files

## 0.1.3

### Added

- `JobExt` to simplify transformation from seriailizable into a `Job`
- `SimpleQueue::setup_from_url(&str)`
- `SimpleQueue::new_from_url(&str)`
- Changelog
