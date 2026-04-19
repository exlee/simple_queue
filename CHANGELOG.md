# Changelog

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
