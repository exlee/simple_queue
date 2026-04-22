# CHANGELOG Audit Report

## (a) Does CHANGELOG.md exist?
**Yes.** `/Users/xlii/Ownbench/rust/simple_queue/CHANGELOG.md` exists and contains entries for 0.1.3, 0.1.4, 0.2.0, 0.2.1/0.2.2, and 0.2.3, plus an empty `## Unreleased` section at the top.

## (b) Last released version
**0.2.3** (change ID `onzzyvuqyzru`).

## (c) Changes since previous version (0.2.2)

Source: `jj diff --from 'ywvvrnrklznp'` (0.2.2 change) and commit log.

### Added
- `completed_at` timestamp tracking on jobs (new field on `Job` struct)
- v2 migration (`migrations/0002_queue_completed_at.sql`) adding `completed_at` column to main, DLQ, and archive tables
- `completed_at` column also added to base migration (`migrations/0001_queue_init.sql`) for fresh installs

### Changed
- Queue-specific configuration builders (`with_queue_semaphore`, `with_queue_strategy`, `with_queue_backoff_strategy`) changed to take `&self` and return `&Self` so they can be used on references (direct or `Arc`)
- Reaper now sets `completed_at = CURRENT_TIMESTAMP` when marking jobs as permanently failed
- Job completion (`update_job` in `src/queue/logic.rs`) now sets `completed_at = CURRENT_TIMESTAMP`
- Janitor now moves dead jobs (unprocessable, cancelled, critical, bad job) to **DLQ** instead of archive, and also moves failed jobs that have reached max attempts to DLQ

### Removed
- The reaper's old update query (without `completed_at`) was replaced

### Internal / Not user-facing
- `build.rs`: `SQLX_OFFLINE=true` env var re-commented out (was enabled in 0.2.2)
- `.sqlx/` offline query metadata regenerated
- `examples/test_run.rs`: updated demo configuration and added simulated failure case

## (d) Are any entries missing from the changelog?
**No significant user-facing changes are missing.** The 0.2.3 section in CHANGELOG.md accurately covers all library-level additions and changes.

One minor omission:
- **`build.rs` change**: In 0.2.2 the changelog noted "Set SQLX_OFFLINE=true in build.rs". This has been **reverted** (re-commented out) in 0.2.3, which is not mentioned. This is arguably internal tooling and may not warrant a changelog entry, but it is a discrepancy with the prior noted state.

The `## Unreleased` section is correctly empty — there are no commits above the `v0.2.3` change.
