# Snapshot Expiration and Orphan File Cleanup

## Overview

The cleanup module provides production-ready table maintenance operations for Apache Iceberg tables managed by iceberg-compaction. It addresses two critical operational needs:

1. **Snapshot Expiration**: Remove old snapshot metadata to reduce table metadata size
2. **Orphan File Cleanup**: Delete unreferenced files to reclaim storage space

## Features

### Snapshot Expiration

The `SnapshotExpiration` struct handles removing old snapshots and their associated files:

- **Configurable retention**: Expire snapshots older than a specified duration
- **Minimum retention**: Always keep the most recent N snapshots
- **Dry-run support**: Preview what would be expired before actual deletion
- **Detailed metrics**: Track snapshots expired and files cleaned up
- **Safety guarantees**:
  - Current snapshot is never expired
  - Snapshots referenced by branches/tags are protected
  - Best-effort file deletion (failures logged but don't fail the operation)

### Orphan File Cleanup

The `OrphanFileCleanup` struct removes files that are no longer referenced by any snapshot:

- **Full orphan detection**: Scans entire table storage location
- **Types of orphans cleaned**:
  - Data files from expired snapshots
  - Manifest files from expired snapshots
  - Manifest list files from expired snapshots
  - Files from failed/interrupted uploads (never committed to any manifest)
  - Stray files from external tools or manual operations
- **Safety mechanisms**:
  - Only deletes files older than configured retention period
  - Files without timestamps are skipped (protects in-progress writes)
  - Dry-run mode for preview

### Combined Maintenance

The `Maintenance` struct orchestrates both operations in the correct sequence:

1. Expire snapshots (metadata operation)
2. Clean up orphan files (storage operation)

Each phase is independently configurable and can be enabled/disabled.

## Architecture

### Module Structure

```
core/src/cleanup/
├── mod.rs                  # Module definition, shared constants
├── expire_snapshots.rs     # SnapshotExpiration implementation
├── orphan_files.rs         # OrphanFileCleanup implementation
└── maintenance.rs          # Combined Maintenance workflow
```

### Technical Implementation

#### Snapshot Expiration

Our implementation uses iceberg-rust's Transaction API:

```rust
use iceberg::transaction::Transaction;

let tx = Transaction::new(&table);
let tx = tx
    .expire_snapshot()
    .retain_last(5)
    .expire_older_than(timestamp_ms)
    .clear_expire_files(true)  // Automatically clean up files
    .apply(tx)?;
table = tx.commit(&catalog).await?;
```

However, our current implementation (`expire_snapshots.rs`) uses a custom approach that:
1. Identifies snapshots to expire based on age and retain_last
2. Collects all files referenced by valid (non-expired) snapshots
3. Identifies files only referenced by expired snapshots
4. Deletes orphaned files (data, manifest, manifest list)

**Note**: The current implementation deletes files but does not commit metadata changes to the catalog. For production use with metadata updates, consider using the upstream `Transaction::expire_snapshot()` API directly.

#### Orphan File Cleanup

Our implementation leverages the upstream `RemoveOrphanFilesAction` from iceberg-rust (risingwavelabs fork, PR #116):

```rust
use iceberg::actions::RemoveOrphanFilesAction;

let action = RemoveOrphanFilesAction::new(table)
    .older_than(duration)
    .dry_run(false)
    .load_concurrency(16)
    .delete_concurrency(10);

let orphan_files = action.execute().await?;
```

The upstream action:
1. Builds a set of all "reachable" files from:
   - Current and historical metadata files
   - All snapshots' manifest lists
   - All manifests referenced by manifest lists
   - All data files referenced by manifests
2. Lists ALL files under the table storage location using `file_io.list()`
3. Filters for orphans: files not in reachable set, older than threshold
4. Deletes orphan files concurrently

### Metrics

The module integrates with the existing metrics system:

| Metric | Type | Description |
|--------|------|-------------|
| `iceberg_compaction_snapshot_expiration_counter` | Counter | Number of snapshots expired |
| `iceberg_compaction_snapshot_expiration_duration` | Histogram | Duration of snapshot expiration |
| `iceberg_compaction_snapshot_cleanup_files_counter` | Counter | Files cleaned during expiration |
| `iceberg_compaction_orphan_cleanup_counter` | Counter | Number of orphan files deleted |
| `iceberg_compaction_orphan_cleanup_duration` | Histogram | Duration of orphan cleanup |
| `iceberg_compaction_orphan_bytes_freed_counter` | Counter | Bytes freed by orphan cleanup |

## Usage

### Basic Snapshot Expiration

```rust
use iceberg_compaction_core::cleanup::{SnapshotExpiration, ExpireSnapshotsConfigBuilder};
use std::time::Duration;

let config = ExpireSnapshotsConfigBuilder::default()
    .older_than(Duration::from_secs(7 * 24 * 60 * 60))  // 7 days
    .retain_last(3u32)                                    // Keep 3 most recent
    .dry_run(true)                                        // Preview first
    .build()?;

let result = SnapshotExpiration::new(table, config).execute().await?;
println!("Would expire {} snapshots", result.snapshots_expired);
```

### Basic Orphan Cleanup

```rust
use iceberg_compaction_core::cleanup::{OrphanFileCleanup, RemoveOrphanFilesConfigBuilder};
use std::time::Duration;

let config = RemoveOrphanFilesConfigBuilder::default()
    .older_than(Duration::from_secs(7 * 24 * 60 * 60))  // 7 days
    .dry_run(false)                                      // Actually delete
    .build()?;

let result = OrphanFileCleanup::new(table, config).execute().await?;
println!("Deleted {} orphan files", result.files_deleted);
```

### Combined Maintenance

```rust
use iceberg_compaction_core::cleanup::{Maintenance, MaintenanceConfigBuilder};
use std::time::Duration;

let config = MaintenanceConfigBuilder::default()
    .expire_snapshots(Some(
        ExpireSnapshotsConfigBuilder::default()
            .older_than(Duration::from_secs(3 * 24 * 60 * 60))
            .retain_last(10u32)
            .build()?
    ))
    .remove_orphans(Some(
        RemoveOrphanFilesConfigBuilder::default()
            .older_than(Duration::from_secs(3 * 24 * 60 * 60))
            .build()?
    ))
    .build()?;

let result = Maintenance::new(table, config).execute().await?;
```

### Production Example

See `examples/cleanup/src/main.rs` for a complete working example including:
- REST catalog configuration
- Multiple usage patterns
- Production configuration template

## Configuration Constants

| Constant | Default | Description |
|----------|---------|-------------|
| `DEFAULT_OLDER_THAN_DAYS` | 7 | Default retention period |
| `DEFAULT_MIN_SNAPSHOTS_TO_KEEP` | 1 | Minimum snapshots to retain |
| `DEFAULT_LOAD_CONCURRENCY` | 16 | Concurrency for loading manifests |
| `DEFAULT_DELETE_CONCURRENCY` | 10 | Concurrency for file deletion |

## Safety Considerations

### Retention Period

The default 7-day retention period is intentional:
- Protects in-progress writes that haven't been committed yet
- Allows time to recover from accidental operations
- Matches iceberg-rust's default behavior

### Dry-Run Mode

Always test with `dry_run(true)` before running in production:
```rust
// First, preview what would happen
let config = ExpireSnapshotsConfigBuilder::default()
    .dry_run(true)
    .build()?;
let result = SnapshotExpiration::new(table, config).execute().await?;

// Then, if satisfied, run for real
let config = ExpireSnapshotsConfigBuilder::default()
    .dry_run(false)
    .build()?;
```

### Branch Protection

Snapshots referenced by branches or tags are automatically protected and will not be expired.

### Error Handling

File deletion uses best-effort semantics:
- Individual file deletion failures are logged as warnings
- Failures do not abort the entire operation
- This prevents transient errors from blocking cleanup

## Testing

### Unit Tests

Run unit tests with:
```bash
cargo test -p iceberg-compaction-core cleanup
```

### Integration Tests

Run integration tests (requires Docker for test fixtures):
```bash
cd integration-tests && cargo test cleanup
```

Integration tests cover:
- Snapshot expiration dry-run
- Orphan cleanup dry-run
- Combined maintenance workflow
- Partial configurations (expire-only, orphan-only)

## Future Improvements

### Short Term

1. **Integrate Transaction API**: Update `SnapshotExpiration` to use `Transaction::expire_snapshot()` for atomic metadata updates
2. **Bytes freed tracking**: Calculate and report actual bytes freed in orphan cleanup
3. **Error categorization**: Distinguish between transient and permanent errors

### Medium Term

1. **Rewrite manifests**: Add manifest compaction to reduce metadata size
2. **Partition-level cleanup**: Support cleaning specific partitions
3. **Scheduled maintenance**: Built-in scheduling for regular cleanup

### Long Term

1. **Incremental cleanup**: Track and clean files incrementally rather than full scans
2. **Cross-table cleanup**: Handle orphan files across related tables
3. **Policy-based retention**: Support complex retention policies (e.g., daily for 7 days, weekly for 4 weeks)

## Dependencies

This feature leverages the following from iceberg-rust (risingwavelabs fork):

- `RemoveOrphanFilesAction` (PR #116, merged Jan 28, 2026)
- `cleanup_expired_files()` (PR #112, merged Jan 16, 2026)
- `Transaction::expire_snapshot()` (existing)

Current dependency: `iceberg @ 72b0729b94435a958554e009a940502a3ebeb88a`

## References

- [iceberg-rust PR #112: cleanup_expired_files](https://github.com/risingwavelabs/iceberg-rust/pull/112)
- [iceberg-rust PR #116: delete_orphan_files](https://github.com/risingwavelabs/iceberg-rust/pull/116)
- [iceberg-compaction Issue #115](https://github.com/nimtable/iceberg-compaction/issues/115)
