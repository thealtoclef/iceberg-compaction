/*
 * Copyright 2025 iceberg-compaction
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Orphan file cleanup operations.

use derive_builder::Builder;
use iceberg::table::Table;
use iceberg::Result;
use std::time::Duration;
use tracing::info;

use super::{DEFAULT_DELETE_CONCURRENCY, DEFAULT_LOAD_CONCURRENCY, DEFAULT_OLDER_THAN_DAYS};

/// Configuration for orphan file cleanup.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into, strip_option))]
pub struct RemoveOrphanFilesConfig {
    /// Only delete files older than this duration.
    #[builder(default = "Duration::from_secs(DEFAULT_OLDER_THAN_DAYS * 24 * 60 * 60)")]
    pub older_than: Duration,

    /// Perform actual deletion (false = dry run).
    #[builder(default = "false")]
    pub dry_run: bool,

    /// Concurrency for loading files.
    #[builder(default = "DEFAULT_LOAD_CONCURRENCY")]
    pub load_concurrency: usize,

    /// Concurrency for deleting files.
    #[builder(default = "DEFAULT_DELETE_CONCURRENCY")]
    pub delete_concurrency: usize,
}

impl Default for RemoveOrphanFilesConfig {
    fn default() -> Self {
        RemoveOrphanFilesConfigBuilder::default()
            .build()
            .expect("RemoveOrphanFilesConfig default should always build")
    }
}

/// Result of orphan file cleanup.
#[derive(Debug, Default)]
pub struct RemoveOrphanFilesResult {
    /// List of orphan files found.
    pub orphan_files: Vec<String>,
    /// Number of files actually deleted.
    pub files_deleted: usize,
    /// Total bytes freed.
    pub bytes_freed: u64,
}

/// Orphan file cleanup operations.
///
/// This operation identifies and deletes files that are:
/// - Not referenced by any current snapshot
/// - Files from expired snapshots (after running snapshot expiration)
/// - Files from failed writes that were never committed to any manifest
/// - Manifest files, manifest list files, and data files that are no longer reachable
///
/// # What Gets Cleaned
/// - Data files not referenced by any snapshot
/// - Manifest files from expired snapshots
/// - Manifest list files from expired snapshots
/// - Files from failed/interrupted uploads that never made it into a manifest
///
/// # Safety
/// - Only deletes files that are not referenced by any current snapshot
/// - Respects the `older_than` retention period to protect in-progress writes
/// - Files without timestamps are skipped (protection against deleting active writes)
/// - Dry-run mode available for preview
pub struct OrphanFileCleanup {
    table: Table,
    config: RemoveOrphanFilesConfig,
}

impl OrphanFileCleanup {
    pub fn new(table: Table, config: RemoveOrphanFilesConfig) -> Self {
        Self { table, config }
    }

    /// Execute orphan file cleanup.
    ///
    /// # Returns
    /// Result containing cleanup details, or error if operation fails.
    pub async fn execute(self) -> Result<RemoveOrphanFilesResult> {
        info!(
            "Starting orphan file cleanup (older than {:?}, dry_run={})",
            self.config.older_than, self.config.dry_run
        );

        // Use the upstream RemoveOrphanFilesAction from iceberg-rust
        use iceberg::actions::RemoveOrphanFilesAction;

        let action = RemoveOrphanFilesAction::new(self.table)
            .older_than(self.config.older_than)
            .dry_run(self.config.dry_run)
            .load_concurrency(self.config.load_concurrency)
            .delete_concurrency(self.config.delete_concurrency);

        let orphan_files = action.execute().await?;

        let files_deleted = if self.config.dry_run {
            info!(
                "Dry run complete: found {} orphan files (not deleted)",
                orphan_files.len()
            );
            0
        } else {
            info!(
                "Orphan cleanup complete: deleted {} files",
                orphan_files.len()
            );
            orphan_files.len()
        };

        Ok(RemoveOrphanFilesResult {
            orphan_files,
            files_deleted,
            bytes_freed: 0, // Would need additional logic to calculate bytes
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_orphan_files_config_default() {
        let config = RemoveOrphanFilesConfig::default();
        assert_eq!(config.older_than, Duration::from_secs(7 * 24 * 60 * 60));
        assert_eq!(config.load_concurrency, DEFAULT_LOAD_CONCURRENCY);
        assert_eq!(config.delete_concurrency, DEFAULT_DELETE_CONCURRENCY);
        assert!(!config.dry_run);
    }

    #[test]
    fn test_remove_orphan_files_config_builder() {
        let config = RemoveOrphanFilesConfigBuilder::default()
            .older_than(Duration::from_secs(14 * 24 * 60 * 60))
            .dry_run(true)
            .load_concurrency(32usize)
            .delete_concurrency(20usize)
            .build()
            .unwrap();

        assert_eq!(config.older_than, Duration::from_secs(14 * 24 * 60 * 60));
        assert!(config.dry_run);
        assert_eq!(config.load_concurrency, 32);
        assert_eq!(config.delete_concurrency, 20);
    }

    #[test]
    fn test_remove_orphan_files_result_default() {
        let result = RemoveOrphanFilesResult::default();
        assert!(result.orphan_files.is_empty());
        assert_eq!(result.files_deleted, 0);
        assert_eq!(result.bytes_freed, 0);
    }
}
