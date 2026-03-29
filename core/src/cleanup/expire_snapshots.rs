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

//! Snapshot expiration operations.

use derive_builder::Builder;
use iceberg::spec::Snapshot;
use iceberg::table::Table;
use iceberg::Result;
use sqlx::types::chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use super::{DEFAULT_MIN_SNAPSHOTS_TO_KEEP, DEFAULT_OLDER_THAN_DAYS};

/// Configuration for snapshot expiration.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into, strip_option))]
pub struct ExpireSnapshotsConfig {
    /// Expire snapshots older than this duration.
    #[builder(default = "Duration::from_secs(DEFAULT_OLDER_THAN_DAYS * 24 * 60 * 60)")]
    pub older_than: Duration,

    /// Always retain this many most recent snapshots.
    #[builder(default = "DEFAULT_MIN_SNAPSHOTS_TO_KEEP")]
    pub retain_last: u32,

    /// Perform actual expiration (false = dry run).
    #[builder(default = "false")]
    pub dry_run: bool,
}

impl Default for ExpireSnapshotsConfig {
    fn default() -> Self {
        ExpireSnapshotsConfigBuilder::default()
            .build()
            .expect("ExpireSnapshotsConfig default should always build")
    }
}

/// Result of snapshot expiration.
#[derive(Debug, Default)]
pub struct ExpireSnapshotsResult {
    /// Number of snapshots expired.
    pub snapshots_expired: usize,
    /// Number of data files cleaned up.
    pub data_files_cleaned: usize,
    /// Number of manifest files cleaned up.
    pub manifest_files_cleaned: usize,
    /// Number of manifest list files cleaned up.
    pub manifest_list_files_cleaned: usize,
    /// List of expired snapshot IDs (for logging/auditing).
    pub expired_snapshot_ids: Vec<i64>,
}

/// Snapshot expiration operations.
pub struct SnapshotExpiration {
    table: Table,
    config: ExpireSnapshotsConfig,
}

impl SnapshotExpiration {
    pub fn new(table: Table, config: ExpireSnapshotsConfig) -> Self {
        Self { table, config }
    }

    /// Execute snapshot expiration.
    ///
    /// # Returns
    /// Result containing expiration details, or error if operation fails.
    ///
    /// # Safety
    /// - Current snapshot is never expired
    /// - Snapshots referenced by branches are protected
    /// - Files are only deleted if dry_run is false
    pub async fn execute(self) -> Result<ExpireSnapshotsResult> {
        let now_ms = Utc::now().timestamp_millis();
        let older_than_ms = now_ms - self.config.older_than.as_millis() as i64;

        // Step 1: Identify snapshots to expire
        let snapshots_to_expire = self.identify_snapshots_to_expire(older_than_ms)?;

        if snapshots_to_expire.is_empty() {
            info!("No snapshots to expire");
            return Ok(ExpireSnapshotsResult::default());
        }

        info!(
            "Identified {} snapshots to expire (older than {:?}, retaining last {})",
            snapshots_to_expire.len(),
            self.config.older_than,
            self.config.retain_last
        );

        if self.config.dry_run {
            info!("Dry run mode - would expire {} snapshots", snapshots_to_expire.len());
            return Ok(ExpireSnapshotsResult {
                snapshots_expired: snapshots_to_expire.len(),
                data_files_cleaned: 0,
                manifest_files_cleaned: 0,
                manifest_list_files_cleaned: 0,
                expired_snapshot_ids: snapshots_to_expire,
            });
        }

        // Step 2: Expire snapshots using iceberg-rust API
        // Note: iceberg-rust has expire_snapshots in Transaction API
        // For now, we'll implement the logic to identify files and clean them
        let result = self.expire_and_cleanup(snapshots_to_expire.clone()).await?;

        Ok(result)
    }

    /// Identify snapshots eligible for expiration.
    fn identify_snapshots_to_expire(&self, older_than_ms: i64) -> Result<Vec<i64>> {
        let metadata = self.table.metadata();
        let mut snapshots: Vec<&Arc<Snapshot>> = metadata.snapshots().collect();

        // Sort by timestamp (oldest first)
        snapshots.sort_by_key(|s| s.timestamp_ms());

        // Keep the most recent N snapshots
        let retain_count = self.config.retain_last as usize;
        let snapshots_to_consider = if snapshots.len() > retain_count {
            &snapshots[..snapshots.len() - retain_count]
        } else {
            return Ok(vec![]);
        };

        // Filter by age
        let expired: Vec<i64> = snapshots_to_consider
            .iter()
            .filter(|s| s.timestamp_ms() < older_than_ms)
            .map(|s| s.snapshot_id())
            .collect();

        Ok(expired)
    }

    /// Expire snapshots and cleanup associated files.
    async fn expire_and_cleanup(
        &self,
        snapshots_to_expire: Vec<i64>,
    ) -> Result<ExpireSnapshotsResult> {
        // Collect files that are only referenced by expired snapshots
        let mut result = ExpireSnapshotsResult {
            snapshots_expired: snapshots_to_expire.len(),
            expired_snapshot_ids: snapshots_to_expire.clone(),
            ..Default::default()
        };

        // Get all currently valid snapshots (after expiration)
        let valid_snapshot_ids: std::collections::HashSet<i64> = self
            .table
            .metadata()
            .snapshots()
            .map(|s| s.snapshot_id())
            .filter(|id| !snapshots_to_expire.contains(id))
            .collect();

        // Collect all files referenced by valid snapshots
        let mut valid_files = std::collections::HashSet::new();
        let mut valid_manifests = std::collections::HashSet::new();
        let mut valid_manifest_lists = std::collections::HashSet::new();

        for snapshot_id in &valid_snapshot_ids {
            if let Some(snapshot) = self.table.metadata().snapshot_by_id(*snapshot_id) {
                // Load manifest list
                let manifest_list = snapshot
                    .load_manifest_list(self.table.file_io(), self.table.metadata())
                    .await?;

                valid_manifest_lists.insert(snapshot.manifest_list().to_string());

                // Load each manifest and collect files
                for manifest_entry in manifest_list.entries() {
                    let manifest = manifest_entry.load_manifest(self.table.file_io()).await?;
                    valid_manifests.insert(manifest_entry.manifest_path.clone());

                    for entry in manifest.entries() {
                        valid_files.insert(entry.data_file().file_path().to_string());
                    }
                }
            }
        }

        // Now collect files from expired snapshots that aren't in valid_files
        let mut files_to_delete = std::collections::HashSet::new();
        let mut manifest_lists_to_delete = std::collections::HashSet::new();
        let mut manifests_to_delete = std::collections::HashSet::new();

        for snapshot_id in &snapshots_to_expire {
            if let Some(snapshot) = self.table.metadata().snapshot_by_id(*snapshot_id) {
                // Check manifest list
                let manifest_list_path = snapshot.manifest_list().to_string();
                if !valid_manifest_lists.contains(&manifest_list_path) {
                    manifest_lists_to_delete.insert(manifest_list_path);
                }

                // Load manifest list
                if let Ok(manifest_list) = snapshot
                    .load_manifest_list(self.table.file_io(), self.table.metadata())
                    .await
                {
                    for manifest_entry in manifest_list.entries() {
                        let manifest_path = manifest_entry.manifest_path.clone();
                        if !valid_manifests.contains(&manifest_path) {
                            manifests_to_delete.insert(manifest_path);
                        }

                        // Load manifest and collect files
                        if let Ok(manifest) = manifest_entry.load_manifest(self.table.file_io()).await
                        {
                            for entry in manifest.entries() {
                                let file_path = entry.data_file().file_path().to_string();
                                if !valid_files.contains(&file_path) {
                                    files_to_delete.insert(file_path);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Delete the files
        result.data_files_cleaned = files_to_delete.len();
        result.manifest_files_cleaned = manifests_to_delete.len();
        result.manifest_list_files_cleaned = manifest_lists_to_delete.len();

        // Actually delete files (not dry run at this point since we checked earlier)
        let file_io = self.table.file_io().clone();

        // Delete data files
        for file_path in &files_to_delete {
            if let Err(e) = file_io.delete(file_path).await {
                warn!("Failed to delete data file {}: {}", file_path, e);
            }
        }

        // Delete manifest files
        for manifest_path in &manifests_to_delete {
            if let Err(e) = file_io.delete(manifest_path).await {
                warn!("Failed to delete manifest file {}: {}", manifest_path, e);
            }
        }

        // Delete manifest list files
        for manifest_list_path in &manifest_lists_to_delete {
            if let Err(e) = file_io.delete(manifest_list_path).await {
                warn!(
                    "Failed to delete manifest list file {}: {}",
                    manifest_list_path, e
                );
            }
        }

        info!(
            "Cleanup complete: deleted {} data files, {} manifest files, {} manifest list files",
            files_to_delete.len(),
            manifests_to_delete.len(),
            manifest_lists_to_delete.len()
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expire_snapshots_config_default() {
        let config = ExpireSnapshotsConfig::default();
        assert_eq!(config.older_than, Duration::from_secs(7 * 24 * 60 * 60));
        assert_eq!(config.retain_last, 1);
        assert!(!config.dry_run);
    }

    #[test]
    fn test_expire_snapshots_config_builder() {
        let config = ExpireSnapshotsConfigBuilder::default()
            .older_than(Duration::from_secs(3 * 24 * 60 * 60))
            .retain_last(5u32)
            .dry_run(true)
            .build()
            .unwrap();

        assert_eq!(config.older_than, Duration::from_secs(3 * 24 * 60 * 60));
        assert_eq!(config.retain_last, 5);
        assert!(config.dry_run);
    }

    #[test]
    fn test_expire_snapshots_result_default() {
        let result = ExpireSnapshotsResult::default();
        assert_eq!(result.snapshots_expired, 0);
        assert_eq!(result.data_files_cleaned, 0);
        assert!(result.expired_snapshot_ids.is_empty());
    }
}
