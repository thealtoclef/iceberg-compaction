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

//! Combined maintenance workflow.

use crate::Result;
use derive_builder::Builder;
use iceberg::table::Table;
use tracing::info;

use super::{
    ExpireSnapshotsConfig, ExpireSnapshotsResult, OrphanFileCleanup, RemoveOrphanFilesConfig,
    RemoveOrphanFilesResult, SnapshotExpiration,
};

/// Configuration for full maintenance.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct MaintenanceConfig {
    /// Snapshot expiration config (None = skip expiration)
    #[builder(default)]
    pub expire_snapshots: Option<ExpireSnapshotsConfig>,

    /// Orphan file cleanup config (None = skip orphan cleanup)
    #[builder(default)]
    pub remove_orphans: Option<RemoveOrphanFilesConfig>,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        MaintenanceConfigBuilder::default()
            .build()
            .expect("MaintenanceConfig default should always build")
    }
}

/// Result of full maintenance.
#[derive(Debug, Default)]
pub struct MaintenanceResult {
    pub expire_result: Option<ExpireSnapshotsResult>,
    pub orphan_result: Option<RemoveOrphanFilesResult>,
}

/// Maintenance operations - combines snapshot expiration and orphan cleanup.
///
/// # Execution Order
/// 1. Expire snapshots (metadata operation)
/// 2. Cleanup expired files (storage operation)
/// 3. Remove orphan files (catches remaining unreferenced files)
///
/// # Example
///
/// ```rust,no_run
/// use iceberg_compaction_core::cleanup::{Maintenance, MaintenanceConfigBuilder, ExpireSnapshotsConfigBuilder, RemoveOrphanFilesConfigBuilder};
/// use std::time::Duration;
///
/// # async fn example(table: iceberg::table::Table) -> Result<(), Box<dyn std::error::Error>> {
/// let config = MaintenanceConfigBuilder::default()
///     .expire_snapshots(Some(
///         ExpireSnapshotsConfigBuilder::default()
///             .older_than(Duration::from_secs(7 * 24 * 60 * 60))
///             .build()?
///     ))
///     .remove_orphans(Some(
///         RemoveOrphanFilesConfigBuilder::default()
///             .older_than(Duration::from_secs(7 * 24 * 60 * 60))
///             .build()?
///     ))
///     .build()?;
///
/// let result = Maintenance::new(table, config).execute().await?;
/// # Ok(())
/// # }
/// ```
pub struct Maintenance {
    table: Table,
    config: MaintenanceConfig,
}

impl Maintenance {
    pub fn new(table: Table, config: MaintenanceConfig) -> Self {
        Self { table, config }
    }

    /// Execute full maintenance workflow.
    ///
    /// # Returns
    /// Result containing results from both operations (if enabled).
    pub async fn execute(self) -> Result<MaintenanceResult> {
        info!("Starting Iceberg table maintenance");

        let mut result = MaintenanceResult::default();

        // Step 1: Expire snapshots (if configured)
        if let Some(expire_config) = self.config.expire_snapshots.clone() {
            info!("Phase 1: Expiring snapshots");
            let expiration = SnapshotExpiration::new(self.table.clone(), expire_config);
            let expire_result = expiration.execute().await?;

            info!(
                "Snapshot expiration complete: {} snapshots expired, {} files cleaned",
                expire_result.snapshots_expired, expire_result.data_files_cleaned
            );

            result.expire_result = Some(expire_result);
        } else {
            info!("Phase 1: Skipping snapshot expiration (not configured)");
        }

        // Step 2: Remove orphan files (if configured)
        if let Some(orphan_config) = self.config.remove_orphans.clone() {
            info!("Phase 2: Removing orphan files");
            let cleanup = OrphanFileCleanup::new(self.table.clone(), orphan_config);
            let orphan_result = cleanup.execute().await?;

            info!(
                "Orphan cleanup complete: {} files deleted, {} bytes freed",
                orphan_result.files_deleted, orphan_result.bytes_freed
            );

            result.orphan_result = Some(orphan_result);
        } else {
            info!("Phase 2: Skipping orphan cleanup (not configured)");
        }

        info!("Table maintenance complete");
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cleanup::{ExpireSnapshotsConfigBuilder, RemoveOrphanFilesConfigBuilder};
    use std::time::Duration;

    #[test]
    fn test_maintenance_config_default() {
        let config = MaintenanceConfig::default();
        assert!(config.expire_snapshots.is_none());
        assert!(config.remove_orphans.is_none());
    }

    #[test]
    fn test_maintenance_config_builder() {
        let config = MaintenanceConfigBuilder::default()
            .expire_snapshots(Some(
                ExpireSnapshotsConfigBuilder::default()
                    .older_than(Duration::from_secs(3 * 24 * 60 * 60))
                    .retain_last(5u32)
                    .build()
                    .unwrap(),
            ))
            .remove_orphans(Some(
                RemoveOrphanFilesConfigBuilder::default()
                    .older_than(Duration::from_secs(3 * 24 * 60 * 60))
                    .dry_run(true)
                    .build()
                    .unwrap(),
            ))
            .build()
            .unwrap();

        assert!(config.expire_snapshots.is_some());
        assert!(config.remove_orphans.is_some());

        let expire = config.expire_snapshots.as_ref().unwrap();
        assert_eq!(expire.older_than, Duration::from_secs(3 * 24 * 60 * 60));
        assert_eq!(expire.retain_last, 5);

        let orphan = config.remove_orphans.as_ref().unwrap();
        assert_eq!(orphan.older_than, Duration::from_secs(3 * 24 * 60 * 60));
        assert!(orphan.dry_run);
    }

    #[test]
    fn test_maintenance_result_default() {
        let result = MaintenanceResult::default();
        assert!(result.expire_result.is_none());
        assert!(result.orphan_result.is_none());
    }

    #[test]
    fn test_maintenance_config_partial() {
        // Only expire snapshots
        let config = MaintenanceConfigBuilder::default()
            .expire_snapshots(Some(
                ExpireSnapshotsConfigBuilder::default()
                    .older_than(Duration::from_secs(7 * 24 * 60 * 60))
                    .build()
                    .unwrap(),
            ))
            .build()
            .unwrap();

        assert!(config.expire_snapshots.is_some());
        assert!(config.remove_orphans.is_none());

        // Only orphan cleanup
        let config = MaintenanceConfigBuilder::default()
            .remove_orphans(Some(
                RemoveOrphanFilesConfigBuilder::default()
                    .older_than(Duration::from_secs(7 * 24 * 60 * 60))
                    .build()
                    .unwrap(),
            ))
            .build()
            .unwrap();

        assert!(config.expire_snapshots.is_none());
        assert!(config.remove_orphans.is_some());
    }
}
