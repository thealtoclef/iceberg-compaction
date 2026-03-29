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

//! Example: Snapshot expiration and orphan file cleanup
//!
//! This example demonstrates how to use the cleanup module to:
//! 1. Expire old snapshots with configurable retention
//! 2. Clean up orphaned files
//! 3. Run full maintenance workflow

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iceberg_compaction_core::cleanup::{
    ExpireSnapshotsConfigBuilder, Maintenance, MaintenanceConfigBuilder, OrphanFileCleanup,
    RemoveOrphanFilesConfigBuilder, SnapshotExpiration,
};
use iceberg_compaction_core::iceberg::io::{
    S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_compaction_core::iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure the warehouse and catalog
    let mut iceberg_configs = HashMap::new();

    let load_credentials_from_env = true;
    if load_credentials_from_env {
        iceberg_configs.insert(S3_DISABLE_CONFIG_LOAD.to_owned(), "false".to_owned());
    } else {
        iceberg_configs.insert(S3_DISABLE_CONFIG_LOAD.to_owned(), "true".to_owned());
        iceberg_configs.insert(S3_REGION.to_owned(), "us-east-1".to_owned());
        iceberg_configs.insert(S3_ENDPOINT.to_owned(), "http://localhost:9000".to_owned());
        iceberg_configs.insert(S3_ACCESS_KEY_ID.to_owned(), "minioadmin".to_owned());
        iceberg_configs.insert(S3_SECRET_ACCESS_KEY.to_owned(), "minioadmin".to_owned());
    }

    let catalog = Arc::new(
        iceberg_compaction_core::iceberg_catalog_rest::RestCatalogBuilder::default()
            .load("rest", iceberg_configs)
            .await
            .expect("failed to build rest catalog"),
    );

    let namespace_ident = NamespaceIdent::new("my_namespace".into());
    let table_ident = TableIdent::new(namespace_ident, "my_table".into());

    // Load the table
    let table = catalog.load_table(&table_ident).await?;

    // Example 1: Snapshot Expiration Only (Dry Run)
    println!("=== Example 1: Expire Snapshots (Dry Run) ===");
    let expire_config = ExpireSnapshotsConfigBuilder::default()
        .older_than(Duration::from_secs(7 * 24 * 60 * 60))
        .retain_last(3u32)
        .dry_run(true) // Preview first
        .build()?;

    let expiration = SnapshotExpiration::new(table.clone(), expire_config);
    let expire_result = expiration.execute().await?;
    println!(
        "Would expire {} snapshots, cleanup {} files",
        expire_result.snapshots_expired, expire_result.data_files_cleaned
    );
    println!("Expired snapshot IDs: {:?}", expire_result.expired_snapshot_ids);

    // Example 2: Orphan File Cleanup Only (Dry Run)
    println!("\n=== Example 2: Orphan File Cleanup (Dry Run) ===");
    let orphan_config = RemoveOrphanFilesConfigBuilder::default()
        .older_than(Duration::from_secs(7 * 24 * 60 * 60))
        .dry_run(true)
        .build()?;

    let cleanup = OrphanFileCleanup::new(table.clone(), orphan_config);
    let orphan_result = cleanup.execute().await?;
    println!(
        "Found {} orphan files ({} bytes)",
        orphan_result.orphan_files.len(),
        orphan_result.bytes_freed
    );

    // Example 3: Full Maintenance (Both Operations)
    println!("\n=== Example 3: Full Maintenance ===");
    let maintenance_config = MaintenanceConfigBuilder::default()
        .expire_snapshots(Some(
            ExpireSnapshotsConfigBuilder::default()
                .older_than(Duration::from_secs(3 * 24 * 60 * 60))
                .retain_last(10u32)
                .build()?,
        ))
        .remove_orphans(Some(
            RemoveOrphanFilesConfigBuilder::default()
                .older_than(Duration::from_secs(3 * 24 * 60 * 60))
                .build()?,
        ))
        .build()?;

    let maintenance = Maintenance::new(table.clone(), maintenance_config);
    let maintenance_result = maintenance.execute().await?;

    if let Some(expire) = &maintenance_result.expire_result {
        println!(
            "Expired {} snapshots, cleaned up {} files",
            expire.snapshots_expired, expire.data_files_cleaned
        );
    }
    if let Some(orphan) = &maintenance_result.orphan_result {
        println!(
            "Deleted {} orphan files, freed {} bytes",
            orphan.files_deleted, orphan.bytes_freed
        );
    }

    // Example 4: Production Configuration
    // In production, you would typically:
    // - Run with dry_run=false to actually delete files
    // - Use appropriate retention periods based on your SLA
    // - Schedule maintenance during low-traffic periods
    println!("\n=== Example 4: Production Configuration ===");
    let production_config = MaintenanceConfigBuilder::default()
        .expire_snapshots(Some(
            ExpireSnapshotsConfigBuilder::default()
                .older_than(Duration::from_secs(7 * 24 * 60 * 60)) // Keep 7 days of snapshots
                .retain_last(5u32) // Always keep at least 5 snapshots
                .dry_run(false) // Actually delete files
                .build()?,
        ))
        .remove_orphans(Some(
            RemoveOrphanFilesConfigBuilder::default()
                .older_than(Duration::from_secs(7 * 24 * 60 * 60)) // Only delete files older than 7 days
                .dry_run(false) // Actually delete orphans
                .build()?,
        ))
        .build()?;

    println!("Production configuration ready:");
    println!(
        "  - Expire snapshots older than {} days",
        production_config
            .expire_snapshots
            .as_ref()
            .map(|c| c.older_than.as_secs() / 86400)
            .unwrap_or(0)
    );
    println!(
        "  - Retain at least {} most recent snapshots",
        production_config
            .expire_snapshots
            .as_ref()
            .map(|c| c.retain_last)
            .unwrap_or(0)
    );
    println!(
        "  - Clean orphans older than {} days",
        production_config
            .remove_orphans
            .as_ref()
            .map(|c| c.older_than.as_secs() / 86400)
            .unwrap_or(0)
    );

    // Note: Uncomment to run production cleanup
    // let maintenance = Maintenance::new(table.clone(), production_config);
    // let result = maintenance.execute().await?;
    // println!("Maintenance complete: {:?}", result);

    Ok(())
}
