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

//! Snapshot expiration and orphan file cleanup operations.
//!
//! This module provides maintenance operations for Iceberg tables:
//! - [`SnapshotExpiration`]: Expire old snapshots and cleanup associated files
//! - [`OrphanFileCleanup`]: Delete files not referenced by any snapshot
//! - [`Maintenance`]: Combined workflow for full table maintenance
//!
//! # Overview
//!
//! The cleanup module provides production-ready table maintenance operations that:
//! - Reduce metadata size by removing old snapshots
//! - Free storage by deleting orphaned files
//! - Support dry-run mode for preview before deletion
//! - Include comprehensive metrics for observability
//!
//! # Example
//!
//! ```rust,no_run
//! use iceberg_compaction_core::cleanup::{SnapshotExpiration, ExpireSnapshotsConfigBuilder};
//! use std::time::Duration;
//!
//! # async fn example(table: iceberg::table::Table) -> Result<(), Box<dyn std::error::Error>> {
//! let config = ExpireSnapshotsConfigBuilder::default()
//!     .older_than(Duration::from_secs(7 * 24 * 60 * 60))
//!     .retain_last(3u32)
//!     .build()?;
//!
//! let result = SnapshotExpiration::new(table, config).execute().await?;
//! println!("Expired {} snapshots", result.snapshots_expired);
//! # Ok(())
//! # }
//! ```

/// Default retention period: 7 days (matches iceberg-rust default)
pub const DEFAULT_OLDER_THAN_DAYS: u64 = 7;
/// Default minimum snapshots to retain
pub const DEFAULT_MIN_SNAPSHOTS_TO_KEEP: u32 = 1;
/// Default concurrency for loading manifest files
pub const DEFAULT_LOAD_CONCURRENCY: usize = 16;
/// Default concurrency for file deletion operations
pub const DEFAULT_DELETE_CONCURRENCY: usize = 10;

mod expire_snapshots;
mod maintenance;
mod orphan_files;

pub use expire_snapshots::{
    ExpireSnapshotsConfig, ExpireSnapshotsConfigBuilder, ExpireSnapshotsResult, SnapshotExpiration,
};
pub use maintenance::{Maintenance, MaintenanceConfig, MaintenanceConfigBuilder, MaintenanceResult};
pub use orphan_files::{
    OrphanFileCleanup, RemoveOrphanFilesConfig, RemoveOrphanFilesConfigBuilder,
    RemoveOrphanFilesResult,
};
