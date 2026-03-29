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

//! iceberg-compaction CLI - Apache Iceberg table compaction and maintenance
//!
//! # Examples
//!
//! ```bash
//! # Run compaction on a table
//! iceberg-compaction compact --config config.yaml
//!
//! # Run cleanup (snapshot expiration + orphan removal)
//! iceberg-compaction cleanup --config config.yaml
//!
//! # Dry-run mode
//! iceberg-compaction compact --config config.yaml --dry-run
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use iceberg::NamespaceIdent;
use iceberg_compaction_core::cleanup::{
    ExpireSnapshotsConfigBuilder, Maintenance, MaintenanceConfigBuilder,
    RemoveOrphanFilesConfigBuilder,
};
use iceberg_compaction_core::compaction::CompactionBuilder;
use iceberg_compaction_core::config::CompactionConfigBuilder;
use iceberg_compaction_core::iceberg::{Catalog, CatalogBuilder, TableIdent};
use serde::Deserialize;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

/// iceberg-compaction - Apache Iceberg table compaction and maintenance CLI
#[derive(Parser)]
#[command(name = "iceberg-compaction")]
#[command(author = "iceberg-compaction contributors")]
#[command(version = "0.1.0")]
#[command(about = "Apache Iceberg table compaction and maintenance", long_about = None)]
struct Cli {
    /// Path to configuration file (YAML or TOML)
    #[arg(short, long, env = "ICEBERG_COMPACTION_CONFIG")]
    config: PathBuf,

    /// Output format
    #[arg(short, long, value_enum, default_value = "text")]
    format: OutputFormat,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compact an Iceberg table
    Compact {
        /// Catalog name (from config)
        #[arg(short, long)]
        catalog: Option<String>,

        /// Namespace (dot-separated, e.g., "prod.analytics")
        #[arg(short, long)]
        namespace: Option<String>,

        /// Table name
        #[arg(short, long)]
        table: Option<String>,

        /// Dry run (show what would happen without making changes)
        #[arg(long, default_value = "false")]
        dry_run: bool,
    },

    /// Run cleanup (expire snapshots + remove orphan files)
    Cleanup {
        /// Catalog name (from config)
        #[arg(short, long)]
        catalog: Option<String>,

        /// Namespace (dot-separated, e.g., "prod.analytics")
        #[arg(short, long)]
        namespace: Option<String>,

        /// Table name
        #[arg(short, long)]
        table: Option<String>,

        /// Dry run (show what would happen without making changes)
        #[arg(long, default_value = "false")]
        dry_run: bool,

        /// Snapshots older than this duration to expire (e.g., "7d", "24h")
        #[arg(long, default_value = "7d")]
        older_than: String,

        /// Minimum number of snapshots to retain
        #[arg(long, default_value = "3")]
        retain_last: u32,

        /// Orphan files older than this duration to remove (e.g., "7d", "24h")
        #[arg(long, default_value = "7d")]
        orphan_older_than: String,
    },

    /// Validate configuration file
    Validate,
}

#[derive(Clone, Debug, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

/// Configuration file structure
#[derive(Debug, Deserialize)]
struct Config {
    /// Catalog configurations
    pub catalogs: Vec<CatalogConfig>,

    /// Default catalog name
    #[serde(default = "default_catalog")]
    pub default_catalog: String,

    /// Default namespace
    #[serde(default)]
    pub default_namespace: Option<String>,

    /// Compaction settings (reserved for future use)
    #[allow(dead_code)]
    #[serde(default)]
    pub compaction: CompactionSettings,

    /// Cleanup settings (reserved for future use)
    #[allow(dead_code)]
    #[serde(default)]
    pub cleanup: CleanupSettings,
}

fn default_catalog() -> String {
    "default".to_string()
}

#[derive(Debug, Deserialize)]
struct CatalogConfig {
    /// Catalog name
    pub name: String,

    /// Catalog type (currently only "rest" supported)
    #[serde(default = "default_catalog_type")]
    pub catalog_type: String,

    /// REST catalog URI
    pub uri: String,

    /// Warehouse location (reserved for future use)
    #[allow(dead_code)]
    pub warehouse: Option<String>,

    /// Storage credentials
    #[serde(default)]
    pub storage: StorageConfig,
}

fn default_catalog_type() -> String {
    "rest".to_string()
}

#[derive(Debug, Deserialize, Default)]
struct StorageConfig {
    /// S3 endpoint (for S3-compatible storage)
    pub endpoint: Option<String>,

    /// S3 region
    pub region: Option<String>,

    /// S3 access key ID
    pub access_key_id: Option<String>,

    /// S3 secret access key
    pub secret_access_key: Option<String>,

    /// GCS service account key (JSON string or path) - reserved for future use
    #[allow(dead_code)]
    pub gcs_key: Option<String>,

    /// ADLS account name - reserved for future use
    #[allow(dead_code)]
    pub adls_account_name: Option<String>,

    /// ADLS account key - reserved for future use
    #[allow(dead_code)]
    pub adls_account_key: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct CompactionSettings {
    /// Target file size in bytes (reserved for future use)
    #[allow(dead_code)]
    #[serde(default = "default_target_file_size")]
    pub target_file_size: u64,

    /// Maximum number of input files per compaction task (reserved for future use)
    #[allow(dead_code)]
    #[serde(default = "default_max_input_files")]
    pub max_input_files: usize,
}

fn default_target_file_size() -> u64 {
    512 * 1024 * 1024 // 512 MB
}

fn default_max_input_files() -> usize {
    100
}

#[derive(Debug, Deserialize, Default)]
struct CleanupSettings {
    /// Expire snapshots older than this duration (reserved for future use)
    #[allow(dead_code)]
    #[serde(default = "default_older_than")]
    pub older_than: String,

    /// Minimum snapshots to retain (reserved for future use)
    #[allow(dead_code)]
    #[serde(default = "default_retain_last")]
    pub retain_last: u32,

    /// Remove orphan files older than this duration (reserved for future use)
    #[allow(dead_code)]
    #[serde(default = "default_orphan_older_than")]
    pub orphan_older_than: String,
}

fn default_older_than() -> String {
    "7d".to_string()
}

fn default_retain_last() -> u32 {
    3
}

fn default_orphan_older_than() -> String {
    "7d".to_string()
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(log_level.parse().unwrap()))
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    // Load configuration
    let config = load_config(&cli.config)?;

    // Execute command
    match cli.command {
        Commands::Compact {
            catalog,
            namespace,
            table,
            dry_run,
        } => {
            run_compact(&config, catalog, namespace, table, dry_run, &cli.format).await?;
        }
        Commands::Cleanup {
            catalog,
            namespace,
            table,
            dry_run,
            older_than,
            retain_last,
            orphan_older_than,
        } => {
            run_cleanup(
                &config,
                catalog,
                namespace,
                table,
                dry_run,
                &older_than,
                retain_last,
                &orphan_older_than,
                &cli.format,
            )
            .await?;
        }
        Commands::Validate => {
            println!("Configuration file is valid: {}", cli.config.display());
            println!("\nCatalogs configured:");
            for catalog in &config.catalogs {
                println!("  - {} (type: {}, uri: {})", catalog.name, catalog.catalog_type, catalog.uri);
            }
            if !config.default_namespace.is_some() {
                println!("\nDefault namespace: not set (must be specified per-command)");
            } else {
                println!("\nDefault namespace: {}", config.default_namespace.as_ref().unwrap());
            }
        }
    }

    Ok(())
}

/// Load configuration from YAML or TOML file
fn load_config(path: &PathBuf) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;

    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("yaml");

    match extension.to_lowercase().as_str() {
        "yaml" | "yml" => {
            serde_yaml::from_str(&content).with_context(|| "Failed to parse YAML config")
        }
        "toml" => {
            // Support TOML via serde_yaml's loose parsing or convert
            // For now, we'll try to parse TOML as YAML (works for simple configs)
            // For full TOML support, add toml crate
            toml::from_str(&content).with_context(|| "Failed to parse TOML config")
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported config file extension: {}. Use .yaml, .yml, or .toml",
            extension
        )),
    }
}

/// Parse duration string (e.g., "7d", "24h", "60m", "3600s")
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.ends_with('d') {
        let days: u64 = s[..s.len() - 1].parse().context("Invalid days value")?;
        Ok(Duration::from_secs(days * 24 * 60 * 60))
    } else if s.ends_with('h') {
        let hours: u64 = s[..s.len() - 1].parse().context("Invalid hours value")?;
        Ok(Duration::from_secs(hours * 60 * 60))
    } else if s.ends_with('m') {
        let mins: u64 = s[..s.len() - 1].parse().context("Invalid minutes value")?;
        Ok(Duration::from_secs(mins * 60))
    } else if s.ends_with('s') {
        let secs: u64 = s[..s.len() - 1].parse().context("Invalid seconds value")?;
        Ok(Duration::from_secs(secs))
    } else {
        // Default to seconds
        let secs: u64 = s.parse().context("Invalid duration value")?;
        Ok(Duration::from_secs(secs))
    }
}

/// Get catalog from config by name
fn get_catalog_config(config: &Config, name: Option<String>) -> Result<&CatalogConfig> {
    let name = name.unwrap_or_else(|| config.default_catalog.clone());
    config
        .catalogs
        .iter()
        .find(|c| c.name == name)
        .with_context(|| format!("Catalog '{}' not found in config", name))
}

/// Build Iceberg catalog from config
async fn build_catalog(catalog_config: &CatalogConfig) -> Result<Arc<dyn Catalog>> {
    let mut props = std::collections::HashMap::new();

    // Add storage properties
    if let Some(endpoint) = &catalog_config.storage.endpoint {
        props.insert("s3.endpoint".to_string(), endpoint.clone());
    }
    if let Some(region) = &catalog_config.storage.region {
        props.insert("s3.region".to_string(), region.clone());
    }
    if let Some(access_key) = &catalog_config.storage.access_key_id {
        props.insert("s3.access-key-id".to_string(), access_key.clone());
    }
    if let Some(secret_key) = &catalog_config.storage.secret_access_key {
        props.insert("s3.secret-access-key".to_string(), secret_key.clone());
    }

    // Disable AWS config load if endpoint is specified (for MinIO, etc.)
    if catalog_config.storage.endpoint.is_some() {
        props.insert("s3.disable-config-load".to_string(), "true".to_string());
    }

    // Build REST catalog
    let catalog = iceberg_compaction_core::iceberg_catalog_rest::RestCatalogBuilder::default()
        .load(&catalog_config.name, props)
        .await
        .with_context(|| format!("Failed to build catalog '{}'", catalog_config.name))?;

    Ok(Arc::new(catalog))
}

/// Run compaction command
async fn run_compact(
    config: &Config,
    catalog_name: Option<String>,
    namespace: Option<String>,
    table_name: Option<String>,
    dry_run: bool,
    format: &OutputFormat,
) -> Result<()> {
    let catalog_config = get_catalog_config(config, catalog_name)?;
    let catalog = build_catalog(catalog_config).await?;

    // Determine namespace
    let namespace_str = namespace
        .or(config.default_namespace.clone())
        .ok_or_else(|| anyhow::anyhow!("Namespace not specified. Use --namespace or set default_namespace in config"))?;

    // Determine table name
    let table_name = table_name.ok_or_else(|| anyhow::anyhow!("Table name not specified. Use --table"))?;

    // Parse namespace (dot-separated, e.g., "prod.analytics" or "default")
    let namespace_parts: Vec<&str> = namespace_str.split('.').collect();
    let namespace_ident = NamespaceIdent::new(namespace_parts.into_iter().map(String::from).collect());

    let table_ident = TableIdent::new(namespace_ident, table_name);

    if dry_run {
        info!("DRY RUN: No changes will be made");
    }

    info!("Starting compaction for table: {}", table_ident);

    let compaction_config = CompactionConfigBuilder::default()
        .build()
        .context("Failed to build compaction config")?;

    let compaction = CompactionBuilder::new(catalog, table_ident.clone())
        .with_config(Arc::new(compaction_config))
        .with_catalog_name(catalog_config.name.clone())
        .build();

    match compaction.compact().await {
        Ok(Some(result)) => {
            let stats = &result.stats;
            match format {
                OutputFormat::Text => {
                    info!("Compaction completed successfully!");
                    println!("Compaction Results:");
                    println!("  Input files:  {}", stats.input_files_count);
                    println!("  Output files: {}", stats.output_files_count);
                    println!("  Input bytes:  {}", stats.input_total_bytes);
                    println!("  Output bytes: {}", stats.output_total_bytes);
                    let ratio = if stats.input_total_bytes > 0 {
                        (stats.input_total_bytes as f64 / stats.output_total_bytes as f64) * 100.0 - 100.0
                    } else {
                        0.0
                    };
                    println!("  Size reduction: {:.1}%", ratio);
                }
                OutputFormat::Json => {
                    let json = serde_json::json!({
                        "status": "success",
                        "input_files_count": stats.input_files_count,
                        "output_files_count": stats.output_files_count,
                        "input_bytes": stats.input_total_bytes,
                        "output_bytes": stats.output_total_bytes,
                    });
                    println!("{}", serde_json::to_string_pretty(&json)?);
                }
            }
        }
        Ok(None) => {
            info!("No compaction needed - table is empty or has no files to compact");
        }
        Err(e) => {
            error!("Compaction failed: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}

/// Run cleanup command
async fn run_cleanup(
    config: &Config,
    catalog_name: Option<String>,
    namespace: Option<String>,
    table_name: Option<String>,
    dry_run: bool,
    older_than: &str,
    retain_last: u32,
    orphan_older_than: &str,
    format: &OutputFormat,
) -> Result<()> {
    let catalog_config = get_catalog_config(config, catalog_name)?;
    let catalog = build_catalog(catalog_config).await?;

    // Determine namespace
    let namespace_str = namespace
        .or(config.default_namespace.clone())
        .ok_or_else(|| anyhow::anyhow!("Namespace not specified. Use --namespace or set default_namespace in config"))?;

    // Determine table name
    let table_name = table_name.ok_or_else(|| anyhow::anyhow!("Table name not specified. Use --table"))?;

    // Parse namespace (dot-separated, e.g., "prod.analytics" or "default")
    let namespace_parts: Vec<&str> = namespace_str.split('.').collect();
    let namespace_ident = NamespaceIdent::new(namespace_parts.into_iter().map(String::from).collect());

    let table_ident = TableIdent::new(namespace_ident, table_name);

    // Load table
    let table = catalog
        .load_table(&table_ident)
        .await
        .with_context(|| format!("Failed to load table: {}", table_ident))?;

    if dry_run {
        info!("DRY RUN: No changes will be made");
    }

    // Parse durations
    let older_than_duration = parse_duration(older_than)
        .with_context(|| format!("Invalid older_than duration: {}", older_than))?;
    let orphan_older_than_duration = parse_duration(orphan_older_than)
        .with_context(|| format!("Invalid orphan_older_than duration: {}", orphan_older_than))?;

    info!("Starting cleanup for table: {}", table_ident);

    // Build maintenance config
    let maintenance_config = MaintenanceConfigBuilder::default()
        .expire_snapshots(Some(
            ExpireSnapshotsConfigBuilder::default()
                .older_than(older_than_duration)
                .retain_last(retain_last)
                .dry_run(dry_run)
                .build()
                .context("Failed to build expire snapshots config")?,
        ))
        .remove_orphans(Some(
            RemoveOrphanFilesConfigBuilder::default()
                .older_than(orphan_older_than_duration)
                .dry_run(dry_run)
                .build()
                .context("Failed to build orphan cleanup config")?,
        ))
        .build()
        .context("Failed to build maintenance config")?;

    let maintenance = Maintenance::new(table, maintenance_config);
    let result = maintenance.execute().await.context("Cleanup execution failed")?;

    // Output results
    match format {
        OutputFormat::Text => {
            info!("Cleanup completed!");
            println!("Cleanup Results:");

            if let Some(expire) = &result.expire_result {
                println!("\nSnapshot Expiration:");
                println!("  Snapshots expired: {}", expire.snapshots_expired);
                println!("  Data files cleaned: {}", expire.data_files_cleaned);
                println!("  Manifest files cleaned: {}", expire.manifest_files_cleaned);
                println!("  Manifest list files cleaned: {}", expire.manifest_list_files_cleaned);
                if !expire.expired_snapshot_ids.is_empty() {
                    println!("  Expired snapshot IDs: {:?}", expire.expired_snapshot_ids);
                }
            } else {
                println!("\nSnapshot Expiration: skipped");
            }

            if let Some(orphan) = &result.orphan_result {
                println!("\nOrphan File Cleanup:");
                println!("  Orphan files found: {}", orphan.orphan_files.len());
                println!("  Files deleted: {}", orphan.files_deleted);
                println!("  Bytes freed: {}", orphan.bytes_freed);
            } else {
                println!("\nOrphan File Cleanup: skipped");
            }
        }
        OutputFormat::Json => {
            let json = serde_json::json!({
                "status": "success",
                "expire_result": result.expire_result.as_ref().map(|e| {
                    serde_json::json!({
                        "snapshots_expired": e.snapshots_expired,
                        "data_files_cleaned": e.data_files_cleaned,
                        "manifest_files_cleaned": e.manifest_files_cleaned,
                        "manifest_list_files_cleaned": e.manifest_list_files_cleaned,
                    })
                }),
                "orphan_result": result.orphan_result.as_ref().map(|o| {
                    serde_json::json!({
                        "orphan_files_found": o.orphan_files.len(),
                        "files_deleted": o.files_deleted,
                        "bytes_freed": o.bytes_freed,
                    })
                }),
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(7 * 24 * 60 * 60));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(24 * 60 * 60));
        assert_eq!(parse_duration("60m").unwrap(), Duration::from_secs(60 * 60));
        assert_eq!(parse_duration("3600s").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("100").unwrap(), Duration::from_secs(100));
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("7x").is_err());
    }
}
