# Usage Guide for iceberg-compaction

This guide covers all ways to use `iceberg-compaction`, from using it as a library to running standalone binaries.

## Table of Contents

1. [Using as a Library Crate](#using-as-a-library-crate)
2. [Running Example Binaries](#running-example-binaries)
3. [Running the Bench Binary](#running-the-bench-binary)
4. [Creating Your Own Binary](#creating-your-own-binary)
5. [Configuration Reference](#configuration-reference)

---

## Using as a Library Crate

`iceberg-compaction` is primarily designed to be used as a Rust library in your own projects.

### Installation

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
iceberg-compaction-core = { git = "https://github.com/nimtable/iceberg-compaction", branch = "main" }
```

### Basic Compaction Example

```rust
use std::collections::HashMap;
use std::sync::Arc;
use iceberg_compaction_core::{
    CompactionBuilder,
    CompactionConfigBuilder,
    iceberg::Catalog,
    iceberg::TableIdent,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure the catalog (REST catalog with S3 storage)
    let mut iceberg_configs = HashMap::new();
    iceberg_configs.insert("s3.endpoint".to_owned(), "http://localhost:9000".to_owned());
    iceberg_configs.insert("s3.region".to_owned(), "us-east-1".to_owned());
    iceberg_configs.insert("s3.access-key-id".to_owned(), "minioadmin".to_owned());
    iceberg_configs.insert("s3.secret-access-key".to_owned(), "minioadmin".to_owned());

    let catalog = Arc::new(
        iceberg_compaction_core::iceberg_catalog_rest::RestCatalogBuilder::default()
            .load("rest", iceberg_configs)
            .await?
    );

    // 2. Define the table to compact
    let table_ident = TableIdent::new(
        iceberg::NamespaceIdent::new("my_namespace".into()),
        "my_table".into()
    );

    // 3. Configure compaction (uses sensible defaults)
    let compaction_config = CompactionConfigBuilder::default().build()?;

    let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
        .with_config(Arc::new(compaction_config))
        .build();

    // 4. Execute compaction
    let result = compaction.compact().await?;

    match result {
        Some(resp) => {
            let stats = &resp.stats;
            println!("Compaction completed: {} files -> {} files",
                     stats.input_files_count, stats.output_files_count);
        }
        None => {
            println!("No compaction needed");
        }
    }

    Ok(())
}
```

### Snapshot Expiration and Orphan Cleanup

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use iceberg_compaction_core::{
    cleanup::{
        SnapshotExpiration, OrphanFileCleanup, Maintenance,
        ExpireSnapshotsConfigBuilder, RemoveOrphanFilesConfigBuilder,
        MaintenanceConfigBuilder,
    },
    iceberg::{Catalog, TableIdent},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup catalog and load table
    let catalog = Arc::new(/* ... catalog setup ... */);
    let table = catalog.load_table(&table_ident).await?;

    // Option A: Expire snapshots only
    let expire_config = ExpireSnapshotsConfigBuilder::default()
        .older_than(Duration::from_secs(7 * 24 * 60 * 60))  // 7 days
        .retain_last(3u32)                                   // Keep 3 most recent
        .dry_run(true)                                       // Preview first
        .build()?;

    let expire_result = SnapshotExpiration::new(table.clone(), expire_config)
        .execute()
        .await?;
    println!("Would expire {} snapshots", expire_result.snapshots_expired);

    // Option B: Orphan cleanup only
    let orphan_config = RemoveOrphanFilesConfigBuilder::default()
        .older_than(Duration::from_secs(7 * 24 * 60 * 60))
        .dry_run(true)
        .build()?;

    let orphan_result = OrphanFileCleanup::new(table.clone(), orphan_config)
        .execute()
        .await?;
    println!("Found {} orphan files", orphan_result.orphan_files.len());

    // Option C: Full maintenance (both operations)
    let maintenance_config = MaintenanceConfigBuilder::default()
        .expire_snapshots(Some(
            ExpireSnapshotsConfigBuilder::default()
                .older_than(Duration::from_secs(3 * 24 * 60 * 60))
                .retain_last(5u32)
                .dry_run(false)  // Actually delete
                .build()?
        ))
        .remove_orphans(Some(
            RemoveOrphanFilesConfigBuilder::default()
                .older_than(Duration::from_secs(3 * 24 * 60 * 60))
                .dry_run(false)
                .build()?
        ))
        .build()?;

    let maintenance_result = Maintenance::new(table.clone(), maintenance_config)
        .execute()
        .await?;

    println!("Maintenance complete");

    Ok(())
}
```

---

## Running Example Binaries

The project includes several example binaries that demonstrate different usage patterns.

### Available Examples

| Example | Description |
|---------|-------------|
| `rest-catalog` | Full compaction example using REST catalog |
| `memory-catalog` | In-memory catalog for testing |
| `cleanup` | Snapshot expiration and orphan cleanup |

### How to Run Examples

```bash
# Run the REST catalog example
cargo run -p iceberg-compaction-example-rest

# Run the memory catalog example
cargo run -p iceberg-compaction-example-memory

# Run the cleanup example
cargo run -p cleanup-example
```

### Customizing Examples

Before running, you may want to modify the example configuration:

1. **Edit catalog settings** - Update S3 endpoint, credentials, namespace, table name
2. **Change compaction parameters** - Modify thresholds, concurrency, etc.
3. **Adjust cleanup retention** - Change snapshot/orphan retention periods

Example configuration in `examples/rest-catalog/src/main.rs`:

```rust
let mut iceberg_configs = HashMap::new();
iceberg_configs.insert("s3.endpoint".to_owned(), "http://your-minio:9000".to_owned());
iceberg_configs.insert("s3.region".to_owned(), "us-west-2".to_owned());
iceberg_configs.insert("s3.access-key-id".to_owned(), "your-key".to_owned());
iceberg_configs.insert("s3.secret-access-key".to_owned(), "your-secret".to_owned());

// Update table identifiers
let namespace_ident = NamespaceIdent::new("your_namespace".into());
let table_ident = TableIdent::new(namespace_ident, "your_table".into());
```

---

## Running the Bench Binary

The `bench` binary is a standalone CLI tool for benchmarking compaction performance.

### Location

```
integration-tests/src/bin/bench.rs
```

### Available Commands

```bash
# Run benchmark (creates table + runs compaction)
cargo run --bin bench -- bench [config_path]

# Run benchmark without creating table (table must exist)
cargo run --bin bench -- bench_without_table [config_path]

# Create mock table only (no compaction)
cargo run --bin bench -- mock_table [config_path]

# Delete table (cleanup)
cargo run --bin bench -- delete_table [config_path]
```

### Configuration File Format

The bench binary uses a YAML configuration file. Example:

```yaml
rest_catalog:
  catalog_name: "test_catalog"
  namespace: "test_db"
  table_name: "test_table"
  uri: "http://localhost:8181"
  s3:
    endpoint: "http://localhost:9000"
    region: "us-east-1"
    access_key: "minioadmin"
    secret_key: "minioadmin"
    bucket: "iceberg-warehouse"

with_compaction_validations: true
```

### Example Usage

```bash
# Run with default config path
cargo run --bin bench

# Run with custom config
cargo run --bin bench -- bench /path/to/config.yaml

# Delete table after testing
cargo run --bin bench -- delete_table /path/to/config.yaml
```

---

## Creating Your Own Binary

To create a standalone binary using `iceberg-compaction`:

### 1. Create Project Structure

```bash
mkdir my-compaction-tool
cd my-compaction-tool
cargo init
```

### 2. Update Cargo.toml

```toml
[package]
name = "my-compaction-tool"
version = "0.1.0"
edition = "2021"

[dependencies]
iceberg-compaction-core = { git = "https://github.com/nimtable/iceberg-compaction", branch = "main" }
iceberg-catalog-rest = { git = "https://github.com/risingwavelabs/iceberg-rust.git", rev = "72b0729" }
tokio = { version = "1", features = ["full"] }
anyhow = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
```

### 3. Create Main Binary

```rust
// src/main.rs
use std::collections::HashMap;
use std::sync::Arc;
use iceberg_compaction_core::{
    CompactionBuilder,
    CompactionConfigBuilder,
    iceberg::{TableIdent, NamespaceIdent},
};
use tracing::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Parse CLI arguments (use clap for production)
    let table_name = std::env::args().nth(1)
        .unwrap_or_else(|| "my_table".to_string());

    // Configure catalog
    let mut configs = HashMap::new();
    configs.insert("s3.endpoint".to_owned(), "http://localhost:9000".to_owned());
    configs.insert("s3.region".to_owned(), "us-east-1".to_owned());
    // ... add credentials from env or config file

    let catalog = Arc::new(
        iceberg_compaction_core::iceberg_catalog_rest::RestCatalogBuilder::default()
            .load("rest", configs)
            .await?
    );

    let table_ident = TableIdent::new(
        NamespaceIdent::new("my_namespace".into()),
        table_name.into()
    );

    info!("Starting compaction for table: {}", table_ident);

    let config = CompactionConfigBuilder::default().build()?;
    let compaction = CompactionBuilder::new(catalog, table_ident)
        .with_config(Arc::new(config))
        .build();

    match compaction.compact().await? {
        Some(result) => {
            info!("Compaction completed successfully");
            info!("  Input files: {}", result.stats.input_files_count);
            info!("  Output files: {}", result.stats.output_files_count);
            info!("  Input bytes: {}", result.stats.input_total_bytes);
            info!("  Output bytes: {}", result.stats.output_total_bytes);
        }
        None => {
            info!("No compaction needed");
        }
    }

    Ok(())
}
```

### 4. Build and Run

```bash
cargo build --release
./target/release/my-compaction-tool my_table_name
```

---

## Configuration Reference

### Compaction Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target_file_size` | 512 MB | Target size for output files |
| `max_file_count` | 100 | Maximum files before triggering compaction |
| `max_file_size` | 1 GB | Maximum file size before compaction |
| `threshold_type` | auto | `auto`, `file_count`, or `file_size` |
| `enable_validation` | true | Validate compaction results |

### Snapshot Expiration Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `older_than` | 7 days | Expire snapshots older than this |
| `retain_last` | 1 | Minimum snapshots to keep |
| `dry_run` | false | Preview without deleting |

### Orphan Cleanup Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `older_than` | 7 days | Only delete files older than this |
| `dry_run` | false | Preview without deleting |
| `load_concurrency` | 16 | Concurrent file listing operations |
| `delete_concurrency` | 10 | Concurrent file deletions |

### Catalog Configuration (REST)

| Key | Description |
|-----|-------------|
| `s3.endpoint` | S3-compatible storage endpoint |
| `s3.region` | S3 region |
| `s3.access-key-id` | Access key |
| `s3.secret-access-key` | Secret key |
| `s3.disable-config-load` | Disable AWS config loading |

---

## Common Patterns

### Production Deployment

For production use, consider:

1. **Credentials from environment**:
```rust
let access_key = std::env::var("AWS_ACCESS_KEY_ID")?;
let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")?;
```

2. **Configuration from file**:
```rust
use serde::Deserialize;

#[derive(Deserialize)]
struct AppConfig {
    catalog_uri: String,
    s3_endpoint: String,
    // ...
}

let config: AppConfig = serde_yaml::from_reader(
    std::fs::File::open("config.yaml")?
)?;
```

3. **Scheduled maintenance**:
```rust
// Use tokio-cron-scheduler or similar
use tokio_cron_scheduler::{Job, JobScheduler};

let sched = JobScheduler::new().await?;
sched.add(Job::new_async("0 0 2 * * *", |_, _| {
    Box::pin(async {
        // Run daily at 2 AM
        run_maintenance().await?;
    })
})?)?;
```

### Error Handling

```rust
use iceberg_compaction_core::CompactionError;

match compaction.compact().await {
    Ok(Some(result)) => { /* success */ }
    Ok(None) => { /* no work needed */ }
    Err(CompactionError::TableNotFound(name)) => { /* handle */ }
    Err(CompactionError::IoError(e)) => { /* handle */ }
    Err(e) => { /* generic error */ }
}
```

---

## See Also

- [Cleanup Feature Documentation](./cleanup-feature.md) - Technical details on snapshot expiration and orphan cleanup
- [README](../README.md) - Project overview and roadmap
