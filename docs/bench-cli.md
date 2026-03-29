# Bench Binary CLI Reference

The `bench` binary is a command-line tool for benchmarking and testing iceberg-compaction operations.

## Location

```
integration-tests/src/bin/bench.rs
```

## Installation

```bash
# Build the binary
cargo build --bin bench

# Binary location
./target/debug/bench
```

## Commands

### `bench` - Run Full Benchmark

Creates a test table and runs compaction benchmark:

```bash
cargo run --bin bench -- bench [config_path]
```

**Default config path:** `integration-tests/config/mock_iceberg_config.yaml`

### `bench_without_table` - Run Without Table Creation

Runs compaction benchmark assuming table already exists:

```bash
cargo run --bin bench -- bench_without_table [config_path]
```

### `mock_table` - Create Test Table Only

Creates a test table without running compaction:

```bash
cargo run --bin bench -- mock_table [config_path]
```

### `delete_table` - Delete Test Table

Deletes the test table (cleanup):

```bash
cargo run --bin bench -- delete_table [config_path]
```

## Configuration File Format

The bench binary uses a YAML configuration file. Here's the full structure:

```yaml
rest_catalog:
  # Catalog identification
  catalog_name: "test_catalog"

  # Table location
  namespace: "test_db"
  table_name: "test_table"

  # REST catalog connection
  uri: "http://localhost:8181"
  warehouse: "s3://iceberg-warehouse/test"

  # S3 storage configuration
  s3:
    endpoint: "http://localhost:9000"
    region: "us-east-1"
    access_key: "minioadmin"
    secret_key: "minioadmin"
    bucket: "iceberg-warehouse"

# Compaction settings
with_compaction_validations: true  # Enable result validation
```

### Configuration Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `rest_catalog.catalog_name` | string | Yes | Name to identify the catalog |
| `rest_catalog.namespace` | string | Yes | Database/namespace name |
| `rest_catalog.table_name` | string | Yes | Table name |
| `rest_catalog.uri` | string | Yes | REST catalog URI |
| `rest_catalog.warehouse` | string | No | Warehouse location |
| `rest_catalog.s3.endpoint` | string | Yes | S3-compatible endpoint |
| `rest_catalog.s3.region` | string | Yes | S3 region |
| `rest_catalog.s3.access_key` | string | Yes | S3 access key |
| `rest_catalog.s3.secret_key` | string | Yes | S3 secret key |
| `rest_catalog.s3.bucket` | string | Yes | S3 bucket name |
| `with_compaction_validations` | boolean | No | Enable validation (default: true) |

## Example Usage

### Local Development with MinIO

```bash
# Start MinIO
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Create bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://iceberg-warehouse

# Start Iceberg REST catalog
docker run -p 8181:8181 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  apache/iceberg-rest-fixture

# Run benchmark
cargo run --bin bench -- bench integration-tests/config/mock_iceberg_config.yaml
```

### Production Testing

```bash
# Create production config
cat > prod-config.yaml <<EOF
rest_catalog:
  catalog_name: "prod_catalog"
  namespace: "analytics"
  table_name: "events"
  uri: "https://catalog.example.com"
  warehouse: "s3://prod-bucket/iceberg"
  s3:
    endpoint: "https://s3.us-west-2.amazonaws.com"
    region: "us-west-2"
    access_key: "${AWS_ACCESS_KEY_ID}"
    secret_key: "${AWS_SECRET_ACCESS_KEY}"
    bucket: "prod-bucket"

with_compaction_validations: false
EOF

# Run benchmark
cargo run --bin bench -- bench prod-config.yaml
```

## Output Format

### Success Output

```
Starting compaction for table: test_db.test_table
Bench over!!!
  - Input files: 10
  - Output files: 2
  - Input bytes: 104857600
  - Output bytes: 98765432
  - Time taken (ms): 5432
```

### Error Output

```
Error: Failed to load catalog: Connection refused
```

## Programmatic Usage

The bench binary can also be used as a library in integration tests:

```rust
use iceberg_compaction_integration_tests::test_utils::{
    mock_iceberg_table,
    delete_table_from_config,
    MockIcebergConfig,
};

#[tokio::test]
async fn test_compaction() {
    let config = MockIcebergConfig::from_yaml_file("config.yaml").unwrap();

    // Setup
    mock_iceberg_table(&config).await.unwrap();

    // Run compaction
    // ... your compaction logic

    // Cleanup
    delete_table_from_config(&config).await.unwrap();
}
```

## Troubleshooting

### Connection Refused

```
Error: Failed to load catalog: Connection refused
```

**Solution:** Ensure the REST catalog and S3-compatible storage are running and accessible.

### Authentication Failed

```
Error: Failed to create table: Invalid credentials
```

**Solution:** Verify S3 access key and secret key in configuration.

### Table Already Exists

```
Error: Table already exists: test_db.test_table
```

**Solution:** Delete existing table first:
```bash
cargo run --bin bench -- delete_table config.yaml
```

### Bucket Not Found

```
Error: Bucket not found: iceberg-warehouse
```

**Solution:** Create the S3 bucket:
```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://iceberg-warehouse
```

## See Also

- [USAGE.md](./USAGE.md) - General usage guide for iceberg-compaction
- [cleanup-feature.md](./cleanup-feature.md) - Snapshot expiration and orphan cleanup
