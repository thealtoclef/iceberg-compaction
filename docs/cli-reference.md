# iceberg-compaction CLI Reference

## Overview

`iceberg-compaction` is a command-line interface for Apache Iceberg table compaction and maintenance operations.

## Installation

### From Source (Cargo)

```bash
cargo install --path core --bin iceberg-compaction --features cli
```

### Using Docker

```bash
# Build the image
docker build -t iceberg-compaction:latest .

# Run the CLI
docker run --rm iceberg-compaction:latest --help
```

**Note:** The production Docker image uses [distroless/cc-debian13](https://github.com/GoogleContainerTools/distroless) as the base:
- Minimal attack surface (no shell, package manager, or unnecessary tools)
- Includes `libgcc` for Rust binary compatibility
- Includes `ca-certificates` for HTTPS connections
- Runs as non-root user (`nonroot`)
- Image size: ~35 MB (vs ~150 MB for full Debian)

For debugging, you can build with the debug variant:
```bash
# Debug image with shell access (development only)
docker build -f Dockerfile.debug -t iceberg-compaction:debug .
docker run --rm -it iceberg-compaction:debug --help
```

### Pre-built Binary

```bash
# Build release binary
cargo build --release --bin iceberg-compaction

# Binary location
./target/release/iceberg-compaction
```

## Quick Start

### 1. Create Configuration File

Create a `config.yaml` file:

```yaml
catalogs:
  - name: default
    catalog_type: rest
    uri: http://localhost:8181
    warehouse: s3://iceberg-warehouse
    storage:
      endpoint: http://localhost:9000
      region: us-east-1
      access_key_id: minioadmin
      secret_access_key: minioadmin

default_catalog: default
default_namespace: prod
```

### 2. Validate Configuration

```bash
iceberg-compaction --config config.yaml validate
```

### 3. Run Compaction

```bash
iceberg-compaction --config config.yaml compact --table events
```

### 4. Run Cleanup

```bash
iceberg-compaction --config config.yaml cleanup --table events
```

## Commands

### `compact` - Compact an Iceberg Table

Merges small files in an Iceberg table to improve query performance.

```bash
iceberg-compaction --config config.yaml compact [OPTIONS]
```

**Options:**

| Option | Description | Required |
|--------|-------------|----------|
| `--catalog <NAME>` | Catalog name from config | No (uses default) |
| `--namespace <NS>` | Namespace (dot-separated, e.g., `prod.analytics`) | Yes (or default in config) |
| `--table <NAME>` | Table name | Yes |
| `--dry-run` | Show what would happen without making changes | No |

**Examples:**

```bash
# Basic compaction
iceberg-compaction --config config.yaml compact --namespace prod --table events

# Using default namespace from config
iceberg-compaction --config config.yaml compact --table events

# Dry run mode
iceberg-compaction --config config.yaml compact --namespace prod --table events --dry-run

# JSON output
iceberg-compaction --config config.yaml compact --namespace prod --table events --format json
```

**Output:**

```
Compaction Results:
  Input files:  50
  Output files: 5
  Input bytes:  524288000
  Output bytes: 520000000
  Size reduction: 0.8%
```

---

### `cleanup` - Expire Snapshots and Remove Orphan Files

Runs maintenance operations to free storage space.

```bash
iceberg-compaction --config config.yaml cleanup [OPTIONS]
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--catalog <NAME>` | Catalog name from config | default |
| `--namespace <NS>` | Namespace (dot-separated) | From config |
| `--table <NAME>` | Table name | Required |
| `--dry-run` | Preview without deleting | false |
| `--older-than <DURATION>` | Expire snapshots older than | 7d |
| `--retain-last <N>` | Minimum snapshots to retain | 3 |
| `--orphan-older-than <DURATION>` | Remove orphans older than | 7d |

**Duration Format:**

- `7d` - 7 days
- `24h` - 24 hours
- `60m` - 60 minutes
- `3600s` - 3600 seconds

**Examples:**

```bash
# Basic cleanup (dry run)
iceberg-compaction --config config.yaml cleanup --namespace prod --table events --dry-run

# Aggressive cleanup (expire snapshots older than 1 day, keep 1)
iceberg-compaction --config config.yaml cleanup \
  --namespace prod \
  --table events \
  --older-than 1d \
  --retain-last 1

# Cleanup orphan files only (set older_than very high)
iceberg-compaction --config config.yaml cleanup \
  --namespace prod \
  --table events \
  --older-than 365d \
  --orphan-older-than 7d

# Production cleanup (actually delete files)
iceberg-compaction --config config.yaml cleanup \
  --namespace prod \
  --table events \
  --older-than 7d \
  --retain-last 3
```

**Output:**

```
Cleanup Results:

Snapshot Expiration:
  Snapshots expired: 5
  Data files cleaned: 25
  Manifest files cleaned: 10
  Manifest list files cleaned: 5
  Expired snapshot IDs: [12345, 12346, 12347, 12348, 12349]

Orphan File Cleanup:
  Orphan files found: 15
  Files deleted: 15
  Bytes freed: 157286400
```

---

### `validate` - Validate Configuration File

Checks if the configuration file is valid.

```bash
iceberg-compaction --config config.yaml validate
```

**Output:**

```
Configuration file is valid: config.yaml

Catalogs configured:
  - default (type: rest, uri: http://localhost:8181)
  - production (type: rest, uri: https://catalog.example.com)

Default namespace: prod
```

---

## Global Options

| Option | Description | Environment Variable |
|--------|-------------|---------------------|
| `--config <PATH>` | Path to configuration file | `ICEBERG_COMPACTION_CONFIG` |
| `--format <FORMAT>` | Output format: `text` or `json` | - |
| `--verbose` | Enable debug logging | - |
| `--help` | Show help message | - |
| `--version` | Show version | - |

## Configuration File Reference

### YAML Format (Recommended)

```yaml
# Catalog configurations
catalogs:
  - name: default
    catalog_type: rest
    uri: http://localhost:8181
    warehouse: s3://iceberg-warehouse
    storage:
      endpoint: http://localhost:9000
      region: us-east-1
      access_key_id: minioadmin
      secret_access_key: minioadmin

# Default catalog name
default_catalog: default

# Default namespace (optional)
default_namespace: prod

# Compaction settings (optional)
compaction:
  target_file_size: 536870912  # 512 MB
  max_input_files: 100

# Cleanup settings (optional)
cleanup:
  older_than: 7d
  retain_last: 3
  orphan_older_than: 7d
```

### TOML Format (Alternative)

```toml
[[catalogs]]
name = "default"
catalog_type = "rest"
uri = "http://localhost:8181"
warehouse = "s3://iceberg-warehouse"

[catalogs.storage]
endpoint = "http://localhost:9000"
region = "us-east-1"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"

default_catalog = "default"
default_namespace = "prod"

[compaction]
target_file_size = 536870912
max_input_files = 100

[cleanup]
older_than = "7d"
retain_last = 3
orphan_older_than = "7d"
```

### Storage Configuration

#### S3 / MinIO

```yaml
storage:
  endpoint: http://localhost:9000  # Optional - omit for AWS S3
  region: us-east-1
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

#### Google Cloud Storage

```yaml
storage:
  region: us-central-1
  gcs_key: /path/to/service-account.json
  # Or use workload identity (no key needed)
```

#### Azure Data Lake Storage

```yaml
storage:
  adls_account_name: mystorageaccount
  adls_account_key: ${ADLS_ACCOUNT_KEY}
```

---

## Docker Usage

### Build Image

```bash
docker build -t iceberg-compaction:latest .
```

### Run with Docker Compose

```bash
# Start all services (MinIO, REST Catalog, compaction CLI)
docker-compose -f docker-compose.example.yml up -d

# Run compaction
docker-compose -f docker-compose.example.yml exec compaction \
  iceberg-compaction --config /config/config.yaml compact --table events

# Run cleanup
docker-compose -f docker-compose.example.yml exec compaction \
  iceberg-compaction --config /config/config.yaml cleanup --table events

# Stop services
docker-compose -f docker-compose.example.yml down
```

### Run with Docker

```bash
# With config mounted from host
docker run --rm \
  -v $(pwd)/config.yaml:/config/config.yaml:ro \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_REGION=us-east-1 \
  iceberg-compaction:latest \
  --config /config/config.yaml compact --namespace prod --table events
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ICEBERG_COMPACTION_CONFIG` | Default config file path |
| `AWS_ACCESS_KEY_ID` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key |
| `AWS_REGION` | S3 region |
| `RUST_LOG` | Logging level (e.g., `debug`, `info`, `warn`) |

---

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Catalog connection error |
| 4 | Table not found |
| 5 | Compaction/cleanup execution error |

---

## Troubleshooting

### Configuration File Not Found

```
Error: Failed to read config file: config.yaml
```

**Solution:** Use absolute path or set `ICEBERG_COMPACTION_CONFIG`:

```bash
export ICEBERG_COMPACTION_CONFIG=/path/to/config.yaml
```

### Catalog Connection Failed

```
Error: Failed to build catalog: Connection refused
```

**Solution:** Check that the REST catalog is running and accessible.

### Namespace Not Found

```
Error: Namespace does not exist
```

**Solution:** Create the namespace first or check the spelling.

### Table Not Found

```
Error: Table does not exist
```

**Solution:** Verify the table exists in the catalog.

### Permission Denied

```
Error: Access denied to S3 bucket
```

**Solution:** Check S3 credentials in configuration.

---

## See Also

- [USAGE.md](./USAGE.md) - General usage guide
- [bench-cli.md](./bench-cli.md) - Bench binary CLI reference
- [cleanup-feature.md](./cleanup-feature.md) - Cleanup feature documentation
- [docker-compose.example.yml](../docker-compose.example.yml) - Docker Compose example
