# Compaction Runtime for Apache Iceberg™

**Disclaimer:** This project is not affiliated with or endorsed by the Apache Software Foundation. “Apache”, “Apache Iceberg”, and related marks are trademarks of the ASF.

`iceberg-compaction` is a high-performance Rust-based engine that compacts Apache Iceberg™ tables efficiently and safely at scale.


[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 🌟 Core Highlights

- **Rust-Native Performance**: Low-latency, high-throughput compaction with memory safety guarantees
- **DataFusion Engine**: Leverages Apache DataFusion for query planning and vectorized execution
- **Iceberg Native Support**: Full compliance with Iceberg table formats via iceberg-rs
- **Multi-Cloud Ready**: Currently supports AWS S3, with plans for Azure Blob Storage and GCP Cloud Storage

## 🛠️ Basic Functionality

- **Full Compaction**: Merges all data files in an Iceberg table and removes old files
- **Deletion Support**:
  - Positional deletions (POS_DELETE)
  - Equality deletions (EQ_DELETE)

## 📝 Installation

### CLI Tool

```bash
# Build from source
cargo install --path core --bin iceberg-compaction

# Or build release binary
cargo build --release --bin iceberg-compaction
```

### Docker

```bash
# Build image
docker build -t iceberg-compaction:latest .

# Run
docker run --rm iceberg-compaction:latest --help
```

## 🚀 Quick Start

### Using the CLI

```bash
# 1. Create a configuration file (config.yaml)
# See examples/docker-config/config.yaml for a template

# 2. Validate configuration
iceberg-compaction --config config.yaml validate

# 3. Run compaction
iceberg-compaction --config config.yaml compact --namespace prod --table events

# 4. Run cleanup (expire snapshots + remove orphans)
iceberg-compaction --config config.yaml cleanup --namespace prod --table events
```

### Using Docker Compose

```bash
# Start all services (MinIO, REST Catalog, compaction CLI)
docker-compose -f docker-compose.example.yml up -d

# Run compaction
docker-compose -f docker-compose.example.yml exec compaction \
  iceberg-compaction --config /config/config.yaml compact --table events

# Run cleanup
docker-compose -f docker-compose.example.yml exec compaction \
  iceberg-compaction --config /config/config.yaml cleanup --table events
```

### As a Library

```rust
use iceberg_compaction_core::{CompactionBuilder, CompactionConfigBuilder};

let compaction = CompactionBuilder::new(catalog, table_ident)
    .with_config(Arc::new(CompactionConfigBuilder::default().build()?))
    .build();

let result = compaction.compact().await?;
```

## 📝 Examples

### REST Catalog Example

We provide a complete working example using a REST catalog. This example demonstrates how to use iceberg-compaction for Iceberg table compaction with a REST catalog backend:

```bash
# Navigate to the example directory
cd examples/rest-catalog

# Run the example
cargo run
```

The example includes:
- Setting up a REST Iceberg catalog with S3 storage
- Configuring authentication and connection settings
- Performing table compaction using iceberg-compaction

For more details, see the [rest-catalog example](./examples/rest-catalog/).

### Benchmark Tool

See the [bench binary CLI reference](./docs/bench-cli.md) for benchmarking compaction performance with mock data.

### Other Examples

| Example | Description | Command |
|---------|-------------|---------|
| `memory-catalog` | In-memory catalog for testing | `cargo run -p iceberg-compaction-example-memory` |
| `cleanup` | Snapshot expiration and orphan cleanup | `cargo run -p cleanup-example` |

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [**cli-reference.md**](./docs/cli-reference.md) | **CLI tool reference** - Installation, commands, options, examples |
| [**USAGE.md**](./docs/USAGE.md) | Comprehensive usage guide - library usage, examples, CLI reference, configuration |
| [**bench-cli.md**](./docs/bench-cli.md) | Detailed reference for the bench binary CLI |
| [**cleanup-feature.md**](./docs/cleanup-feature.md) | Technical documentation for snapshot expiration and orphan cleanup |

## Development Roadmap

### Performance 🚀

- [ ] Partial compaction: Support incremental compaction strategies
- [ ] Compaction Policy: Multiple built-in policies (size-based, time-based, cost-optimized)
- [ ] Built-in cache: Metadata and query result caching for improved performance

### Stability 🔒

- [ ] Spill to disk: Handle large datasets that exceed memory limits
- [ ] Network rebuild: Robust handling of network failures and retries
- [ ] Task breakpoint resume: Resume operations from failure points
- [ ] E2E test framework: Comprehensive testing infrastructure

### Observability 📊

- [ ] Job progress display: Progress tracking
- [ ] Comprehensive compaction metrics: Detailed performance and operation metrics

### Customizability 🎛️

- [ ] Tune parquet: Configurable Parquet writer parameters
- [ ] Fine-grained configurable compaction parameters: Extensive customization options

### Functionality ⚙️

#### iceberg-rust

- [x] Expire snapshot
- [x] File clean: Delete orphan files
- [ ] Rewrite manifest

#### iceberg-compaction

- [x] CLI tool for compaction and cleanup
- [x] Docker support
- [ ] Binpack/Sort/ZOrder Compaction
- [ ] Clustering / Order by: Support for data reorganization and sorting
