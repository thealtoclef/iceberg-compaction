# Build stage
FROM rust:1.85-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Create a new directory for the app
WORKDIR /app

# Copy the Cargo files first for better caching
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml ./core/

# Fetch dependencies (this layer caches unless dependencies change)
RUN cargo fetch --locked

# Copy the source code
COPY core/src ./core/src

# Build the binary in release mode
RUN cargo build --bin iceberg-compaction --release

# Runtime stage - distroless cc for libgcc support
# distroless/cc-debian13 includes ca-certificates, so no need to copy
FROM gcr.io/distroless/cc-debian13

# Copy the binary from the builder
COPY --from=builder /app/target/release/iceberg-compaction /usr/local/bin/

# Use nonroot user (built into distroless images)
USER nonroot

# Set the entrypoint
ENTRYPOINT ["iceberg-compaction"]

# Default command shows help
CMD ["--help"]
