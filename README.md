# EmberDB

A FHIR-optimized time-series database designed specifically for continuous patient monitoring data.

## Overview

EmberDB is a specialized database system that combines the efficiency of time-series storage with FHIR (Fast Healthcare Interoperability Resources) compliance. It's built to handle the unique challenges of medical time-series data, particularly continuous patient monitoring.

### Key Features

- **Time-Series Optimized**: Efficient storage and retrieval of high-velocity medical data
- **FHIR-Compliant**: Native support for FHIR data structures and queries
- **Correlation-Aware**: Optimized for analyzing multiple vital signs together
- **Pattern Detection**: Fast detection of clinically significant patterns across vital signs
- **Memory Optimized**: Hot-cold data separation for optimal performance

## Architecture

EmberDB is built around two core components:

### TimeChunk
The fundamental storage unit that handles a specific time window of data. Each chunk:
- Manages a fixed time window (e.g., 1 hour of data)
- Groups related measurements
- Handles compression for older data
- Provides efficient time-range queries

### StorageEngine
The main orchestrator that:
- Manages multiple TimeChunks
- Handles data ingestion and querying
- Implements hot-cold data separation
- Maintains FHIR compliance

## Getting Started

```bash
# Clone the repository
git clone https://github.com/yourusername/emberdb
cd emberdb

# Build the project
cargo build

# Run tests
cargo test
```

## Benchmarks

Head-to-head results on the synthetic MIMIC-IV workload (500 patients, 48h ICU
stays, 6 vitals @ 5-min cadence, 1,728,000 chartevents, seed 42) are in
`head_to_head_results.csv`. EmberDB is compared against SQLite, TimescaleDB
(PostgreSQL 16) and InfluxDB 2.7, the last two run in Docker. All numbers are
single-node on one Apple Silicon host and use a synthetic schema.

| Metric | EmberDB | SQLite | TimescaleDB | InfluxDB |
|---|---|---|---|---|
| Ingest (rec/s) | **1,176,849** | 124,863 | 309,707 | 287,412 |
| Single vital 1h (us) | **4.0** | 37.1 | 561.6 | 4,949 |
| Full patient stay (us) | 1,145.9 | **746.9** | 3,628 | 13,913 |
| Cohort vital 1h (us) | 967.9 | 21,191 | **847.3** | 12,692 |
| Latest vital (us) | **31.3** | 96.5 | 2,466 | 4,618 |
| Storage (B/rec) | 201.4 | 103.1 | 129.6 | **26.8** |

EmberDB wins ingestion and patient-scoped point queries; TimescaleDB wins the
cohort scan and InfluxDB stores the data far more compactly. Reproduce with:

```bash
# Start the purpose-built baselines
docker run -d --name influx-bench -p 8087:8086 influxdb:2.7
docker run -d --name tsdb-bench -e POSTGRES_PASSWORD=pw -p 5433:5432 timescale/timescaledb:latest-pg16
# (one-time InfluxDB setup: org=emberbench, bucket=vitals, token in benches/baseline_bench.rs)

cargo build --release --bins
./target/release/mimic_bench          # EmberDB vs SQLite -> mimic_bench_results.csv
./target/release/baseline_bench       # InfluxDB + TimescaleDB -> baseline_results.csv
./target/release/ember_storage_probe  # EmberDB on-disk B/rec
```

## Current Status

EmberDB is currently in early development. 

### Implemented Features ✅
- Basic time-series storage
- Time chunk management
- Memory-efficient data structures
- Hot-cold data separation

### In Progress 🚧
- FHIR compliance layer
  - Basic FHIR Observation mapping
  - Resource validation
  - FHIR search capabilities
- Compression strategies
  - Implementing delta encoding
  - Evaluating different compression algorithms for medical data
- Pattern detection optimizations
  - Multi-vital correlation detection
  - Anomaly detection algorithms
- Disk persistence
  - Write-ahead logging
  - Data recovery mechanisms

### Upcoming Features 📋
- Authentication and authorization
- Multi-tenant support
- Distributed storage capabilities
- Real-time alerting system
- HIPAA compliance features
  - Audit logging
  - Data encryption at rest
  - Access control lists
- Query optimization engine
- Backup and restore functionality
- Data retention policies
- Integration APIs
  - REST API
  - gRPC interface
  - HL7v2 compatibility layer

### Performance Goals 🎯
- Sub-millisecond query response for recent data
- Support for 100,000+ data points per second per node
- 10:1 minimum compression ratio for historical data
- 99.99% uptime

## Technical Details

Written in Rust for:
- Memory safety without garbage collection (Initial ideas was to use Go since libreprose.com was in Go, but Go apparently pauses exec for GC? Anyway, good time to practice Rust)
- High performance
- Reliable concurrent operations

## Why EmberDB?

Traditional time-series databases excel at handling individual metrics but struggle with the unique requirements of continuous patient monitoring:

- Need for temporal correlation across multiple vital signs
- Complex pattern detection requirements
- FHIR compliance requirements
- Strict data retention and privacy rules

EmberDB addresses these challenges while maintaining the performance characteristics of modern time-series databases.


### TODOS
Pattern detection (trend analysis, anomaly detection)
Aggregation queries across patients/cohorts
Performance Optimizations
Implement data compression for time-series chunks
Add caching layer for frequent queries
Memory-mapped file support for larger datasets
Add support for FHIR search parameters
Implement FHIR Bulk Data API
Add FHIR validation against profiles
Multi-node distribution support
Hot/cold storage tiering for historical data
Backup and recovery utilities
Add OAuth2/SMART on FHIR support
Role-based access control for resources
Audit logging for compliance