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

Head-to-head results on the MIMIC-IV-schema synthetic workload (500 patients, 48h
ICU stays, 6 vitals @ 5-min cadence, 1,728,000 chartevents, seed 42) are in
`head_to_head_results.csv`. EmberDB is compared against SQLite, TimescaleDB
(PostgreSQL 16) and InfluxDB 2.7, the last two run in Docker. All numbers are
single-node on one Apple Silicon host and use a synthetic schema.

| Metric | EmberDB | SQLite | TimescaleDB | InfluxDB |
|---|---|---|---|---|
| Ingest (rec/s) | **1,424,089** | 151,113 | 363,352 | 301,453 |
| Single vital 1h (us) | **3.3** | 31.0 | 531.3 | 5,383 |
| Full patient stay (us) | 955.9 | **624.8** | 3,078 | 13,167 |
| Cohort vital 1h (us) | 817.1 | 18,525 | **751.8** | 12,073 |
| Latest vital (us) | **28.1** | 80.5 | 1,178 | 4,226 |
| Storage (B/rec) | 201.4 | 103.1 | 129.3 | **26.8** |

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

### Real MIMIC-IV demo benchmark

The same four systems are also benchmarked on the **real**, open-access MIMIC-IV
Clinical Database Demo v2.2 (~100 patients, Open Data Commons ODbL, no
credentialing). The driver `benches/mimic_real_bench.rs` ingests the 78,441
mapped vital-sign rows from `icu/chartevents.csv` and runs the same four query
shapes; results are in `mimic_demo_results.csv` and `mimic_demo_report.md`.

| Metric | EmberDB | SQLite | TimescaleDB | InfluxDB |
|---|---|---|---|---|
| Ingest (rec/s) | **1,944,141** | 236,602 | 158,006 | 328,673 |
| Single vital 1h (us) | **0.5** | 142.6 | 554.7 | 4,758 |
| Cohort vital 1h (us) | **155.9** | 2,760 | 427.9 | 4,007 |
| Full patient stay (us) | 4,334 | **1,687** | 2,151 | 14,546 |
| Latest vital (us) | 7,538 | **165.0** | 1,768 | 4,758 |

The ingest and point/cohort-query wins hold on real data; `latest_vital` and
`full_patient_stay` regress because the de-identified MIMIC timestamps span ~90
years, scattering EmberDB's hourly chunks. Full MIMIC-IV (vs the demo subset)
needs PhysioNet credentialed access and is left for future validation.

```bash
curl -sSL -o /tmp/mimic-demo.zip \
  https://physionet.org/static/published-projects/mimic-iv-demo/mimic-iv-clinical-database-demo-2.2.zip
unzip -o /tmp/mimic-demo.zip "*/icu/chartevents.csv.gz" -d /tmp/mimic-demo
gunzip -kf /tmp/mimic-demo/mimic-iv-clinical-database-demo-2.2/icu/chartevents.csv.gz
cargo run --release --bin mimic_real_bench   # -> mimic_demo_results.csv
```

### Chartevents schema (synthetic == real shape)

The synthetic generator and the real loader now use the **same 11-column
MIMIC-IV `chartevents` schema** with human-readable timestamps:

```
subject_id,hadm_id,stay_id,caregiver_id,charttime,storetime,itemid,value,valuenum,valueuom,warning
```

`mimic_bench` writes its synthetic data to `/tmp/emberdb_synth_chartevents.csv`
with `charttime`/`storetime` as `YYYY-MM-DD HH:MM:SS` strings, byte-identical in
header to the real demo file. One loader, `mimic::parse_chartevents_csv`, reads
both: `mimic::parse_charttime` accepts either an integer Unix epoch (legacy
synthetic) or an ISO datetime string (real MIMIC), and the parser dispatches on
column count (11 = real, 8 = legacy). Verified end-to-end: the production loader
parses the real demo file to the same 78,441 mapped vitals the bespoke driver
produced. The remaining synthetic/real difference is in the *data* (clean 5-min
Gaussian over ~3.5 days vs irregular ~hourly over a ~90-year de-identified span),
not the schema.

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