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
