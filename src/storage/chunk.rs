use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use super::Record;
use std::path::Path;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CompressionState {
    Uncompressed,
    Compressed,
    InProgress,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(dead_code)]
pub struct ChunkMetadata {
    created_at: i64,
    last_access: i64,
    compression_ratio: f64,
    record_count: usize,
    size_bytes: usize,
}

#[derive(Debug)]
pub enum ChunkError {
    OutOfTimeRange(String),
    CompressionFailed(String),
    DiskWriteFailed(String),
    ValidationFailed(String),
    DataCorrupted(String),
    IndexError(String),
    SerializationFailed(String),
    DeserializationFailed(String),
    DiskReadFailed(String),
}

impl std::fmt::Display for ChunkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkError::OutOfTimeRange(msg) => write!(f, "Time range error: {}", msg),
            ChunkError::CompressionFailed(msg) => write!(f, "Compression error: {}", msg),
            ChunkError::DiskWriteFailed(msg) => write!(f, "Disk write error: {}", msg),
            ChunkError::ValidationFailed(msg) => write!(f, "Validation error: {}", msg),
            ChunkError::DataCorrupted(msg) => write!(f, "Data corruption: {}", msg),
            ChunkError::IndexError(msg) => write!(f, "Index error: {}", msg),
            ChunkError::SerializationFailed(msg) => write!(f, "Serialization error: {}", msg),
            ChunkError::DeserializationFailed(msg) => write!(f, "Deserialization error: {}", msg),
            ChunkError::DiskReadFailed(msg) => write!(f, "Disk read error: {}", msg),
        }
    }
}

impl std::error::Error for ChunkError {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeChunk {
    pub start_time: i64,
    pub end_time: i64,
    pub records: HashMap<String, Vec<Record>>,
    pub metadata: ChunkMetadata,
    pub compression_state: CompressionState,
    #[serde(skip)]
    pub dirty: bool, // Flag to indicate if chunk has been modified since last flush
}

impl TimeChunk {
    pub fn new(start_time: i64, end_time: i64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        TimeChunk {
            start_time,
            end_time,
            records: HashMap::new(),
            metadata: ChunkMetadata {
                created_at: now,
                last_access: now,
                compression_ratio: 1.0,
                record_count: 0,
                size_bytes: 0,
            },
            compression_state: CompressionState::Uncompressed,
            dirty: true,
        }
    }

    pub fn append(&mut self, record: Record) -> std::result::Result<(), ChunkError> {
        if !self.can_accept(record.timestamp) {
            return Err(ChunkError::OutOfTimeRange("Record timestamp outside chunk range".to_string()));
        }

        self.records
            .entry(record.metric_name.clone())
            .or_insert_with(Vec::new)
            .push(record);

        self.metadata.record_count += 1;
        self.update_access_time();
        self.dirty = true;
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        // Example implementation - could be based on size, record count, or other metrics
        self.metadata.record_count > 10_000 || self.get_size() > 1_000_000
    }

    pub fn can_accept(&self, timestamp: i64) -> bool {
        timestamp >= self.start_time && timestamp < self.end_time
    }

    pub fn get_size(&self) -> usize {
        self.records.iter().fold(0, |acc, (k, v)| {
            acc + k.len() + (v.len() * std::mem::size_of::<Record>())
        })
    }

    pub fn get_range(&self, start: i64, end: i64, metric: &str) -> std::result::Result<Vec<&Record>, ChunkError> {
        if start > self.end_time || end < self.start_time {
            return Ok(Vec::new());
        }

        self.records
            .get(metric)
            .map(|records| {
                records
                    .iter()
                    .filter(|r| r.timestamp >= start && r.timestamp < end)
                    .collect()
            })
            .ok_or_else(|| ChunkError::IndexError(format!("Metric not found: {}", metric)))
    }

    pub fn get_metric(&mut self, metric: &str) -> std::result::Result<&Vec<Record>, ChunkError> {
        self.update_access_time();
        self.records
            .get(metric)
            .ok_or(ChunkError::IndexError(format!("Metric not found: {}", metric)))
    }

    pub fn get_latest(&self, metric: &str) -> std::result::Result<&Record, ChunkError> {
        self.records
            .get(metric)
            .and_then(|records| records.last())
            .ok_or_else(|| ChunkError::IndexError(format!("No records found for metric: {}", metric)))
    }

    pub fn get_metrics_list(&self) -> Vec<String> {
        self.records.keys().cloned().collect()
    }

    pub fn summarize(&self, metric: &str) -> std::result::Result<ChunkSummary, ChunkError> {
        let records = self.records
            .get(metric)
            .ok_or_else(|| ChunkError::IndexError(format!("Metric not found: {}", metric)))?;
        
        if records.is_empty() {
            return Err(ChunkError::IndexError(format!("No records found for metric: {}", metric)));
        }

        let sum: f64 = records.iter().map(|r| r.value).sum();
        let count = records.len();
        let avg = sum / count as f64;

        Ok(ChunkSummary {
            count,
            min: records.iter().map(|r| r.value).fold(f64::INFINITY, f64::min),
            max: records.iter().map(|r| r.value).fold(f64::NEG_INFINITY, f64::max),
            avg,
        })
    }

    pub fn compress(&mut self) -> std::result::Result<(), ChunkError> {
        self.compression_state = CompressionState::InProgress;
        
        for records in self.records.values_mut() {
            // Delta encoding for timestamps
            let mut last_timestamp = 0;
            for record in records.iter_mut() {
                let delta = record.timestamp - last_timestamp;
                last_timestamp = record.timestamp;
                record.timestamp = delta;
            }
            
            // Value compression using gorilla algorithm would go here
        }
        
        self.compression_state = CompressionState::Compressed;
        self.metadata.compression_ratio = self.calculate_compression_ratio();
        self.dirty = true;
        Ok(())
    }

    pub fn validate(&self) -> std::result::Result<(), ChunkError> {
        // Basic validation checks
        if self.start_time >= self.end_time {
            return Err(ChunkError::ValidationFailed("Invalid time range".to_string()));
        }

        for (metric, records) in &self.records {
            if records.is_empty() {
                continue;
            }
            
            // Check records are ordered
            let mut prev_time = records[0].timestamp;
            for record in records.iter().skip(1) {
                if record.timestamp < prev_time {
                    return Err(ChunkError::ValidationFailed(
                        format!("Records not in time order for metric {}", metric)
                    ));
                }
                prev_time = record.timestamp;
            }
            
            // Check bounds
            for record in records {
                if !self.can_accept(record.timestamp) {
                    return Err(ChunkError::ValidationFailed(
                        format!("Record outside chunk range for metric {}", metric)
                    ));
                }
            }
        }

        Ok(())
    }

    fn update_access_time(&mut self) {
        self.metadata.last_access = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
    
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

    pub fn calculate_compression_ratio(&self) -> f64 {
        // Simple implementation for now
        1.0
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkSummary {
    pub count: usize,
    pub min: f64,
    pub max: f64,
    pub avg: f64,
}

// Add From implementations for error conversion
impl From<std::io::Error> for ChunkError {
    fn from(error: std::io::Error) -> Self {
        ChunkError::DiskWriteFailed(format!("IO Error: {}", error))
    }
}

impl From<serde_json::Error> for ChunkError {
    fn from(error: serde_json::Error) -> Self {
        ChunkError::DataCorrupted(format!("JSON error: {}", error))
    }
}