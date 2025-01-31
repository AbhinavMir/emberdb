use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use super::Record;
use std::fs::File;
use std::io::{BufWriter, BufReader};
use serde_json;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompressionState {
    Uncompressed,
    Compressed,
    InProgress,
}

#[derive(Debug, Serialize, Deserialize)]
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimeChunk {
    start_time: i64,
    end_time: i64,
    records: HashMap<String, Vec<Record>>,
    metadata: ChunkMetadata,
    compression_state: CompressionState,
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
            .ok_or_else(|| ChunkError::IndexError("Metric not found".to_string()))
    }

    pub fn get_metric(&mut self, metric: &str) -> std::result::Result<&Vec<Record>, ChunkError> {
        self.update_access_time();
        self.records
            .get(metric)
            .ok_or(ChunkError::IndexError("Metric not found".to_string()))
    }

    pub fn get_latest(&self, metric: &str) -> std::result::Result<&Record, ChunkError> {
        self.records
            .get(metric)
            .and_then(|records| records.last())
            .ok_or_else(|| ChunkError::IndexError("No records found".to_string()))
    }

    pub fn get_metrics_list(&self) -> Vec<String> {
        self.records.keys().cloned().collect()
    }

    pub fn summarize(&self, metric: &str) -> std::result::Result<ChunkSummary, ChunkError> {
        let records = self.records
            .get(metric)
            .ok_or_else(|| ChunkError::IndexError("Metric not found".to_string()))?;
        
        if records.is_empty() {
            return Err(ChunkError::IndexError("No records found".to_string()));
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
            
            // Value compression using gorilla algorithm
            // Implement value compression here
        }
        
        self.compression_state = CompressionState::Compressed;
        self.metadata.compression_ratio = self.calculate_compression_ratio();
        Ok(())
    }

    pub fn validate(&self) -> std::result::Result<(), ChunkError> {
        // Basic validation checks
        if self.start_time >= self.end_time {
            return Err(ChunkError::ValidationFailed("Invalid time range".to_string()));
        }

        for (_, records) in &self.records {
            for record in records {
                if !self.can_accept(record.timestamp) {
                    return Err(ChunkError::ValidationFailed("Record outside chunk range".to_string()));
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

    pub fn flush_to_disk(&self) -> std::result::Result<(), ChunkError> {
        let path = self.get_chunk_path();
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        
        serde_json::to_writer(writer, &self)
            .map_err(|e| ChunkError::DiskWriteFailed(e.to_string()))
    }

    pub fn load_from_disk(path: &str) -> std::result::Result<Self, ChunkError> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        
        serde_json::from_reader(reader)
            .map_err(|e| ChunkError::DataCorrupted(e.to_string()))
    }

    pub fn cleanup(&mut self) -> std::result::Result<(), ChunkError> {
        // Implement cleanup logic (e.g., removing old data)
        self.update_access_time();
        Ok(())
    }

    pub fn merge(&mut self, other: TimeChunk) -> std::result::Result<(), ChunkError> {
        if other.end_time < self.start_time || other.start_time > self.end_time {
            return Err(ChunkError::OutOfTimeRange("Chunks don't overlap".to_string()));
        }

        for (metric, records) in other.records {
            self.records
                .entry(metric)
                .or_insert_with(Vec::new)
                .extend(records);
        }

        self.metadata.record_count += other.metadata.record_count;
        self.update_access_time();
        Ok(())
    }

    fn calculate_compression_ratio(&self) -> f64 {
        // Simple implementation for now
        1.0
    }

    fn get_chunk_path(&self) -> String {
        format!("data/chunk_{}-{}.json", self.start_time, self.end_time)
    }
}

#[derive(Debug)]
pub struct ChunkSummary {
    pub count: usize,
    pub min: f64,
    pub max: f64,
    pub avg: f64,
}

// Add From implementations for error conversion
impl From<std::io::Error> for ChunkError {
    fn from(_error: std::io::Error) -> Self {
        ChunkError::DiskWriteFailed("IO Error occurred".to_string())
    }
}

impl From<serde_json::Error> for ChunkError {
    fn from(_error: serde_json::Error) -> Self {
        ChunkError::DataCorrupted("JSON serialization error".to_string())
    }
}