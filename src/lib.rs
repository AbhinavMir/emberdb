//! EmberDB: A FHIR-optimized time-series database
//! 
//! EmberDB provides specialized storage for FHIR resources with a focus on
//! efficient time-series operations and hot/warm/cold data management.

pub mod fhir;
pub mod storage;
pub mod timeseries; 

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;
use crate::storage::{TimeChunk, Record, ChunkError};
use crate::config::Config;

#[derive(Debug, Clone)]
pub enum Value {
    Float(f64),
    Integer(i64),
    String(String),
}

#[derive(Debug)]
pub enum StorageError {
    ChunkNotFound(String),
    ChunkError(ChunkError),
    InvalidTimeRange(String),
    ConfigError(String),
}

impl From<ChunkError> for StorageError {
    fn from(error: ChunkError) -> Self {
        StorageError::ChunkError(error)
    }
}

#[derive(Debug)]
pub struct StorageEngine {
    chunks: RwLock<HashMap<i64, TimeChunk>>,
    chunk_duration: Duration,
}

impl StorageEngine {
    pub fn new(config: &Config) -> Self {
        StorageEngine {
            chunks: RwLock::new(HashMap::new()),
            chunk_duration: config.chunk_duration,
        }
    }

    pub fn insert(&self, record: Record) -> Result<(), StorageError> {
        let chunk_id = self.get_chunk_id(record.timestamp);
        let mut chunks = self.chunks.write().unwrap();
        
        // Create new chunk if needed
        if !chunks.contains_key(&chunk_id) {
            let start_time = chunk_id;
            let end_time = start_time + self.chunk_duration.as_secs() as i64;
            chunks.insert(chunk_id, TimeChunk::new(start_time, end_time));
        }

        // Insert into appropriate chunk
        chunks.get_mut(&chunk_id)
            .ok_or_else(|| StorageError::ChunkNotFound("Chunk not found after creation".to_string()))?
            .append(record)
            .map_err(StorageError::from)
    }

    pub fn query_range(&self, start: i64, end: i64, metric: &str) -> Result<Vec<Record>, StorageError> {
        if start >= end {
            return Err(StorageError::InvalidTimeRange("Start time must be before end time".to_string()));
        }

        let chunks = self.chunks.read().unwrap();
        let mut results = Vec::new();

        // Find all chunks that overlap with the query range
        let start_chunk = self.get_chunk_id(start);
        let end_chunk = self.get_chunk_id(end);

        for chunk_id in (start_chunk..=end_chunk).step_by(self.chunk_duration.as_secs() as usize) {
            if let Some(chunk) = chunks.get(&chunk_id) {
                let records = chunk.get_range(start, end, metric)
                    .map_err(StorageError::from)?;
                results.extend(records.into_iter().cloned());
            }
        }

        Ok(results)
    }

    pub fn get_latest(&self, metric: &str) -> Result<Record, StorageError> {
        let chunks = self.chunks.read().unwrap();
        let mut latest: Option<&Record> = None;
        
        // Search through chunks in reverse chronological order
        for chunk in chunks.values() {
            if let Ok(record) = chunk.get_latest(metric) {
                if latest.is_none() || record.timestamp > latest.unwrap().timestamp {
                    latest = Some(record);
                }
            }
        }

        latest.cloned()
            .ok_or_else(|| StorageError::ChunkNotFound("No data found for metric".to_string()))
    }

    fn get_chunk_id(&self, timestamp: i64) -> i64 {
        // Round down to nearest chunk boundary
        timestamp - (timestamp % self.chunk_duration.as_secs() as i64)
    }

    pub fn cleanup_old_chunks(&self, retention: Duration) -> Result<(), StorageError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
            
        let cutoff = now - retention.as_secs() as i64;
        let mut chunks = self.chunks.write().unwrap();
        
        chunks.retain(|&chunk_start, _| chunk_start >= cutoff);
        Ok(())
    }
}