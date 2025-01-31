//! Core storage engine
//! 
//! Handles the fundamental storage operations including:
//! - Data persistence
//! - Indexing
//! - Hot/warm/cold data management

mod chunk;
pub use chunk::{TimeChunk, ChunkError};

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;
use crate::config::Config;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub timestamp: i64,
    pub metric_name: String,
    pub value: f64,
}

#[derive(Debug)]
pub enum StorageError {
    ChunkNotFound(String),
    ChunkError(ChunkError),
    InvalidTimeRange(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::ChunkNotFound(msg) => write!(f, "Chunk not found: {}", msg),
            StorageError::ChunkError(err) => write!(f, "Chunk error: {:?}", err),
            StorageError::InvalidTimeRange(msg) => write!(f, "Invalid time range: {}", msg),
        }
    }
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
        
        if !chunks.contains_key(&chunk_id) {
            let start_time = chunk_id;
            let end_time = start_time + self.chunk_duration.as_secs() as i64;
            chunks.insert(chunk_id, TimeChunk::new(start_time, end_time));
        }

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

        let start_chunk = self.get_chunk_id(start);
        let end_chunk = self.get_chunk_id(end);

        for chunk_id in (start_chunk..=end_chunk).step_by(self.chunk_duration.as_secs() as usize) {
            if let Some(chunk) = chunks.get(&chunk_id) {
                let records = chunk.get_range(start, end, metric)
                    .map_err(StorageError::from)?;
                results.extend(records.iter().map(|&r| r.clone()));
            }
        }

        Ok(results)
    }

    pub fn get_latest(&self, metric: &str) -> Result<Record, StorageError> {
        let chunks = self.chunks.read().unwrap();
        let mut latest: Option<&Record> = None;
        
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_config() -> Config {
        Config {
            storage: crate::config::StorageConfig {
                path: "./data".to_string(),
                max_chunk_size: 1048576,
            },
            api: crate::config::ApiConfig {
                host: "127.0.0.1".to_string(),
                port: 3000,
            },
            chunk_duration: Duration::from_secs(3600),
        }
    }

    #[test]
    fn test_basic_operations() {
        let config = create_test_config();
        let storage = StorageEngine::new(&config);

        let record = Record {
            timestamp: 1000,
            metric_name: "test".to_string(),
            value: 42.0,
        };

        assert!(storage.insert(record.clone()).is_ok());
        
        let result = storage.get_latest("test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().value, 42.0);
    }
} 