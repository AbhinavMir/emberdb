//! EmberDB: A FHIR-optimized time-series database
//! 
//! EmberDB provides specialized storage for FHIR resources with a focus on
//! efficient time-series operations and hot/warm/cold data management.

pub mod fhir;
pub mod storage;
pub mod timeseries; 

use std::collections::HashMap;
use crate::storage::TimeChunk;
use crate::storage::ChunkError;

#[derive(Debug, Clone)]
pub enum Value {
    Float(f64),
    Integer(i64),
    String(String),
}

#[derive(Debug)]
pub enum StorageError {
    ChunkNotFound,
    ChunkError(ChunkError),
    InvalidTimeRange,
}

impl From<ChunkError> for StorageError {
    fn from(error: ChunkError) -> Self {
        StorageError::ChunkError(error)
    }
}

#[derive(Debug)]
pub struct StorageEngine {
    // For now, just keep everything in memory
    chunks: HashMap<i64, TimeChunk>,  // Use TimeChunk directly
}

impl StorageEngine {
    pub fn new() -> Self {
        StorageEngine {
            chunks: HashMap::new(),
        }
    }

    pub fn insert(&mut self, record: storage::Record) -> Result<(), StorageError> {
        // Determine which chunk this belongs to
        let chunk_id = self.get_chunk_id(record.timestamp);
        
        // Create new chunk if needed
        if !self.chunks.contains_key(&chunk_id) {
            let start_time = chunk_id;
            let end_time = start_time + self.chunk_duration();
            self.chunks.insert(chunk_id, TimeChunk::new(start_time, end_time));
        }

        // Insert into appropriate chunk
        self.chunks.get_mut(&chunk_id)
            .ok_or(StorageError::ChunkNotFound)?
            .append(record)
            .map_err(StorageError::ChunkError)
    }

    fn chunk_duration(&self) -> i64 {
        3600 // 1 hour in seconds
    }

    fn get_chunk_id(&self, timestamp: i64) -> i64 {
        // Round down to nearest hour
        timestamp - (timestamp % self.chunk_duration())
    }
}