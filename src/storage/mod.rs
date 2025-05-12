//! Core storage engine
//! 
//! Handles the fundamental storage operations including:
//! - Data persistence
//! - Indexing
//! - Hot/warm/cold data management

mod chunk;
pub use chunk::{TimeChunk, ChunkError};
mod persistence;
use persistence::PersistenceManager;

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::{RwLock, Arc};
use std::time::Duration;
use std::path::PathBuf;
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
    PersistenceError(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::ChunkNotFound(msg) => write!(f, "Chunk not found: {}", msg),
            StorageError::ChunkError(err) => write!(f, "Chunk error: {:?}", err),
            StorageError::InvalidTimeRange(msg) => write!(f, "Invalid time range: {}", msg),
            StorageError::PersistenceError(msg) => write!(f, "Persistence error: {}", msg),
        }
    }
}

impl From<ChunkError> for StorageError {
    fn from(error: ChunkError) -> Self {
        StorageError::ChunkError(error)
    }
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        StorageError::PersistenceError(format!("IO error: {}", error))
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::ChunkError(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct StorageEngine {
    chunks: RwLock<HashMap<i64, TimeChunk>>,
    chunk_duration: Duration,
    persistence: Arc<PersistenceManager>,
    persistence_enabled: bool,
}

impl StorageEngine {
    pub fn new(config: &Config) -> Result<Self, StorageError> {
        // Create the storage directories
        let data_path = PathBuf::from(&config.storage.path);
        let persistence = match PersistenceManager::new(&data_path) {
            Ok(p) => Arc::new(p),
            Err(e) => return Err(StorageError::PersistenceError(format!("Failed to initialize persistence: {}", e))),
        };
        
        let mut engine = StorageEngine {
            chunks: RwLock::new(HashMap::new()),
            chunk_duration: config.chunk_duration,
            persistence,
            persistence_enabled: true,
        };
        
        // Recover from disk and WAL
        engine.recover()?;
        
        Ok(engine)
    }
    
    /// Recover chunks from disk and replay the WAL to recover recent records
    fn recover(&mut self) -> Result<(), StorageError> {
        // First, load any existing chunks from disk
        let chunk_ids = self.persistence.list_chunks()?;
        let mut chunks = self.chunks.write().unwrap();
        
        for chunk_id in chunk_ids {
            match self.persistence.load_chunk(chunk_id) {
                Ok(chunk) => {
                    chunks.insert(chunk_id, chunk);
                },
                Err(e) => {
                    // Log the error, but continue loading other chunks
                    eprintln!("Error loading chunk {}: {:?}", chunk_id, e);
                }
            }
        }
        
        // Then, replay the WAL to recover any records not yet in chunks
        let wal_records = self.persistence.replay_wal()?;
        drop(chunks); // Release the lock before inserting records
        
        for record in wal_records {
            if let Err(e) = self.insert_internal(record, false) {
                eprintln!("Error during WAL replay: {:?}", e);
            }
        }
        
        Ok(())
    }

    /// Insert a record into the appropriate time chunk
    pub fn insert(&self, record: Record) -> Result<(), StorageError> {
        self.insert_internal(record, self.persistence_enabled)
    }
    
    /// Internal insert method that can optionally write to WAL
    fn insert_internal(&self, record: Record, write_wal: bool) -> Result<(), StorageError> {
        // First, write to WAL if persistence is enabled
        if write_wal {
            self.persistence.append_record(&record)?;
        }
        
        let chunk_id = self.get_chunk_id(record.timestamp);
        let mut chunks = self.chunks.write().unwrap();
        
        // Create new chunk if needed
        if !chunks.contains_key(&chunk_id) {
            let start_time = chunk_id;
            let end_time = start_time + self.chunk_duration.as_secs() as i64;
            chunks.insert(chunk_id, TimeChunk::new(start_time, end_time));
        }

        // Insert into appropriate chunk
        let chunk = chunks.get_mut(&chunk_id)
            .ok_or_else(|| StorageError::ChunkNotFound("Chunk not found after creation".to_string()))?;
        
        chunk.append(record).map_err(StorageError::from)?;
        
        // Check if the chunk is full and should be persisted
        let should_persist = chunk.is_full();
        
        // If we need to persist, clone the chunk before releasing the lock
        let chunk_to_persist = if should_persist && self.persistence_enabled {
            Some((chunk_id, chunk.clone()))
        } else {
            None
        };
        
        // Release the lock
        drop(chunks);
        
        // Persist the chunk if needed
        if let Some((_, chunk)) = chunk_to_persist {
            self.persist_chunk(&chunk)?;
        }
        
        Ok(())
    }

    /// Persist a chunk to disk
    fn persist_chunk(&self, chunk: &TimeChunk) -> Result<(), StorageError> {
        if !self.persistence_enabled {
            return Ok(());
        }
        
        // Save the chunk
        self.persistence.save_chunk(chunk)?;
        
        // Mark the chunk as durable in the WAL
        let chunk_duration_secs = self.chunk_duration.as_secs() as i64;
        self.persistence.mark_chunk_durable(chunk.start_time, chunk_duration_secs)?;
        
        // Mark chunk as clean
        let mut chunks = self.chunks.write().unwrap();
        if let Some(chunk) = chunks.get_mut(&chunk.start_time) {
            chunk.mark_clean();
        }
        
        Ok(())
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
            .ok_or_else(|| StorageError::ChunkNotFound(format!("No data found for metric: {}", metric)))
    }

    fn get_chunk_id(&self, timestamp: i64) -> i64 {
        timestamp - (timestamp % self.chunk_duration.as_secs() as i64)
    }

    /// Persist all dirty chunks to disk
    pub fn flush_all(&self) -> Result<(), StorageError> {
        if !self.persistence_enabled {
            return Ok(());
        }
        
        let chunks = self.chunks.read().unwrap();
        
        for chunk in chunks.values() {
            if chunk.is_dirty() {
                self.persist_chunk(chunk)?;
            }
        }
        
        // Truncate the WAL after all chunks are persisted
        self.persistence.truncate_wal()?;
        
        Ok(())
    }

    pub fn cleanup_old_chunks(&self, retention: Duration) -> Result<(), StorageError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
            
        let cutoff = now - retention.as_secs() as i64;
        
        // First flush all chunks to disk before removing old ones
        self.flush_all()?;
        
        // Then remove old chunks
        let mut chunks = self.chunks.write().unwrap();
        chunks.retain(|&chunk_start, _| chunk_start >= cutoff);
        
        Ok(())
    }
    
    /// Enable or disable persistence
    pub fn set_persistence(&mut self, enabled: bool) {
        self.persistence_enabled = enabled;
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
                port: 5432,
            },
            chunk_duration: Duration::from_secs(3600),
        }
    }

    #[test]
    fn test_basic_operations() {
        let config = create_test_config();
        let storage = StorageEngine::new(&config).unwrap();
        
        // Disable persistence for tests
        let mut storage_mut = unsafe { &mut *((&storage) as *const StorageEngine as *mut StorageEngine) };
        storage_mut.set_persistence(false);

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