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
use std::sync::{RwLock, Arc, Mutex};
use std::time::Duration;
use std::path::PathBuf;
use crate::config::Config;
use std::fmt;
use crate::timeseries::query::DebugMetricsInfo;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub timestamp: i64,      // When the measurement was taken
    pub metric_name: String, // Identifier for the measurement type
    pub value: f64,          // The numeric value
    pub context: HashMap<String, String>, // Additional context (device_id, etc.)
    pub resource_type: String, // FHIR resource type (Observation, DeviceMetric, etc.)
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
    persistence_enabled: AtomicBool,
    active_records: Mutex<HashMap<String, i64>>, // metric_name -> latest timestamp
    debug_mode: RwLock<DebugSettings>,           // Performance optimization settings
}

#[derive(Debug, Clone, Copy)]
struct DebugSettings {
    memory_mode: bool,       // Skip disk operations when possible
    disable_wal: bool,       // Skip WAL writes
    batch_size: usize,       // Batch size for operations
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
            persistence_enabled: AtomicBool::new(true),
            active_records: Mutex::new(HashMap::new()),
            debug_mode: RwLock::new(DebugSettings {
                memory_mode: false,
                disable_wal: false,
                batch_size: 500,
            }),
        };
        
        // Recover from disk and WAL
        engine.recover()?;
        
        Ok(engine)
    }
    
    /// Recover chunks from disk and replay the WAL to recover recent records
    fn recover(&mut self) -> Result<(), StorageError> {
        println!("Starting recovery process...");
        
        // First, load any existing chunks from disk
        let chunk_ids = self.persistence.list_chunks()?;
        println!("Found {} chunks on disk", chunk_ids.len());
        
        let mut chunks = self.chunks.write().unwrap();
        
        for chunk_id in chunk_ids {
            println!("Loading chunk {} from disk", chunk_id);
            match self.persistence.load_chunk(chunk_id) {
                Ok(chunk) => {
                    println!("Successfully loaded chunk {} with {} records", 
                             chunk_id, 
                             chunk.records.values().map(|v| v.len()).sum::<usize>());
                    chunks.insert(chunk_id, chunk);
                },
                Err(e) => {
                    // Log the error, but continue loading other chunks
                    eprintln!("Error loading chunk {}: {:?}", chunk_id, e);
                }
            }
        }
        
        // Then, replay the WAL to recover any records not yet in chunks
        println!("Replaying write-ahead log...");
        let wal_records = self.persistence.replay_wal()?;
        println!("Found {} records in WAL", wal_records.len());
        
        drop(chunks); // Release the lock before inserting records
        
        for (i, record) in wal_records.into_iter().enumerate() {
            println!("Replaying WAL record {}: metric={}, value={}", 
                     i, record.metric_name, record.value);
            if let Err(e) = self.insert_internal(record, false) {
                eprintln!("Error during WAL replay: {:?}", e);
            }
        }
        
        println!("Recovery process completed");
        Ok(())
    }

    /// Insert a record into the appropriate time chunk
    pub fn insert(&self, record: Record) -> Result<(), StorageError> {
        self.insert_internal(record, self.persistence_enabled.load(Ordering::SeqCst))
    }
    
    /// Internal insert method that can optionally write to WAL
    fn insert_internal(&self, record: Record, write_wal: bool) -> Result<(), StorageError> {
        // First, write to WAL if persistence is enabled
        if write_wal && self.persistence_enabled.load(Ordering::SeqCst) {
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
        
        // If the chunk is full, we need to persist it, but we'll do that after releasing the lock
        let chunk_to_persist = if should_persist && self.persistence_enabled.load(Ordering::SeqCst) {
            Some((chunk_id, chunk.clone()))
        } else {
            None
        };
        
        // Release the lock
        drop(chunks);
        
        // Persist the chunk if needed
        if let Some((chunk_id, chunk)) = chunk_to_persist {
            // Save the chunk
            self.persistence.save_chunk(&chunk)?;
            
            // Mark the chunk as durable in the WAL
            let chunk_duration_secs = self.chunk_duration.as_secs() as i64;
            self.persistence.mark_chunk_durable(chunk.start_time, chunk_duration_secs)?;
            
            // Mark chunk as clean with a separate write lock
            let mut chunks = self.chunks.write().unwrap();
            if let Some(chunk) = chunks.get_mut(&chunk_id) {
                chunk.mark_clean();
            }
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

    pub fn get_latest(&self, metric: &str) -> Result<Option<Record>, StorageError> {
        let chunks = self.chunks.read().unwrap();
        let mut latest: Option<&Record> = None;
        
        for chunk in chunks.values() {
            match chunk.get_latest(metric) {
                Ok(Some(record)) => {
                    if latest.is_none() || record.timestamp > latest.unwrap().timestamp {
                        latest = Some(record);
                    }
                },
                Ok(None) => continue, // No record found in this chunk
                Err(e) => return Err(StorageError::ChunkError(e)),
            }
        }

        Ok(latest.cloned())
    }

    fn get_chunk_id(&self, timestamp: i64) -> i64 {
        timestamp - (timestamp % self.chunk_duration.as_secs() as i64)
    }

    /// Persist all dirty chunks to disk
    pub fn flush_all(&self) -> Result<(), StorageError> {
        if !self.persistence_enabled.load(Ordering::SeqCst) {
            println!("Persistence disabled, skipping flush");
            return Ok(());
        }
        
        println!("Starting to flush all dirty chunks to disk...");
        
        // First, identify dirty chunks while holding the read lock
        let chunks_to_flush = {
            let chunks = self.chunks.read().unwrap();
            println!("Total chunks in memory: {}", chunks.len());
            
            chunks.iter()
                .filter(|(_, chunk)| chunk.is_dirty())
                .map(|(id, chunk)| (*id, chunk.clone()))
                .collect::<Vec<_>>()
        };
        
        // Now flush each dirty chunk without holding any locks
        let mut flushed_count = 0;
        for (chunk_id, chunk) in &chunks_to_flush {
            println!("Flushing dirty chunk with ID: {}", chunk_id);
            
            // Save the chunk
            if let Err(e) = self.persistence.save_chunk(chunk) {
                println!("Error saving chunk {}: {:?}", chunk_id, e);
                return Err(e);
            }
            
            // Mark the chunk as durable in the WAL
            let chunk_duration_secs = self.chunk_duration.as_secs() as i64;
            if let Err(e) = self.persistence.mark_chunk_durable(chunk.start_time, chunk_duration_secs) {
                println!("Error marking chunk {} as durable: {:?}", chunk_id, e);
                return Err(e);
            }
            
            flushed_count += 1;
        }
        
        // Finally, mark all flushed chunks as clean with a write lock
        if !chunks_to_flush.is_empty() {
            let mut chunks = self.chunks.write().unwrap();
            for (chunk_id, _) in chunks_to_flush {
                if let Some(chunk) = chunks.get_mut(&chunk_id) {
                    chunk.mark_clean();
                }
            }
        }
        
        println!("Flushed {} dirty chunks", flushed_count);
        
        // Truncate the WAL after all chunks are persisted
        println!("Truncating WAL...");
        match self.persistence.truncate_wal() {
            Ok(_) => println!("WAL truncated successfully"),
            Err(e) => {
                println!("Error truncating WAL: {:?}", e);
                return Err(e);
            }
        }
        
        println!("Flush completed successfully");
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
        self.persistence_enabled.store(enabled, Ordering::SeqCst);
    }

    pub fn get_matching_metrics(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        println!("StorageEngine: finding metrics with prefix: {}", prefix);
        let chunks = self.chunks.read().unwrap();
        let mut matching_metrics = Vec::new();
        
        for chunk in chunks.values() {
            // Collect all metric names that start with the prefix
            for metric_name in chunk.records.keys() {
                if metric_name.starts_with(prefix) && !matching_metrics.contains(metric_name) {
                    println!("Found matching metric: {}", metric_name);
                    matching_metrics.push(metric_name.clone());
                }
            }
        }
        
        Ok(matching_metrics)
    }
    
    /// Get metrics by resource type
    pub fn get_metrics_by_resource_type(&self, resource_type: &str) -> Result<Vec<String>, StorageError> {
        println!("StorageEngine: finding metrics for resource type: {}", resource_type);
        let chunks = self.chunks.read().unwrap();
        let mut matching_metrics = Vec::new();
        
        for chunk in chunks.values() {
            if let Some(metrics) = chunk.resource_metrics.get(resource_type) {
                for metric in metrics {
                    if !matching_metrics.contains(metric) {
                        matching_metrics.push(metric.clone());
                    }
                }
            }
        }
        
        Ok(matching_metrics)
    }
    
    /// Query records by resource type and time range
    pub fn query_by_resource_type(&self, resource_type: &str, start: i64, end: i64) 
        -> Result<Vec<Record>, StorageError> 
    {
        println!("StorageEngine: querying records for resource type: {}", resource_type);
        
        // First get all metrics for this resource type
        let mut metrics = self.get_metrics_by_resource_type(resource_type).unwrap_or_default();
        
        // If no metrics in the index, fall back to checking all metrics
        if metrics.is_empty() {
            println!("No metrics found in resource_metrics index, checking all metrics");
            let chunks = self.chunks.read().unwrap();
            for chunk in chunks.values() {
                for (metric, records) in &chunk.records {
                    // Check a sample record to see if it has the right resource_type
                    if let Some(record) = records.first() {
                        if record.resource_type == resource_type {
                            metrics.push(metric.clone());
                        }
                    }
                }
            }
        }
        
        println!("Found {} metrics for resource type {}", metrics.len(), resource_type);
        
        let mut results = Vec::new();
        
        // Then query each metric within the time range
        for metric in metrics {
            let records = self.query_range(start, end, &metric)?;
            results.extend(records);
        }
        
        Ok(results)
    }

    /// Get debug metrics information
    pub fn debug_metrics(&self) -> Result<DebugMetricsInfo, StorageError> {
        let chunks = self.chunks.read().unwrap();
        let mut all_metrics = Vec::new();
        let mut resource_metrics = HashMap::new();
        
        // Collect metrics from all chunks
        for chunk in chunks.values() {
            // Get all metrics
            for metric in chunk.records.keys() {
                if !all_metrics.contains(metric) {
                    all_metrics.push(metric.clone());
                }
            }
            
            // Get resource metrics mapping
            for (resource_type, metrics) in &chunk.resource_metrics {
                let entry = resource_metrics
                    .entry(resource_type.clone())
                    .or_insert_with(Vec::new);
                
                for metric in metrics {
                    if !entry.contains(metric) {
                        entry.push(metric.clone());
                    }
                }
            }
        }
        
        // Basic storage info
        let storage_info = format!("Chunks: {}, Metrics: {}, Resource types: {}",
            chunks.len(),
            all_metrics.len(),
            resource_metrics.len()
        );
        
        Ok(DebugMetricsInfo {
            metrics: all_metrics,
            resource_metrics,
            storage_info,
        })
    }

    pub fn chunk_duration(&self) -> Duration {
        self.chunk_duration
    }
    
    /// Append multiple records to the WAL in a single operation 
    pub fn append_records_to_wal(&self, records: Vec<Record>) -> Result<(), StorageError> {
        if !self.persistence_enabled.load(Ordering::SeqCst) || records.is_empty() {
            return Ok(());
        }
        
        // Batch write to WAL
        self.persistence.append_records(&records)?;
        
        // Update the active records map
        let mut active_records = self.active_records.lock().unwrap();
        for record in &records {
            active_records.insert(record.metric_name.clone(), record.timestamp);
        }
        
        Ok(())
    }
    
    /// Insert a batch of records into a specific chunk
    pub fn insert_batch(&self, chunk_id: i64, records: Vec<Record>) -> Result<(), StorageError> {
        if records.is_empty() {
            return Ok(());
        }
        
        let mut chunks = self.chunks.write().unwrap();
        
        // Create new chunk if needed
        if !chunks.contains_key(&chunk_id) {
            let start_time = chunk_id;
            let end_time = start_time + self.chunk_duration.as_secs() as i64;
            chunks.insert(chunk_id, TimeChunk::new(start_time, end_time));
        }

        // Get the chunk
        let chunk = chunks.get_mut(&chunk_id)
            .ok_or_else(|| StorageError::ChunkNotFound("Chunk not found after creation".to_string()))?;
        
        // Insert all records
        for record in records {
            if let Err(e) = chunk.append(record) {
                return Err(e.into());
            }
        }
        
        // Check if the chunk is full and should be persisted
        let should_persist = chunk.is_full();
        
        // If the chunk is full, we need to persist it, but we'll do that after releasing the lock
        let chunk_to_persist = if should_persist && self.persistence_enabled.load(Ordering::SeqCst) {
            Some((chunk_id, chunk.clone()))
        } else {
            None
        };
        
        // Release the lock
        drop(chunks);
        
        // Persist the chunk if needed
        if let Some((chunk_id, chunk)) = chunk_to_persist {
            // Save the chunk
            self.persistence.save_chunk(&chunk)?;
            
            // Mark the chunk as durable in the WAL
            let chunk_duration_secs = self.chunk_duration.as_secs() as i64;
            self.persistence.mark_chunk_durable(chunk.start_time, chunk_duration_secs)?;
            
            // Mark chunk as clean with a separate write lock
            let mut chunks = self.chunks.write().unwrap();
            if let Some(chunk) = chunks.get_mut(&chunk_id) {
                chunk.mark_clean();
            }
        }
        
        Ok(())
    }

    /// Set debug settings for performance testing
    pub fn set_debug_settings(&self, memory_mode: bool, disable_wal: bool, batch_size: Option<usize>) -> Result<(), StorageError> {
        let mut debug_settings = self.debug_mode.write().unwrap();
        debug_settings.memory_mode = memory_mode;
        debug_settings.disable_wal = disable_wal;
        
        if let Some(size) = batch_size {
            debug_settings.batch_size = size;
        }
        
        // Apply persistence settings immediately using AtomicBool
        self.persistence_enabled.store(!memory_mode, Ordering::SeqCst);
        
        Ok(())
    }
}

// Add this function outside the StorageEngine implementation
// to make it available for the query engine
pub fn chunk_id_for_timestamp(timestamp: i64, chunk_duration: Duration) -> i64 {
    timestamp - (timestamp % chunk_duration.as_secs() as i64)
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
            context: HashMap::new(),
            resource_type: "Observation".to_string(),
        };

        assert!(storage.insert(record.clone()).is_ok());
        
        let result = storage.get_latest("test");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().value, 42.0);
    }
} 