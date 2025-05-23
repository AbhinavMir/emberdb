use std::path::{Path, PathBuf};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::collections::HashMap;
use std::sync::Mutex;
use serde_json;

use super::chunk::TimeChunk;
use super::Record;
use super::StorageError;

/// Manages storage and retrieval of chunks from disk
#[derive(Debug)]
pub struct PersistenceManager {
    base_path: PathBuf,
    wal: WriteAheadLog,
    active_records: Mutex<HashMap<String, i64>>, // metric_name -> latest timestamp
}

impl PersistenceManager {
    pub fn new(base_path: impl AsRef<Path>) -> io::Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        
        // Create the base directory if it doesn't exist
        fs::create_dir_all(&base_path)?;
        
        // Create subdirectories
        let chunks_dir = base_path.join("chunks");
        let wal_dir = base_path.join("wal");
        fs::create_dir_all(&chunks_dir)?;
        fs::create_dir_all(&wal_dir)?;
        
        let wal = WriteAheadLog::new(wal_dir)?;
        
        Ok(PersistenceManager {
            base_path,
            wal,
            active_records: Mutex::new(HashMap::new()),
        })
    }
    
    /// Save a chunk to disk
    pub fn save_chunk(&self, chunk: &TimeChunk) -> Result<(), StorageError> {
        let chunk_path = self.get_chunk_path(chunk.start_time);
        let serialized = serde_json::to_vec(chunk)
            .map_err(|e| StorageError::PersistenceError(format!("Serialization failed: {}", e)))?;
        
        // Write to a temporary file first
        let temp_path = chunk_path.with_extension("tmp");
        let mut file = File::create(&temp_path)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to create file: {}", e)))?;
        
        file.write_all(&serialized)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to write data: {}", e)))?;
        
        // Ensure data is flushed to disk
        file.sync_all()
            .map_err(|e| StorageError::PersistenceError(format!("Failed to sync data: {}", e)))?;
        
        // Rename temp file to final name (atomic operation on most filesystems)
        fs::rename(&temp_path, &chunk_path)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to rename file: {}", e)))?;
        
        Ok(())
    }
    
    /// Load a chunk from disk
    pub fn load_chunk(&self, chunk_id: i64) -> Result<TimeChunk, StorageError> {
        let chunk_path = self.get_chunk_path(chunk_id);
        
        let mut file = File::open(&chunk_path)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to open chunk file: {}", e)))?;
        
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to read chunk file: {}", e)))?;
        
        let chunk: TimeChunk = serde_json::from_slice(&buffer)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to deserialize chunk: {}", e)))?;
        
        Ok(chunk)
    }
    
    /// List all available chunk IDs on disk
    pub fn list_chunks(&self) -> Result<Vec<i64>, StorageError> {
        let chunks_dir = self.base_path.join("chunks");
        let mut chunk_ids = Vec::new();
        
        for entry in fs::read_dir(&chunks_dir)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to read chunks directory: {}", e)))? {
                
            let entry = entry
                .map_err(|e| StorageError::PersistenceError(format!("Failed to read directory entry: {}", e)))?;
            let path = entry.path();
            
            if path.extension().map_or(false, |ext| ext == "chunk") {
                if let Some(stem) = path.file_stem() {
                    if let Some(stem_str) = stem.to_str() {
                        if let Ok(chunk_id) = stem_str.parse::<i64>() {
                            chunk_ids.push(chunk_id);
                        }
                    }
                }
            }
        }
        
        chunk_ids.sort();
        Ok(chunk_ids)
    }
    
    /// Append a record to the WAL for durability
    pub fn append_record(&self, record: &Record) -> Result<(), StorageError> {
        // Append to WAL first
        self.wal.append_record(record)
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        
        // Update the active records map
        let mut active_records = self.active_records.lock().unwrap();
        active_records.insert(record.metric_name.clone(), record.timestamp);
        
        Ok(())
    }
    
    /// Append multiple records to the WAL in a batch for better performance
    pub fn append_records(&self, records: &[Record]) -> Result<(), StorageError> {
        if records.is_empty() {
            return Ok(());
        }
        
        // Special case: we can skip disk operations if running in memory mode
        if self.base_path.as_os_str().is_empty() {
            return Ok(());
        }
        
        // Fast path: If many records, use a more efficient batch approach
        if records.len() > 100 {
            let mut all_data = Vec::with_capacity(records.len() * 100); // Rough estimate
            
            // Pre-serialize everything
            for record in records {
                let serialized = serde_json::to_vec(record)
                    .map_err(|e| StorageError::PersistenceError(format!("Serialization failed: {}", e)))?;
                
                // Store the record size as a 4-byte header
                let record_size = serialized.len() as u32;
                all_data.extend_from_slice(&record_size.to_be_bytes());
                all_data.extend_from_slice(&serialized);
            }
            
            // Write everything in one operation
            let wal_path = self.get_wal_path();
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .map_err(|e| StorageError::PersistenceError(format!("Failed to open WAL: {}", e)))?;
                
            file.write_all(&all_data)
                .map_err(|e| StorageError::PersistenceError(format!("Failed to write to WAL: {}", e)))?;
                
            return Ok(());
        }
        
        // Slower path for fewer records: use existing approach
        for record in records {
            self.append_record(record)?;
        }
        
        Ok(())
    }
    
    /// Replay WAL to recover data after a crash
    pub fn replay_wal(&self) -> Result<Vec<Record>, StorageError> {
        self.wal.replay()
            .map_err(|e| StorageError::PersistenceError(e.to_string()))
    }
    
    /// Truncate WAL after chunks are safely persisted
    pub fn truncate_wal(&self) -> Result<(), StorageError> {
        println!("Truncating WAL...");
        
        // Don't lock the entire file, just create a new one and atomically replace it
        let log_path = self.wal.wal_path.join("records.wal");
        let temp_path = self.wal.wal_path.join("records.wal.new");
        
        println!("Creating new empty WAL file at {:?}", temp_path);
        
        // Create a new empty file
        {
            let file = File::create(&temp_path)
                .map_err(|e| StorageError::PersistenceError(format!("Failed to create new WAL file: {}", e)))?;
            
            // Explicitly close the file here
            drop(file);
        }
        
        // Atomically replace the old file with the new one
        println!("Replacing old WAL with new empty file");
        fs::rename(&temp_path, &log_path)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to replace WAL file: {}", e)))?;
        
        // Now reopen the file in the mutex
        println!("Reopening WAL file handle");
        let new_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| StorageError::PersistenceError(format!("Failed to open new WAL file: {}", e)))?;
        
        // Replace the file in our mutex
        {
            println!("Acquiring WAL file lock to update handle");
            match self.wal.log_file.lock() {
                Ok(mut log_file) => {
                    println!("Lock acquired, replacing WAL file handle");
                    *log_file = new_file;
                    println!("WAL file handle replaced successfully");
                },
                Err(e) => {
                    println!("Error acquiring WAL lock: {:?}", e);
                    return Err(StorageError::PersistenceError(format!("Mutex error: {:?}", e)));
                }
            }
        }
        
        println!("WAL truncation completed successfully");
        Ok(())
    }
    
    /// Mark chunk WAL records as durable, removing them from active records
    pub fn mark_chunk_durable(&self, chunk_id: i64, chunk_duration_secs: i64) -> Result<(), StorageError> {
        let chunk_end_time = chunk_id + chunk_duration_secs;
        let mut active_records = self.active_records.lock().unwrap();
        
        // Remove all records that are now safely in a persisted chunk
        active_records.retain(|_, timestamp| *timestamp >= chunk_end_time);
        
        Ok(())
    }
    
    // Helper method to get the path for a chunk file
    fn get_chunk_path(&self, chunk_id: i64) -> PathBuf {
        self.base_path.join("chunks").join(format!("{}.chunk", chunk_id))
    }

    // Helper method to get the path for the WAL file
    fn get_wal_path(&self) -> PathBuf {
        self.base_path.join("wal").join("records.wal")
    }
}

/// Write-ahead log for crash recovery
#[derive(Debug)]
pub struct WriteAheadLog {
    wal_path: PathBuf,
    log_file: Mutex<File>,
}

impl WriteAheadLog {
    pub fn new(wal_dir: impl AsRef<Path>) -> io::Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        fs::create_dir_all(&wal_dir)?;
        
        let log_path = wal_dir.join("records.wal");
        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&log_path)?;
        
        Ok(WriteAheadLog {
            wal_path: wal_dir,
            log_file: Mutex::new(log_file),
        })
    }
    
    /// Append a record to the WAL
    pub fn append_record(&self, record: &Record) -> io::Result<()> {
        let serialized = serde_json::to_vec(record)?;
        let record_size = serialized.len() as u32;
        
        let mut log_file = self.log_file.lock().unwrap();
        
        // Write 4-byte size header followed by record data
        log_file.write_all(&record_size.to_be_bytes())?;
        log_file.write_all(&serialized)?;
        log_file.sync_data()?; // Ensure data is flushed to disk
        
        Ok(())
    }
    
    /// Replay the WAL to recover records
    pub fn replay(&self) -> io::Result<Vec<Record>> {
        let mut log_file = self.log_file.lock().unwrap();
        log_file.seek(SeekFrom::Start(0))?;
        
        let mut records = Vec::new();
        
        // Read each record
        loop {
            // Read record size (4 bytes)
            let mut size_buf = [0u8; 4];
            match log_file.read_exact(&mut size_buf) {
                Ok(_) => {
                    let record_size = u32::from_be_bytes(size_buf) as usize;
                    
                    // Read the record data
                    let mut record_data = vec![0u8; record_size];
                    log_file.read_exact(&mut record_data)?;
                    
                    // Deserialize
                    let record: Record = serde_json::from_slice(&record_data)?;
                    records.push(record);
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Reached the end of the file
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        
        Ok(records)
    }
} 