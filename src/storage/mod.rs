//! Core storage engine
//! 
//! Handles the fundamental storage operations including:
//! - Data persistence
//! - Indexing
//! - Hot/warm/cold data management

mod chunk;

// Re-export TimeChunk so it can be used by other modules
pub use chunk::TimeChunk;

// Re-export Record since it's used by TimeChunk
#[derive(Debug, Clone)]
pub struct Record {
    pub timestamp: i64,
    pub metric_name: String,
    pub value: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_chunk() {
        let mut chunk = TimeChunk::new(0, 100);
        let record = Record {
            timestamp: 50,
            metric_name: "test".to_string(),
            value: 42.0,
        };
        
        assert!(chunk.append(record).is_ok());
    }
} 