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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_record(timestamp: i64, name: &str, value: f64) -> Record {
        Record {
            timestamp,
            metric_name: name.to_string(),
            value,
        }
    }

    #[test]
    fn test_basic_chunk_operations() {
        let mut chunk = TimeChunk::new(0, 100);
        let record = create_test_record(50, "test", 42.0);
        
        assert!(chunk.append(record).is_ok());
        
        let metrics = chunk.get_metrics_list();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0], "test");
    }

    #[test]
    fn test_time_range_validation() {
        let mut chunk = TimeChunk::new(100, 200);
        
        // Test records outside range
        let record1 = create_test_record(50, "test", 42.0);  // Too early
        let record2 = create_test_record(250, "test", 42.0); // Too late
        let record3 = create_test_record(150, "test", 42.0); // Just right
        
        assert!(chunk.append(record1).is_err());
        assert!(chunk.append(record2).is_err());
        assert!(chunk.append(record3).is_ok());
    }

    #[test]
    fn test_metric_retrieval() {
        let mut chunk = TimeChunk::new(0, 100);
        
        // Add multiple records for different metrics
        chunk.append(create_test_record(10, "cpu", 50.0)).unwrap();
        chunk.append(create_test_record(20, "cpu", 60.0)).unwrap();
        chunk.append(create_test_record(30, "memory", 80.0)).unwrap();
        
        // Test get_metric
        let cpu_metrics = chunk.get_metric("cpu").unwrap();
        assert_eq!(cpu_metrics.len(), 2);
        
        // Test get_latest
        let latest_cpu = chunk.get_latest("cpu").unwrap();
        assert_eq!(latest_cpu.value, 60.0);
        
        // Test non-existent metric
        assert!(chunk.get_metric("nonexistent").is_err());
    }

    #[test]
    fn test_range_queries() {
        let mut chunk = TimeChunk::new(0, 100);
        
        // Add records across the time range
        for i in 0..5 {
            chunk.append(create_test_record(i * 20, "test", i as f64)).unwrap();
        }
        
        // Test various ranges
        let results = chunk.get_range(10, 50, "test").unwrap();
        assert_eq!(results.len(), 2); // Should include records at t=20 and t=40
        
        let empty_results = chunk.get_range(90, 100, "test").unwrap();
        assert_eq!(empty_results.len(), 0);
    }

    #[test]
    fn test_chunk_summary() {
        let mut chunk = TimeChunk::new(0, 100);
        
        // Add some test data
        chunk.append(create_test_record(10, "temp", 20.0)).unwrap();
        chunk.append(create_test_record(20, "temp", 25.0)).unwrap();
        chunk.append(create_test_record(30, "temp", 30.0)).unwrap();
        
        let summary = chunk.summarize("temp").unwrap();
        assert_eq!(summary.count, 3);
        assert_eq!(summary.min, 20.0);
        assert_eq!(summary.max, 30.0);
        assert_eq!(summary.avg, 25.0);
    }

    #[test]
    fn test_chunk_merge() {
        let mut chunk1 = TimeChunk::new(0, 100);
        let mut chunk2 = TimeChunk::new(50, 150);
        
        chunk1.append(create_test_record(25, "test", 1.0)).unwrap();
        chunk2.append(create_test_record(75, "test", 2.0)).unwrap();
        
        assert!(chunk1.merge(chunk2).is_ok());
        
        let merged_data = chunk1.get_metric("test").unwrap();
        assert_eq!(merged_data.len(), 2);
    }

    #[test]
    fn test_compression_state() {
        let mut chunk = TimeChunk::new(0, 100);
        chunk.append(create_test_record(50, "test", 42.0)).unwrap();
        
        assert!(chunk.compress().is_ok());
        // Add more specific compression tests once the compression logic is implemented
    }

    #[test]
    fn test_validation() {
        // Test invalid time range
        let chunk = TimeChunk::new(100, 0);
        assert!(chunk.validate().is_err());
        
        // Test valid chunk
        let mut valid_chunk = TimeChunk::new(0, 100);
        valid_chunk.append(create_test_record(50, "test", 42.0)).unwrap();
        assert!(valid_chunk.validate().is_ok());
    }
} 