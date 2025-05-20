use std::sync::Arc;
use crate::storage::{self, StorageEngine, Record, StorageError};
use std::time::Duration;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::timeseries::functions::{
    TimeSeriesFunctions, TrendAnalysis, TimeSeriesStats, OutlierDetection
};
use std::fmt;

#[derive(Debug, Clone)]
pub struct TimeSeriesQuery {
    pub start_time: i64,
    pub end_time: i64,
    pub metrics: Vec<String>,
    pub aggregation: Option<Aggregation>,
    pub interval: Option<Duration>,
}

#[derive(Debug, Clone)]
pub enum Aggregation {
    Mean,
    Max,
    Min,
    Count,
    Sum,
}

#[derive(Debug)]
pub enum QueryError {
    StorageError(String),
    InvalidTimeRange(String),
    MetricNotFound(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            QueryError::InvalidTimeRange(msg) => write!(f, "Invalid time range: {}", msg),
            QueryError::MetricNotFound(msg) => write!(f, "Metric not found: {}", msg),
        }
    }
}

impl From<StorageError> for QueryError {
    fn from(error: StorageError) -> Self {
        QueryError::StorageError(format!("{:?}", error))
    }
}

// Add this new struct for debug info
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DebugMetricsInfo {
    pub metrics: Vec<String>,
    pub resource_metrics: HashMap<String, Vec<String>>,
    pub storage_info: String,
}

// Additional structure to represent chunked time data
#[derive(Debug, Serialize, Deserialize)]
pub struct TimeChunk {
    pub start_time: i64,
    pub end_time: i64,
    pub records: Vec<Record>,
}

pub struct QueryEngine {
    storage: Arc<StorageEngine>,
}

impl QueryEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        QueryEngine { storage }
    }

    pub fn store_record(&self, record: Record) -> Result<(), QueryError> {
        self.storage.insert(record)
            .map_err(|e| QueryError::StorageError(e.to_string()))
    }
    
    pub fn store_records(&self, records: Vec<Record>) -> Result<(), QueryError> {
        if records.is_empty() {
            return Ok(());
        }
        
        // Group records by chunk to reduce lock contention
        let mut records_by_chunk = std::collections::HashMap::new();
        
        // Pre-process to group records by chunk ID
        for record in records {
            let chunk_id = storage::chunk_id_for_timestamp(record.timestamp, self.storage.chunk_duration());
            records_by_chunk.entry(chunk_id).or_insert_with(Vec::new).push(record);
        }
        
        // First, write everything to WAL in a single operation if possible
        if let Err(e) = self.storage.append_records_to_wal(records_by_chunk.values().flatten().cloned().collect()) {
            return Err(QueryError::StorageError(e.to_string()));
        }
        
        // Then store records in each chunk
        for (chunk_id, chunk_records) in records_by_chunk {
            if let Err(e) = self.storage.insert_batch(chunk_id, chunk_records) {
                return Err(QueryError::StorageError(e.to_string()));
            }
        }
        
        Ok(())
    }

    pub fn query_range(&self, query: TimeSeriesQuery) -> Result<Vec<Record>, QueryError> {
        if query.start_time >= query.end_time {
            return Err(QueryError::InvalidTimeRange(
                "Start time must be before end time".to_string()
            ));
        }

        let mut results = Vec::new();
        
        for metric in &query.metrics {
            let records = self.storage.as_ref()
                .query_range(query.start_time, query.end_time, metric)
                .map_err(|e| QueryError::StorageError(e.to_string()))?;

            if let Some(aggregation) = &query.aggregation {
                results.extend(self.aggregate_records(records, aggregation, query.interval));
            } else {
                results.extend(records);
            }
        }

        Ok(results)
    }

    pub fn query_latest(&self, metric: &str) -> Result<Option<Record>, QueryError> {
        self.storage.as_ref()
            .get_latest(metric)
            .map_err(|e| QueryError::StorageError(e.to_string()))
    }

    pub fn get_metrics_by_prefix(&self, prefix: &str) -> Result<Option<Record>, QueryError> {
        println!("Searching for metrics with prefix: {}", prefix);
        
        let metrics = self.storage.as_ref().get_matching_metrics(prefix)
            .map_err(|e| QueryError::StorageError(e.to_string()))?;
        
        println!("Found matching metrics: {:?}", metrics);
        
        if metrics.is_empty() {
            return Ok(None);
        }
        
        let metric = &metrics[0];
        self.query_latest(metric)
    }

    /// Query records by resource type and time range
    pub fn query_by_resource_type(&self, resource_type: &str, start_time: i64, end_time: i64) 
        -> Result<Vec<Record>, QueryError> 
    {
        if start_time >= end_time {
            return Err(QueryError::InvalidTimeRange(
                "Start time must be before end time".to_string()
            ));
        }
        
        println!("Querying records for resource type: {}", resource_type);
        
        self.storage.as_ref()
            .query_by_resource_type(resource_type, start_time, end_time)
            .map_err(|e| QueryError::StorageError(e.to_string()))
    }
    
    /// Get metrics for a specific resource type
    pub fn get_metrics_by_resource_type(&self, resource_type: &str) -> Result<Vec<String>, QueryError> {
        println!("Getting metrics for resource type: {}", resource_type);
        
        self.storage.as_ref()
            .get_metrics_by_resource_type(resource_type)
            .map_err(|e| QueryError::StorageError(e.to_string()))
    }

    fn aggregate_records(
        &self,
        records: Vec<Record>,
        aggregation: &Aggregation,
        interval: Option<Duration>
    ) -> Vec<Record> {
        if records.is_empty() {
            return vec![];
        }

        match interval {
            Some(interval) => self.aggregate_by_interval(records, aggregation, interval),
            None => vec![self.aggregate_all(records, aggregation)]
        }
    }

    fn aggregate_by_interval(
        &self,
        records: Vec<Record>,
        aggregation: &Aggregation,
        interval: Duration
    ) -> Vec<Record> {
        let mut grouped: HashMap<i64, Vec<Record>> = HashMap::new();
        let interval_secs = interval.as_secs() as i64;

        for record in records {
            let interval_start = record.timestamp - (record.timestamp % interval_secs);
            grouped.entry(interval_start)
                .or_insert_with(Vec::new)
                .push(record);
        }

        grouped.into_iter()
            .map(|(_timestamp, group)| self.aggregate_all(group, aggregation))
            .collect()
    }

    fn aggregate_all(&self, records: Vec<Record>, aggregation: &Aggregation) -> Record {
        let first_record = &records[0];
        let values: Vec<f64> = records.iter().map(|r| r.value).collect();
        
        let value = match aggregation {
            Aggregation::Mean => values.iter().sum::<f64>() / values.len() as f64,
            Aggregation::Max => values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b)),
            Aggregation::Min => values.iter().fold(f64::INFINITY, |a, &b| a.min(b)),
            Aggregation::Count => values.len() as f64,
            Aggregation::Sum => values.iter().sum(),
        };

        Record {
            timestamp: first_record.timestamp,
            metric_name: first_record.metric_name.clone(),
            value,
            context: first_record.context.clone(),
            resource_type: first_record.resource_type.clone(),
        }
    }

    /// Get debug info about metrics and resources
    pub fn debug_metrics(&self) -> Result<DebugMetricsInfo, QueryError> {
        // Get the raw debug info from storage
        self.storage.as_ref()
            .debug_metrics()
            .map_err(|e| QueryError::StorageError(e.to_string()))
    }

    /// Query data in specific time chunks
    pub fn query_time_chunked(&self, resource_type: &str, start_time: i64, end_time: i64, chunk_size_secs: u64) 
        -> Result<Vec<TimeChunk>, QueryError> 
    {
        if start_time >= end_time {
            return Err(QueryError::InvalidTimeRange(
                "Start time must be before end time".to_string()
            ));
        }
        
        println!("Querying time-chunked data for resource type: {} from {} to {} with chunk size {}s", 
            resource_type, start_time, end_time, chunk_size_secs);
        
        // First get all matching records
        let records = self.query_by_resource_type(resource_type, start_time, end_time)?;
        
        // Group them by time chunks
        let chunk_size = chunk_size_secs as i64;
        let mut chunked_data: HashMap<i64, Vec<Record>> = HashMap::new();
        
        for record in records {
            // Calculate which chunk this belongs to
            let chunk_start = record.timestamp - (record.timestamp % chunk_size);
            
            chunked_data.entry(chunk_start)
                .or_insert_with(Vec::new)
                .push(record);
        }
        
        // Convert to our response format
        let mut result = Vec::new();
        for (chunk_start, records) in chunked_data {
            let chunk = TimeChunk {
                start_time: chunk_start,
                end_time: chunk_start + chunk_size,
                records,
            };
            result.push(chunk);
        }
        
        // Sort chunks by start time
        result.sort_by_key(|chunk| chunk.start_time);
        
        println!("Found {} time chunks with data", result.len());
        Ok(result)
    }

    /// Calculate trend analysis for a specific metric
    pub fn calculate_trend(&self, metric: &str, start_time: i64, end_time: i64) 
        -> Result<TrendAnalysis, QueryError> 
    {
        let records = self.storage.as_ref()
            .query_range(start_time, end_time, metric)
            .map_err(|e| QueryError::StorageError(e.to_string()))?;
            
        Ok(TimeSeriesFunctions::calculate_trend(&records))
    }
    
    /// Calculate trend analysis for records by resource type
    pub fn calculate_trend_by_resource(&self, resource_type: &str, metric_pattern: &str, start_time: i64, end_time: i64) 
        -> Result<Vec<TrendAnalysis>, QueryError> 
    {
        // Get all metric names for this resource type
        let metrics = self.storage.as_ref()
            .get_metrics_by_resource_type(resource_type)
            .map_err(|e| QueryError::StorageError(e.to_string()))?;
            
        // Filter metrics by pattern
        let matching_metrics: Vec<String> = metrics.into_iter()
            .filter(|m| m.contains(metric_pattern))
            .collect();
            
        if matching_metrics.is_empty() {
            return Ok(Vec::new());
        }
        
        // Calculate trend for each matching metric
        let mut results = Vec::new();
        
        for metric in matching_metrics {
            let records = self.storage.as_ref()
                .query_range(start_time, end_time, &metric)
                .map_err(|e| QueryError::StorageError(e.to_string()))?;
                
            if !records.is_empty() {
                results.push(TimeSeriesFunctions::calculate_trend(&records));
            }
        }
        
        // Sort results by absolute slope (largest change first)
        results.sort_by(|a, b| b.slope.abs().partial_cmp(&a.slope.abs()).unwrap());
        
        Ok(results)
    }
    
    /// Calculate statistics for a metric
    pub fn calculate_stats(&self, metric: &str, start_time: i64, end_time: i64) 
        -> Result<TimeSeriesStats, QueryError> 
    {
        let records = self.storage.as_ref()
            .query_range(start_time, end_time, metric)
            .map_err(|e| QueryError::StorageError(e.to_string()))?;
            
        Ok(TimeSeriesFunctions::calculate_stats(&records))
    }
    
    /// Detect outliers for a metric
    pub fn detect_outliers(&self, metric: &str, start_time: i64, end_time: i64, threshold: f64) 
        -> Result<OutlierDetection, QueryError> 
    {
        let records = self.storage.as_ref()
            .query_range(start_time, end_time, metric)
            .map_err(|e| QueryError::StorageError(e.to_string()))?;
            
        Ok(TimeSeriesFunctions::detect_outliers(&records, threshold))
    }
    
    /// Calculate rate of change for a metric
    pub fn calculate_rate_of_change(&self, metric: &str, start_time: i64, end_time: i64, period_seconds: i64) 
        -> Result<Vec<Record>, QueryError> 
    {
        let records = self.storage.as_ref()
            .query_range(start_time, end_time, metric)
            .map_err(|e| QueryError::StorageError(e.to_string()))?;
            
        Ok(TimeSeriesFunctions::calculate_rate_of_change(&records, period_seconds))
    }

    /// Set debug settings for performance optimization
    pub fn set_debug_settings(&self, memory_mode: bool, disable_wal: bool, batch_size: Option<usize>) -> Result<(), QueryError> {
        // Log what we're trying to do
        println!("Setting debug mode: memory_mode={}, disable_wal={}, batch_size={:?}", 
                 memory_mode, disable_wal, batch_size);
        
        // Now we can directly call set_debug_settings on storage since it handles thread safety
        self.storage.set_debug_settings(memory_mode, disable_wal, batch_size)
            .map_err(|e| QueryError::StorageError(e.to_string()))
    }
}

impl TimeSeriesQuery {
    pub fn execute(&self, _engine: &StorageEngine) -> Result<Vec<crate::storage::Record>, QueryError> {
        todo!("Implement execute")
    }
} 