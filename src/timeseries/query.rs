use std::sync::Arc;
use crate::storage::{StorageEngine, Record, StorageError};
use std::time::Duration;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

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
        for record in records {
            self.store_record(record)?;
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

    pub fn query_latest(&self, metric: &str) -> Result<Record, QueryError> {
        self.storage.as_ref()
            .get_latest(metric)
            .map_err(|e| QueryError::StorageError(e.to_string()))
    }

    pub fn get_metrics_by_prefix(&self, prefix: &str) -> Result<Record, QueryError> {
        println!("Searching for metrics with prefix: {}", prefix);
        
        let metrics = self.storage.as_ref().get_matching_metrics(prefix)
            .map_err(|e| QueryError::StorageError(e.to_string()))?;
        
        println!("Found matching metrics: {:?}", metrics);
        
        if metrics.is_empty() {
            return Err(QueryError::StorageError("No matching metrics found".to_string()));
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
}

impl TimeSeriesQuery {
    pub fn execute(&self, _engine: &StorageEngine) -> Result<Vec<crate::storage::Record>, QueryError> {
        todo!("Implement execute")
    }
} 