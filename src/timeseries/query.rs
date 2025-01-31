use std::sync::Arc;
use crate::storage::{StorageEngine, Record};
use std::time::Duration;
use std::collections::HashMap;

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

pub struct QueryEngine {
    storage: Arc<StorageEngine>,
}

impl QueryEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        QueryEngine { storage }
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
        }
    }
}

impl TimeSeriesQuery {
    pub fn execute(&self, _engine: &StorageEngine) -> Result<Vec<crate::storage::Record>, QueryError> {
        todo!("Implement execute")
    }
} 