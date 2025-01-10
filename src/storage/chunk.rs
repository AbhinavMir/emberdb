use super::Record;
use std::collections::HashMap;

#[derive(Debug)]
pub struct TimeChunk {
    start_time: i64,
    end_time: i64,
    // Group records by metric name for faster access
    records: HashMap<String, Vec<Record>>,
}

impl TimeChunk {
    pub fn new(start_time: i64, end_time: i64) -> Self {
        TimeChunk {
            start_time,
            end_time,
            records: HashMap::new(),
        }
    }

    pub fn append(&mut self, record: Record) -> Result<(), &'static str> {
        if record.timestamp < self.start_time || record.timestamp >= self.end_time {
            return Err("Record timestamp outside chunk range");
        }

        self.records
            .entry(record.metric_name.clone())
            .or_insert_with(Vec::new)
            .push(record);

        Ok(())
    }
}