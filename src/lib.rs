//! EmberDB: A FHIR-optimized time-series database
//! 
//! EmberDB provides specialized storage for FHIR resources with a focus on
//! efficient time-series operations and hot/warm/cold data management.

pub mod fhir;
pub mod storage;
pub mod timeseries; 

use std::collections::HashMap;
use crate::storage::TimeChunk;

#[derive(Debug, Clone)]
pub enum Value {
    Float(f64),
    Integer(i64),
    String(String),
}

#[derive(Debug)]
pub struct StorageEngine {
    // For now, just keep everything in memory
    chunks: HashMap<i64, TimeChunk>,  // Use TimeChunk directly
}