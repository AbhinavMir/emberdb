use crate::storage::ChunkError;
use crate::timeseries::query::QueryError;
use crate::fhir::FHIRError;
use crate::config::ConfigError;
use std::fmt;

#[derive(Debug)]
pub enum ApiError {
    NotFound(String),
    BadRequest(String),
    InternalError(String),
}

#[derive(Debug)]
pub enum EmberError {
    Storage(ChunkError),
    Query(QueryError),
    Fhir(FHIRError),
    Config(ConfigError),
    Api(ApiError),
}

impl std::fmt::Display for EmberError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmberError::Storage(e) => write!(f, "Storage error: {:?}", e),
            EmberError::Query(e) => write!(f, "Query error: {:?}", e),
            EmberError::Fhir(e) => write!(f, "FHIR error: {:?}", e),
            EmberError::Config(e) => write!(f, "Config error: {:?}", e),
            EmberError::Api(e) => write!(f, "API error: {:?}", e),
        }
    }
}

impl std::error::Error for EmberError {} 