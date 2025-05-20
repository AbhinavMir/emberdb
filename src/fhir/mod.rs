//! FHIR-specific types and operations
//! 
//! This module contains the core FHIR resource types and operations that
//! EmberDB supports.

pub mod resources;
pub mod conversion;

use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub enum FHIRError {
    ConversionError(String),
    ValidationError(String),
    NotFound(String),
}

/// Core FHIR data types that map to time-series data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FHIRObservation {
    /// Simple numeric observations like heart rate, temperature
    Numeric {
        code: String,         // The observation code (e.g., "8867-4" for heart rate)
        value: f64,           // The numeric value
        unit: String,         // The unit of measurement
        timestamp: i64,       // When the observation was recorded
        patient_id: String,   // The patient this observation belongs to
        device_id: Option<String>, // Optional device that recorded this observation
    },
    
    /// Component observations like blood pressure with multiple numeric components
    Component {
        code: String,         // Main observation code
        components: Vec<ObservationComponent>, // Component values
        timestamp: i64,
        patient_id: String,
        device_id: Option<String>,
    },
    
    /// Sampled data like ECG readings, EEG, etc.
    SampledData {
        code: String,         // The observation code
        period: f64,          // Time period between samples (in milliseconds)
        factor: f64,          // Scaling factor to apply to values
        data: Vec<f64>,       // The actual data points
        start_time: i64,      // When sampling started
        patient_id: String,
        device_id: Option<String>,
    },
}

/// Component value for complex observations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationComponent {
    pub code: String,
    pub value: f64,
    pub unit: String,
}

/// Converts between FHIR resources and internal time-series format
pub trait FHIRConverter {
    fn to_records(&self) -> Vec<crate::storage::Record>;
    fn from_records(records: &[crate::storage::Record]) -> Result<Self, FHIRError> 
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert!(true);
    }
} 