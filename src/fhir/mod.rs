//! FHIR-specific types and operations
//! 
//! This module contains the core FHIR resource types and operations that
//! EmberDB supports.

pub mod resources;
pub mod conversion;

#[derive(Debug)]
pub enum FHIRError {
    ConversionError(&'static str),
    ValidationError(&'static str),
    NotFound(&'static str),
}

/// Core FHIR data types that map to time-series data
#[derive(Debug, Clone)]
pub enum FHIRObservation {
    Numeric {
        code: String,
        value: f64,
        unit: String,
        timestamp: i64,
        patient_id: String,
    },
    // Add other types as needed
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