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

/// MedicationAdministration resource for tracking medication events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MedicationAdministration {
    pub medication_code: String,      // Medication code (RxNorm, NDC, etc.)
    pub medication_display: String,   // Human-readable medication name
    pub dose_value: f64,              // Numeric dose amount
    pub dose_unit: String,            // Unit for the dose (mg, mL, etc.)
    pub route: String,                // Administration route (oral, IV, etc.)
    pub timestamp: i64,               // When medication was administered
    pub patient_id: String,           // Patient receiving medication
    pub practitioner_id: Option<String>, // Healthcare provider who administered
    pub status: String,               // Status of the administration (completed, etc.)
}

/// Device readings and telemetry 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceObservation {
    pub device_id: String,            // Device identifier
    pub device_type: String,          // Type of device (ventilator, pump, etc.)
    pub metric_type: String,          // Type of metric (setting, measurement, alert)
    pub code: String,                 // Observation code
    pub value: f64,                   // Observed value
    pub unit: String,                 // Unit of measurement
    pub timestamp: i64,               // When observation was recorded
    pub patient_id: Option<String>,   // Associated patient (if applicable)
    pub status: String,               // Device status during measurement
}

/// VitalSigns profile - a specialized observation for vital measurements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VitalSigns {
    pub vital_type: VitalType,        // Type of vital sign
    pub value: f64,                   // The measurement value
    pub unit: String,                 // Unit of measurement
    pub timestamp: i64,               // When vitals were measured
    pub patient_id: String,           // Patient these vitals belong to
    pub method: Option<String>,       // Measurement method if applicable
    pub position: Option<String>,     // Patient position during measurement
    pub reliability: Option<String>,  // Reliability indicator
}

/// Standard vital sign types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VitalType {
    HeartRate,                 // Heart rate in beats per minute
    BloodPressure {            // Blood pressure with components
        systolic: f64,         // Systolic value
        diastolic: f64,        // Diastolic value
    },
    RespiratoryRate,           // Respiratory rate in breaths per minute
    OxygenSaturation,          // SpO2 as percentage
    Temperature,               // Body temperature
    Weight,                    // Body weight
    Height,                    // Body height
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