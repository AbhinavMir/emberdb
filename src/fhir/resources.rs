use crate::fhir::{FHIRObservation, FHIRError, ObservationComponent};
use crate::fhir::conversion::FHIRConverter;
use crate::storage::Record;
use std::collections::HashMap;

// Basic FHIR resource definitions
pub struct Patient {
    pub id: String,
    // @todo: add more fields as needed
}

pub enum FHIRResource {
    Observation(FHIRObservation),
    Patient(Patient),
}

impl FHIRConverter for FHIRObservation {
    fn to_records(&self) -> Vec<Record> {
        match self {
            FHIRObservation::Numeric { code, value, unit, timestamp, patient_id, device_id } => {
                let mut context = HashMap::new();
                if let Some(device) = device_id {
                    context.insert("device_id".to_string(), device.clone());
                }
                
                vec![Record {
                    timestamp: *timestamp,
                    metric_name: format!("{}|{}|{}", patient_id, code, unit),
                    value: *value,
                    context,
                    resource_type: "Observation".to_string(),
                }]
            },
            
            FHIRObservation::Component { code, components, timestamp, patient_id, device_id } => {
                let mut records = Vec::new();
                let mut context = HashMap::new();
                
                if let Some(device) = device_id {
                    context.insert("device_id".to_string(), device.clone());
                }
                
                // Add a record for each component
                for component in components {
                    records.push(Record {
                        timestamp: *timestamp,
                        metric_name: format!("{}|{}|{}|{}", patient_id, code, component.code, component.unit),
                        value: component.value,
                        context: context.clone(),
                        resource_type: "Observation".to_string(),
                    });
                }
                
                records
            },
            
            FHIRObservation::SampledData { code, period, factor, data, start_time, patient_id, device_id } => {
                let mut records = Vec::new();
                let mut context = HashMap::new();
                
                if let Some(device) = device_id {
                    context.insert("device_id".to_string(), device.clone());
                }
                
                // Add metadata to context
                context.insert("sample_type".to_string(), "sampled_data".to_string());
                context.insert("period_ms".to_string(), period.to_string());
                context.insert("factor".to_string(), factor.to_string());
                
                // For sampled data, create individual records for each data point
                // This enables normal time-series operations on each point
                for (i, value) in data.iter().enumerate() {
                    let point_timestamp = *start_time + (i as i64 * (*period as i64) / 1000); // Convert ms to seconds
                    
                    records.push(Record {
                        timestamp: point_timestamp,
                        metric_name: format!("{}|{}|sampled", patient_id, code),
                        value: *value * *factor, // Apply scaling factor
                        context: context.clone(),
                        resource_type: "Observation".to_string(),
                    });
                }
                
                records
            },
        }
    }

    fn from_records(records: &[Record]) -> Result<Self, FHIRError> {
        if records.is_empty() {
            return Err(FHIRError::ConversionError("No records provided".to_string()));
        }

        // Assuming all records have the same patient_id and similar structure
        let record = &records[0];
        let parts: Vec<&str> = record.metric_name.split('|').collect();
        
        if parts.len() < 3 {
            return Err(FHIRError::ConversionError(
                format!("Invalid metric name format: {}", record.metric_name)
            ));
        }
        
        let patient_id = parts[0].to_string();
        let code = parts[1].to_string();
        
        // Get device_id from context if available
        let device_id = record.context.get("device_id").cloned();
        
        // Check if this is a component observation (has 4 parts)
        if parts.len() >= 4 && parts[2] != "sampled" {
            // This is a component of a multi-component observation
            let parent_code = code.clone();
            let _component_code = parts[2].to_string();
            let _component_unit = parts[3].to_string();
            
            // Group records by timestamp to reassemble components
            let mut components_by_time = HashMap::new();
            
            for rec in records {
                let rec_parts: Vec<&str> = rec.metric_name.split('|').collect();
                if rec_parts.len() >= 4 && rec_parts[1] == parent_code.as_str() {
                    let comp_code = rec_parts[2].to_string();
                    let comp_unit = rec_parts[3].to_string();
                    
                    let component = ObservationComponent {
                        code: comp_code,
                        value: rec.value,
                        unit: comp_unit,
                    };
                    
                    components_by_time
                        .entry(rec.timestamp)
                        .or_insert_with(Vec::new)
                        .push(component);
                }
            }
            
            // Use the first timestamp's components
            if let Some((timestamp, components)) = components_by_time.into_iter().next() {
                return Ok(FHIRObservation::Component {
                    code: parent_code,
                    components,
                    timestamp,
                    patient_id,
                    device_id,
                });
            }
        }
        
        // Check if this is sampled data
        if parts.len() >= 3 && parts[2] == "sampled" {
            // Get metadata from context
            let period = record.context.get("period_ms")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(1000.0); // Default to 1 second
                
            let factor = record.context.get("factor")
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(1.0);
            
            // Sort records by timestamp
            let mut sorted_records = records.to_vec();
            sorted_records.sort_by_key(|r| r.timestamp);
            
            // Extract the values
            let data: Vec<f64> = sorted_records.iter().map(|r| r.value / factor).collect();
            let start_time = sorted_records.first().map(|r| r.timestamp).unwrap_or(0);
            
            return Ok(FHIRObservation::SampledData {
                code,
                period,
                factor,
                data,
                start_time,
                patient_id,
                device_id,
            });
        }
        
        // Default to simple numeric observation
        let unit = parts.get(2).unwrap_or(&"").to_string();
        Ok(FHIRObservation::Numeric {
            code,
            value: record.value,
            unit,
            timestamp: record.timestamp,
            patient_id,
            device_id,
        })
    }
} 