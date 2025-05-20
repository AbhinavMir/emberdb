use crate::fhir::{FHIRObservation, FHIRError, ObservationComponent, 
                   MedicationAdministration, DeviceObservation, VitalSigns, VitalType};
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
    MedicationAdministration(MedicationAdministration),
    DeviceObservation(DeviceObservation),
    VitalSigns(VitalSigns),
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

impl FHIRConverter for MedicationAdministration {
    fn to_records(&self) -> Vec<Record> {
        let mut context = HashMap::new();
        
        // Add medication metadata to context
        context.insert("medication_display".to_string(), self.medication_display.clone());
        context.insert("route".to_string(), self.route.clone());
        context.insert("status".to_string(), self.status.clone());
        
        if let Some(practitioner) = &self.practitioner_id {
            context.insert("practitioner_id".to_string(), practitioner.clone());
        }
        
        // Create the metric name in format: {patient_id}|{medication_code}|{dose_unit}
        let metric_name = format!("{}|{}|{}", self.patient_id, self.medication_code, self.dose_unit);
        
        vec![Record {
            timestamp: self.timestamp,
            metric_name,
            value: self.dose_value,
            context,
            resource_type: "MedicationAdministration".to_string(),
        }]
    }

    fn from_records(records: &[Record]) -> Result<Self, FHIRError> {
        if records.is_empty() {
            return Err(FHIRError::ConversionError("No records provided".to_string()));
        }

        let record = &records[0];
        
        // Parse metric name components (patient_id|medication_code|dose_unit)
        let parts: Vec<&str> = record.metric_name.split('|').collect();
        if parts.len() < 3 {
            return Err(FHIRError::ConversionError(
                format!("Invalid metric name format: {}", record.metric_name)
            ));
        }
        
        let patient_id = parts[0].to_string();
        let medication_code = parts[1].to_string();
        let dose_unit = parts[2].to_string();
        
        // Extract metadata from context
        let medication_display = record.context.get("medication_display")
            .cloned()
            .unwrap_or_else(|| "Unknown Medication".to_string());
            
        let route = record.context.get("route")
            .cloned()
            .unwrap_or_else(|| "unspecified".to_string());
            
        let status = record.context.get("status")
            .cloned()
            .unwrap_or_else(|| "completed".to_string());
            
        let practitioner_id = record.context.get("practitioner_id").cloned();
        
        Ok(MedicationAdministration {
            medication_code,
            medication_display,
            dose_value: record.value,
            dose_unit,
            route,
            timestamp: record.timestamp,
            patient_id,
            practitioner_id,
            status,
        })
    }
}

impl FHIRConverter for DeviceObservation {
    fn to_records(&self) -> Vec<Record> {
        let mut context = HashMap::new();
        
        // Add device metadata to context
        context.insert("device_type".to_string(), self.device_type.clone());
        context.insert("metric_type".to_string(), self.metric_type.clone());
        context.insert("status".to_string(), self.status.clone());
        
        // Add patient reference if present
        if let Some(patient_id) = &self.patient_id {
            context.insert("patient_id".to_string(), patient_id.clone());
        }
        
        // For device observations, use device ID as the first component
        // Format: {device_id}|{code}|{unit}
        let metric_name = format!("{}|{}|{}", self.device_id, self.code, self.unit);
        
        vec![Record {
            timestamp: self.timestamp,
            metric_name,
            value: self.value,
            context,
            resource_type: "DeviceObservation".to_string(),
        }]
    }

    fn from_records(records: &[Record]) -> Result<Self, FHIRError> {
        if records.is_empty() {
            return Err(FHIRError::ConversionError("No records provided".to_string()));
        }

        let record = &records[0];
        
        // Parse metric name components (device_id|code|unit)
        let parts: Vec<&str> = record.metric_name.split('|').collect();
        if parts.len() < 3 {
            return Err(FHIRError::ConversionError(
                format!("Invalid metric name format for device: {}", record.metric_name)
            ));
        }
        
        let device_id = parts[0].to_string();
        let code = parts[1].to_string();
        let unit = parts[2].to_string();
        
        // Extract metadata from context
        let device_type = record.context.get("device_type")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
            
        let metric_type = record.context.get("metric_type")
            .cloned()
            .unwrap_or_else(|| "measurement".to_string());
            
        let status = record.context.get("status")
            .cloned()
            .unwrap_or_else(|| "final".to_string());
            
        // Patient association is optional for device observations
        let patient_id = record.context.get("patient_id").cloned();
        
        Ok(DeviceObservation {
            device_id,
            device_type,
            metric_type,
            code,
            value: record.value,
            unit,
            timestamp: record.timestamp,
            patient_id,
            status,
        })
    }
}

impl FHIRConverter for VitalSigns {
    fn to_records(&self) -> Vec<Record> {
        let mut context = HashMap::new();
        let mut records = Vec::new();
        
        // Add optional metadata to context if present
        if let Some(method) = &self.method {
            context.insert("method".to_string(), method.clone());
        }
        
        if let Some(position) = &self.position {
            context.insert("position".to_string(), position.clone());
        }
        
        if let Some(reliability) = &self.reliability {
            context.insert("reliability".to_string(), reliability.clone());
        }
        
        // Process based on vital type
        match &self.vital_type {
            VitalType::BloodPressure { systolic, diastolic } => {
                // For blood pressure, create two separate records
                
                // Systolic record
                let mut systolic_context = context.clone();
                systolic_context.insert("component".to_string(), "systolic".to_string());
                systolic_context.insert("bp_diastolic".to_string(), diastolic.to_string());
                
                let systolic_record = Record {
                    timestamp: self.timestamp,
                    metric_name: format!("{}|8480-6|{}", self.patient_id, self.unit), // 8480-6 is LOINC for systolic
                    value: *systolic,
                    context: systolic_context,
                    resource_type: "VitalSigns".to_string(),
                };
                records.push(systolic_record);
                
                // Diastolic record
                let mut diastolic_context = context.clone();
                diastolic_context.insert("component".to_string(), "diastolic".to_string());
                diastolic_context.insert("bp_systolic".to_string(), systolic.to_string());
                
                let diastolic_record = Record {
                    timestamp: self.timestamp,
                    metric_name: format!("{}|8462-4|{}", self.patient_id, self.unit), // 8462-4 is LOINC for diastolic
                    value: *diastolic,
                    context: diastolic_context,
                    resource_type: "VitalSigns".to_string(),
                };
                records.push(diastolic_record);
            },
            _ => {
                // For all other vitals, create a single record with the appropriate LOINC code
                let code = match &self.vital_type {
                    VitalType::HeartRate => "8867-4",        // Heart rate
                    VitalType::RespiratoryRate => "9279-1",  // Respiratory rate
                    VitalType::OxygenSaturation => "59408-5", // SpO2
                    VitalType::Temperature => "8310-5",      // Body temperature
                    VitalType::Weight => "29463-7",          // Body weight
                    VitalType::Height => "8302-2",           // Body height
                    _ => unreachable!(), // BloodPressure already handled
                };
                
                // Add vital type to context
                context.insert("vital_type".to_string(), format!("{:?}", self.vital_type));
                
                let record = Record {
                    timestamp: self.timestamp,
                    metric_name: format!("{}|{}|{}", self.patient_id, code, self.unit),
                    value: self.value,
                    context,
                    resource_type: "VitalSigns".to_string(),
                };
                records.push(record);
            }
        }
        
        records
    }

    fn from_records(records: &[Record]) -> Result<Self, FHIRError> {
        if records.is_empty() {
            return Err(FHIRError::ConversionError("No records provided".to_string()));
        }

        let record = &records[0];
        
        // Parse metric name components (patient_id|code|unit)
        let parts: Vec<&str> = record.metric_name.split('|').collect();
        if parts.len() < 3 {
            return Err(FHIRError::ConversionError(
                format!("Invalid metric name format: {}", record.metric_name)
            ));
        }
        
        let patient_id = parts[0].to_string();
        let code = parts[1].to_string();
        let unit = parts[2].to_string();
        
        // Extract optional metadata
        let method = record.context.get("method").cloned();
        let position = record.context.get("position").cloned();
        let reliability = record.context.get("reliability").cloned();
        
        // Determine vital type from LOINC code
        let vital_type = match code.as_str() {
            "8867-4" => VitalType::HeartRate,
            "9279-1" => VitalType::RespiratoryRate,
            "59408-5" => VitalType::OxygenSaturation,
            "8310-5" => VitalType::Temperature,
            "29463-7" => VitalType::Weight,
            "8302-2" => VitalType::Height,
            "8480-6" => {
                // Systolic BP - need to look for diastolic value
                let diastolic = record.context.get("bp_diastolic")
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.0);
                
                VitalType::BloodPressure {
                    systolic: record.value,
                    diastolic,
                }
            },
            "8462-4" => {
                // Diastolic BP - need to look for systolic value
                let systolic = record.context.get("bp_systolic")
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.0);
                
                VitalType::BloodPressure {
                    systolic,
                    diastolic: record.value,
                }
            },
            _ => {
                // Unknown code - try to determine from context
                if let Some(vital_type_str) = record.context.get("vital_type") {
                    match vital_type_str.as_str() {
                        "HeartRate" => VitalType::HeartRate,
                        "RespiratoryRate" => VitalType::RespiratoryRate,
                        "OxygenSaturation" => VitalType::OxygenSaturation,
                        "Temperature" => VitalType::Temperature,
                        "Weight" => VitalType::Weight,
                        "Height" => VitalType::Height,
                        _ => return Err(FHIRError::ConversionError(
                            format!("Unknown vital type: {}", vital_type_str)
                        )),
                    }
                } else {
                    return Err(FHIRError::ConversionError(
                        format!("Unknown vital code: {}", code)
                    ));
                }
            }
        };
        
        Ok(VitalSigns {
            vital_type,
            value: record.value,
            unit,
            timestamp: record.timestamp,
            patient_id,
            method,
            position,
            reliability,
        })
    }
} 