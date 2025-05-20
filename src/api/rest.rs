use std::sync::Arc;
use warp::Filter;
use warp::reply::{Json, with_header};
use std::convert::Infallible;
use serde::{Deserialize, Serialize};
use crate::timeseries::query::QueryEngine;
use crate::fhir::{FHIRObservation, ObservationComponent};
use crate::fhir::{MedicationAdministration, DeviceObservation, VitalSigns, VitalType};
use crate::fhir::conversion::FHIRConverter;
use crate::storage::Record;
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
pub struct FHIRObservationComponentRequest {
    pub code: CodeBlock,
    pub valueQuantity: ValueQuantity,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SampledData {
    pub origin: ValueQuantity,
    pub period: f64,
    pub factor: Option<f64>,
    pub dimensions: u32,
    pub data: String, // Space-separated values
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FHIRObservationRequest {
    pub resourceType: String,
    pub status: String,
    pub code: CodeBlock,
    pub subject: Reference,
    pub effectiveDateTime: String,
    
    // Value fields (one will be populated based on type)
    pub valueQuantity: Option<ValueQuantity>,
    pub component: Option<Vec<FHIRObservationComponentRequest>>,
    pub valueSampledData: Option<SampledData>,
    
    // Optional device reference
    pub device: Option<Reference>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeBlock {
    pub coding: Vec<Coding>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Coding {
    pub system: String,
    pub code: String,
    pub display: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Reference {
    pub reference: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValueQuantity {
    pub value: f64,
    pub unit: String,
    pub system: String,
    pub code: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse {
    pub status: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

// New request for medication administration
#[derive(Debug, Serialize, Deserialize)]
pub struct MedicationAdministrationRequest {
    pub resourceType: String, // Should be "MedicationAdministration"
    pub status: String,       // administration status
    pub medication: CodeBlock, // medication code and display
    pub dosage: DosageQuantity, // medication dose
    pub route: Coding,        // administration route
    pub subject: Reference,   // patient reference
    pub effectiveDateTime: String, // when medication was administered
    pub performer: Option<Reference>, // practitioner who administered
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DosageQuantity {
    pub value: f64,
    pub unit: String,
    pub system: String,
    pub code: String,
}

// New request for device observation
#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceObservationRequest {
    pub resourceType: String, // Should be "DeviceObservation"
    pub status: String,
    pub device: Reference,    // device reference
    pub code: CodeBlock,      // observation code
    pub valueQuantity: ValueQuantity, // measured value
    pub effectiveDateTime: String,
    pub subject: Option<Reference>, // optional patient reference
    pub deviceType: String,   // type of device
    pub metricType: String,   // type of metric
}

// New request for vital signs
#[derive(Debug, Serialize, Deserialize)]
pub struct VitalSignsRequest {
    pub resourceType: String, // Should be "VitalSigns"
    pub code: CodeBlock,      // vital sign code
    pub subject: Reference,   // patient reference
    pub effectiveDateTime: String,
    
    // Value types - one will be populated
    pub valueQuantity: Option<ValueQuantity>, // For single measurements
    pub component: Option<Vec<FHIRObservationComponentRequest>>, // For blood pressure
    
    // Optional metadata
    pub method: Option<Coding>, // measurement method
    pub position: Option<Coding>, // patient position
    pub reliability: Option<String>, // reliability indicator
}

// Add this new request struct after the existing request structs
#[derive(Debug, Serialize, Deserialize)]
pub struct FHIRBundle {
    pub resourceType: String,  // Should be "Bundle"
    pub type_: String,         // Should be "transaction" or "batch"
    pub entry: Vec<BundleEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BundleEntry {
    pub resource: serde_json::Value,
    pub request: BundleRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BundleRequest {
    pub method: String,
    pub url: String,
}

// Add this request struct near the other request structs
#[derive(Debug, Serialize, Deserialize)]
pub struct DebugSettings {
    pub memory_mode: bool,
    pub disable_wal: bool,
    pub batch_size: Option<usize>,
}

pub struct RestApi {
    query_engine: Arc<QueryEngine>,
}

impl RestApi {
    pub fn new(query_engine: Arc<QueryEngine>) -> Self {
        RestApi { query_engine }
    }

    pub fn routes(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        // Add OPTIONS route for CORS preflight requests
        let cors_options = warp::options()
            .map(|| {
                warp::reply::with_header(
                    warp::reply::with_header(
                        warp::reply::with_header(
                            warp::reply(),
                            "Access-Control-Allow-Origin", "*"
                        ),
                        "Access-Control-Allow-Methods", "GET, POST, OPTIONS"
                    ),
                    "Access-Control-Allow-Headers", "Content-Type"
                )
            });
        
        // Basic CRUD endpoints
        cors_options
            .or(self.get_observation())
            .or(self.post_observation())
            .or(self.post_bundle())  // Add the new bundle endpoint
            .or(self.get_patient())
            .or(self.post_medication_administration())
            .or(self.post_device_observation())
            .or(self.post_vital_signs())
            .or(self.get_resource_by_type())
            .or(self.debug_metrics())
            .or(self.get_time_chunked())
            // Time-series analysis endpoints
            .or(self.get_trend_analysis())
            .or(self.get_stats())
            .or(self.get_outliers())
            .or(self.get_rate_of_change())
            .or(self.debug_settings())
            .map(|reply| {
                // Add CORS headers to all responses
                with_header(
                    with_header(
                        with_header(
                            reply,
                            "Access-Control-Allow-Origin", "*"
                        ),
                        "Access-Control-Allow-Methods", "GET, POST, OPTIONS"
                    ),
                    "Access-Control-Allow-Headers", "Content-Type"
                )
            })
    }

    fn get_observation(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir" / "Observation")
            .and(warp::get())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and_then(move |params: std::collections::HashMap<String, String>| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Extract patient and code from query params if available
                    let patient = params.get("patient");
                    let code = params.get("code");
                    
                    if let (Some(patient_id), Some(code_value)) = (patient, code) {
                        // Format metric name with a wildcard for the unit part
                        let metric_pattern = format!("{}|{}|", patient_id, code_value);
                        
                        println!("Querying metric pattern: {}", metric_pattern);
                        
                        // Query for records with this metric prefix
                        match query_engine.get_metrics_by_prefix(&metric_pattern) {
                            Ok(Some(record)) => {
                                let response = ApiResponse {
                                    status: "success".to_string(),
                                    message: "Observation found".to_string(),
                                    data: Some(format_record_for_api(&record)),
                                };
                                Ok::<Json, Infallible>(warp::reply::json(&response))
                            },
                            Ok(None) => {
                                let response = ApiResponse {
                                    status: "error".to_string(),
                                    message: "No observations found".to_string(), 
                                    data: None,
                                };
                                Ok::<Json, Infallible>(warp::reply::json(&response))
                            },
                            Err(e) => {
                                let response = ApiResponse {
                                    status: "error".to_string(),
                                    message: format!("Error querying observations: {:?}", e),
                                    data: None,
                                };
                                Ok::<Json, Infallible>(warp::reply::json(&response))
                            }
                        }
                    } else {
                        // Return all observations (not implemented yet)
                        let response = ApiResponse {
                            status: "error".to_string(),
                            message: "Listing all observations not implemented yet".to_string(),
                            data: None,
                        };
                        Ok::<Json, Infallible>(warp::reply::json(&response))
                    }
                }
            })
    }

    async fn handle_observation_request(
        observation: FHIRObservationRequest, 
        query_engine: Arc<QueryEngine>
    ) -> Result<impl warp::Reply, Infallible> {
        // Parse the timestamp
        let timestamp = match parse_iso8601_to_unix(&observation.effectiveDateTime) {
            Ok(ts) => ts,
            Err(_) => {
                let response = ApiResponse {
                    status: "error".to_string(),
                    message: "Invalid timestamp format".to_string(),
                    data: None,
                };
                return Ok(warp::reply::json(&response));
            }
        };
        
        // Extract patient ID
        let patient_id = observation.subject.reference.replace("Patient/", "");
        
        // Extract device ID if present
        let device_id = observation.device.as_ref().map(|dev| dev.reference.replace("Device/", ""));
        
        // Get the main code
        let coding = &observation.code.coding[0];
        let code = coding.code.clone();
        
        // Create the appropriate FHIR Observation based on which value field is present
        let fhir_observation = if let Some(value_quantity) = &observation.valueQuantity {
            // Numeric observation
            FHIRObservation::Numeric {
                code,
                value: value_quantity.value,
                unit: value_quantity.unit.clone(),
                timestamp,
                patient_id: patient_id.clone(),
                device_id: device_id.clone(),
            }
        } else if let Some(components) = &observation.component {
            // Component observation
            let mut observation_components = Vec::new();
            
            for component in components {
                let comp_coding = &component.code.coding[0];
                let comp_value = &component.valueQuantity;
                
                observation_components.push(ObservationComponent {
                    code: comp_coding.code.clone(),
                    value: comp_value.value,
                    unit: comp_value.unit.clone(),
                });
            }
            
            FHIRObservation::Component {
                code,
                components: observation_components,
                timestamp,
                patient_id: patient_id.clone(),
                device_id: device_id.clone(),
            }
        } else if let Some(sampled_data) = &observation.valueSampledData {
            // Sampled data observation
            // Parse the space-separated data values
            let values: Vec<f64> = sampled_data.data
                .split_whitespace()
                .filter_map(|s| s.parse::<f64>().ok())
                .collect();
                
            FHIRObservation::SampledData {
                code,
                period: sampled_data.period,
                factor: sampled_data.factor.unwrap_or(1.0),
                data: values,
                start_time: timestamp,
                patient_id: patient_id.clone(),
                device_id: device_id.clone(),
            }
        } else {
            // No known value type
            let response = ApiResponse {
                status: "error".to_string(),
                message: "No valid observation value provided".to_string(),
                data: None,
            };
            return Ok(warp::reply::json(&response));
        };
        
        // Convert to records and store
        let records = fhir_observation.to_records();
        println!("Storing observation with metric names: {:?}", 
                records.iter().map(|r| &r.metric_name).collect::<Vec<_>>());
        
        for record in records {
            if let Err(err) = query_engine.store_record(record) {
                let response = ApiResponse {
                    status: "error".to_string(),
                    message: format!("Failed to store observation: {:?}", err),
                    data: None,
                };
                return Ok(warp::reply::json(&response));
            }
        }
        
        let response = ApiResponse {
            status: "success".to_string(),
            message: "Observation stored successfully".to_string(),
            data: Some(serde_json::to_value(observation).unwrap()),
        };
        Ok(warp::reply::json(&response))
    }

    fn post_observation(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir" / "Observation")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |observation: FHIRObservationRequest| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    Self::handle_observation_request(observation, query_engine).await
                }
            })
    }

    fn get_patient(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("fhir" / "Patient")
            .and(warp::get())
            .map(|| {
                let response = ApiResponse {
                    status: "error".to_string(),
                    message: "Patient resource not implemented yet".to_string(),
                    data: None,
                };
                warp::reply::json(&response)
            })
    }

    // New method to query resources by type
    fn get_resource_by_type(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir" / "resources" / String)
            .and(warp::get())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and_then(move |resource_type: String, params: std::collections::HashMap<String, String>| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Get time range from query params, with defaults
                    let now = chrono::Utc::now().timestamp();
                    let start_time = params.get("_since")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0); // Default to all records (timestamp 0)
                    
                    let end_time = params.get("_until")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now);
                    
                    // Query by resource type
                    match query_engine.query_by_resource_type(&resource_type, start_time, end_time) {
                        Ok(records) => {
                            let response = ApiResponse {
                                status: "success".to_string(),
                                message: format!("Found {} records for {}", records.len(), resource_type),
                                data: Some(serde_json::to_value(format_records_for_api(&records)).unwrap()),
                            };
                            Ok::<Json, Infallible>(warp::reply::json(&response))
                        },
                        Err(_) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: format!("No records found for {}", resource_type),
                                data: None,
                            };
                            Ok::<Json, Infallible>(warp::reply::json(&response))
                        }
                    }
                }
            })
    }

    // Debug endpoint to see all metrics and resource types
    fn debug_metrics(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("debug" / "metrics")
            .and(warp::get())
            .and_then(move || {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Get internal data about metrics and resources
                    let debug_info = query_engine.debug_metrics().unwrap_or_default();
                    
                    let response = ApiResponse {
                        status: "success".to_string(),
                        message: "Debug metrics info".to_string(),
                        data: Some(serde_json::to_value(debug_info).unwrap()),
                    };
                    Ok::<Json, Infallible>(warp::reply::json(&response))
                }
            })
    }

    // New endpoint for time-chunked queries
    fn get_time_chunked(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir" / "timeseries")
            .and(warp::get())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and_then(move |params: std::collections::HashMap<String, String>| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Extract parameters
                    let resource_type = params.get("resource_type").map(|s| s.to_string()).unwrap_or("Observation".to_string());
                    
                    // Parse time parameters
                    let now = chrono::Utc::now().timestamp();
                    let start_time = params.get("start")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now - 86400); // Default to last 24 hours
                    
                    let end_time = params.get("end")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now);
                    
                    // Parse chunk size (in seconds)
                    let chunk_size = params.get("chunk_size")
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(3600); // Default to 1 hour
                    
                    // Query with time chunking
                    match query_engine.query_time_chunked(&resource_type, start_time, end_time, chunk_size) {
                        Ok(chunks) => {
                            // Transform each chunk to have better-formatted records
                            let formatted_chunks: Vec<serde_json::Value> = chunks.iter().map(|chunk| {
                                serde_json::json!({
                                    "start_time": chunk.start_time,
                                    "end_time": chunk.end_time,
                                    "records": format_records_for_api(&chunk.records)
                                })
                            }).collect();
                            
                            let response = ApiResponse {
                                status: "success".to_string(),
                                message: format!("Found data in {} time chunks", chunks.len()),
                                data: Some(serde_json::to_value(formatted_chunks).unwrap()),
                            };
                            Ok::<Json, Infallible>(warp::reply::json(&response))
                        },
                        Err(_e) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Error querying time chunks".to_string(),
                                data: None,
                            };
                            Ok::<Json, Infallible>(warp::reply::json(&response))
                        }
                    }
                }
            })
    }

    fn post_medication_administration(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir" / "MedicationAdministration")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |request: MedicationAdministrationRequest| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Validate resource type
                    if request.resourceType != "MedicationAdministration" {
                        let response = ApiResponse {
                            status: "error".to_string(),
                            message: "Invalid resource type".to_string(),
                            data: None,
                        };
                        return Ok::<Json, Infallible>(warp::reply::json(&response));
                    }
                    
                    // Parse timestamp
                    let timestamp = match parse_iso8601_to_unix(&request.effectiveDateTime) {
                        Ok(ts) => ts,
                        Err(_) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Invalid timestamp format".to_string(),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    };
                    
                    // Extract patient ID
                    let patient_id = request.subject.reference.replace("Patient/", "");
                    
                    // Extract practitioner ID if present
                    let practitioner_id = request.performer.as_ref()
                        .map(|performer| performer.reference.replace("Practitioner/", ""));
                    
                    // Extract medication information
                    let coding = &request.medication.coding[0];
                    
                    // Create MedicationAdministration
                    let med_administration = MedicationAdministration {
                        medication_code: coding.code.clone(),
                        medication_display: coding.display.clone(),
                        dose_value: request.dosage.value,
                        dose_unit: request.dosage.unit.clone(),
                        route: request.route.display.clone(),
                        timestamp,
                        patient_id,
                        practitioner_id,
                        status: request.status.clone(),
                    };
                    
                    // Convert to records and store
                    let records = med_administration.to_records();
                    println!("Storing medication administration with metric name: {:?}", 
                            records.iter().map(|r| &r.metric_name).collect::<Vec<_>>());
                    
                    for record in records {
                        if let Err(err) = query_engine.store_record(record) {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: format!("Failed to store medication administration: {:?}", err),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    }
                    
                    let response = ApiResponse {
                        status: "success".to_string(),
                        message: "Medication administration stored successfully".to_string(),
                        data: Some(serde_json::to_value(request).unwrap()),
                    };
                    Ok(warp::reply::json(&response))
                }
            })
    }

    fn post_device_observation(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir" / "DeviceObservation")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |request: DeviceObservationRequest| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Validate resource type
                    if request.resourceType != "DeviceObservation" {
                        let response = ApiResponse {
                            status: "error".to_string(),
                            message: "Invalid resource type".to_string(),
                            data: None,
                        };
                        return Ok::<Json, Infallible>(warp::reply::json(&response));
                    }
                    
                    // Parse timestamp
                    let timestamp = match parse_iso8601_to_unix(&request.effectiveDateTime) {
                        Ok(ts) => ts,
                        Err(_) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Invalid timestamp format".to_string(),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    };
                    
                    // Extract device ID
                    let device_id = request.device.reference.replace("Device/", "");
                    
                    // Extract patient ID if present
                    let patient_id = request.subject.as_ref()
                        .map(|subject| subject.reference.replace("Patient/", ""));
                    
                    // Extract code
                    let coding = &request.code.coding[0];
                    
                    // Create device observation
                    let device_observation = DeviceObservation {
                        device_id,
                        device_type: request.deviceType.clone(),
                        metric_type: request.metricType.clone(),
                        code: coding.code.clone(),
                        value: request.valueQuantity.value,
                        unit: request.valueQuantity.unit.clone(),
                        timestamp,
                        patient_id,
                        status: request.status.clone(),
                    };
                    
                    // Convert to records and store
                    let records = device_observation.to_records();
                    println!("Storing device observation with metric name: {:?}", 
                            records.iter().map(|r| &r.metric_name).collect::<Vec<_>>());
                    
                    for record in records {
                        if let Err(err) = query_engine.store_record(record) {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: format!("Failed to store device observation: {:?}", err),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    }
                    
                    let response = ApiResponse {
                        status: "success".to_string(),
                        message: "Device observation stored successfully".to_string(),
                        data: Some(serde_json::to_value(request).unwrap()),
                    };
                    Ok(warp::reply::json(&response))
                }
            })
    }

    fn post_vital_signs(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir" / "VitalSigns")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |request: VitalSignsRequest| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Validate resource type
                    if request.resourceType != "VitalSigns" {
                        let response = ApiResponse {
                            status: "error".to_string(),
                            message: "Invalid resource type".to_string(),
                            data: None,
                        };
                        return Ok::<Json, Infallible>(warp::reply::json(&response));
                    }
                    
                    // Parse timestamp
                    let timestamp = match parse_iso8601_to_unix(&request.effectiveDateTime) {
                        Ok(ts) => ts,
                        Err(_) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Invalid timestamp format".to_string(),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    };
                    
                    // Extract patient ID
                    let patient_id = request.subject.reference.replace("Patient/", "");
                    
                    // Extract optional metadata
                    let method = request.method.as_ref().map(|m| m.display.clone());
                    let position = request.position.as_ref().map(|p| p.display.clone());
                    let reliability = request.reliability.clone();
                    
                    // Get main code
                    let coding = &request.code.coding[0];
                    let code = coding.code.clone();
                    
                    // Determine vital type and create VitalSigns object
                    let vital_signs = if let Some(value_quantity) = &request.valueQuantity {
                        // Single vital sign
                        let vital_type = match code.as_str() {
                            "8867-4" => VitalType::HeartRate,
                            "9279-1" => VitalType::RespiratoryRate,
                            "59408-5" => VitalType::OxygenSaturation,
                            "8310-5" => VitalType::Temperature,
                            "29463-7" => VitalType::Weight,
                            "8302-2" => VitalType::Height,
                            _ => {
                                let response = ApiResponse {
                                    status: "error".to_string(),
                                    message: format!("Unknown vital sign code: {}", code),
                                    data: None,
                                };
                                return Ok(warp::reply::json(&response));
                            }
                        };
                        
                        // Create VitalSigns object
                        VitalSigns {
                            vital_type,
                            value: value_quantity.value,
                            unit: value_quantity.unit.clone(),
                            timestamp,
                            patient_id,
                            method,
                            position,
                            reliability,
                        }
                    } else if let Some(components) = &request.component {
                        // Check if this is blood pressure (has systolic and diastolic)
                        if code == "85354-9" && components.len() == 2 { // 85354-9 is BP panel
                            // Find systolic and diastolic components
                            let mut systolic = None;
                            let mut diastolic = None;
                            
                            for component in components {
                                let comp_code = &component.code.coding[0].code;
                                if comp_code == "8480-6" { // Systolic
                                    systolic = Some(component.valueQuantity.value);
                                } else if comp_code == "8462-4" { // Diastolic
                                    diastolic = Some(component.valueQuantity.value);
                                }
                            }
                            
                            if let (Some(sys), Some(dia)) = (systolic, diastolic) {
                                // Get unit from first component
                                let unit = components[0].valueQuantity.unit.clone();
                                
                                VitalSigns {
                                    vital_type: VitalType::BloodPressure {
                                        systolic: sys,
                                        diastolic: dia,
                                    },
                                    value: sys, // Store systolic as the main value for consistency
                                    unit,
                                    timestamp,
                                    patient_id,
                                    method,
                                    position,
                                    reliability,
                                }
                            } else {
                                let response = ApiResponse {
                                    status: "error".to_string(),
                                    message: "Blood pressure must have both systolic and diastolic components".to_string(),
                                    data: None,
                                };
                                return Ok(warp::reply::json(&response));
                            }
                        } else {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Invalid component-based vital sign".to_string(),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    } else {
                        let response = ApiResponse {
                            status: "error".to_string(),
                            message: "No valid vital sign value provided".to_string(),
                            data: None,
                        };
                        return Ok(warp::reply::json(&response));
                    };
                    
                    // Convert to records and store
                    let records = vital_signs.to_records();
                    println!("Storing vital signs with metric names: {:?}", 
                            records.iter().map(|r| &r.metric_name).collect::<Vec<_>>());
                    
                    for record in records {
                        if let Err(err) = query_engine.store_record(record) {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: format!("Failed to store vital signs: {:?}", err),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    }
                    
                    let response = ApiResponse {
                        status: "success".to_string(),
                        message: "Vital signs stored successfully".to_string(),
                        data: Some(serde_json::to_value(request).unwrap()),
                    };
                    Ok(warp::reply::json(&response))
                }
            })
    }

    /// Endpoint for trend analysis
    fn get_trend_analysis(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("timeseries" / "trend")
            .and(warp::get())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and_then(move |params: std::collections::HashMap<String, String>| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Parse parameters
                    let resource_type = params.get("resource_type")
                        .map(|s| s.to_string())
                        .unwrap_or("Observation".to_string());
                        
                    let metric = params.get("metric")
                        .map(|s| s.to_string())
                        .unwrap_or("".to_string());
                        
                    let now = chrono::Utc::now().timestamp();
                    let start_time = params.get("start")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now - 86400); // Default to last 24 hours
                    
                    let end_time = params.get("end")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now);
                    
                    if metric.is_empty() {
                        // If no specific metric, do resource-wide analysis
                        let pattern = params.get("pattern").map(|s| s.to_string()).unwrap_or("".to_string());
                        
                        match query_engine.calculate_trend_by_resource(&resource_type, &pattern, start_time, end_time) {
                            Ok(trends) => {
                                let response = ApiResponse {
                                    status: "success".to_string(),
                                    message: format!("Found trend analysis for {} metrics", trends.len()),
                                    data: Some(serde_json::to_value(trends).unwrap()),
                                };
                                Ok::<Json, Infallible>(warp::reply::json(&response))
                            },
                            Err(e) => {
                                let response = ApiResponse {
                                    status: "error".to_string(),
                                    message: format!("Failed to calculate trends: {:?}", e),
                                    data: None,
                                };
                                Ok(warp::reply::json(&response))
                            }
                        }
                    } else {
                        // Specific metric trend analysis
                        match query_engine.calculate_trend(&metric, start_time, end_time) {
                            Ok(trend) => {
                                let response = ApiResponse {
                                    status: "success".to_string(),
                                    message: format!("Trend analysis for metric: {}", metric),
                                    data: Some(serde_json::to_value(trend).unwrap()),
                                };
                                Ok::<Json, Infallible>(warp::reply::json(&response))
                            },
                            Err(e) => {
                                let response = ApiResponse {
                                    status: "error".to_string(),
                                    message: format!("Failed to calculate trend: {:?}", e),
                                    data: None,
                                };
                                Ok(warp::reply::json(&response))
                            }
                        }
                    }
                }
            })
    }
    
    /// Endpoint for statistics
    fn get_stats(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("timeseries" / "stats")
            .and(warp::get())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and_then(move |params: std::collections::HashMap<String, String>| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Required parameter: metric
                    let metric = match params.get("metric") {
                        Some(m) => m.to_string(),
                        None => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Missing required parameter: metric".to_string(),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    };
                    
                    // Parse time parameters
                    let now = chrono::Utc::now().timestamp();
                    let start_time = params.get("start")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now - 86400); // Default to last 24 hours
                    
                    let end_time = params.get("end")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now);
                    
                    // Calculate statistics
                    match query_engine.calculate_stats(&metric, start_time, end_time) {
                        Ok(stats) => {
                            let response = ApiResponse {
                                status: "success".to_string(),
                                message: format!("Statistics for metric: {}", metric),
                                data: Some(serde_json::to_value(stats).unwrap()),
                            };
                            Ok::<Json, Infallible>(warp::reply::json(&response))
                        },
                        Err(e) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: format!("Failed to calculate statistics: {:?}", e),
                                data: None,
                            };
                            Ok(warp::reply::json(&response))
                        }
                    }
                }
            })
    }
    
    /// Endpoint for outlier detection
    fn get_outliers(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("timeseries" / "outliers")
            .and(warp::get())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and_then(move |params: std::collections::HashMap<String, String>| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Required parameter: metric
                    let metric = match params.get("metric") {
                        Some(m) => m.to_string(),
                        None => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Missing required parameter: metric".to_string(),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    };
                    
                    // Parse time parameters
                    let now = chrono::Utc::now().timestamp();
                    let start_time = params.get("start")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now - 86400); // Default to last 24 hours
                    
                    let end_time = params.get("end")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now);
                    
                    // Parse threshold
                    let threshold = params.get("threshold")
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(2.0); // Default Z-score threshold of 2.0
                    
                    // Detect outliers
                    match query_engine.detect_outliers(&metric, start_time, end_time, threshold) {
                        Ok(outliers) => {
                            let response = ApiResponse {
                                status: "success".to_string(),
                                message: format!("Found {} outliers for metric: {}", outliers.outliers.len(), metric),
                                data: Some(serde_json::to_value(outliers).unwrap()),
                            };
                            Ok::<Json, Infallible>(warp::reply::json(&response))
                        },
                        Err(e) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: format!("Failed to detect outliers: {:?}", e),
                                data: None,
                            };
                            Ok(warp::reply::json(&response))
                        }
                    }
                }
            })
    }
    
    /// Endpoint for rate of change calculation
    fn get_rate_of_change(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("timeseries" / "rate")
            .and(warp::get())
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and_then(move |params: std::collections::HashMap<String, String>| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Required parameter: metric
                    let metric = match params.get("metric") {
                        Some(m) => m.to_string(),
                        None => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Missing required parameter: metric".to_string(),
                                data: None,
                            };
                            return Ok(warp::reply::json(&response));
                        }
                    };
                    
                    // Parse time parameters
                    let now = chrono::Utc::now().timestamp();
                    let start_time = params.get("start")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now - 86400); // Default to last 24 hours
                    
                    let end_time = params.get("end")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(now);
                    
                    // Parse period
                    let period = params.get("period")
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(3600); // Default to hourly rate
                    
                    // Calculate rate of change
                    match query_engine.calculate_rate_of_change(&metric, start_time, end_time, period) {
                        Ok(rates) => {
                            let response = ApiResponse {
                                status: "success".to_string(),
                                message: format!("Calculated {} rate points for metric: {}", rates.len(), metric),
                                data: Some(serde_json::to_value(format_records_for_api(&rates)).unwrap()),
                            };
                            Ok::<Json, Infallible>(warp::reply::json(&response))
                        },
                        Err(e) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: format!("Failed to calculate rate of change: {:?}", e),
                                data: None,
                            };
                            Ok(warp::reply::json(&response))
                        }
                    }
                }
            })
    }

    fn post_bundle(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |bundle: FHIRBundle| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Verify this is a Bundle
                    if bundle.resourceType != "Bundle" {
                        let response = ApiResponse {
                            status: "error".to_string(),
                            message: "Expected a FHIR Bundle".to_string(),
                            data: None,
                        };
                        return Ok::<Json, Infallible>(warp::reply::json(&response));
                    }
                    
                    let mut processed_count = 0;
                    let mut errors = Vec::new();
                    let mut records_to_store: Vec<Record> = Vec::new();
                    
                    // Process each entry in the bundle
                    for entry in bundle.entry {
                        // Check if this is an Observation POST
                        if let Some(resource_type) = entry.resource.get("resourceType").and_then(|v| v.as_str()) {
                            if resource_type == "Observation" && entry.request.method == "POST" {
                                // Parse the observation
                                match serde_json::from_value::<FHIRObservationRequest>(entry.resource.clone()) {
                                    Ok(observation) => {
                                        // Parse the timestamp
                                        match parse_iso8601_to_unix(&observation.effectiveDateTime) {
                                            Ok(timestamp) => {
                                                // Extract patient ID
                                                let patient_id = observation.subject.reference.replace("Patient/", "");
                                                
                                                // Extract device ID if present
                                                let device_id = observation.device.as_ref().map(|dev| dev.reference.replace("Device/", ""));
                                                
                                                // Get the main code
                                                let coding = &observation.code.coding[0];
                                                let code = coding.code.clone();
                                                
                                                // Create the appropriate FHIR Observation
                                                let fhir_observation = if let Some(value_quantity) = &observation.valueQuantity {
                                                    // Numeric observation
                                                    Some(FHIRObservation::Numeric {
                                                        code,
                                                        value: value_quantity.value,
                                                        unit: value_quantity.unit.clone(),
                                                        timestamp,
                                                        patient_id: patient_id.clone(),
                                                        device_id: device_id.clone(),
                                                    })
                                                } else if let Some(components) = &observation.component {
                                                    // Component observation
                                                    let mut observation_components = Vec::new();
                                                    
                                                    for component in components {
                                                        let comp_coding = &component.code.coding[0];
                                                        let comp_value = &component.valueQuantity;
                                                        
                                                        observation_components.push(ObservationComponent {
                                                            code: comp_coding.code.clone(),
                                                            value: comp_value.value,
                                                            unit: comp_value.unit.clone(),
                                                        });
                                                    }
                                                    
                                                    Some(FHIRObservation::Component {
                                                        code,
                                                        components: observation_components,
                                                        timestamp,
                                                        patient_id: patient_id.clone(),
                                                        device_id: device_id.clone(),
                                                    })
                                                } else if let Some(sampled_data) = &observation.valueSampledData {
                                                    // Sampled data observation
                                                    // Parse the space-separated data values
                                                    let values: Vec<f64> = sampled_data.data
                                                        .split_whitespace()
                                                        .filter_map(|s| s.parse::<f64>().ok())
                                                        .collect();
                                                        
                                                    Some(FHIRObservation::SampledData {
                                                        code,
                                                        period: sampled_data.period,
                                                        factor: sampled_data.factor.unwrap_or(1.0),
                                                        data: values,
                                                        start_time: timestamp,
                                                        patient_id: patient_id.clone(),
                                                        device_id: device_id.clone(),
                                                    })
                                                } else {
                                                    None
                                                };
                                                
                                                if let Some(obs) = fhir_observation {
                                                    // Convert to records and store in batch
                                                    let new_records = obs.to_records();
                                                    records_to_store.extend(new_records);
                                                    processed_count += 1;
                                                } else {
                                                    errors.push(format!("No valid observation value provided"));
                                                }
                                            },
                                            Err(_) => {
                                                errors.push(format!("Invalid timestamp format"));
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        errors.push(format!("Failed to parse observation: {}", e));
                                    }
                                }
                            }
                        }
                    }
                    
                    // Store all records in a single batch operation
                    if !records_to_store.is_empty() {
                        if let Err(err) = query_engine.store_records(records_to_store) {
                            errors.push(format!("Failed to store some records: {:?}", err));
                        }
                    }
                    
                    let response = ApiResponse {
                        status: if errors.is_empty() { "success".to_string() } else { "partial".to_string() },
                        message: format!("Processed {} observations with {} errors", processed_count, errors.len()),
                        data: if errors.is_empty() { 
                            None 
                        } else { 
                            Some(serde_json::to_value(errors).unwrap()) 
                        },
                    };
                    
                    Ok::<Json, Infallible>(warp::reply::json(&response))
                }
            })
    }

    fn debug_settings(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("debug" / "settings")
            .and(warp::post())
            .and(warp::body::json())
            .map(move |settings: DebugSettings| {
                // Apply settings to the query engine
                if let Err(e) = query_engine.set_debug_settings(settings.memory_mode, settings.disable_wal, settings.batch_size) {
                    return warp::reply::with_status(
                        warp::reply::json(&json!({
                            "status": "error",
                            "message": format!("Failed to apply debug settings: {}", e)
                        })),
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
                
                warp::reply::with_status(
                    warp::reply::json(&json!({
                        "status": "success",
                        "message": "Debug settings applied"
                    })),
                    warp::http::StatusCode::OK,
                )
            })
    }
}

// Helper function to parse ISO8601 timestamp to Unix timestamp
fn parse_iso8601_to_unix(iso_time: &str) -> Result<i64, Box<dyn std::error::Error>> {
    // This is a simplistic implementation
    // In a real app, use a proper datetime crate
    let timestamp = chrono::DateTime::parse_from_rfc3339(iso_time)?
        .timestamp();
    Ok(timestamp)
}

/// Helper function to transform a Record into an API-friendly response
fn format_record_for_api(record: &Record) -> serde_json::Value {
    // Extract components from metric name (format: "{patient_id}|{code}|{unit}")
    let parts: Vec<&str> = record.metric_name.split('|').collect();
    
    // Extract patient ID, code, and unit
    let patient_id = parts.get(0).unwrap_or(&"unknown");
    let code = parts.get(1).unwrap_or(&"unknown");
    let unit = parts.get(2).unwrap_or(&"unknown");
    
    // Add code display name when possible
    let code_display = match *code {
        "8867-4" => "Heart Rate",
        "85354-9" => "Blood Pressure Panel",
        "8480-6" => "Systolic Blood Pressure",
        "8462-4" => "Diastolic Blood Pressure",
        "8310-5" => "Body Temperature",
        "9279-1" => "Respiratory Rate",
        "59408-5" => "Oxygen Saturation",
        "2339-0" => "Blood Glucose",
        _ => ""
    };
    
    // Format the timestamp as an ISO string for convenience
    let iso_date = if record.timestamp > 0 {
        use chrono::{DateTime, Utc};
        DateTime::<Utc>::from_timestamp(record.timestamp, 0)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| "invalid_timestamp".to_string())
    } else {
        "unknown".to_string()
    };
    
    // Build an enhanced API response
    let mut response = serde_json::json!({
        "id": format!("{}:{}", record.resource_type, record.metric_name),
        "resourceType": record.resource_type,
        "timestamp": record.timestamp,
        "iso_date": iso_date,
        "value": record.value,
        "subject": {
            "reference": format!("Patient/{}", patient_id)
        },
        "metric_name": record.metric_name,
        "metric_components": {
            "patient_id": patient_id,
            "code": code,
            "unit": unit
        },
        "code_system": "http://loinc.org",
        "code_display": code_display
    });
    
    // Add context elements directly to the top level
    if !record.context.is_empty() {
        let obj = response.as_object_mut().unwrap();
        for (key, value) in &record.context {
            obj.insert(key.clone(), serde_json::Value::String(value.clone()));
        }
    }
    
    response
}

/// Helper functions to format multiple records
fn format_records_for_api(records: &[Record]) -> Vec<serde_json::Value> {
    records.iter()
        .map(|record| format_record_for_api(record))
        .collect()
} 