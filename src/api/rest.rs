use std::sync::Arc;
use warp::Filter;
use warp::reply::Json;
use std::convert::Infallible;
use serde::{Deserialize, Serialize};
use crate::timeseries::query::QueryEngine;
use crate::fhir::{FHIRObservation, ObservationComponent};
use crate::fhir::conversion::FHIRConverter;
use crate::storage::Record;

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Coding {
    pub system: String,
    pub code: String,
    pub display: String,
}

#[derive(Debug, Serialize, Deserialize)]
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

pub struct RestApi {
    query_engine: Arc<QueryEngine>,
}

impl RestApi {
    pub fn new(query_engine: Arc<QueryEngine>) -> Self {
        RestApi { query_engine }
    }

    pub fn routes(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        // Basic CRUD endpoints
        self.get_observation()
            .or(self.post_observation())
            .or(self.get_patient())
            .or(self.get_resource_by_type())
            .or(self.debug_metrics())
            .or(self.get_time_chunked())
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
                            Ok(record) => {
                                let response = ApiResponse {
                                    status: "success".to_string(),
                                    message: "Observation found".to_string(),
                                    data: Some(format_record_for_api(&record)),
                                };
                                Ok::<Json, Infallible>(warp::reply::json(&response))
                            },
                            Err(_) => {
                                let response = ApiResponse {
                                    status: "error".to_string(),
                                    message: "No observations found".to_string(),
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
    // Extract patient ID from metric name (format: "{patient_id}|{code}|{unit}")
    let patient_id = record.metric_name
        .split('|')
        .next()
        .unwrap_or("unknown");
        
    // Build a better API response
    let mut response = serde_json::json!({
        "id": format!("{}:{}", record.resource_type, record.metric_name),
        "resourceType": record.resource_type,
        "timestamp": record.timestamp,
        "value": record.value,
        "subject": {
            "reference": format!("Patient/{}", patient_id)
        },
        "metric_name": record.metric_name,
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

// Function to format a collection of records for API response
fn format_records_for_api(records: &[Record]) -> Vec<serde_json::Value> {
    records.iter().map(format_record_for_api).collect()
} 