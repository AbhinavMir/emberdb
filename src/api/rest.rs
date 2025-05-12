use std::sync::Arc;
use warp::Filter;
use warp::reply::Json;
use std::convert::Infallible;
use serde::{Deserialize, Serialize};
use crate::timeseries::query::QueryEngine;
use crate::fhir::{FHIRObservation};
use crate::fhir::conversion::FHIRConverter;
use crate::storage::Record;

#[derive(Debug, Serialize, Deserialize)]
pub struct FHIRObservationRequest {
    pub resourceType: String,
    pub status: String,
    pub code: CodeBlock,
    pub subject: Reference,
    pub effectiveDateTime: String,
    pub valueQuantity: ValueQuantity,
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
                                    data: Some(serde_json::to_value(record).unwrap()),
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

    fn post_observation(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let query_engine = Arc::clone(&self.query_engine);
        
        warp::path!("fhir" / "Observation")
            .and(warp::post())
            .and(warp::body::json())
            .and_then(move |observation: FHIRObservationRequest| {
                let query_engine = Arc::clone(&query_engine);
                async move {
                    // Convert the FHIR Observation request to our internal format
                    let coding = &observation.code.coding[0];
                    let patient_id = observation.subject.reference.replace("Patient/", "");
                    
                    // Parse the timestamp
                    let timestamp = match parse_iso8601_to_unix(&observation.effectiveDateTime) {
                        Ok(ts) => ts,
                        Err(_) => {
                            let response = ApiResponse {
                                status: "error".to_string(),
                                message: "Invalid timestamp format".to_string(),
                                data: None,
                            };
                            return Ok::<Json, Infallible>(warp::reply::json(&response));
                        }
                    };
                    
                    // Create a FHIR Observation
                    let fhir_observation = FHIRObservation::Numeric {
                        code: coding.code.clone(),
                        value: observation.valueQuantity.value,
                        unit: observation.valueQuantity.unit.clone(),
                        timestamp,
                        patient_id,
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
                            return Ok::<Json, Infallible>(warp::reply::json(&response));
                        }
                    }
                    
                    let response = ApiResponse {
                        status: "success".to_string(),
                        message: "Observation stored successfully".to_string(),
                        data: Some(serde_json::to_value(observation).unwrap()),
                    };
                    Ok::<Json, Infallible>(warp::reply::json(&response))
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
}

// Helper function to parse ISO8601 timestamp to Unix timestamp
fn parse_iso8601_to_unix(iso_time: &str) -> Result<i64, Box<dyn std::error::Error>> {
    // This is a simplistic implementation
    // In a real app, use a proper datetime crate
    let timestamp = chrono::DateTime::parse_from_rfc3339(iso_time)?
        .timestamp();
    Ok(timestamp)
} 