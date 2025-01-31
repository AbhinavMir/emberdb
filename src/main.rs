use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use crate::storage::StorageEngine;
use crate::api::rest::RestApi;
use crate::timeseries::query::QueryEngine;
use crate::config::load_config;

mod api;
mod config;
mod error;
mod fhir;
mod storage;
mod timeseries;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize components
    let config = load_config(Path::new("./config.yaml"))?;
    let storage = StorageEngine::new();  // Remove config.storage parameter since it's not used in new()
    let query_engine = QueryEngine::new(Arc::new(storage));
    let api = RestApi::new(Arc::new(query_engine));

    println!("Starting server on http://127.0.0.1:3000");
    
    // Start the server
    warp::serve(api.routes())
        .run(([127, 0, 0, 1], 3000))
        .await;

    Ok(())
}
