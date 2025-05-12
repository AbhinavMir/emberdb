use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::oneshot;
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
    let config = load_config(Path::new("config.yaml"))
        .map_err(|e| Box::<dyn Error>::from(e))?;
    
    println!("Starting EmberDB with storage path: {}", config.storage.path);
    
    // Initialize storage with persistence
    let storage = StorageEngine::new(&config)
        .map_err(|e| Box::<dyn Error>::from(e))?;
    let storage = Arc::new(storage);
    
    let query_engine = Arc::new(QueryEngine::new(Arc::clone(&storage)));
    let api = RestApi::new(Arc::clone(&query_engine));

    println!("Starting server on {}:{}", config.api.host, config.api.port);
    
    // Create a channel for shutdown signal
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    
    // Set up server with graceful shutdown
    let routes = api.routes();
    let addr = ([127, 0, 0, 1], config.api.port);
    
    // Create server future but don't run it yet
    let (_, server) = warp::serve(routes)
        .bind_with_graceful_shutdown(addr, async move {
            shutdown_rx.await.ok();
            println!("Shutting down server...");
        });
    
    // Create task for running the server
    let server_handle = tokio::spawn(server);
    
    // Wait for Ctrl+C 
    signal::ctrl_c().await?;
    println!("Ctrl+C received, starting graceful shutdown");
    
    // Start shutdown process
    shutdown_tx.send(()).ok();
    
    // Wait for server to exit
    server_handle.await.map_err(|e| Box::<dyn Error>::from(e))?;
    
    // Flush all data to disk before exiting
    println!("Flushing data to disk...");
    
    // Downcast to get access to the raw StorageEngine
    let storage_ref = Arc::as_ref(&storage);
    
    // Flush all chunks to disk
    if let Err(e) = storage_ref.flush_all() {
        eprintln!("Error flushing data: {:?}", e);
    } else {
        println!("Data successfully flushed to disk");
    }
    
    println!("Server shutdown complete");
    Ok(())
}
