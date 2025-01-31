use emberdb;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use warp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize components
    let config = load_config(Path::new("config.yaml"))?;
    let storage = StorageEngine::new(config.storage);
    let query_engine = QueryEngine::new(Arc::new(storage));
    let api = RestApi::new(Arc::new(query_engine));

    // Start the server
    warp::serve(api.routes())
        .run(([127, 0, 0, 1], 3000))
        .await;

    Ok(())
}
