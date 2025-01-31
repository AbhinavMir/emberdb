#[derive(Debug)]
pub enum EmberError {
    Storage(StorageError),
    Query(QueryError),
    Fhir(FHIRError),
    Config(ConfigError),
    Api(ApiError),
}

impl std::error::Error for EmberError {} 