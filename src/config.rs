#[derive(Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub api: ApiConfig,
    pub chunk_duration: Duration,
}

pub fn load_config(path: &Path) -> Result<Config, ConfigError> {
    // Load and validate configuration
} 