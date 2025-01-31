use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use std::fmt;
use std::error::Error;

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub path: String,
    pub max_chunk_size: usize,
}

#[derive(Debug, Deserialize)]
pub struct ApiConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub api: ApiConfig,
    #[serde(with = "duration_parser")]
    pub chunk_duration: Duration,
}

#[derive(Debug)]
pub enum ConfigError {
    IoError(std::io::Error),
    ParseError(serde_yaml::Error),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::IoError(e) => write!(f, "IO error: {}", e),
            ConfigError::ParseError(e) => write!(f, "Parse error: {}", e),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ConfigError::IoError(e) => Some(e),
            ConfigError::ParseError(e) => Some(e),
        }
    }
}

pub fn load_config(path: &Path) -> Result<Config, ConfigError> {
    let contents = std::fs::read_to_string(path)
        .map_err(ConfigError::IoError)?;
    
    serde_yaml::from_str(&contents)
        .map_err(ConfigError::ParseError)
}

mod duration_parser {
    use serde::{self, Deserialize, Deserializer};
    use std::time::Duration;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }

    fn parse_duration(duration_str: &str) -> Result<Duration, String> {
        let (value_str, unit) = duration_str.split_at(duration_str.len() - 1);
        let value: u64 = value_str.parse().map_err(|_| "Invalid duration value".to_string())?;

        match unit {
            "s" => Ok(Duration::from_secs(value)),
            "m" => Ok(Duration::from_secs(value * 60)),
            "h" => Ok(Duration::from_secs(value * 3600)),
            "d" => Ok(Duration::from_secs(value * 86400)),
            _ => Err(format!("Invalid duration unit: {}", unit)),
        }
    }
} 