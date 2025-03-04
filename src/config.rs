//! Configuration module for the application.
//!
//! This module provides a configuration system based on YAML files.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::path::Path;
use thiserror::Error;
use tracing::info;

/// Configuration error type
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Invalid database provider
    #[error("Database provider error: {0}")]
    InvalidDatabaseProvider(String),

    /// Other configuration errors
    #[error("Configuration error: {0}")]
    Other(#[from] config::ConfigError),
}

/// Database provider types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatabaseProvider {
    /// Mock database provider
    Mock,
    /// Postgres database provider
    Postgres,
}

/// Configuration for a data provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataProviderConfig {
    /// Name of the data provider
    pub name: String,
    /// Type of database provider
    pub provider: DatabaseProvider,
    /// Database connection settings
    pub settings: HashMap<String, String>,
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// List of data providers
    pub providers: Vec<DataProviderConfig>,
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum number of entries in the cache
    pub max_entries: usize,
    /// Time to live in seconds
    pub ttl_seconds: u64,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Bind address for the server
    pub bind_address: String,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    pub level: String,
}

/// Application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Database configuration
    pub database: DatabaseConfig,
    /// Cache configuration
    pub cache: CacheConfig,
    /// Server configuration
    pub server: ServerConfig,
    /// Logging configuration
    pub logging: LoggingConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            providers: vec![
                DataProviderConfig {
                    name: "users".to_string(),
                    provider: DatabaseProvider::Mock,
                    settings: HashMap::new(),
                },
            ],
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl_seconds: 60,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:6379".to_string(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig::default(),
            cache: CacheConfig::default(),
            server: ServerConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl AppConfig {
    /// Loads the configuration from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        info!("Loading configuration from {}", path.display());

        let mut cfg = config::Config::builder();

        // Start with default values
        cfg = cfg.add_source(config::File::from_str(
            include_str!("../config/default.yaml"),
            config::FileFormat::Yaml,
        ));

        // Override with the specified file if it exists
        if path.exists() {
            cfg = cfg.add_source(config::File::from(path));
        }

        // Override with environment variables
        cfg = cfg.add_source(
            config::Environment::with_prefix("PRISM_CACHE")
                .separator("__")
                .try_parsing(true),
        );

        // Build the config
        let config = cfg.build().map_err(ConfigError::Other)?;

        // Deserialize
        config.try_deserialize().map_err(|e| {
            if e.to_string().contains("database.provider") {
                // If it's a database provider error, try to extract the invalid value
                if let Some(invalid_value) = e.to_string().split('`').nth(1) {
                    return ConfigError::InvalidDatabaseProvider(format!(
                        "Invalid database provider '{}'. Available providers are: mock",
                        invalid_value
                    ));
                }
            }
            ConfigError::Other(e)
        })
    }
}
