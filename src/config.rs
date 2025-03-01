//! Configuration module for the application.
//!
//! This module provides a configuration system based on YAML files.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

/// Database provider type
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[serde(try_from = "String")]
pub enum DatabaseProvider {
    /// Mock database (in-memory, for testing)
    Mock,
    /// SQL database
    Sql,
    // Add more database providers as needed
}

impl TryFrom<String> for DatabaseProvider {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "mock" => Ok(DatabaseProvider::Mock),
            "sql" => Ok(DatabaseProvider::Sql),
            _ => Err(format!(
                "Invalid database provider '{}'. Available providers are: mock, sql",
                s
            )),
        }
    }
}

impl Default for DatabaseProvider {
    fn default() -> Self {
        Self::Mock
    }
}

/// Database configuration
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DatabaseConfig {
    /// Database provider to use
    #[serde(default)]
    pub provider: DatabaseProvider,

    /// Connection string for the database (if applicable)
    #[serde(default)]
    pub connection_string: Option<String>,

    /// Additional provider-specific settings
    #[serde(default)]
    pub settings: HashMap<String, String>,
}

/// Cache configuration
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct CacheConfig {
    /// Maximum number of entries in the cache
    #[serde(default = "default_cache_max_entries")]
    pub max_entries: usize,

    /// Time-to-live for cache entries in seconds
    #[serde(default = "default_cache_ttl")]
    pub ttl_seconds: u64,
}

fn default_cache_max_entries() -> usize {
    10000
}

fn default_cache_ttl() -> u64 {
    300 // 5 minutes
}

/// Server configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    /// Address to bind to
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
}

fn default_bind_address() -> String {
    "127.0.0.1:6379".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
        }
    }
}

/// Logging configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LoggingConfig {
    /// Log level
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Whether to log to a file
    #[serde(default)]
    pub file: Option<String>,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            file: None,
        }
    }
}

/// Application configuration
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct AppConfig {
    /// Database configuration
    #[serde(default)]
    pub database: DatabaseConfig,

    /// Cache configuration
    #[serde(default)]
    pub cache: CacheConfig,

    /// Server configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Logging configuration
    #[serde(default)]
    pub logging: LoggingConfig,
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
                        "Invalid database provider '{}'. Available providers are: mock, sql",
                        invalid_value
                    ));
                }
            }
            ConfigError::Other(e)
        })
    }
}
