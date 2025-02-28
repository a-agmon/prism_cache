//! Configuration module for the application.
//!
//! This module provides a configuration system based on YAML files.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::info;

/// Database provider type
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseProvider {
    /// In-memory database
    InMemory,
    /// SQL database
    Sql,
    // Add more database providers as needed
}

impl Default for DatabaseProvider {
    fn default() -> Self {
        Self::InMemory
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
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, config::ConfigError> {
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
            config::Environment::with_prefix("LAKE_CACHE")
                .separator("__")
                .try_parsing(true),
        );

        // Build the config
        let config = cfg.build()?;

        // Deserialize
        let app_config: AppConfig = config.try_deserialize()?;

        Ok(app_config)
    }
}
