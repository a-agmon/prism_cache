//! Storage module for the application.
//!
//! This module provides a unified interface for storing and retrieving data
//! from different storage backends.

pub mod database;
pub mod moka_cache;

use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, trace, warn};
use std::collections::HashMap;

use crate::config::{AppConfig, DataProviderConfig};
use database::{DatabaseType, create_database};
use moka_cache::MokaBasedCache;

/// Type alias for storage results.
pub type StorageResult<T> = Result<T, StorageError>;

/// Error type for storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Error from the database.
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// Record not in database.
    #[error("Record not in database: {0}")]
    RecordNotInDatabase(String),

    /// Error from the cache.
    #[error("Cache error: {0}")]
    CacheError(String),

    /// Record not found in cache.
    #[error("Record not found in cache: {0}")]
    RecordNotFoundInCache(String),

    /// Entity not found.
    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    /// Field not found.
    #[error("Field not found: {0}")]
    FieldNotFound(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Provider not found.
    #[error("Provider not found: {0}")]
    ProviderNotFound(String),
}

/// Database adapter trait for interacting with different database backends.
#[async_trait]
pub trait DatabaseAdapter: Send + Sync {
    /// Fetches records from the database that match the given entity and id pattern.
    /// Returns a vector of JSON values representing the matching records.
    ///
    /// The id parameter can contain wildcards or patterns depending on the database implementation.
    /// If fields is empty, returns all fields for each matching record.
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
    ) -> StorageResult<Vec<Value>>;
}

/// Cache adapter trait.
///
/// This trait defines the interface for cache adapters.
/// Note: We still need async_trait here because this trait is used as a trait object (dyn CacheAdapter).
#[async_trait]
pub trait CacheAdapter: Send + Sync {
    /// Gets fields from the cache.
    ///
    /// If fields is empty, returns all fields.
    async fn get_record(&self, entity: &str, id: &str) -> StorageResult<Value>;

    /// Sets fields in the cache.
    async fn set_record(&self, entity: &str, id: &str, data: &Value) -> StorageResult<()>;

    /// Checks if an entity exists in the cache.
    #[allow(dead_code)]
    async fn exists(&self, entity: &str, id: &str) -> StorageResult<bool>;
}

/// Storage service that combines database and cache adapters.
///
/// This service provides a unified interface for storing and retrieving data
/// from different storage backends.
pub struct StorageService {
    /// Database adapters mapped by provider name
    providers: HashMap<String, Arc<DatabaseType>>,
    /// Cache adapter.
    cache: Arc<dyn CacheAdapter>,
}

impl StorageService {
    /// Creates a new storage service with the given configuration.
    ///
    /// This method initializes the database and cache adapters based on the
    /// provided configuration.
    pub async fn new(config: &AppConfig) -> StorageResult<Self> {
        info!("Initializing storage service with configuration");

        // Initialize database adapters based on configuration
        let mut providers = HashMap::new();
        for provider_config in &config.database.providers {
            info!("Initializing provider: {}", provider_config.name);
            let db = create_database(
                &provider_config.provider,
                provider_config.settings.clone(),
            ).await?;
            providers.insert(provider_config.name.clone(), Arc::new(db));
        }

        // Initialize cache adapter using Moka
        info!(
            "Initializing Moka cache with max entries: {}, TTL: {} seconds",
            config.cache.max_entries, config.cache.ttl_seconds
        );
        let cache = Arc::new(MokaBasedCache::new(config.cache.clone()));

        Ok(Self { providers, cache })
    }

    /// Fetches a record from the storage.
    ///
    /// This method first tries to get the record from the cache.
    /// If the record is not found in the cache, it falls back to the database.
    /// If the record is found in the database, it is stored in the cache.
    pub async fn fetch_record(
        &self,
        provider_name: &str,
        id: &str,
    ) -> StorageResult<Value> {
        debug!("Fetching record from provider: {}, id: {}", provider_name, id);

        // Try to get from cache first
        let cache_key = format!("{}:{}", provider_name, id);
        match self.cache.get_record(provider_name, id).await {
            Ok(data) => {
                trace!("Cache hit for {}:{}", provider_name, id);
                return Ok(data);
            }
            Err(StorageError::RecordNotFoundInCache(_)) => {
                trace!("Cache miss for {}:{}", provider_name, id);
            }
            Err(e) => {
                warn!("Cache error: {}", e);
                // Continue to database as fallback
            }
        }

        // Fetch from database
        self.fetch_from_database(provider_name, id).await
    }

    /// Fetches a record from the database.
    async fn fetch_from_database(
        &self,
        provider_name: &str,
        id: &str,
    ) -> StorageResult<Value> {
        trace!("Fetching from database: provider={}, id={}", provider_name, id);

        // Get the provider
        let provider = self.providers.get(provider_name)
            .ok_or_else(|| StorageError::ProviderNotFound(provider_name.to_string()))?;

        // Fetch from database
        let records = provider.fetch_record(provider_name, id).await?;
        
        if records.is_empty() {
            return Err(StorageError::RecordNotInDatabase(format!(
                "Record not found: {}:{}",
                provider_name, id
            )));
        }

        // Take the first record
        let record = records[0].clone();
        
        // Store in cache
        if let Err(e) = self.cache.set_record(provider_name, id, &record).await {
            warn!("Failed to cache record: {}", e);
        }

        Ok(record)
    }
}

/// Extracts required keys from a HashMap and reports any missing keys
pub fn assert_required_settings(
    settings: &HashMap<String, String>,
    required_keys: &[&str],
) -> StorageResult<()> {
    let missing_keys: Vec<&str> = required_keys
        .iter()
        .filter(|key| !settings.contains_key(**key))
        .copied()
        .collect();
    
    if !missing_keys.is_empty() {
        return Err(StorageError::ConfigError(format!(
            "Missing required settings: {}",
            missing_keys.join(", ")
        )));
    }
    
    Ok(())
}

/// Example function that demonstrates how to use extract_required_settings
pub fn validate_connection_settings(settings: &HashMap<String, String>) -> StorageResult<()> {
    // Define the required keys for a database connection
    let required_keys = ["host", "port", "user", "password", "database"];
    
    // Check if all required keys are present
    assert_required_settings(settings, &required_keys)?;
    
    // Additional validation could be done here
    // For example, checking if port is a valid number
    if let Some(port) = settings.get("port") {
        if port.parse::<u16>().is_err() {
            return Err(StorageError::ConfigError(
                "Port must be a valid number between 0 and 65535".to_string()
            ));
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_extract_required_settings() {
        let mut settings = HashMap::new();
        settings.insert("host".to_string(), "localhost".to_string());
        settings.insert("port".to_string(), "5432".to_string());
        
        let required_keys = ["host", "port", "user"];
        let result = assert_required_settings(&settings, &required_keys);
        
        assert!(result.is_err());
        if let Err(StorageError::ConfigError(msg)) = result {
            assert!(msg.contains("user"));
        } else {
            panic!("Expected ConfigError");
        }
        
        settings.insert("user".to_string(), "postgres".to_string());
        let result = assert_required_settings(&settings, &required_keys);
        assert!(result.is_ok());
    }
}
