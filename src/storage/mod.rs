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
use tracing::{debug, info};
use std::collections::HashMap;

use crate::config::AppConfig;
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
    /// Database adapter.
    db: Arc<DatabaseType>,
    /// Cache adapter.
    cache: Arc<dyn CacheAdapter>,
}

impl StorageService {
    /// Creates a new storage service with the given configuration.
    ///
    /// This method initializes the database and cache adapters based on the
    /// provided configuration.
    pub fn new(config: &AppConfig) -> StorageResult<Self> {
        info!("Initializing storage service with configuration");

        // Initialize database adapter based on configuration
        let db = Arc::new(create_database(
            &config.database.provider,
            config.database.settings.clone(),
        ));

        // Initialize cache adapter using Moka
        info!(
            "Initializing Moka cache with max entries: {}, TTL: {} seconds",
            config.cache.max_entries, config.cache.ttl_seconds
        );
        let cache = Arc::new(MokaBasedCache::new(config.cache.clone()));

        Ok(Self { db, cache })
    }

    /// Fetches fields from the storage.
    ///
    /// This method first tries to get the fields from the cache.
    /// If the fields are not found in the cache, it falls back to the database.
    /// If the fields are found in the database, they are stored in the cache.
    ///
    /// If fields is empty, returns all fields.
    pub async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<Value> {
        // Try to get from cache first
        let cache_result = self.cache.get_record(entity, id).await;

        match cache_result {
            Ok(data) => {
                debug!("Record [{entity}]:[{id}] Retrieved from Cache");
                // If fields is empty, return all fields
                if fields.is_empty() {
                    return Ok(data);
                }
                
                // Filter the requested fields
                let mut result = json!({});
                for &field in fields {
                    if data[field] != Value::Null {
                        result[field] = data[field].clone();
                    }
                }
                Ok(result)
            }
            Err(StorageError::RecordNotFoundInCache(_)) => {
                debug!("Record [{entity}]:[{id}] Not found in Cache, fetching from Database");
                self.fetch_from_database(entity, id, fields).await
            }
            Err(e) => Err(e),
        }
    }

    async fn fetch_from_database(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<Value> {
        // Fetch from database
        let db_result = self.db.fetch_record(entity, id).await;

        match db_result {
            Ok(records) => {
                if records.is_empty() {
                    return Err(StorageError::RecordNotInDatabase(format!(
                        "Record [{entity}]:[{id}] not found in database"
                    )));
                }

                // For simplicity, we just take the first record
                let record = &records[0];
                
                // Create a filtered result if fields are specified
                let result = if fields.is_empty() {
                    record.clone()
                } else {
                    let mut filtered = json!({});
                    for &field in fields {
                        if record[field] != Value::Null {
                            filtered[field] = record[field].clone();
                        }
                    }
                    filtered
                };

                // Store in cache for future use
                self.cache.set_record(entity, id, &result).await?;

                Ok(result)
            }
            Err(e) => Err(e),
        }
    }
}

/// Extracts required keys from a HashMap and reports any missing keys
pub fn assert_required_settings(
    settings: &HashMap<String, String>,
    required_keys: &[&str],
) -> StorageResult<()> {
    let mut missing_keys = Vec::new();
    
    for &key in required_keys {
        if !settings.contains_key(key) {
            missing_keys.push(key);
        }
    }
    
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
        settings.insert("user".to_string(), "postgres".to_string());
        
        // Missing "port", "password", and "database"
        let result = assert_required_settings(&settings, &["host", "port", "user", "password", "database"]);
        assert!(result.is_err());
        
        if let Err(StorageError::ConfigError(msg)) = result {
            assert!(msg.contains("port"));
            assert!(msg.contains("password"));
            assert!(msg.contains("database"));
        } else {
            panic!("Expected ConfigError");
        }
        
        // Add the missing keys
        settings.insert("port".to_string(), "5432".to_string());
        settings.insert("password".to_string(), "secret".to_string());
        settings.insert("database".to_string(), "mydb".to_string());
        
        // Now it should pass
        let result = assert_required_settings(&settings, &["host", "port", "user", "password", "database"]);
        assert!(result.is_ok());
    }
}
