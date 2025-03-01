//! Storage module for the application.
//!
//! This module provides a unified interface for storing and retrieving data
//! from different storage backends.

pub mod database;
pub mod moka_cache;

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};

use crate::config::AppConfig;
use database::{create_database, DatabaseType};
use moka_cache::MokaBasedCache;

/// Type alias for entity data, which is a map of field names to values.
pub type EntityData = HashMap<String, String>;

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

    /// Entity not found.
    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    /// Field not found.
    #[allow(dead_code)]
    #[error("Field not found: {0}")]
    FieldNotFound(String),

    /// Configuration error.
    #[allow(dead_code)]
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Database adapter trait for interacting with different database backends.
#[async_trait]
pub trait DatabaseAdapter: Send + Sync {
    /// Fetches records from the database that match the given entity and id pattern.
    /// Returns a vector of matching records.
    ///
    /// The id parameter can contain wildcards or patterns depending on the database implementation.
    /// If fields is empty, returns all fields for each matching record.
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<Vec<EntityData>>;
}

/// Cache adapter trait.
///
/// This trait defines the interface for cache adapters.
#[async_trait]
pub trait CacheAdapter: Send + Sync {
    /// Gets fields from the cache.
    ///
    /// If fields is empty, returns all fields.
    async fn get_fields(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<EntityData>;

    /// Sets fields in the cache.
    async fn set_fields(&self, entity: &str, id: &str, data: &EntityData) -> StorageResult<()>;

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

    /// Creates a new in-memory storage service for testing.
    ///
    /// This method is useful for tests and examples.
    #[allow(dead_code)]
    pub fn new_in_memory() -> Self {
        // Create a default configuration for testing
        let config = AppConfig::default();

        Self::new(&config).expect("Failed to create in-memory storage service")
    }

    /// Fetches fields from the storage.
    ///
    /// This method first tries to get the fields from the cache.
    /// If the fields are not found in the cache, it falls back to the database.
    /// If the fields are found in the database, they are stored in the cache.
    ///
    /// If fields is empty, returns all fields.
    pub async fn fetch_fields(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<EntityData> {
        debug!("Fetching fields {:?} for {}:{}", fields, entity, id);

        // Try to get from cache first
        let cache_result = self.cache.get_fields(entity, id, fields).await;

        match cache_result {
            Ok(data) if !data.is_empty() => {
                debug!("Cache hit for {}:{}", entity, id);
                Ok(data)
            }
            _ => {
                debug!("Cache miss for {}:{}, fetching from database", entity, id);
                // Fetch from database
                let db_result = self.db.fetch_record(entity, id, fields).await?;

                // Store in cache
                if !db_result.is_empty() {
                    debug!("Storing {}:{} in cache", entity, id);
                    self.cache.set_fields(entity, id, &db_result[0]).await?;
                }

                Ok(if db_result.is_empty() {
                    EntityData::new()
                } else {
                    db_result[0].clone()
                })
            }
        }
    }
}
