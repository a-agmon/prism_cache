//! Moka-based cache implementation.
//!
//! This module provides a high-performance concurrent cache implementation
//! using the Moka caching library.

use async_trait::async_trait;
use moka::future::Cache as MokaCache;
use std::time::Duration;
use tracing::debug;

use crate::config::CacheConfig;
use crate::storage::{CacheAdapter, EntityData, StorageError, StorageResult};

/// Cache key type combining entity and id
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct CacheKey {
    entity: String,
    id: String,
}

/// Moka-based cache adapter that provides concurrent caching with automatic eviction.
///
/// This adapter uses Moka's high-performance concurrent cache implementation with:
/// - Time-based expiration (TTL)
/// - Size-based eviction (LRU)
/// - Thread-safe operations
/// - Asynchronous API
pub struct MokaBasedCache {
    /// The underlying Moka cache instance
    cache: MokaCache<CacheKey, EntityData>,
}

impl MokaBasedCache {
    /// Creates a new Moka-based cache with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        let cache = MokaCache::builder()
            // Set the maximum cache size
            .max_capacity(config.max_entries as u64)
            // Set the time-to-live (TTL)
            .time_to_live(Duration::from_secs(config.ttl_seconds))
            // Build the cache
            .build();

        Self { cache }
    }

    /// Creates a cache key from entity and id
    fn create_key(entity: &str, id: &str) -> CacheKey {
        CacheKey {
            entity: entity.to_string(),
            id: id.to_string(),
        }
    }
}

#[async_trait]
impl CacheAdapter for MokaBasedCache {
    async fn get_fields(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<EntityData> {
        debug!("Cache: Getting fields {:?} for {}:{}", fields, entity, id);

        let key = Self::create_key(entity, id);

        if let Some(entry) = self.cache.get(&key).await {
            // If fields is empty, return all fields
            if fields.is_empty() {
                return Ok(entry);
            }

            // Filter the requested fields
            let mut result = EntityData::new();
            for &field in fields {
                if let Some(value) = entry.get(field) {
                    result.insert(field.to_string(), value.clone());
                }
            }
            Ok(result)
        } else {
            Ok(EntityData::new())
        }
    }

    async fn set_fields(&self, entity: &str, id: &str, data: &EntityData) -> StorageResult<()> {
        let key = Self::create_key(entity, id);
        self.cache.insert(key, data.clone()).await;
        Ok(())
    }

    async fn exists(&self, entity: &str, id: &str) -> StorageResult<bool> {
        let key = Self::create_key(entity, id);
        Ok(self.cache.get(&key).await.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_basic_cache_operations() {
        let config = CacheConfig {
            max_entries: 100,
            ttl_seconds: 60,
        };
        let cache = MokaBasedCache::new(config);

        // Create test data
        let mut data = EntityData::new();
        data.insert("name".to_string(), "Test User".to_string());
        data.insert("email".to_string(), "test@example.com".to_string());

        // Test set_fields
        cache.set_fields("users", "1", &data).await.unwrap();

        // Test exists
        assert!(cache.exists("users", "1").await.unwrap());
        assert!(!cache.exists("users", "2").await.unwrap());

        // Test get_fields with specific fields
        let result = cache.get_fields("users", "1", &["name"]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("name").unwrap(), "Test User");

        // Test get_fields with all fields
        let result = cache.get_fields("users", "1", &[]).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("name").unwrap(), "Test User");
        assert_eq!(result.get("email").unwrap(), "test@example.com");
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let config = CacheConfig {
            max_entries: 100,
            ttl_seconds: 1, // 1 second TTL for testing
        };
        let cache = MokaBasedCache::new(config);

        // Create test data
        let mut data = EntityData::new();
        data.insert("name".to_string(), "Test User".to_string());

        // Set data in cache
        cache.set_fields("users", "1", &data).await.unwrap();

        // Verify it exists
        assert!(cache.exists("users", "1").await.unwrap());

        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify it's gone
        assert!(!cache.exists("users", "1").await.unwrap());
    }
}
