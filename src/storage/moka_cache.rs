//! Moka-based cache implementation.
//!
//! This module provides a high-performance concurrent cache implementation
//! using the Moka caching library.

use async_trait::async_trait;
use moka::future::Cache as MokaCache;
use serde_json::{Value, json};
use std::time::Duration;
use tracing::debug;

use crate::config::CacheConfig;
use crate::storage::{CacheAdapter, StorageError, StorageResult};

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
    cache: MokaCache<CacheKey, Value>,
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
            entity: entity.into(),
            id: id.into(),
        }
    }
}

#[async_trait]
impl CacheAdapter for MokaBasedCache {
    async fn get_record(&self, entity: &str, id: &str) -> StorageResult<Value> {
        let key = Self::create_key(entity, id);
        if let Some(entry) = self.cache.get(&key).await {
            return Ok(entry);
        } else {
            Err(StorageError::RecordNotFoundInCache(format!(
                "Cache Key {:?} not found in Cache",
                key
            )))
        }
    }

    async fn set_record(&self, entity: &str, id: &str, data: &Value) -> StorageResult<()> {
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

    #[tokio::test]
    async fn test_basic_cache_operations() {
        let config = CacheConfig {
            max_entries: 100,
            ttl_seconds: 60,
        };
        let cache = MokaBasedCache::new(config);

        // Create test data
        let data = json!({
            "name": "Test User",
            "email": "test@example.com"
        });

        // Test set_fields
        cache.set_record("users", "1", &data).await.unwrap();

        // Test exists
        assert!(cache.exists("users", "1").await.unwrap());
        assert!(!cache.exists("users", "2").await.unwrap());

        // Test get_record
        let result = cache.get_record("users", "1").await.unwrap();
        assert_eq!(result["name"], "Test User");
        assert_eq!(result["email"], "test@example.com");
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let config = CacheConfig {
            max_entries: 100,
            ttl_seconds: 1, // 1 second TTL for testing
        };
        let cache = MokaBasedCache::new(config);

        // Create test data
        let data = json!({
            "name": "Test User"
        });

        // Set data in cache
        cache.set_record("users", "1", &data).await.unwrap();

        // Verify it exists
        assert!(cache.exists("users", "1").await.unwrap());

        // Wait for expiration
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify it's gone
        assert!(!cache.exists("users", "1").await.unwrap());
    }
}
