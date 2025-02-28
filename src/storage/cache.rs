//! In-memory cache implementation.

use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::debug;

use crate::config::CacheConfig;
use crate::storage::{CacheAdapter, EntityData, StorageError, StorageResult};

/// Cache entry with expiration time
struct CacheEntry {
    /// The data stored in the cache
    data: EntityData,
    /// When this entry was created
    #[allow(dead_code)]
    created_at: Instant,
    /// When this entry expires
    expires_at: Instant,
}

/// In-memory cache adapter that stores data in memory.
///
/// This adapter is used for caching entity data in memory.
/// Data is lost when the application restarts.
pub struct Cache {
    /// Data structure: entity -> id -> field -> value
    data: Arc<Mutex<HashMap<String, HashMap<String, CacheEntry>>>>,
    /// Configuration for the cache
    config: CacheConfig,
    /// Queue of keys in insertion order for LRU eviction
    lru_queue: Arc<Mutex<VecDeque<(String, String)>>>,
}

impl Cache {
    /// Creates a new in-memory cache with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            config,
            lru_queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Evicts expired entries and ensures the cache doesn't exceed the maximum size
    fn evict_entries(&self) -> StorageResult<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|e| StorageError::CacheError(format!("Failed to acquire lock: {}", e)))?;

        let mut lru_queue = self
            .lru_queue
            .lock()
            .map_err(|e| StorageError::CacheError(format!("Failed to acquire lock: {}", e)))?;

        // Evict expired entries
        let now = Instant::now();
        let mut to_remove = Vec::new();

        // Find expired entries
        for (entity, ids) in data.iter() {
            for (id, entry) in ids.iter() {
                if entry.expires_at <= now {
                    to_remove.push((entity.clone(), id.clone()));
                }
            }
        }

        // Remove expired entries
        for (entity, id) in &to_remove {
            if let Some(ids) = data.get_mut(entity) {
                ids.remove(id);
                if ids.is_empty() {
                    data.remove(entity);
                }
            }

            // Remove from LRU queue
            if let Some(pos) = lru_queue.iter().position(|(e, i)| e == entity && i == id) {
                lru_queue.remove(pos);
            }
        }

        // If we're still over capacity, evict oldest entries
        while lru_queue.len() > self.config.max_entries {
            if let Some((entity, id)) = lru_queue.pop_front() {
                if let Some(ids) = data.get_mut(&entity) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        data.remove(&entity);
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl CacheAdapter for Cache {
    async fn get_fields(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<EntityData> {
        debug!("Cache: Getting fields {:?} for {}:{}", fields, entity, id);

        // Evict expired entries
        self.evict_entries()?;

        let data = self
            .data
            .lock()
            .map_err(|e| StorageError::CacheError(format!("Failed to acquire lock: {}", e)))?;

        let mut result = EntityData::new();

        if let Some(entities) = data.get(entity) {
            if let Some(entry) = entities.get(id) {
                // Check if the entry has expired
                if entry.expires_at <= Instant::now() {
                    return Ok(result);
                }

                // If fields is empty, return all fields
                if fields.is_empty() {
                    return Ok(entry.data.clone());
                }

                for &field in fields {
                    if let Some(value) = entry.data.get(field) {
                        result.insert(field.to_string(), value.clone());
                    }
                }
            }
        }

        Ok(result)
    }

    async fn set_fields(&self, entity: &str, id: &str, data: &EntityData) -> StorageResult<()> {
        debug!("Cache: Setting fields for {}:{}: {:?}", entity, id, data);

        // Evict expired entries
        self.evict_entries()?;

        let mut cache_data = self
            .data
            .lock()
            .map_err(|e| StorageError::CacheError(format!("Failed to acquire lock: {}", e)))?;

        let mut lru_queue = self
            .lru_queue
            .lock()
            .map_err(|e| StorageError::CacheError(format!("Failed to acquire lock: {}", e)))?;

        // Ensure entity exists
        let entities = cache_data
            .entry(entity.to_string())
            .or_insert_with(HashMap::new);

        // Check if this is a new entry
        let is_new = !entities.contains_key(id);

        // Calculate expiration time
        let now = Instant::now();
        let ttl = Duration::from_secs(self.config.ttl_seconds);
        let expires_at = now + ttl;

        // Get or create the entry
        let entry = entities
            .entry(id.to_string())
            .or_insert_with(|| CacheEntry {
                data: EntityData::new(),
                created_at: now,
                expires_at,
            });

        // Update the entry
        entry.expires_at = expires_at;

        // Update fields
        for (field, value) in data {
            entry.data.insert(field.clone(), value.clone());
        }

        // Update LRU queue
        if is_new {
            // Add to the end of the queue
            lru_queue.push_back((entity.to_string(), id.to_string()));
        } else {
            // Move to the end of the queue
            if let Some(pos) = lru_queue.iter().position(|(e, i)| e == entity && i == id) {
                let item = lru_queue.remove(pos).unwrap();
                lru_queue.push_back(item);
            } else {
                // This shouldn't happen, but just in case
                lru_queue.push_back((entity.to_string(), id.to_string()));
            }
        }

        Ok(())
    }

    async fn exists(&self, entity: &str, id: &str) -> StorageResult<bool> {
        // Evict expired entries
        self.evict_entries()?;

        let data = self
            .data
            .lock()
            .map_err(|e| StorageError::CacheError(format!("Failed to acquire lock: {}", e)))?;

        let now = Instant::now();

        Ok(data
            .get(entity)
            .and_then(|entities| entities.get(id))
            .map(|entry| entry.expires_at > now)
            .unwrap_or(false))
    }
}
