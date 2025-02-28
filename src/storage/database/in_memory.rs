//! In-memory database adapter implementation.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::debug;

use crate::storage::{DatabaseAdapter, EntityData, StorageError, StorageResult};

/// In-memory database adapter that stores data in memory.
///
/// This adapter is used for testing and development.
/// Data is lost when the application restarts.
pub struct InMemoryAdapter {
    /// Data structure: entity -> id -> field -> value
    data: Arc<Mutex<HashMap<String, HashMap<String, EntityData>>>>,
}

impl InMemoryAdapter {
    /// Creates a new in-memory database adapter.
    pub fn new() -> Self {
        debug!("Creating new in-memory database adapter");

        // Initialize with some test data
        let mut data = HashMap::new();

        // Add test user
        let mut users = HashMap::new();
        let mut user1 = EntityData::new();
        user1.insert("name".to_string(), "John Doe".to_string());
        user1.insert("email".to_string(), "john@example.com".to_string());
        users.insert("user1".to_string(), user1);
        data.insert("users".to_string(), users);

        // Add test product
        let mut products = HashMap::new();
        let mut product1 = EntityData::new();
        product1.insert("name".to_string(), "Test Product".to_string());
        product1.insert("price".to_string(), "19.99".to_string());
        products.insert("prod1".to_string(), product1);
        data.insert("products".to_string(), products);

        Self {
            data: Arc::new(Mutex::new(data)),
        }
    }

    /// Creates an empty in-memory adapter with no data
    #[allow(dead_code)]
    pub fn new_empty() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Adds or updates an entity in the database
    #[allow(dead_code)]
    pub fn set_entity(&self, entity: &str, id: &str, entity_data: EntityData) -> StorageResult<()> {
        let mut data = self
            .data
            .lock()
            .map_err(|e| StorageError::DatabaseError(format!("Failed to acquire lock: {}", e)))?;

        let entities = data.entry(entity.to_string()).or_insert_with(HashMap::new);
        entities.insert(id.to_string(), entity_data);

        Ok(())
    }
}

#[async_trait]
impl DatabaseAdapter for InMemoryAdapter {
    async fn fetch_fields(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<EntityData> {
        debug!(
            "InMemory: Fetching fields {:?} for {}:{}",
            fields, entity, id
        );

        let data = self
            .data
            .lock()
            .map_err(|e| StorageError::DatabaseError(format!("Failed to acquire lock: {}", e)))?;

        // Get entity map
        let entity_map = data
            .get(entity)
            .ok_or_else(|| StorageError::EntityNotFound(format!("Entity not found: {}", entity)))?;

        // Get entity data
        let entity_data = entity_map.get(id).ok_or_else(|| {
            StorageError::EntityNotFound(format!("ID not found: {}:{}", entity, id))
        })?;

        // If fields is empty, return all fields
        if fields.is_empty() {
            return Ok(entity_data.clone());
        }

        // Filter fields
        let mut result = EntityData::new();
        for &field in fields {
            if let Some(value) = entity_data.get(field) {
                result.insert(field.to_string(), value.clone());
            }
        }

        Ok(result)
    }
}
