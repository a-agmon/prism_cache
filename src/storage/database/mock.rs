//! In-memory database adapter implementation.

use crate::storage::{DatabaseAdapter, StorageError, StorageResult, assert_required_settings};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{debug, info};

/// Mock database adapter for testing
pub struct MockAdapter {
    data: HashMap<String, HashMap<String, Value>>,
}

impl MockAdapter {
    /// Creates a new mock adapter
    pub fn new(_settings: HashMap<String, String>) -> Self {
        let mut data = HashMap::new();
        
        // Add some sample data for testing
        let mut users = HashMap::new();
        users.insert(
            "123".to_string(),
            json!({
                "id": "123",
                "name": "John Doe",
                "email": "john@example.com",
                "age": 30
            }),
        );
        users.insert(
            "456".to_string(),
            json!({
                "id": "456",
                "name": "Jane Smith",
                "email": "jane@example.com",
                "age": 25
            }),
        );
        data.insert("users".to_string(), users);
        
        // Add products data
        let mut products = HashMap::new();
        products.insert(
            "789".to_string(),
            json!({
                "id": "789",
                "name": "Laptop",
                "price": 999.99,
                "stock": 10
            }),
        );
        products.insert(
            "101".to_string(),
            json!({
                "id": "101",
                "name": "Smartphone",
                "price": 499.99,
                "stock": 20
            }),
        );
        data.insert("products".to_string(), products);
        
        Self { data }
    }

    /// Example of creating a mock adapter with required settings
    pub fn with_required_settings(settings: HashMap<String, String>) -> StorageResult<Self> {
        // Check for required settings
        let required_keys = ["data_source", "max_records"];
        assert_required_settings(&settings, &required_keys)?;

        info!("Creating mock database adapter with required settings");
        info!("Data source: {}", settings.get("data_source").unwrap());
        info!("Max records: {}", settings.get("max_records").unwrap());

        Ok(Self::new(settings))
    }
}

#[async_trait]
impl DatabaseAdapter for MockAdapter {
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
    ) -> StorageResult<Vec<Value>> {
        debug!("MockAdapter: Fetching record for entity={}, id={}", entity, id);
        
        // Check if the entity exists
        let entity_data = self.data.get(entity).ok_or_else(|| {
            StorageError::EntityNotFound(format!("Entity '{}' not found", entity))
        })?;
        
        // Check if the ID exists
        let record = entity_data.get(id).ok_or_else(|| {
            StorageError::RecordNotInDatabase(format!("Record '{}' not found", id))
        })?;
        
        Ok(vec![record.clone()])
    }
}
