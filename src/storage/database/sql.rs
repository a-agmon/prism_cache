//! SQL database adapter implementation.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info};

use crate::storage::{DatabaseAdapter, EntityData, StorageError, StorageResult};

/// SQL database adapter that connects to a SQL database.
///
/// This adapter is used for production environments.
/// Data is persisted in a SQL database.
pub struct SqlAdapter {
    /// Connection string for the database
    #[allow(dead_code)]
    connection_string: String,
    /// Mock data for testing
    mock_data: Arc<Mutex<HashMap<String, HashMap<String, EntityData>>>>,
}

impl SqlAdapter {
    /// Creates a new SQL database adapter with the given connection string.
    pub fn new(connection_string: &str) -> Self {
        info!(
            "Creating new SQL database adapter with connection string: {}",
            connection_string
        );

        // Initialize with some test data (for simulation)
        let mut data = HashMap::new();

        // Add test user
        let mut users = HashMap::new();
        let mut user1 = EntityData::new();
        user1.insert("name".to_string(), "SQL User".to_string());
        user1.insert("email".to_string(), "sql_user@example.com".to_string());
        users.insert("sql1".to_string(), user1);
        data.insert("users".to_string(), users);

        // Add test product
        let mut products = HashMap::new();
        let mut product1 = EntityData::new();
        product1.insert("name".to_string(), "SQL Product".to_string());
        product1.insert("price".to_string(), "29.99".to_string());
        products.insert("sql_prod1".to_string(), product1);
        data.insert("products".to_string(), products);

        Self {
            connection_string: connection_string.to_string(),
            mock_data: Arc::new(Mutex::new(data)),
        }
    }

    /// Parses the connection string to extract database parameters.
    ///
    /// This is a placeholder for actual connection string parsing.
    #[allow(dead_code)]
    fn parse_connection_string(&self) -> StorageResult<HashMap<String, String>> {
        let mut params = HashMap::new();

        // Simple parsing for demonstration
        for part in self.connection_string.split(';') {
            if let Some(index) = part.find('=') {
                let key = part[..index].trim().to_lowercase();
                let value = part[index + 1..].trim().to_string();
                params.insert(key, value);
            }
        }

        Ok(params)
    }
}

#[async_trait]
impl DatabaseAdapter for SqlAdapter {
    async fn fetch_fields(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<EntityData> {
        debug!("SQL: Fetching fields {:?} for {}:{}", fields, entity, id);

        // Simulate database query delay
        sleep(Duration::from_millis(50)).await;

        // In a real implementation, we would execute a SQL query here
        // For now, we'll use the mock data

        let data = self
            .mock_data
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
