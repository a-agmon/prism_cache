//! In-memory database adapter implementation.

use async_trait::async_trait;
use std::collections::HashMap;
use tracing::debug;

use crate::storage::{DatabaseAdapter, EntityData, StorageError, StorageResult};

/// Mock database adapter that stores data in memory.
///
/// This adapter is used for testing and development.
/// Data is lost when the application restarts.
#[derive(Debug)]
pub struct MockAdapter {
    data: HashMap<String, EntityData>,
}

impl MockAdapter {
    /// Creates a new in-memory database adapter.
    pub fn new() -> Self {
        debug!("Creating mock database adapter");
        let mut data = HashMap::new();

        let mut user1 = EntityData::new();
        user1.insert("name".to_string(), "John Doe".to_string());
        user1.insert("email".to_string(), "john@example.com".to_string());
        user1.insert("age".to_string(), "30".to_string());
        user1.insert("id".to_string(), "1".to_string());
        data.insert("user1".to_string(), user1);

        // create 2 more users
        let mut user2 = EntityData::new();
        user2.insert("name".to_string(), "Jane Doe".to_string());
        user2.insert("email".to_string(), "jane@example.com".to_string());
        user2.insert("age".to_string(), "25".to_string());
        user2.insert("id".to_string(), "2".to_string());
        data.insert("user2".to_string(), user2);

        let mut user3 = EntityData::new();
        user3.insert("name".to_string(), "Jim Doe".to_string());
        user3.insert("email".to_string(), "jim@example.com".to_string());
        user3.insert("age".to_string(), "35".to_string());
        user3.insert("id".to_string(), "3".to_string());
        data.insert("user3".to_string(), user3);

        Self { data }
    }
}

#[async_trait]
impl DatabaseAdapter for MockAdapter {
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

        // Find the entry with matching id in our data
        let entry = self
            .data
            .values()
            .find(|data| data.get("id").map_or(false, |v| v == id))
            .ok_or_else(|| {
                StorageError::RecordNotInDatabase(format!("Entity with id {} not found", id))
            })?;

        // Create a new EntityData with only the requested fields
        let mut result = EntityData::new();
        for field in fields {
            if let Some(value) = entry.get(*field) {
                result.insert(field.to_string(), value.clone());
            }
        }

        Ok(result)
    }
}
