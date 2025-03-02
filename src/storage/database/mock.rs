//! In-memory database adapter implementation.

use async_trait::async_trait;
use serde_json::{Value, json};
use std::collections::HashMap;
use tracing::{debug, info};

use crate::storage::{DatabaseAdapter, StorageError, StorageResult};

/// Mock database adapter that stores data in memory.
///
/// This adapter is used for testing and development.
/// Data is lost when the application restarts.
#[derive(Debug)]
pub struct MockAdapter {
    data: HashMap<String, Value>,
    settings: HashMap<String, String>,
}

impl MockAdapter {
    /// Creates a new in-memory database adapter.
    pub fn new(settings: HashMap<String, String>) -> Self {
        info!("Creating mock database adapter");
        let mut data = HashMap::new();
        // Create sample data
        data.insert(
            "user1".into(),
            json!({
                "name": "John Doe",
                "email": "john@example.com",
                "age": 30,
                "id": "1"
            }),
        );

        data.insert(
            "user2".into(),
            json!({
                "name": "Jane Doe",
                "email": "jane@example.com",
                "age": 25,
                "id": "2"
            }),
        );

        data.insert(
            "user3".into(),
            json!({
                "name": "Jim Doe",
                "email": "jim@example.com",
                "age": 35,
                "id": "3"
            }),
        );

        Self { data, settings }
    }

    /// Fetches a record by ID from the mock database
    fn get_record(&self, id: &str) -> StorageResult<Value> {
        self.data
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::RecordNotInDatabase(format!("Record not found: {id}")))
    }
}

#[async_trait]
impl DatabaseAdapter for MockAdapter {
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
        _fields: &[&str], // Ignore fields parameter
    ) -> StorageResult<Vec<Value>> {
        debug!("InMemory DB: Fetching records for {entity}:{id}");
        let entry = self.get_record(id)?;
        Ok(vec![entry])
    }
}
