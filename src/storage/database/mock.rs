//! In-memory database adapter implementation.

use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{debug, info};

use crate::storage::{DatabaseAdapter, EntityData, StorageError, StorageResult};

/// Mock database adapter that stores data in memory.
///
/// This adapter is used for testing and development.
/// Data is lost when the application restarts.
#[derive(Debug)]
pub struct MockAdapter {
    data: HashMap<String, EntityData>,
    settings: HashMap<String, String>,
}

impl MockAdapter {
    /// Creates a new in-memory database adapter.
    pub fn new(settings: HashMap<String, String>) -> Self {
        info!("Creating mock database adapter with settings:");
        for (key, value) in settings.iter() {
            info!("  {}: {}", key, value);
        }

        let mut data = HashMap::new();

        // Get settings or use defaults
        let name_prefix = settings.get("name_prefix").map_or("", |s| s.as_str());
        let default_age = settings.get("default_age").map_or("30", |s| s.as_str());

        let mut user1 = EntityData::new();
        user1.insert("name".to_string(), format!("{}John Doe", name_prefix));
        user1.insert("email".to_string(), "john@example.com".to_string());
        user1.insert("age".to_string(), default_age.to_string());
        user1.insert("id".to_string(), "1".to_string());
        data.insert("user1".to_string(), user1);

        // create 2 more users
        let mut user2 = EntityData::new();
        user2.insert("name".to_string(), format!("{}Jane Doe", name_prefix));
        user2.insert("email".to_string(), "jane@example.com".to_string());
        user2.insert("age".to_string(), "25".to_string());
        user2.insert("id".to_string(), "2".to_string());
        data.insert("user2".to_string(), user2);

        let mut user3 = EntityData::new();
        user3.insert("name".to_string(), format!("{}Jim Doe", name_prefix));
        user3.insert("email".to_string(), "jim@example.com".to_string());
        user3.insert("age".to_string(), "35".to_string());
        user3.insert("id".to_string(), "3".to_string());
        data.insert("user3".to_string(), user3);

        Self { data, settings }
    }

    /// Gets a setting value or returns the default
    pub fn get_setting(&self, key: &str, default: &str) -> String {
        self.settings
            .get(key)
            .map_or(default.to_string(), |s| s.clone())
    }
}

#[async_trait]
impl DatabaseAdapter for MockAdapter {
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<Vec<EntityData>> {
        debug!(
            "InMemory: Fetching records {:?} for {}:{} (with settings: {:?})",
            fields, entity, id, self.settings
        );

        // Find all entries with matching id pattern
        // For mock implementation, we only support exact match and '*' wildcard
        let matching_entries: Vec<&EntityData> = self
            .data
            .values()
            .filter(|data| {
                if id == "*" {
                    true // Match all records
                } else if let Some(record_id) = data.get("id") {
                    record_id == id // Exact match
                } else {
                    false
                }
            })
            .collect();

        if matching_entries.is_empty() {
            return Err(StorageError::RecordNotInDatabase(format!(
                "No records found matching id pattern: {}",
                id
            )));
        }

        // Create new EntityData instances with only the requested fields
        let mut results = Vec::new();
        for entry in matching_entries {
            let mut result = EntityData::new();
            // If fields is empty, return all fields
            if fields.is_empty() {
                result = entry.clone();
            } else {
                for field in fields {
                    if let Some(value) = entry.get(*field) {
                        result.insert(field.to_string(), value.clone());
                    }
                }
            }
            results.push(result);
        }

        Ok(results)
    }
}
