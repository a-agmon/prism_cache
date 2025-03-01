//! Database module for the application.
//!
//! This module provides implementations for different database backends.

pub mod mock;

use std::collections::HashMap;

pub use mock::MockAdapter;

use crate::storage::{DatabaseAdapter, EntityData, StorageResult};
use async_trait::async_trait;

/// Database adapter type
#[derive(Debug)]
pub enum DatabaseType {
    /// In-memory database adapter
    Mock(MockAdapter),
}

#[async_trait]
impl DatabaseAdapter for DatabaseType {
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<Vec<EntityData>> {
        match self {
            DatabaseType::Mock(adapter) => adapter.fetch_record(entity, id, fields).await,
        }
    }
}

/// Create a new database adapter based on configuration
pub fn create_database(
    provider: &crate::config::DatabaseProvider,
    settings: HashMap<String, String>,
) -> DatabaseType {
    match provider {
        crate::config::DatabaseProvider::Mock => DatabaseType::Mock(MockAdapter::new(settings)),
    }
}
