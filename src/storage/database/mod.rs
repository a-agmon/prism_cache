//! Database module for the application.
//!
//! This module provides implementations for different database backends.

pub mod mock;
pub mod postgres;

use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use crate::storage::{DatabaseAdapter, StorageResult};
pub use mock::MockAdapter;

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
    ) -> StorageResult<Vec<Value>> {
        match self {
            Self::Mock(adapter) => adapter.fetch_record(entity, id, fields).await,
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
