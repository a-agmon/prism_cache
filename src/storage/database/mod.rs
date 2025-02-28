//! Database module for the application.
//!
//! This module provides implementations for different database backends.

pub mod in_memory;
pub mod sql;

pub use in_memory::InMemoryAdapter;
pub use sql::SqlAdapter;

use crate::storage::{DatabaseAdapter, StorageResult};
use async_trait::async_trait;

/// Database adapter type
#[derive(Debug)]
pub enum DatabaseType {
    /// In-memory database adapter
    InMemory(InMemoryAdapter),
    /// SQL database adapter
    Sql(SqlAdapter),
}

#[async_trait]
impl DatabaseAdapter for DatabaseType {
    async fn fetch_fields(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<crate::storage::EntityData> {
        match self {
            DatabaseType::InMemory(adapter) => adapter.fetch_fields(entity, id, fields).await,
            DatabaseType::Sql(adapter) => adapter.fetch_fields(entity, id, fields).await,
        }
    }
}

/// Create a new database adapter based on configuration
pub fn create_database(
    provider: &crate::config::DatabaseProvider,
    connection_string: Option<&str>,
) -> DatabaseType {
    match provider {
        crate::config::DatabaseProvider::InMemory => DatabaseType::InMemory(InMemoryAdapter::new()),
        crate::config::DatabaseProvider::Sql => {
            let conn_string = connection_string.unwrap_or("default_connection");
            DatabaseType::Sql(SqlAdapter::new(conn_string))
        }
    }
}
