//! Database module for the application.
//!
//! This module provides implementations for different database backends.

pub mod mock;
pub mod sql;

use std::collections::HashMap;

pub use mock::MockAdapter;
pub use sql::SqlAdapter;


use crate::storage::{DatabaseAdapter, StorageResult};
use async_trait::async_trait;

/// Database adapter type
#[derive(Debug)]
pub enum DatabaseType {
    /// In-memory database adapter
    Mock(MockAdapter),
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
            DatabaseType::Mock(adapter) => adapter.fetch_fields(entity, id, fields).await,
            DatabaseType::Sql(adapter) => adapter.fetch_fields(entity, id, fields).await,
        }
    }
}

/// Create a new database adapter based on configuration
pub fn create_database(
    provider: &crate::config::DatabaseProvider,
    connection_string: Option<&str>,
    settings: HashMap<String, String>,
) -> DatabaseType {
    match provider {
        crate::config::DatabaseProvider::Mock => DatabaseType::Mock(MockAdapter::new()),
        crate::config::DatabaseProvider::Sql => {
            let conn_string = connection_string.unwrap_or("default_connection");
            DatabaseType::Sql(SqlAdapter::new(conn_string))
        }
    }
}
