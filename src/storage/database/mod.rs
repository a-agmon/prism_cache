//! Database module for the application.
//!
//! This module provides implementations for different database backends.

pub mod mock;
pub mod postgres;

use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use crate::config::DatabaseProvider;
use crate::storage::{DatabaseAdapter, StorageError, StorageResult};
pub use mock::MockAdapter;
pub use postgres::PostgresAdapter;
/// Database adapter type
pub enum DatabaseType {
    /// In-memory database adapter
    Mock(MockAdapter),
    /// Postgres database adapter
    Postgres(PostgresAdapter),
}

#[async_trait]
impl DatabaseAdapter for DatabaseType {
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
    ) -> StorageResult<Vec<Value>> {
        match self {
            Self::Mock(adapter) => adapter.fetch_record(entity, id).await,
            Self::Postgres(adapter) => adapter.fetch_record(entity, id).await,
        }
    }
}

/// Create a new database adapter based on configuration
pub async fn create_database(
    provider: &DatabaseProvider,
    settings: HashMap<String, String>,
) -> Result<DatabaseType, StorageError> {
    match provider {
        DatabaseProvider::Mock => Ok(DatabaseType::Mock(MockAdapter::new(settings))),
        DatabaseProvider::Postgres => {
            let adapter = PostgresAdapter::new(&settings).await?;
            Ok(DatabaseType::Postgres(adapter))
        }
    }
}
