//! Database module for the application.
//!
//! This module provides implementations for different database backends.

pub mod in_memory;
pub mod sql;

pub use in_memory::InMemoryAdapter;
pub use sql::SqlAdapter;

use crate::storage::{DatabaseAdapter, StorageResult};
use std::collections::HashMap;

/// Configuration for database adapters
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum DatabaseConfig {
    /// In-memory database configuration
    InMemory,
    /// SQL database configuration with connection string
    Sql(String),
    // Add more database types as needed
}

/// Creates a database adapter based on the provided configuration
#[allow(dead_code)]
pub fn create_adapter(config: DatabaseConfig) -> StorageResult<Box<dyn DatabaseAdapter>> {
    match config {
        DatabaseConfig::InMemory => Ok(Box::new(InMemoryAdapter::new())),
        DatabaseConfig::Sql(conn_string) => Ok(Box::new(SqlAdapter::new(&conn_string))),
    }
}

/// Registry of available database adapters
pub struct DatabaseRegistry {
    factories: HashMap<
        String,
        Box<dyn Fn(String) -> StorageResult<Box<dyn DatabaseAdapter>> + Send + Sync>,
    >,
}

impl DatabaseRegistry {
    /// Creates a new empty registry
    pub fn new() -> Self {
        let mut registry = Self {
            factories: HashMap::new(),
        };

        // Register built-in adapters
        registry.register("memory", |_| Ok(Box::new(InMemoryAdapter::new())));
        registry.register("sql", |conn_string| {
            Ok(Box::new(SqlAdapter::new(&conn_string)))
        });

        registry
    }

    /// Registers a new database adapter factory
    pub fn register<F>(&mut self, name: &str, factory: F)
    where
        F: Fn(String) -> StorageResult<Box<dyn DatabaseAdapter>> + Send + Sync + 'static,
    {
        self.factories.insert(name.to_string(), Box::new(factory));
    }

    /// Creates a database adapter by name with the given connection string
    #[allow(dead_code)]
    pub fn create(
        &self,
        name: &str,
        connection_string: &str,
    ) -> StorageResult<Box<dyn DatabaseAdapter>> {
        match self.factories.get(name) {
            Some(factory) => factory(connection_string.to_string()),
            None => Err(crate::storage::StorageError::ConfigError(format!(
                "Unknown database adapter: {}",
                name
            ))),
        }
    }
}

impl Default for DatabaseRegistry {
    fn default() -> Self {
        Self::new()
    }
}
