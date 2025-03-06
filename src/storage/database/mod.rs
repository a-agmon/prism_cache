//! Database module for the application.
//!
//! This module provides implementations for different database backends.

pub mod az_delta;
pub mod mock;
pub mod postgres;
use async_trait::async_trait;
use datafusion::arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::HashMap;

use crate::config::DatabaseProvider;
use crate::storage::{DatabaseAdapter, StorageError, StorageResult};
pub use az_delta::AzDeltaAdapter;
pub use mock::MockAdapter;
pub use postgres::PostgresAdapter;
/// Database adapter type
pub enum DatabaseType {
    /// In-memory database adapter
    Mock(MockAdapter),
    /// Postgres database adapter
    Postgres(PostgresAdapter),
    /// Azure Delta database adapter
    AzDelta(AzDeltaAdapter),
}

#[async_trait]
impl DatabaseAdapter for DatabaseType {
    async fn fetch_record(&self, entity: &str, id: &str) -> StorageResult<Vec<Value>> {
        match self {
            Self::Mock(adapter) => adapter.fetch_record(entity, id).await,
            Self::Postgres(adapter) => adapter.fetch_record(entity, id).await,
            Self::AzDelta(adapter) => adapter.fetch_record(entity, id).await,
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
        DatabaseProvider::AzDelta => {
            let adapter = AzDeltaAdapter::new(settings).await?;
            Ok(DatabaseType::AzDelta(adapter))
        }
    }
}

pub fn record_batch_to_json(record: &RecordBatch) -> serde_json::Value {
    let schema = record.schema();
    let mut json_map = serde_json::Map::new();

    for (i, field) in schema.fields().iter().enumerate() {
        let col = record.column(i);
        let col_name = field.name().to_string();

        let col_value = match field.data_type() {
            DataType::Utf8 => col
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|arr| arr.value(0).to_string()),
            DataType::Int32 => col
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(|arr| arr.value(0).to_string()),
            DataType::Int64 => col
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|arr| arr.value(0).to_string()),
            DataType::Float64 => col
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|arr| arr.value(0).to_string()),
            DataType::Boolean => col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|arr| arr.value(0).to_string()),
            _ => Some("Unsupported type".to_string()),
        }
        .unwrap_or_default();

        json_map.insert(col_name, serde_json::Value::String(col_value));
    }

    serde_json::Value::Object(json_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_record_batch_to_json() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["John"])),
                Arc::new(Int64Array::from(vec![30])),
            ],
        )
        .unwrap();

        let json = record_batch_to_json(&batch);
        assert_eq!(json["name"], "John");
        assert_eq!(json["age"], "30");

        // also add a to_string() tesst
        let json_str = json.to_string();
        assert_eq!(json_str, "{\"age\":\"30\",\"name\":\"John\"}");
    }
}
