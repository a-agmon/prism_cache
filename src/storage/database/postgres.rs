use async_trait::async_trait;
use duckdb::arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use duckdb::arrow::datatypes::DataType;
use duckdb::{Connection, Result, arrow::array::RecordBatch, params};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tracing::{debug, trace};

use crate::storage::{DatabaseAdapter, StorageError, StorageResult, assert_required_settings};

#[derive(Debug)]
pub struct PostgresAdapter {
    connection: Mutex<Connection>,
    id_field: String,
    fields: String,
}

impl PostgresAdapter {
    pub async fn new(settings: &HashMap<String, String>) -> Result<Self, StorageError> {
        let required_keys = ["user", "password", "host", "port", "dbname", "fields"];
        assert_required_settings(settings, &required_keys)?;
        // Now we can safely unwrap these values
        let fields = settings.get("fields").unwrap();
        let conn_str = format!(
            "postgresql://{}:{}@{}:{}/{}",
            settings.get("user").unwrap(),
            settings.get("password").unwrap(),
            settings.get("host").unwrap(),
            settings.get("port").unwrap(),
            settings.get("dbname").unwrap()
        );
        let attach_str = format!(
            "ATTACH '{}' AS db (TYPE postgres, SCHEMA 'public');",
            conn_str
        );

        let conn =
            Connection::open_in_memory().map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        debug!("Installing postgres");
        conn.execute_batch("INSTALL postgres; LOAD postgres;")
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        debug!("Attaching to {}", conn_str);
        conn.execute_batch(&attach_str)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        debug!("Successfully attached to {}", conn_str);
        Ok(Self {
            connection: Mutex::new(conn),
            id_field: "employee_id".to_string(),
            fields: fields.to_string(),
        })
    }
}

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
    async fn fetch_record(&self, entity: &str, id: &str) -> StorageResult<Vec<Value>> {
        trace!("Fetching record for entity: {}", entity);
        // create the sql statement to fetch the record
        let sql = format!(
            "SELECT {} FROM db.{} WHERE {} = ?",
            self.fields, entity, self.id_field
        );
        trace!("Building Query: {}", sql);
        let conn = self.connection.lock().await;
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        // Execute query and handle the Result
        let results: Vec<RecordBatch> = stmt
            .query_arrow(params![id])
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?
            .collect();
        trace!("Executed Query");
        if let Some(record) = results.first() {
            trace!("Converting RecordBatch to JSON");
            let json_value = record_batch_to_json(record);
            return Ok(vec![json_value]);
        }
        trace!("No record found");
        Ok(vec![])
    }
}

fn record_batch_to_json(record: &RecordBatch) -> serde_json::Value {
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
    use duckdb::arrow::array::{Int64Array, StringArray};
    use duckdb::arrow::datatypes::{DataType, Field, Schema};
    use duckdb::arrow::record_batch::RecordBatch;
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
