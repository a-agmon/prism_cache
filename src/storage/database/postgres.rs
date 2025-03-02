use async_trait::async_trait;
use serde_json::Value;
use duckdb::{params, Connection, Result};
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::storage::{assert_required_settings, DatabaseAdapter, StorageError, StorageResult};

pub struct PostgresAdapter {
    connection: Mutex<Connection>,
    id_field: String,
    fields: String,
}

impl PostgresAdapter {
    pub async fn new(settings: &HashMap<String, String>) -> Result<Self, StorageError> {
        // Check for required settings
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

        let attach_str = format!("ATTACH '{}' AS db (TYPE postgres, SCHEMA 'public');", conn_str);
       
        let conn = Connection::open_in_memory()
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        conn.execute("INSTALL postgres; LOAD postgres;", params![])
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        conn.execute(&attach_str, params![])
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(Self { 
            connection: Mutex::new(conn),
            id_field: "id".to_string(),
            fields: fields.to_string(),
        })
    }
}

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
    ) -> StorageResult<Vec<Value>> {
        // create the sql statement to fetch the record
        let sql = format!(
            "SELECT {} FROM db.{} WHERE {} = ?",
            self.fields, entity, self.id_field
        );
        let mut conn = self.connection.lock().await;
        let mut stmt = conn.prepare(&sql).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        // Execute query and handle the Result
        let rows_result = stmt.query_map(params![id], |row| {
            let json_str: String = row.get(0)?;
            Ok(json_str)
        }).map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        
        // Manually collect results into a Vec
        let mut results = Vec::new();
        for row_result in rows_result {
            match row_result {
                Ok(json_str) => {
                    let value: Value = serde_json::from_str(&json_str)
                        .map_err(|e| StorageError::DatabaseError(format!("JSON parse error: {}", e)))?;
                    results.push(value);
                },
                Err(e) => return Err(StorageError::DatabaseError(e.to_string())),
            }
        }
        
        Ok(results)
    }
}
