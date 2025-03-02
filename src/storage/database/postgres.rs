use async_trait::async_trait;
use serde_json::{Value, json};
use sqlx::{
    Column, Pool, Postgres, Row,
    postgres::{PgPoolOptions, PgRow},
};

use crate::storage::{DatabaseAdapter, StorageError, StorageResult};

pub struct PostgresAdapter {
    pool: Pool<Postgres>,
    id_field: String,
}

impl PostgresAdapter {
    pub async fn new(connection_string: &str) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;
        Ok(Self {
            pool,
            id_field: "id".to_string(),
        })
    }
}

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
    async fn fetch_record(
        &self,
        entity: &str,
        id: &str,
        fields: &[&str],
    ) -> StorageResult<Vec<Value>> {
        let fields_str = if fields.is_empty() {
            "*".to_string()
        } else {
            fields.join(", ")
        };

        let query = format!(
            "SELECT {} FROM {} WHERE {} = $1",
            fields_str, entity, self.id_field
        );

        let rows = sqlx::query(&query)
            .bind(id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        let mut results = Vec::new();
        for row in rows {
            let mut obj = json!({});
            if fields.is_empty() {
                // If no fields specified, try to get all columns
                for column in row.columns() {
                    let name = column.name();
                    if let Ok(value) = row.try_get::<String, _>(name) {
                        obj[name] = json!(value);
                    }
                }
            } else {
                for &field in fields {
                    if let Ok(value) = row.try_get::<String, _>(field) {
                        obj[field] = json!(value);
                    }
                }
            }
            results.push(obj);
        }

        Ok(results)
    }
}
