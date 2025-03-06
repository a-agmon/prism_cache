use async_trait::async_trait;

use datafusion::arrow::{
    array::{BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray},
    datatypes::DataType,
};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tracing::{debug, trace};

use crate::storage::{DatabaseAdapter, StorageError, StorageResult, assert_required_settings};

const USER_KEY: &str = "user";
const PASSWORD_KEY: &str = "password";
const HOST_KEY: &str = "host";
const PORT_KEY: &str = "port";
const DBNAME_KEY: &str = "dbname";
const FIELDS_KEY: &str = "fields";

#[derive(Debug)]
pub struct PostgresAdapter {
    //connection: Mutex<Connection>,
    id_field: String,
    fields: String,
}

impl PostgresAdapter {
    pub async fn new(settings: &HashMap<String, String>) -> Result<Self, StorageError> {
        let required_keys = [
            USER_KEY,
            PASSWORD_KEY,
            HOST_KEY,
            PORT_KEY,
            DBNAME_KEY,
            FIELDS_KEY,
        ];
        assert_required_settings(settings, &required_keys)?;
        // Now we can safely unwrap these values
        let fields = settings.get(FIELDS_KEY).unwrap();
        let conn_str = format!(
            "postgresql://{}:{}@{}:{}/{}",
            settings.get(USER_KEY).unwrap(),
            settings.get(PASSWORD_KEY).unwrap(),
            settings.get(HOST_KEY).unwrap(),
            settings.get(PORT_KEY).unwrap(),
            settings.get(DBNAME_KEY).unwrap()
        );
        Ok(Self {
            //connection: Mutex::new(conn),
            id_field: "employee_id".to_string(),
            fields: fields.to_string(),
        })
    }
}

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
    async fn fetch_record(&self, entity: &str, id: &str) -> StorageResult<Vec<Value>> {
        trace!("Fetching record for entity: {}", entity);

        //let json_value = record_batch_to_json(record);
        //return Ok(vec![json_value]);
        //trace!("No record found");
        Ok(vec![])
    }
}

