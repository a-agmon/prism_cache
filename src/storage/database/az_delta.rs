use crate::storage::database::record_batch_to_json;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use deltalake::open_table_with_storage_options;
use deltalake::storage::object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};
use url::Url;

use crate::storage::{DatabaseAdapter, StorageError, StorageResult, assert_required_settings};

pub struct AzDeltaAdapter {
    session: SessionContext,
    table_name: String,
    record_query: String, // should look like "SELECT * FROM table_name WHERE id = {}"
}

impl AzDeltaAdapter {
    pub async fn new(settings: HashMap<String, String>) -> StorageResult<Self> {
        deltalake::azure::register_handlers(None);

        // Verify required settings
        let required_keys = ["delta_table_name", "delta_table_path", "delta_record_query"];
        assert_required_settings(&settings, &required_keys)?;

        // Extract settings - we can safely unwrap since we already asserted they exist
        let table_name = settings.get("delta_table_name").unwrap().clone();
        let record_query = settings.get("delta_record_query").unwrap().clone();
        let table_path = settings.get("delta_table_path").unwrap().clone();

        // Get optional settings
        let bearer_token = settings.get("azure_bearer_token").map(|s| s.as_str());

        // Setup Azure storage
        let azure = get_azure_object_storage(&table_path, bearer_token)
            .map_err(|e| StorageError::DatabaseError(format!("Azure storage error: {}", e)))?;

        let azure_store = Arc::new(azure);
        let ctx = SessionContext::new();

        let store_url = Url::parse(&table_path)
            .map_err(|e| StorageError::DatabaseError(format!("Invalid az storage URL: {}", e)))?;

        ctx.runtime_env()
            .register_object_store(&store_url, azure_store.clone());

        register_deltalake_table(&ctx, &table_path, &table_name, bearer_token)
            .await
            .map_err(|e| {
                StorageError::DatabaseError(format!("Failed to register Delta table: {}", e))
            })?;

        Ok(Self {
            session: ctx,
            table_name: table_name,
            record_query: record_query,
        })
    }
}

#[async_trait]
impl DatabaseAdapter for AzDeltaAdapter {
    async fn fetch_record(&self, entity: &str, id: &str) -> StorageResult<Vec<Value>> {
        let query = self.record_query.replace("{}", id);
        let df = self
            .session
            .sql(&query)
            .await
            .map_err(|e| StorageError::DatabaseError(format!("SQL query error: {}", e)))?;

        let batch = df
            .collect()
            .await
            .map_err(|e| StorageError::DatabaseError(format!("Data collection error: {}", e)))?;

        let batch = match batch.len() {
            0 => {
                return Err(StorageError::RecordNotInDatabase(format!(
                    "Record '{}' not found",
                    id
                )));
            }
            1 => batch.first().unwrap(),
            _ => {
                warn!("More than one record found for id: {}", id);
                batch.first().unwrap()
            }
        };

        let json_value = record_batch_to_json(&batch);
        Ok(vec![json_value])
    }
}

async fn register_deltalake_table(
    ctx: &SessionContext,
    store_url_str: &str,
    table_name: &str,
    bearer_token: Option<&str>,
) -> anyhow::Result<()> {
    info!("registering table: {}", table_name);
    let table_path = format!("{}/{}", store_url_str, table_name);
    let storage_options = match bearer_token {
        Some(token) => HashMap::from([(String::from("bearer_token"), String::from(token))]),
        None => HashMap::from([(String::from("use_azure_cli"), String::from("true"))]),
    };
    let delta_table = open_table_with_storage_options(table_path, storage_options).await?;

    // Now we can directly register the delta_table since we're using compatible versions
    ctx.register_table(table_name, Arc::new(delta_table))?;
    Ok(())
}

fn get_azure_object_storage(
    store_url_str: &str,
    bearer_token: Option<&str>,
) -> anyhow::Result<MicrosoftAzure> {
    let mut builder = MicrosoftAzureBuilder::new().with_url(store_url_str);
    if let Some(token) = bearer_token {
        info!("using bearer token for azure object storage");
        builder = builder.with_bearer_token_authorization(token);
    } else {
        info!("using azure cli for azure object storage");
        builder = builder.with_use_azure_cli(true);
    }
    let azure = builder.build()?;
    Ok(azure)
}
