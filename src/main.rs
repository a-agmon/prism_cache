use std::{error::Error, str::FromStr};
use std::path::Path;
use std::sync::Arc;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

mod commands;
mod config;
mod redis_protocol;
mod server;
mod storage;

use config::{AppConfig, ConfigError};
use server::Server;
use storage::StorageService;

/// Initialize the logging system with the configured level
fn init_logging(log_level: &str) -> Result<(), Box<dyn Error>> {
    let level = Level::from_str(log_level).unwrap();
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

/// Load the application configuration from file or use defaults
fn load_config() -> Result<AppConfig, Box<dyn Error>> {
    let config_path = Path::new("config/default.yaml");
    let config = if config_path.exists() {
        info!("Loading configuration from {}", config_path.display());
        match AppConfig::from_file(config_path) {
            Ok(config) => config,
            Err(ConfigError::InvalidDatabaseProvider(msg)) => {
                error!("Configuration Error: {}", msg);
                error!(
                    "Please check your configuration file at {}",
                    config_path.display()
                );
                return Err(Box::new(ConfigError::InvalidDatabaseProvider(msg)));
            }
            Err(e) => return Err(Box::new(e)),
        }
    } else {
        info!("Configuration file not found, using defaults");
        AppConfig::default()
    };
    Ok(config)
}

/// Initialize the storage service
fn init_storage(config: &AppConfig) -> Result<Arc<StorageService>, Box<dyn Error>> {
    let storage = Arc::new(StorageService::new(config)?);
    info!("Storage service initialized successfully");
    Ok(storage)
}

/// Run the server
async fn run_server(config: AppConfig, storage: Arc<StorageService>) -> Result<(), Box<dyn Error>> {
    let server = Server::new(config.server.clone(), Arc::clone(&storage));
    info!("Server running on {}", config.server.bind_address);
    server.run().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = load_config()?;
    init_logging(&config.logging.level)?;
    info!("Starting Prism Cache server");
    let storage = init_storage(&config)?;
    run_server(config, storage).await?;
    Ok(())
}
