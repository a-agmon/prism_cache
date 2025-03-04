use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::str::FromStr;

use config::{AppConfig, ConfigError};
use server::Server;
use storage::StorageService;
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

mod commands;
mod config;
mod redis_protocol;
mod server;
mod storage;

/// Initialize logging
fn init_logging(log_level: &str) -> Result<(), Box<dyn Error>> {
    let level = Level::from_str(log_level).unwrap_or(Level::INFO);
    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

/// Load configuration from file or use defaults
fn load_config() -> Result<AppConfig, Box<dyn Error>> {
    let config_path = Path::new("config.toml");
    let config = if config_path.exists() {
        info!("Loading configuration from {}", config_path.display());
        let content = std::fs::read_to_string(config_path)?;
        match toml::from_str(&content) {
            Ok(config) => {
                debug!("Configuration loaded successfully");
                config
            }
            Err(e) => {
                let msg = format!(
                    "Failed to parse configuration file {}: {}",
                    config_path.display(),
                    e
                );
                error!("{}", msg);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, msg)));
            }
        }
    } else {
        info!("Configuration file not found, using defaults");
        AppConfig::default()
    };
    Ok(config)
}

/// Initialize the storage service
async fn init_storage(config: &AppConfig) -> Result<Arc<StorageService>, Box<dyn Error>> {
    let storage = Arc::new(StorageService::new(config).await?);
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
    
    // Log information about configured providers
    for provider in &config.database.providers {
        info!(
            "Configured provider: {} (type: {:?})",
            provider.name, provider.provider
        );
    }
    
    let storage = init_storage(&config).await?;
    run_server(config, storage).await?;
    Ok(())
}
