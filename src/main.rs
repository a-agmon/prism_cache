use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod commands;
mod config;
mod redis_protocol;
mod server;
mod storage;

use config::AppConfig;
use server::Server;
use storage::StorageService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Lake Cache server");

    // Load configuration
    let config_path = Path::new("config/default.yaml");
    let config = if config_path.exists() {
        info!("Loading configuration from {}", config_path.display());
        AppConfig::from_file(config_path)?
    } else {
        info!("Configuration file not found, using defaults");
        AppConfig::default()
    };

    // Initialize storage service and wrap it in an Arc for sharing
    let storage = Arc::new(StorageService::new(&config)?);
    info!("Storage service initialized successfully");

    // Initialize server with the storage service
    let server = Server::new(config.server.clone(), Arc::clone(&storage));

    // Run server
    info!("Server running on {}", config.server.bind_address);
    server.run().await?;

    Ok(())
}
