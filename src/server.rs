//! Server module for the application.
//!
//! This module provides the TCP server implementation for the Redis protocol.

use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::commands::handle_command;
use crate::config;
use crate::redis_protocol::RedisFrame;
use crate::storage::StorageService;

/// Server implementation for the Redis protocol.
pub struct Server {
    /// Server configuration.
    config: config::ServerConfig,
    /// Storage service for data operations.
    storage: Arc<StorageService>,
}

impl Server {
    /// Creates a new server with the given configuration and storage service.
    pub fn new(config: config::ServerConfig, storage: Arc<StorageService>) -> Self {
        Self { config, storage }
    }

    /// Runs the server.
    ///
    /// This method binds to the configured address and listens for incoming connections.
    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        info!("Listening on {}", self.config.bind_address);

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("Accepted connection from: {}", addr);
                    let storage = Arc::clone(&self.storage);
                    tokio::spawn(async move {
                        if let Err(e) = Self::process_client(socket, storage).await {
                            error!("Error processing client: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Processes a client connection.
    ///
    /// This method reads from the socket, parses a Redis command, and sends
    /// the response back to the client.
    async fn process_client(
        mut socket: TcpStream,
        storage: Arc<StorageService>,
    ) -> Result<(), Box<dyn Error>> {
        let mut buffer = [0; 1024];

        loop {
            let n = match socket.read(&mut buffer).await {
                Ok(0) => {
                    info!("Client disconnected");
                    return Ok(());
                }
                Ok(n) => n,
                Err(e) => {
                    error!("Failed to read from socket: {}", e);
                    return Err(e.into());
                }
            };

            // Try to parse the command
            match RedisFrame::parse(&buffer[..n]) {
                Ok(frame) => {
                    // Handle the command
                    let response = match handle_command(frame, Arc::clone(&storage)).await {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            let error_response = RedisFrame::Error(format!("ERR {}", e));
                            error_response.to_bytes()
                        }
                    };

                    // Send the response
                    socket.write_all(&response).await?;
                }
                Err(e) => {
                    error!("Failed to parse command: {}", e);
                    let error_response = RedisFrame::Error(format!("ERR {}", e));
                    socket.write_all(&error_response.to_bytes()).await?;
                }
            }
        }
    }
}
