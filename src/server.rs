//! Server module for the application.
//!
//! This module provides the TCP server implementation for the Redis protocol.

use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

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
    /// This method binds to the configured address and listens for incoming
    /// connections.
    pub async fn run(&self) -> Result<(), Box<dyn Error>> {
        // Bind to the server address
        let listener = TcpListener::bind(&self.config.bind_address).await?;
        info!("Listening on {}", self.config.bind_address);

        // Accept connections and process them
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!("Accepted connection from: {}", addr);
                    // Clone the Arc to the storage service for this connection
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
    /// This method reads from the socket, parses Redis commands, and sends
    /// responses back to the client.
    async fn process_client(
        mut socket: TcpStream,
        storage: Arc<StorageService>,
    ) -> Result<(), Box<dyn Error>> {
        let mut buffer = [0; 1024];
        let mut command_buffer = Vec::new();

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

            // Append the new data to our command buffer
            command_buffer.extend_from_slice(&buffer[..n]);

            // Try to process as many complete commands as possible
            let mut processed = 0;
            while processed < command_buffer.len() {
                // Try to parse a command from the current position
                match RedisFrame::parse(&command_buffer[processed..]) {
                    Ok(frame) => {
                        //debug!("Successfully parsed frame: {:?}", frame);

                        // Handle the command with access to the storage service
                        let response = match handle_command(frame, Arc::clone(&storage)).await {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                let error_response = RedisFrame::Error(format!("ERR {}", e));
                                error_response.to_bytes()
                            }
                        };

                        // Send the response
                        socket.write_all(&response).await?;

                        // Move past this command in the buffer
                        // Since we don't know exactly how many bytes were consumed,
                        // we'll just clear the buffer and break out of the loop
                        processed = command_buffer.len();
                        break;
                    }
                    Err(e) => {
                        // If we get an "Unexpected end of data" error, we need more data
                        if e.to_string().contains("Unexpected end of data")
                            || e.to_string().contains("Empty data")
                        {
                            debug!("Incomplete command, waiting for more data");
                            break;
                        } else {
                            // For other errors, report to the client and try to continue
                            error!("Failed to parse frame: {}", e);
                            let error_response = RedisFrame::Error(format!("ERR {}", e));
                            socket.write_all(&error_response.to_bytes()).await?;

                            // Since we don't know how to recover, clear the buffer and start fresh
                            processed = command_buffer.len();
                            break;
                        }
                    }
                }
            }

            // Remove processed data from the buffer
            if processed > 0 {
                command_buffer.drain(0..processed);
            }

            // If the buffer gets too large without being able to parse a command,
            // something is wrong - clear it to prevent memory issues
            if command_buffer.len() > 10240 {
                // 10KB limit
                error!(
                    "Command buffer too large ({}), clearing",
                    command_buffer.len()
                );
                command_buffer.clear();
                let error_response = RedisFrame::Error("ERR Command too large".into());
                socket.write_all(&error_response.to_bytes()).await?;
            }
        }
    }
}
