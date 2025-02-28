use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, instrument};

mod commands;
mod redis_protocol;

use commands::handle_command;
use redis_protocol::RedisFrame;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    info!("Redis server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("Accepted connection from: {}", addr);
                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket).await {
                        error!("Error processing connection: {:?}", e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {:?}", e);
            }
        }
    }
}

#[instrument(skip(socket))]
async fn process_socket(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = [0u8; 4096];

    loop {
        let n = match socket.read(&mut buffer).await {
            Ok(0) => {
                debug!("Connection closed by client");
                return Ok(());
            }
            Ok(n) => n,
            Err(e) => {
                error!("Failed to read from socket: {:?}", e);
                return Err(e.into());
            }
        };

        let data = &buffer[..n];
        debug!("Received {} bytes", n);

        match RedisFrame::parse(data) {
            Ok(frame) => {
                debug!("Parsed Redis frame: {:?}", frame);
                let response = handle_command(frame).await?;
                socket.write_all(&response).await?;
            }
            Err(e) => {
                error!("Failed to parse Redis frame: {:?}", e);
                let error_response = format!("-ERR {}\r\n", e);
                socket.write_all(error_response.as_bytes()).await?;
            }
        }
    }
}
