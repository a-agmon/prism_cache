//! Command handling for Redis protocol.
//!
//! This module handles Redis commands and translates them to storage operations.

use std::sync::Arc;
use tracing::{debug, error, info};

use crate::redis_protocol::{RedisError, RedisFrame};
use crate::storage::{StorageError, StorageService};

/// Maps storage errors to Redis errors.
fn map_error(err: StorageError) -> RedisError {
    match err {
        StorageError::EntityNotFound(msg) => RedisError::NotFound(msg),
        StorageError::FieldNotFound(msg) => RedisError::NotFound(msg),
        StorageError::RecordNotInDatabase(msg) => RedisError::NotFound(msg),
        StorageError::DatabaseError(msg) => {
            RedisError::Internal(format!("Database error: {}", msg))
        }
        StorageError::CacheError(msg) => RedisError::Internal(format!("Cache error: {}", msg)),
        StorageError::ConfigError(msg) => RedisError::Internal(format!("Config error: {}", msg)),
    }
}

/// Handles a Redis command.
///
/// This function dispatches the command to the appropriate handler.
pub async fn handle_command(
    frame: RedisFrame,
    storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    //debug!("Handling command: {:?}", frame);

    // Extract command and arguments
    let (command, args) = match frame {
        RedisFrame::Array(items) if !items.is_empty() => {
            let command = match &items[0] {
                RedisFrame::BulkString(cmd) => cmd.to_uppercase(),
                _ => {
                    return Err(RedisError::Protocol(
                        "Expected bulk string for command".into(),
                    ))
                }
            };

            // Create a new vector for the arguments
            let args = items[1..].to_vec();
            (command, args)
        }
        _ => return Err(RedisError::Protocol("Expected array for command".into())),
    };

    // Dispatch to appropriate handler
    match command.as_str() {
        "PING" => {
            info!("Handling PING command");
            Ok(RedisFrame::SimpleString("PONG".into()).to_bytes())
        }
        "SET" => handle_set(&args, Arc::clone(&storage)).await,
        "GET" => handle_get(&args, Arc::clone(&storage)).await,
        "HGET" => handle_hget(&args, Arc::clone(&storage)).await,
        _ => {
            error!("Unknown command: {}", command);
            Err(RedisError::UnknownCommand(command))
        }
    }
}

/// Handles the SET command.
///
/// SET key value
async fn handle_set(
    args: &[RedisFrame],
    _storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    if args.len() < 2 {
        return Err(RedisError::WrongArity("SET".into()));
    }

    let key = match &args[0] {
        RedisFrame::BulkString(key) => key,
        _ => return Err(RedisError::Protocol("Expected bulk string for key".into())),
    };

    let value = match &args[1] {
        RedisFrame::BulkString(value) => value,
        _ => {
            return Err(RedisError::Protocol(
                "Expected bulk string for value".into(),
            ))
        }
    };

    debug!("SET {} {}", key, value);

    // In a real implementation, we would store the value
    // For now, just return OK
    Ok(RedisFrame::SimpleString("OK".into()).to_bytes())
}

/// Handles the GET command.
///
/// GET key
async fn handle_get(
    args: &[RedisFrame],
    _storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    if args.len() != 1 {
        return Err(RedisError::WrongArity("GET".into()));
    }

    let key = match &args[0] {
        RedisFrame::BulkString(key) => key,
        _ => return Err(RedisError::Protocol("Expected bulk string for key".into())),
    };

    debug!("GET {}", key);

    // In a real implementation, we would retrieve the value
    // For now, just return a mock value
    // Ok(RedisFrame::BulkString(format!("value:{}", key)).to_bytes())

    // Return an array containing the key and "hello world"
    let response = RedisFrame::Array(vec![
        RedisFrame::BulkString(key.clone()),
        RedisFrame::BulkString("hello world".to_string()),
    ]);

    Ok(response.to_bytes())
}

/// Handles the HGET command.
///
/// HGET key field [field2 field3 ...]
/// The standard Redis HGET supports only one field, but we'll extend it to support multiple fields
async fn handle_hget(
    args: &[RedisFrame],
    storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    //debug!("HGET args: {:?}", args);
    if args.is_empty() || args.len() < 2 {
        return Err(RedisError::WrongArity("HGET".into()));
    }

    // key should be <entity>:<id>
    let key = match &args[0] {
        RedisFrame::BulkString(key) => key,
        _ => return Err(RedisError::Protocol("Expected bulk string for key".into())),
    };
    let key_parts: Vec<&str> = key.split(':').collect();
    if key_parts.len() != 2 {
        return Err(RedisError::Protocol(
            "Key should be in format entity:id".into(),
        ));
    }

    let entity = key_parts[0];
    let id = key_parts[1];
    // Extract all fields
    let fields: Result<Vec<&str>, RedisError> = args[1..]
        .iter()
        .map(|arg| match arg {
            RedisFrame::BulkString(field) => Ok(field.as_str()),
            _ => Err(RedisError::Protocol(
                "Expected bulk string for field".into(),
            )),
        })
        .collect();
    let fields = fields?;
    debug!(
        "HGET requested entity:{}, id: {} fields: {:?}",
        entity, id, fields
    );

    // Fetch the fields from storage
    match storage.fetch_fields(entity, id, &fields).await {
        Ok(data) => {
            if fields.len() == 1 {
                // If only one field was requested, return it as a simple value
                let field = fields[0];
                if let Some(value) = data.get(field) {
                    Ok(RedisFrame::BulkString(value.clone()).to_bytes())
                } else {
                    Ok(RedisFrame::Null.to_bytes())
                }
            } else {
                // If multiple fields were requested, return them as an array
                let mut response = Vec::new();
                for field in &fields {
                    if let Some(value) = data.get(*field) {
                        response.push(RedisFrame::BulkString(value.clone()));
                    } else {
                        response.push(RedisFrame::Null);
                    }
                }
                Ok(RedisFrame::Array(response).to_bytes())
            }
        }
        Err(e) => Err(map_error(e)),
    }
}
