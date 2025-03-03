//! Command handling for Redis protocol.
//!
//! This module handles Redis commands and translates them to storage operations.

use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, error, info, trace};

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
        StorageError::RecordNotFoundInCache(msg) => RedisError::NotFound(msg),
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
                    ));
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
            ));
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
    storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    trace!("Entering handle_get with args: {:?}", args);
    if args.len() != 1 {
        debug!("Wrong number of arguments: expected 1, got {}", args.len());
        return Err(RedisError::WrongArity("GET".into()));
    }

    let key = match &args[0] {
        RedisFrame::BulkString(key) => {
            trace!("Extracted key: {}", key);
            key
        }
        _ => return Err(RedisError::Protocol("Expected bulk string for key".into())),
    };

    let (entity, id) = key
        .split_once(':')
        .ok_or(RedisError::Protocol("Expected entity:id format".into()))?;
    debug!("Processing GET request for [{entity}]:[{id}]");

    let record = storage.fetch_record(entity, id, &[]).await;
    match record {
        Ok(record) => {
            trace!("Found record: {}", record);
            Ok(RedisFrame::BulkString(record.to_string()).to_bytes())
        }
        Err(StorageError::EntityNotFound(_)) => {
            debug!("Entity not found for key: {}", key);
            Ok(RedisFrame::Null.to_bytes())
        }
        Err(e) => {
            error!("Error fetching record: {}", e);
            Ok(RedisFrame::Null.to_bytes())
        }, // => error might get lost here. 
    }
}

/// Handles the HGET command.
///
/// HGET key field [field2 field3 ...]
/// The standard Redis HGET supports only one field, but we'll extend it to support multiple fields
async fn handle_hget(
    args: &[RedisFrame],
    storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    debug!("HGET  called -> args: {:?}", args);
    if args.is_empty() || args.len() < 2 {
        return Err(RedisError::WrongArity("HGET".into()));
    }
    let key = match &args[0] {
        RedisFrame::BulkString(key) => key,
        _ => return Err(RedisError::Protocol("Expected bulk string for key".into())),
    };
    let (entity, id) = key
        .split_once(':')
        .ok_or(RedisError::Protocol("Expected entity:id format".into()))?;

    debug!("HGET [{entity}]:[{id}]", entity = entity, id = id);
    let record = storage.fetch_record(entity, id, &[]).await;

    match record {
        Ok(record) => {
            // Extract requested field values from record
            let values: Vec<String> = args[1..]
                .iter()
                .filter_map(|field| {
                    if let RedisFrame::BulkString(field_name) = field {
                        if record[field_name] != Value::Null {
                            Some(record[field_name].to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            Ok(
                RedisFrame::Array(values.into_iter().map(RedisFrame::BulkString).collect())
                    .to_bytes(),
            )
        }
        Err(StorageError::EntityNotFound(_)) => {
            debug!("Entity not found for key: {}", key);
            Ok(RedisFrame::Null.to_bytes())
        }
        Err(e) => Err(map_error(e)),
    }
}
