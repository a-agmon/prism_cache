//! Command handling for Redis protocol.
//!
//! This module handles Redis commands and translates them to storage operations.

use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, error, info, trace};

use crate::redis_protocol::{RedisError, RedisFrame};
use crate::storage::{StorageError, StorageService};
use serde_json::json;

/// Maps a StorageError to a RedisError
fn map_error(err: StorageError) -> RedisError {
    match err {
        StorageError::EntityNotFound(msg) => RedisError::NotFound(msg),
        StorageError::RecordNotInDatabase(msg) => RedisError::NotFound(msg),
        StorageError::RecordNotFoundInCache(msg) => RedisError::NotFound(msg),
        StorageError::FieldNotFound(msg) => RedisError::NotFound(msg),
        StorageError::ProviderNotFound(msg) => RedisError::NotFound(msg),
        StorageError::DatabaseError(msg) => RedisError::Internal(msg),
        StorageError::CacheError(msg) => RedisError::Internal(msg),
        StorageError::ConfigError(msg) => RedisError::Internal(msg),
    }
}

/// Handles a Redis command
///
/// This function dispatches the command to the appropriate handler based on the command name.
pub async fn handle_command(
    frame: RedisFrame,
    storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    let (command, args) = match frame {
        RedisFrame::Array(mut items) => {
            if items.is_empty() {
                return Err(RedisError::Protocol("Empty command".into()));
            }
            let command = match items.remove(0) {
                RedisFrame::BulkString(s) => s.to_uppercase(),
                _ => return Err(RedisError::Protocol("Expected bulk string for command".into())),
            };
            (command, items)
        }
        _ => return Err(RedisError::Protocol("Expected array".into())),
    };

    match command.as_str() {
        "PING" => Ok(RedisFrame::SimpleString("PONG".into()).to_bytes()),
        "SET" => handle_set(&args, storage).await,
        "GET" => handle_get(&args, storage).await,
        "HGET" => handle_hget(&args, storage).await,
        _ => Err(RedisError::UnknownCommand(command)),
    }
}

/// Handles the SET command.
///
/// SET key value
async fn handle_set(
    args: &[RedisFrame],
    _storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    if args.len() != 2 {
        return Err(RedisError::WrongArity("SET".into()));
    }

    let key = match &args[0] {
        RedisFrame::BulkString(key) => key,
        _ => return Err(RedisError::Protocol("Expected bulk string for key".into())),
    };

    let value = match &args[1] {
        RedisFrame::BulkString(value) => value,
        _ => return Err(RedisError::Protocol("Expected bulk string for value".into())),
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

    let (provider_name, id) = key
        .split_once(':')
        .ok_or(RedisError::Protocol("Expected provider:id format".into()))?;
    debug!("Processing GET request for provider [{}] with id [{}]", provider_name, id);

    let record = storage.fetch_record(provider_name, id).await;
    match record {
        Ok(record) => {
            trace!("Found record: {}", record);
            Ok(RedisFrame::BulkString(record.to_string()).to_bytes())
        }
        Err(StorageError::ProviderNotFound(_)) => {
            error!("Provider not found: {}", provider_name);
            Ok(RedisFrame::Null.to_bytes())
        }
        Err(StorageError::RecordNotInDatabase(_)) => {
            debug!("Record not found for key: {}", key);
            Ok(RedisFrame::Null.to_bytes())
        }
        Err(err) => {
            error!("Error fetching record: {:?}", err);
            Err(map_error(err))
        }
    }
}

/// Handles the HGET command.
///
/// HGET key field
async fn handle_hget(
    args: &[RedisFrame],
    storage: Arc<StorageService>,
) -> Result<Vec<u8>, RedisError> {
    if args.len() != 2 {
        return Err(RedisError::WrongArity("HGET".into()));
    }

    let key = match &args[0] {
        RedisFrame::BulkString(key) => key,
        _ => return Err(RedisError::Protocol("Expected bulk string for key".into())),
    };

    let field = match &args[1] {
        RedisFrame::BulkString(field) => field,
        _ => return Err(RedisError::Protocol("Expected bulk string for field".into())),
    };

    let (provider_name, id) = key
        .split_once(':')
        .ok_or(RedisError::Protocol("Expected provider:id format".into()))?;
    debug!("HGET provider [{}] id [{}] field [{}]", provider_name, id, field);

    let record = storage.fetch_record(provider_name, id).await;
    match record {
        Ok(record) => {
            if let Some(value) = record.get(field) {
                if value.is_null() {
                    Ok(RedisFrame::Null.to_bytes())
                } else {
                    Ok(RedisFrame::BulkString(value.to_string()).to_bytes())
                }
            } else {
                Ok(RedisFrame::Null.to_bytes())
            }
        }
        Err(StorageError::ProviderNotFound(_)) => {
            debug!("Provider not found: {}", provider_name);
            Ok(RedisFrame::Null.to_bytes())
        }
        Err(StorageError::RecordNotInDatabase(_)) => {
            debug!("Record not found for key: {}", key);
            Ok(RedisFrame::Null.to_bytes())
        }
        Err(err) => Err(map_error(err)),
    }
}
