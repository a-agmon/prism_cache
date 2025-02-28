use crate::redis_protocol::{RedisError, RedisFrame};
use tracing::{debug, info, instrument};

#[instrument(skip(frame))]
pub async fn handle_command(frame: RedisFrame) -> Result<Vec<u8>, RedisError> {
    match frame {
        RedisFrame::Array(parts) => {
            if parts.is_empty() {
                return Err(RedisError::InvalidCommand("Empty command".to_string()));
            }

            let command = parts[0]
                .as_string()
                .ok_or_else(|| RedisError::InvalidCommand("Command must be a string".to_string()))?
                .to_uppercase();

            debug!("Received command: {}", command);

            match command.as_str() {
                "SET" => handle_set(&parts).await,
                "GET" => handle_get(&parts).await,
                "HGET" => handle_hget(&parts).await,
                _ => {
                    info!("Unsupported command: {}", command);
                    Ok(RedisFrame::Error(format!("ERR unknown command '{}'", command)).to_bytes())
                }
            }
        }
        _ => Err(RedisError::InvalidCommand(
            "Expected array for command".to_string(),
        )),
    }
}

#[instrument(skip(parts))]
async fn handle_set(parts: &[RedisFrame]) -> Result<Vec<u8>, RedisError> {
    if parts.len() < 3 {
        return Err(RedisError::InvalidCommand(
            "SET requires at least key and value arguments".to_string(),
        ));
    }

    let key = parts[1]
        .as_string()
        .ok_or_else(|| RedisError::InvalidCommand("SET key must be a string".to_string()))?;

    let value = parts[2]
        .as_string()
        .ok_or_else(|| RedisError::InvalidCommand("SET value must be a string".to_string()))?;

    info!("SET command received for key: {}", key);
    debug!("Value to set: {}", value);

    // TODO: Implement the actual SET logic here
    // For now, just return OK
    Ok(RedisFrame::SimpleString("OK".to_string()).to_bytes())
}

#[instrument(skip(parts))]
async fn handle_get(parts: &[RedisFrame]) -> Result<Vec<u8>, RedisError> {
    if parts.len() < 2 {
        return Err(RedisError::InvalidCommand(
            "GET requires a key argument".to_string(),
        ));
    }

    let key = parts[1]
        .as_string()
        .ok_or_else(|| RedisError::InvalidCommand("GET key must be a string".to_string()))?;

    info!("GET command received for key: {}", key);

    // TODO: Implement the actual GET logic here
    // For now, just return null
    Ok(RedisFrame::Null.to_bytes())
}

#[instrument(skip(parts))]
async fn handle_hget(parts: &[RedisFrame]) -> Result<Vec<u8>, RedisError> {
    // lets see how many parts we have
    debug!("HGET command received with {} parts", parts.len());
    // the first part needs to be key and id: users:1234
    let key = parts[1]
        .as_string()
        .ok_or_else(|| RedisError::InvalidCommand("HGET key must be a string".to_string()))?;
    // now we need to make sure we have this form <entity>:<id>
    let key_parts: Vec<&str> = key.split(':').collect();
    if key_parts.len() != 2 {
        return Err(RedisError::InvalidCommand(
            "HGET key must be in the form <entity>:<id>".to_string(),
        ));
    }
    let entity = key_parts[0];
    let id = key_parts[1];
    debug!("HGET command for entity: {} and id: {}", entity, id);
    // the rest of the parts are the fields we want to get
    let fields: Result<Vec<&str>, RedisError> = parts[2..]
        .iter()
        .map(|part| {
            part.as_string().ok_or_else(|| {
                RedisError::InvalidCommand("HGET field must be a string".to_string())
            })
        })
        .collect();
    let fields = fields?;
    debug!("Fields to get: {:?}", fields);

    // TODO: Implement the actual GET logic here
    // For now, just return null
    Ok(RedisFrame::Null.to_bytes())
}
