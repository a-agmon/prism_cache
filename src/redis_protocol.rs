//! Redis protocol implementation.
//!
//! This module provides types and functions for working with the Redis protocol.

use thiserror::Error;
use tracing::{debug, error, trace};

/// Error type for Redis protocol operations.
#[derive(Debug, Error)]
pub enum RedisError {
    /// Protocol error.
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Unknown command.
    #[error("Unknown command: {0}")]
    UnknownCommand(String),

    /// Wrong number of arguments.
    #[error("Wrong number of arguments for '{0}' command")]
    WrongArity(String),

    /// Entity not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// Internal server error.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Redis frame type.
///
/// This enum represents the different types of frames in the Redis protocol.
#[derive(Debug, Clone)]
pub enum RedisFrame {
    /// Simple string response.
    SimpleString(String),

    /// Error response.
    Error(String),

    /// Integer response.
    #[allow(dead_code)]
    Integer(i64),

    /// Bulk string response.
    BulkString(String),

    /// Array response.
    Array(Vec<RedisFrame>),

    /// Null response.
    Null,
}

impl RedisFrame {
    /// Returns the string value if this is a string frame.
    #[allow(dead_code)]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            RedisFrame::SimpleString(s) => Some(s),
            RedisFrame::BulkString(s) => Some(s),
            _ => None,
        }
    }

    /// Parses a byte slice into a RedisFrame.
    ///
    /// This is a simplified parser that only handles the basic Redis protocol.
    pub fn parse(data: &[u8]) -> Result<Self, RedisError> {
        if data.is_empty() {
            return Err(RedisError::Protocol("Empty data".into()));
        }

        // Trim any leading whitespace
        let mut start_idx = 0;
        while start_idx < data.len() && (data[start_idx] as char).is_whitespace() {
            start_idx += 1;
        }

        if start_idx >= data.len() {
            return Err(RedisError::Protocol(
                "Empty data after trimming whitespace".into(),
            ));
        }

        // Check if this is a RESP protocol command
        if data[start_idx] != b'*'
            && data[start_idx] != b'+'
            && data[start_idx] != b'-'
            && data[start_idx] != b':'
            && data[start_idx] != b'$'
        {
            debug!("Not a RESP protocol command, treating as plain text");
            return Self::parse_plain_text(data);
        }

        // Parse based on the first byte
        match data[start_idx] {
            b'*' => Self::parse_array(&data[start_idx..]),
            b'+' => Self::parse_simple_string(&data[start_idx..]),
            b'-' => Self::parse_error(&data[start_idx..]),
            b':' => Self::parse_integer(&data[start_idx..]),
            b'$' => Self::parse_bulk_string(&data[start_idx..]),
            _ => Err(RedisError::Protocol(format!(
                "Unknown type byte: {}",
                data[start_idx] as char
            ))),
        }
    }

    /// Parse an array from RESP protocol
    fn parse_array(data: &[u8]) -> Result<Self, RedisError> {
        // Skip the '*' byte
        let mut pos = 1;

        // Parse the array length
        let mut length = 0;
        while pos < data.len() && data[pos] != b'\r' {
            if !data[pos].is_ascii_digit() {
                return Err(RedisError::Protocol(format!(
                    "Expected digit in array length, got: {}",
                    data[pos] as char
                )));
            }
            length = length * 10 + (data[pos] - b'0') as i64;
            pos += 1;
        }

        // Skip CRLF
        if pos + 1 >= data.len() || data[pos] != b'\r' || data[pos + 1] != b'\n' {
            return Err(RedisError::Protocol(
                "Expected CRLF after array length".into(),
            ));
        }
        pos += 2;

        // Parse array elements
        let mut elements = Vec::new();
        for i in 0..length {
            if pos >= data.len() {
                return Err(RedisError::Protocol(format!(
                    "Unexpected end of data while parsing array element {}",
                    i
                )));
            }

            // Parse the element based on its type
            let element = match data[pos] {
                b'*' => Self::parse_array(&data[pos..])?,
                b'+' => Self::parse_simple_string(&data[pos..])?,
                b'-' => Self::parse_error(&data[pos..])?,
                b':' => Self::parse_integer(&data[pos..])?,
                b'$' => Self::parse_bulk_string(&data[pos..])?,
                _ => {
                    let debug_bytes: Vec<String> = data[pos..]
                        .iter()
                        .take(20)
                        .map(|b| format!("{:02X}", b))
                        .collect();
                    return Err(RedisError::Protocol(format!(
                        "Unknown element type byte: {} (hex: {:02X}) at position {}. Next bytes: [{}]",
                        data[pos] as char, data[pos], pos, debug_bytes.join(" ")
                    )));
                }
            };

            // Calculate how many bytes were consumed by this element
            let element_size = match &element {
                RedisFrame::SimpleString(s) => 3 + s.len(), // +, string, CRLF
                RedisFrame::Error(s) => 3 + s.len(),        // -, string, CRLF
                RedisFrame::Integer(i) => 3 + i.to_string().len(), // :, integer, CRLF
                RedisFrame::BulkString(s) => {
                    // $, length, CRLF, string, CRLF
                    5 + s.len() + s.len().to_string().len()
                }
                RedisFrame::Array(elements) => {
                    // This is complex to calculate, so we'll use a different approach
                    // We'll scan for the next element's type marker
                    let mut next_pos = pos + 1;
                    let mut depth = 0;

                    while next_pos < data.len() {
                        if data[next_pos] == b'*' {
                            depth += 1;
                        } else if depth > 0
                            && (data[next_pos] == b'+'
                                || data[next_pos] == b'-'
                                || data[next_pos] == b':'
                                || data[next_pos] == b'$')
                        {
                            depth -= 1;
                        } else if depth == 0
                            && (data[next_pos] == b'*'
                                || data[next_pos] == b'+'
                                || data[next_pos] == b'-'
                                || data[next_pos] == b':'
                                || data[next_pos] == b'$')
                        {
                            break;
                        }
                        next_pos += 1;
                    }

                    if next_pos >= data.len() && i < length - 1 {
                        // We reached the end of data but expected more elements
                        return Err(RedisError::Protocol(
                            "Unexpected end of data while parsing array".into(),
                        ));
                    }

                    next_pos - pos
                }
                RedisFrame::Null => 5, // $-1\r\n
            };

            pos += element_size;
            elements.push(element);
        }

        Ok(RedisFrame::Array(elements))
    }

    /// Parse a simple string from RESP protocol
    fn parse_simple_string(data: &[u8]) -> Result<Self, RedisError> {
        // Skip the '+' byte
        let mut pos = 1;
        let mut string = String::new();

        // Read until CRLF
        while pos < data.len() && data[pos] != b'\r' {
            string.push(data[pos] as char);
            pos += 1;
        }

        // Check for CRLF
        if pos + 1 >= data.len() || data[pos] != b'\r' || data[pos + 1] != b'\n' {
            return Err(RedisError::Protocol(
                "Expected CRLF after simple string".into(),
            ));
        }

        debug!("Parsed simple string: {:?}", string);
        Ok(RedisFrame::SimpleString(string))
    }

    /// Parse an error from RESP protocol
    fn parse_error(data: &[u8]) -> Result<Self, RedisError> {
        // Skip the '-' byte
        let mut pos = 1;
        let mut string = String::new();

        // Read until CRLF
        while pos < data.len() && data[pos] != b'\r' {
            string.push(data[pos] as char);
            pos += 1;
        }

        // Check for CRLF
        if pos + 1 >= data.len() || data[pos] != b'\r' || data[pos + 1] != b'\n' {
            return Err(RedisError::Protocol("Expected CRLF after error".into()));
        }

        debug!("Parsed error: {:?}", string);
        Ok(RedisFrame::Error(string))
    }

    /// Parse an integer from RESP protocol
    fn parse_integer(data: &[u8]) -> Result<Self, RedisError> {
        // Skip the ':' byte
        let mut pos = 1;
        let mut negative = false;
        let mut value = 0;

        // Check for negative sign
        if pos < data.len() && data[pos] == b'-' {
            negative = true;
            pos += 1;
        }

        // Parse digits
        while pos < data.len() && data[pos] != b'\r' {
            if !data[pos].is_ascii_digit() {
                return Err(RedisError::Protocol(format!(
                    "Expected digit in integer, got: {}",
                    data[pos] as char
                )));
            }
            value = value * 10 + (data[pos] - b'0') as i64;
            pos += 1;
        }

        // Apply negative sign
        if negative {
            value = -value;
        }

        // Check for CRLF
        if pos + 1 >= data.len() || data[pos] != b'\r' || data[pos + 1] != b'\n' {
            return Err(RedisError::Protocol("Expected CRLF after integer".into()));
        }

        debug!("Parsed integer: {}", value);
        Ok(RedisFrame::Integer(value))
    }

    /// Parse a bulk string from RESP protocol
    fn parse_bulk_string(data: &[u8]) -> Result<Self, RedisError> {
        // Skip the '$' byte
        let mut pos = 1;
        let mut length = 0;
        let mut negative = false;

        // Check for negative length (null)
        if pos < data.len() && data[pos] == b'-' {
            negative = true;
            pos += 1;
        }

        // Parse length
        while pos < data.len() && data[pos] != b'\r' {
            if !data[pos].is_ascii_digit() {
                return Err(RedisError::Protocol(format!(
                    "Expected digit in bulk string length, got: {}",
                    data[pos] as char
                )));
            }
            length = length * 10 + (data[pos] - b'0') as i64;
            pos += 1;
        }

        // Check for CRLF after length
        if pos + 1 >= data.len() || data[pos] != b'\r' || data[pos + 1] != b'\n' {
            return Err(RedisError::Protocol(
                "Expected CRLF after bulk string length".into(),
            ));
        }
        pos += 2;

        // Handle null bulk string
        if negative {
            return Ok(RedisFrame::Null);
        }

        // Check if we have enough data
        if pos + length as usize + 2 > data.len() {
            return Err(RedisError::Protocol(format!(
                "Bulk string too short: expected {} bytes plus CRLF, got {} bytes",
                length,
                data.len() - pos
            )));
        }

        // Extract string
        let string = String::from_utf8_lossy(&data[pos..pos + length as usize]).to_string();
        pos += length as usize;

        // Check for CRLF after string
        if data[pos] != b'\r' || data[pos + 1] != b'\n' {
            return Err(RedisError::Protocol(format!(
                "Expected CRLF after bulk string, got: {:02X} {:02X}",
                data[pos],
                data[pos + 1]
            )));
        }

        Ok(RedisFrame::BulkString(string))
    }

    /// Parse a plain text command (not in RESP format)
    fn parse_plain_text(data: &[u8]) -> Result<Self, RedisError> {
        // Convert the data to a string
        let raw_input = String::from_utf8_lossy(data);

        // Clean the input: replace all carriage returns and newlines with spaces, then trim whitespace
        let cleaned_input = raw_input
            .replace('\r', " ")
            .replace('\n', " ")
            .trim()
            .to_string();

        // If the input is empty after cleaning, return an error
        if cleaned_input.is_empty() {
            return Err(RedisError::Protocol("Empty command after cleaning".into()));
        }

        // Split by whitespace to get command and arguments
        let parts: Vec<&str> = cleaned_input.split_whitespace().collect();

        if parts.is_empty() {
            return Err(RedisError::Protocol("Empty command after splitting".into()));
        }

        // Create a Redis array frame with bulk strings
        let mut frames = Vec::new();
        for part in parts {
            frames.push(RedisFrame::BulkString(part.to_string()));
        }

        Ok(RedisFrame::Array(frames))
    }

    /// Converts a RedisFrame to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RedisFrame::SimpleString(s) => {
                let mut bytes = Vec::new();
                bytes.push(b'+');
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                bytes
            }
            RedisFrame::Error(s) => {
                let mut bytes = Vec::new();
                bytes.push(b'-');
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                bytes
            }
            RedisFrame::Integer(i) => {
                let mut bytes = Vec::new();
                bytes.push(b':');
                bytes.extend_from_slice(i.to_string().as_bytes());
                bytes.extend_from_slice(b"\r\n");
                bytes
            }
            RedisFrame::BulkString(s) => {
                let mut bytes = Vec::new();
                bytes.push(b'$');
                bytes.extend_from_slice(s.len().to_string().as_bytes());
                bytes.extend_from_slice(b"\r\n");
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                bytes
            }
            RedisFrame::Array(frames) => {
                let mut bytes = Vec::new();
                bytes.push(b'*');
                bytes.extend_from_slice(frames.len().to_string().as_bytes());
                bytes.extend_from_slice(b"\r\n");
                for frame in frames {
                    bytes.extend_from_slice(&frame.to_bytes());
                }
                bytes
            }
            RedisFrame::Null => {
                let mut bytes = Vec::new();
                bytes.push(b'$');
                bytes.extend_from_slice(b"-1\r\n");
                bytes
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let data = b"+OK\r\n";
        let frame = RedisFrame::parse(data).unwrap();

        match frame {
            RedisFrame::SimpleString(s) => assert_eq!(s, "OK"),
            _ => panic!("Expected SimpleString, got {:?}", frame),
        }
    }

    #[test]
    fn test_parse_error() {
        let data = b"-Error message\r\n";
        let frame = RedisFrame::parse(data).unwrap();

        match frame {
            RedisFrame::Error(s) => assert_eq!(s, "Error message"),
            _ => panic!("Expected Error, got {:?}", frame),
        }
    }

    #[test]
    fn test_parse_integer() {
        let data = b":1000\r\n";
        let frame = RedisFrame::parse(data).unwrap();

        match frame {
            RedisFrame::Integer(i) => assert_eq!(i, 1000),
            _ => panic!("Expected Integer, got {:?}", frame),
        }
    }

    #[test]
    fn test_parse_bulk_string() {
        let data = b"$5\r\nhello\r\n";
        let frame = RedisFrame::parse(data).unwrap();

        match frame {
            RedisFrame::BulkString(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected BulkString, got {:?}", frame),
        }
    }

    #[test]
    fn test_parse_null() {
        let data = b"$-1\r\n";
        let frame = RedisFrame::parse(data).unwrap();

        match frame {
            RedisFrame::Null => {}
            _ => panic!("Expected Null, got {:?}", frame),
        }
    }

    #[test]
    fn test_parse_empty_array() {
        let data = b"*0\r\n";
        let frame = RedisFrame::parse(data).unwrap();

        match frame {
            RedisFrame::Array(arr) => assert_eq!(arr.len(), 0),
            _ => panic!("Expected Array, got {:?}", frame),
        }
    }

    #[test]
    fn test_parse_array() {
        let data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let frame = RedisFrame::parse(data).unwrap();

        match frame {
            RedisFrame::Array(arr) => {
                assert_eq!(arr.len(), 3);

                match &arr[0] {
                    RedisFrame::BulkString(s) => assert_eq!(s, "SET"),
                    _ => panic!("Expected BulkString, got {:?}", arr[0]),
                }

                match &arr[1] {
                    RedisFrame::BulkString(s) => assert_eq!(s, "key"),
                    _ => panic!("Expected BulkString, got {:?}", arr[1]),
                }

                match &arr[2] {
                    RedisFrame::BulkString(s) => assert_eq!(s, "value"),
                    _ => panic!("Expected BulkString, got {:?}", arr[2]),
                }
            }
            _ => panic!("Expected Array, got {:?}", frame),
        }
    }

    #[test]
    fn test_parse_nested_array() {
        let data = b"*2\r\n*2\r\n+inner1\r\n+inner2\r\n$5\r\nouter\r\n";
        let frame = RedisFrame::parse(data).unwrap();

        match frame {
            RedisFrame::Array(arr) => {
                assert_eq!(arr.len(), 2);

                match &arr[0] {
                    RedisFrame::Array(inner) => {
                        assert_eq!(inner.len(), 2);
                        match &inner[0] {
                            RedisFrame::SimpleString(s) => assert_eq!(s, "inner1"),
                            _ => panic!("Expected SimpleString, got {:?}", inner[0]),
                        }
                        match &inner[1] {
                            RedisFrame::SimpleString(s) => assert_eq!(s, "inner2"),
                            _ => panic!("Expected SimpleString, got {:?}", inner[1]),
                        }
                    }
                    _ => panic!("Expected Array, got {:?}", arr[0]),
                }

                match &arr[1] {
                    RedisFrame::BulkString(s) => assert_eq!(s, "outer"),
                    _ => panic!("Expected BulkString, got {:?}", arr[1]),
                }
            }
            _ => panic!("Expected Array, got {:?}", frame),
        }
    }

    #[test]
    fn test_to_bytes_simple_string() {
        let frame = RedisFrame::SimpleString("OK".to_string());
        let bytes = frame.to_bytes();
        assert_eq!(bytes, b"+OK\r\n");
    }

    #[test]
    fn test_to_bytes_error() {
        let frame = RedisFrame::Error("Error message".to_string());
        let bytes = frame.to_bytes();
        assert_eq!(bytes, b"-Error message\r\n");
    }

    #[test]
    fn test_to_bytes_integer() {
        let frame = RedisFrame::Integer(1000);
        let bytes = frame.to_bytes();
        assert_eq!(bytes, b":1000\r\n");
    }

    #[test]
    fn test_to_bytes_bulk_string() {
        let frame = RedisFrame::BulkString("hello".to_string());
        let bytes = frame.to_bytes();
        assert_eq!(bytes, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_to_bytes_null() {
        let frame = RedisFrame::Null;
        let bytes = frame.to_bytes();
        assert_eq!(bytes, b"$-1\r\n");
    }

    #[test]
    fn test_to_bytes_array() {
        let frame = RedisFrame::Array(vec![
            RedisFrame::BulkString("SET".to_string()),
            RedisFrame::BulkString("key".to_string()),
            RedisFrame::BulkString("value".to_string()),
        ]);
        let bytes = frame.to_bytes();
        assert_eq!(bytes, b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn test_parse_invalid_protocol() {
        let data = b"invalid data";
        let result = RedisFrame::parse(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_incomplete_simple_string() {
        let data = b"+OK";
        let result = RedisFrame::parse(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_incomplete_bulk_string() {
        let data = b"$5\r\nhell";
        let result = RedisFrame::parse(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_as_string() {
        let simple = RedisFrame::SimpleString("simple".to_string());
        assert_eq!(simple.as_string(), Some("simple"));

        let bulk = RedisFrame::BulkString("bulk".to_string());
        assert_eq!(bulk.as_string(), Some("bulk"));

        let integer = RedisFrame::Integer(42);
        assert_eq!(integer.as_string(), None);
    }

    #[test]
    fn test_roundtrip() {
        // Test that parsing and then serializing gives the original data
        let original = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let frame = RedisFrame::parse(original).unwrap();
        let serialized = frame.to_bytes();
        assert_eq!(serialized, original);
    }
}
