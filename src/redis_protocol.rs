use std::str;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Invalid command: {0}")]
    InvalidCommand(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub enum RedisFrame {
    Array(Vec<RedisFrame>),
    BulkString(String),
    SimpleString(String),
    Error(String),
    Integer(i64),
    Null,
}

impl RedisFrame {
    pub fn parse(data: &[u8]) -> Result<Self, RedisError> {
        let mut parser = Parser::new(data);
        parser.parse_frame()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            RedisFrame::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RedisFrame::Error(s) => format!("-{}\r\n", s).into_bytes(),
            RedisFrame::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RedisFrame::BulkString(s) => {
                let mut bytes = format!("${}\r\n", s.len()).into_bytes();
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                bytes
            }
            RedisFrame::Array(frames) => {
                let mut bytes = format!("*{}\r\n", frames.len()).into_bytes();
                for frame in frames {
                    bytes.extend_from_slice(&frame.to_bytes());
                }
                bytes
            }
            RedisFrame::Null => b"$-1\r\n".to_vec(),
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            RedisFrame::BulkString(s) => Some(s),
            RedisFrame::SimpleString(s) => Some(s),
            _ => None,
        }
    }
}

struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Parser { data, pos: 0 }
    }

    fn parse_frame(&mut self) -> Result<RedisFrame, RedisError> {
        if self.pos >= self.data.len() {
            return Err(RedisError::ProtocolError(
                "Unexpected end of data".to_string(),
            ));
        }

        match self.data[self.pos] {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            _ => Err(RedisError::ProtocolError(format!(
                "Unknown type byte: {}",
                self.data[self.pos] as char
            ))),
        }
    }

    fn parse_simple_string(&mut self) -> Result<RedisFrame, RedisError> {
        self.pos += 1; // Skip '+'
        let start = self.pos;

        while self.pos < self.data.len() - 1 {
            if self.data[self.pos] == b'\r' && self.data[self.pos + 1] == b'\n' {
                let s = str::from_utf8(&self.data[start..self.pos]).map_err(|_| {
                    RedisError::ProtocolError("Invalid UTF-8 in simple string".to_string())
                })?;
                self.pos += 2; // Skip CRLF
                return Ok(RedisFrame::SimpleString(s.to_string()));
            }
            self.pos += 1;
        }

        Err(RedisError::ProtocolError(
            "Unterminated simple string".to_string(),
        ))
    }

    fn parse_error(&mut self) -> Result<RedisFrame, RedisError> {
        self.pos += 1; // Skip '-'
        let start = self.pos;

        while self.pos < self.data.len() - 1 {
            if self.data[self.pos] == b'\r' && self.data[self.pos + 1] == b'\n' {
                let s = str::from_utf8(&self.data[start..self.pos]).map_err(|_| {
                    RedisError::ProtocolError("Invalid UTF-8 in error string".to_string())
                })?;
                self.pos += 2; // Skip CRLF
                return Ok(RedisFrame::Error(s.to_string()));
            }
            self.pos += 1;
        }

        Err(RedisError::ProtocolError(
            "Unterminated error string".to_string(),
        ))
    }

    fn parse_integer(&mut self) -> Result<RedisFrame, RedisError> {
        self.pos += 1; // Skip ':'
        let start = self.pos;

        while self.pos < self.data.len() - 1 {
            if self.data[self.pos] == b'\r' && self.data[self.pos + 1] == b'\n' {
                let s = str::from_utf8(&self.data[start..self.pos]).map_err(|_| {
                    RedisError::ProtocolError("Invalid UTF-8 in integer".to_string())
                })?;
                let i = s
                    .parse::<i64>()
                    .map_err(|_| RedisError::ProtocolError("Invalid integer".to_string()))?;
                self.pos += 2; // Skip CRLF
                return Ok(RedisFrame::Integer(i));
            }
            self.pos += 1;
        }

        Err(RedisError::ProtocolError(
            "Unterminated integer".to_string(),
        ))
    }

    fn parse_bulk_string(&mut self) -> Result<RedisFrame, RedisError> {
        self.pos += 1; // Skip '$'
        let start = self.pos;

        // Find the end of the length
        while self.pos < self.data.len() - 1 {
            if self.data[self.pos] == b'\r' && self.data[self.pos + 1] == b'\n' {
                break;
            }
            self.pos += 1;
        }

        if self.pos >= self.data.len() - 1 {
            return Err(RedisError::ProtocolError(
                "Unterminated bulk string length".to_string(),
            ));
        }

        let length_str = str::from_utf8(&self.data[start..self.pos]).map_err(|_| {
            RedisError::ProtocolError("Invalid UTF-8 in bulk string length".to_string())
        })?;
        let length = length_str
            .parse::<i64>()
            .map_err(|_| RedisError::ProtocolError("Invalid bulk string length".to_string()))?;

        self.pos += 2; // Skip CRLF

        if length == -1 {
            return Ok(RedisFrame::Null);
        }

        let length = length as usize;
        let start = self.pos;

        if self.pos + length + 2 > self.data.len() {
            return Err(RedisError::ProtocolError(
                "Bulk string data too short".to_string(),
            ));
        }

        let s = str::from_utf8(&self.data[start..start + length])
            .map_err(|_| RedisError::ProtocolError("Invalid UTF-8 in bulk string".to_string()))?;

        self.pos += length + 2; // Skip string data and CRLF

        Ok(RedisFrame::BulkString(s.to_string()))
    }

    fn parse_array(&mut self) -> Result<RedisFrame, RedisError> {
        self.pos += 1; // Skip '*'
        let start = self.pos;

        // Find the end of the length
        while self.pos < self.data.len() - 1 {
            if self.data[self.pos] == b'\r' && self.data[self.pos + 1] == b'\n' {
                break;
            }
            self.pos += 1;
        }

        if self.pos >= self.data.len() - 1 {
            return Err(RedisError::ProtocolError(
                "Unterminated array length".to_string(),
            ));
        }

        let length_str = str::from_utf8(&self.data[start..self.pos])
            .map_err(|_| RedisError::ProtocolError("Invalid UTF-8 in array length".to_string()))?;
        let length = length_str
            .parse::<i64>()
            .map_err(|_| RedisError::ProtocolError("Invalid array length".to_string()))?;

        self.pos += 2; // Skip CRLF

        if length == -1 {
            return Ok(RedisFrame::Null);
        }

        let mut elements = Vec::with_capacity(length as usize);

        for _ in 0..length {
            elements.push(self.parse_frame()?);
        }

        Ok(RedisFrame::Array(elements))
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
