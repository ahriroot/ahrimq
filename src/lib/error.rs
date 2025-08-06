use std::fmt;

/// Error type for Amq errors.
///
/// # Variants
///
/// - `UnsupportedPlatform`: Unsupported platform.
/// - `TcpServerClosed`: TCP Server closed the connection.
/// - `TcpServerError(String)`: TCP Server error.
/// - `TcpReceiveError(String)`: TCP Receive error.
/// - `TcpReceiveDataError(String)`: TCP Receive data error.
/// - `TcpSendError(String)`: TCP Send error.
/// - `TcpSendDataError(String)`: TCP Send data error.
/// - `AuthorizationError(String)`: Authorization error.
pub enum AmqError {
    UnsupportedPlatform,
    TcpServerClosed,
    TcpServerError(String),
    TcpReceiveError(String),
    TcpReceiveDataError(String),
    TcpSendError(String),
    TcpSendDataError(String),
    AuthorizationError(String),
}

impl fmt::Display for AmqError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AmqError::UnsupportedPlatform => write!(f, "Unsupported Platform"),
            AmqError::TcpServerClosed => write!(f, "TCP Server Closed"),
            AmqError::TcpServerError(s) => write!(f, "TCP Server Error: {}", s),
            AmqError::TcpReceiveError(s) => write!(f, "TCP Receive Error: {}", s),
            AmqError::TcpReceiveDataError(s) => write!(f, "TCP Receive Data Error: {}", s),
            AmqError::TcpSendError(s) => write!(f, "TCP Send Error: {}", s),
            AmqError::TcpSendDataError(s) => write!(f, "TCP Send Data Error: {}", s),
            AmqError::AuthorizationError(s) => write!(f, "Authorization Error: {}", s),
        }
    }
}

impl fmt::Debug for AmqError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AmqError::UnsupportedPlatform => write!(f, "Unsupported Platform"),
            AmqError::TcpServerClosed => write!(f, "TCP Server Closed"),
            AmqError::TcpServerError(s) => write!(f, "TCP Server Error: {}", s),
            AmqError::TcpReceiveError(s) => write!(f, "TCP Receive Error: {}", s),
            AmqError::TcpReceiveDataError(s) => write!(f, "TCP Receive Data Error: {}", s),
            AmqError::TcpSendError(s) => write!(f, "TCP Send Error: {}", s),
            AmqError::TcpSendDataError(s) => write!(f, "TCP Send Data Error: {}", s),
            AmqError::AuthorizationError(s) => write!(f, "Authorization Error: {}", s),
        }
    }
}

impl std::error::Error for AmqError {}
