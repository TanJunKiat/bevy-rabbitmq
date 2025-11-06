//! Error types for the RabbitMQ plugin

use thiserror::Error;

/// Errors that can occur when working with RabbitMQ
#[derive(Error, Debug)]
pub enum RabbitMqError {
    /// Connection error
    #[error("Failed to connect to RabbitMQ: {0}")]
    ConnectionError(String),

    /// Channel error
    #[error("Failed to create channel: {0}")]
    ChannelError(String),

    /// Queue error
    #[error("Queue operation failed: {0}")]
    QueueError(String),

    /// Serialization error
    #[error("Failed to serialize message: {0}")]
    SerializationError(String),

    /// Deserialization error
    #[error("Failed to deserialize message: {0}")]
    DeserializationError(String),

    /// Publishing error
    #[error("Failed to publish message: {0}")]
    PublishError(String),

    /// Consumer error
    #[error("Failed to consume messages: {0}")]
    ConsumerError(String),
}

impl From<lapin::Error> for RabbitMqError {
    fn from(err: lapin::Error) -> Self {
        RabbitMqError::ConnectionError(err.to_string())
    }
}

impl From<serde_json::Error> for RabbitMqError {
    fn from(err: serde_json::Error) -> Self {
        RabbitMqError::SerializationError(err.to_string())
    }
}
