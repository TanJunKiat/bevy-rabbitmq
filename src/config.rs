//! Configuration for the RabbitMQ plugin

use bevy::prelude::*;

/// Configuration for the RabbitMQ connection
#[derive(Resource, Clone, Debug)]
pub struct RabbitMqConfig {
    /// RabbitMQ connection URI (e.g., "amqp://guest:guest@localhost:5672")
    pub uri: String,

    /// Default exchange name for publishing messages
    pub default_exchange: String,

    /// Default routing key for publishing messages
    pub default_routing_key: String,

    /// Queue to consume messages from
    pub consume_queue: String,

    /// Consumer tag for the queue consumer
    pub consumer_tag: String,

    /// Whether to automatically reconnect on connection loss
    pub auto_reconnect: bool,
}

impl Default for RabbitMqConfig {
    fn default() -> Self {
        Self {
            uri: "amqp://guest:guest@localhost:5672".to_string(),
            default_exchange: "".to_string(), // Default exchange
            default_routing_key: "bevy_messages".to_string(),
            consume_queue: "bevy_messages".to_string(),
            consumer_tag: "bevy_consumer".to_string(),
            auto_reconnect: true,
        }
    }
}

/// Options for publishing a message to RabbitMQ
#[derive(Clone, Debug)]
pub struct PublishOptions {
    /// Exchange name (None uses default from config)
    pub exchange: Option<String>,

    /// Routing key (None uses default from config)
    pub routing_key: Option<String>,

    /// Whether the message should be persistent
    pub persistent: bool,

    /// Message priority (0-9)
    pub priority: Option<u8>,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            exchange: None,
            routing_key: None,
            persistent: true,
            priority: None,
        }
    }
}
