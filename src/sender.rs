//! Sending Bevy events to RabbitMQ

use bevy::prelude::*;
use lapin::{
    options::BasicPublishOptions,
    BasicProperties,
};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{PublishOptions, RabbitMqConfig, RabbitMqConnection, RabbitMqError};

/// Event that requests a message to be sent to RabbitMQ
#[derive(Event, Debug, Clone)]
pub struct SendToRabbitMq {
    /// The message payload (will be serialized to JSON)
    pub payload: String,
    
    /// Publishing options
    pub options: PublishOptions,
}

impl SendToRabbitMq {
    /// Create a new message to send with default options
    pub fn new(payload: String) -> Self {
        Self {
            payload,
            options: PublishOptions::default(),
        }
    }

    /// Create a new message with custom options
    pub fn with_options(payload: String, options: PublishOptions) -> Self {
        Self { payload, options }
    }

    /// Create a message from a serializable type
    pub fn from_serializable<T: Serialize>(data: &T) -> Result<Self, RabbitMqError> {
        let payload = serde_json::to_string(data)?;
        Ok(Self::new(payload))
    }

    /// Create a message from a serializable type with options
    pub fn from_serializable_with_options<T: Serialize>(
        data: &T,
        options: PublishOptions,
    ) -> Result<Self, RabbitMqError> {
        let payload = serde_json::to_string(data)?;
        Ok(Self::with_options(payload, options))
    }
}

/// Resource that holds the sender for outgoing messages
#[derive(Resource)]
pub struct RabbitMqSender {
    sender: mpsc::UnboundedSender<(String, PublishOptions)>,
}

impl RabbitMqSender {
    /// Create a new sender
    pub fn new(sender: mpsc::UnboundedSender<(String, PublishOptions)>) -> Self {
        Self { sender }
    }

    /// Send a message to RabbitMQ
    pub fn send(&self, payload: String, options: PublishOptions) -> Result<(), RabbitMqError> {
        self.sender
            .send((payload, options))
            .map_err(|_| RabbitMqError::PublishError("Channel closed".to_string()))
    }
}

/// System that starts the publishing task
pub fn start_publishing(
    mut commands: Commands,
    connection: Res<RabbitMqConnection>,
    config: Res<RabbitMqConfig>,
) {
    let connection_clone = connection.clone();
    let config_clone = config.clone();
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, PublishOptions)>();

    // Spawn async task to publish messages
    tokio::spawn(async move {
        loop {
            // Wait for connection to be established and create a channel
            let channel = match connection_clone.create_channel().await {
                Ok(ch) => ch,
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            info!("Publishing channel ready");

            // Process messages to publish
            while let Some((payload, options)) = rx.recv().await {
                let exchange = options
                    .exchange
                    .as_deref()
                    .unwrap_or(&config_clone.default_exchange);
                let routing_key = options
                    .routing_key
                    .as_deref()
                    .unwrap_or(&config_clone.default_routing_key);

                let mut properties = BasicProperties::default();
                
                if options.persistent {
                    properties = properties.with_delivery_mode(2);
                }
                
                if let Some(priority) = options.priority {
                    properties = properties.with_priority(priority);
                }

                match channel
                    .basic_publish(
                        exchange,
                        routing_key,
                        BasicPublishOptions::default(),
                        payload.as_bytes(),
                        properties,
                    )
                    .await
                {
                    Ok(_) => {
                        debug!("Published message to exchange: {}, routing_key: {}", exchange, routing_key);
                    }
                    Err(e) => {
                        error!("Failed to publish message: {}", e);
                        // Recreate channel on error
                        break;
                    }
                }
            }

            warn!("Publishing channel closed, attempting to reconnect...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });

    commands.insert_resource(RabbitMqSender::new(tx));
}

/// System that handles SendToRabbitMq events
pub fn send_messages(
    mut events: EventReader<SendToRabbitMq>,
    sender: Res<RabbitMqSender>,
) {
    for event in events.read() {
        if let Err(e) = sender.send(event.payload.clone(), event.options.clone()) {
            error!("Failed to queue message for sending: {}", e);
        }
    }
}
