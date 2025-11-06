//! Receiving messages from RabbitMQ and converting them to Bevy events

use bevy::prelude::*;
use futures_lite::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{RabbitMqConfig, RabbitMqConnection};

/// Event that wraps a RabbitMQ message
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
pub struct RabbitMqMessage {
    /// The message payload as a JSON string
    pub payload: String,
    
    /// The routing key the message was received with
    pub routing_key: String,
    
    /// The exchange the message came from
    pub exchange: String,
}

/// Resource that holds the receiver for incoming messages
#[derive(Resource)]
pub struct RabbitMqReceiver {
    receiver: mpsc::UnboundedReceiver<RabbitMqMessage>,
}

impl RabbitMqReceiver {
    /// Create a new receiver
    pub fn new(receiver: mpsc::UnboundedReceiver<RabbitMqMessage>) -> Self {
        Self { receiver }
    }

    /// Try to receive a message without blocking
    pub fn try_recv(&mut self) -> Option<RabbitMqMessage> {
        self.receiver.try_recv().ok()
    }
}

/// System that starts consuming messages from RabbitMQ
pub fn start_consuming(
    mut commands: Commands,
    connection: Res<RabbitMqConnection>,
    config: Res<RabbitMqConfig>,
) {
    let connection_clone = connection.clone();
    let config_clone = config.clone();
    let (tx, rx) = mpsc::unbounded_channel();

    // Spawn async task to consume messages
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

            // Declare the queue
            let queue_result = channel
                .queue_declare(
                    &config_clone.consume_queue,
                    QueueDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await;

            if let Err(e) = queue_result {
                error!("Failed to declare queue: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }

            info!("Starting to consume from queue: {}", config_clone.consume_queue);

            // Start consuming
            let mut consumer = match channel
                .basic_consume(
                    &config_clone.consume_queue,
                    &config_clone.consumer_tag,
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to start consumer: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Process messages
            while let Some(delivery) = consumer.next().await {
                match delivery {
                    Ok(delivery) => {
                        let payload = String::from_utf8_lossy(&delivery.data).to_string();
                        let routing_key = delivery.routing_key.to_string();
                        let exchange = delivery.exchange.to_string();

                        let message = RabbitMqMessage {
                            payload,
                            routing_key,
                            exchange,
                        };

                        if tx.send(message).is_err() {
                            error!("Failed to send message to Bevy - channel closed");
                            break;
                        }

                        // Acknowledge the message
                        if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                            error!("Failed to acknowledge message: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error receiving message: {}", e);
                        break;
                    }
                }
            }

            warn!("Consumer stopped, attempting to reconnect...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });

    commands.insert_resource(RabbitMqReceiver::new(rx));
}

/// System that converts received RabbitMQ messages to Bevy events
pub fn receive_messages(
    mut receiver: ResMut<RabbitMqReceiver>,
    mut events: EventWriter<RabbitMqMessage>,
) {
    while let Some(message) = receiver.try_recv() {
        debug!("Received message from RabbitMQ: {:?}", message);
        events.send(message);
    }
}
