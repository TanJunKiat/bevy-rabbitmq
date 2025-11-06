//! Basic example demonstrating how to receive messages from RabbitMQ as Bevy events

use bevy::prelude::*;
use bevy_rabbitmq::{RabbitMqConfig, RabbitMqMessage, RabbitMqPlugin};

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .add_plugins(RabbitMqPlugin)
        .insert_resource(RabbitMqConfig {
            uri: "amqp://guest:guest@localhost:5672".to_string(),
            consume_queue: "bevy_messages".to_string(),
            ..Default::default()
        })
        .add_systems(Update, handle_messages)
        .run();
}

fn handle_messages(mut events: EventReader<RabbitMqMessage>) {
    for msg in events.read() {
        info!("Received message:");
        info!("  Exchange: {}", msg.exchange);
        info!("  Routing Key: {}", msg.routing_key);
        info!("  Payload: {}", msg.payload);
    }
}
