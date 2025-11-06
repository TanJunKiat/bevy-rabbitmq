//! Example demonstrating bidirectional communication with RabbitMQ

use bevy::prelude::*;
use bevy_rabbitmq::{RabbitMqConfig, RabbitMqMessage, RabbitMqPlugin, SendToRabbitMq};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct MyMessage {
    id: u32,
    content: String,
}

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .add_plugins(RabbitMqPlugin)
        .insert_resource(RabbitMqConfig {
            uri: "amqp://guest:guest@localhost:5672".to_string(),
            consume_queue: "bevy_messages".to_string(),
            default_routing_key: "bevy_messages".to_string(),
            ..Default::default()
        })
        .add_systems(Startup, setup)
        .add_systems(Update, (
            receive_messages,
            send_periodic_messages,
        ))
        .run();
}

#[derive(Resource)]
struct MessageCounter {
    timer: Timer,
    count: u32,
}

fn setup(mut commands: Commands) {
    commands.insert_resource(MessageCounter {
        timer: Timer::new(Duration::from_secs(5), TimerMode::Repeating),
        count: 0,
    });
    info!("Bidirectional example started");
}

fn receive_messages(mut events: EventReader<RabbitMqMessage>) {
    for msg in events.read() {
        info!("📨 Received from RabbitMQ:");
        info!("   Payload: {}", msg.payload);
        
        // Try to parse as our custom message type
        if let Ok(parsed) = serde_json::from_str::<MyMessage>(&msg.payload) {
            info!("   Parsed message: {:?}", parsed);
        }
    }
}

fn send_periodic_messages(
    time: Res<Time>,
    mut counter: ResMut<MessageCounter>,
    mut events: EventWriter<SendToRabbitMq>,
) {
    counter.timer.tick(time.delta());

    if counter.timer.just_finished() {
        counter.count += 1;

        let msg = MyMessage {
            id: counter.count,
            content: format!("Bevy message #{}", counter.count),
        };

        match SendToRabbitMq::from_serializable(&msg) {
            Ok(event) => {
                info!("📤 Sending to RabbitMQ: {:?}", msg);
                events.send(event);
            }
            Err(e) => error!("Failed to create message: {}", e),
        }
    }
}
