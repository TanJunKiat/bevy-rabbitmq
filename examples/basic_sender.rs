//! Basic example demonstrating how to send Bevy events to RabbitMQ

use bevy::prelude::*;
use bevy_rabbitmq::{RabbitMqConfig, RabbitMqPlugin, SendToRabbitMq};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct GameEvent {
    event_type: String,
    player_id: u32,
    timestamp: u64,
}

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .add_plugins(RabbitMqPlugin)
        .insert_resource(RabbitMqConfig {
            uri: "amqp://guest:guest@localhost:5672".to_string(),
            default_routing_key: "bevy_messages".to_string(),
            ..Default::default()
        })
        .add_systems(Startup, setup)
        .add_systems(Update, send_periodic_messages)
        .run();
}

#[derive(Resource)]
struct MessageTimer {
    timer: Timer,
    counter: u32,
}

fn setup(mut commands: Commands) {
    commands.insert_resource(MessageTimer {
        timer: Timer::new(Duration::from_secs(2), TimerMode::Repeating),
        counter: 0,
    });
    info!("Starting message sender - will send a message every 2 seconds");
}

fn send_periodic_messages(
    time: Res<Time>,
    mut timer: ResMut<MessageTimer>,
    mut events: EventWriter<SendToRabbitMq>,
) {
    timer.timer.tick(time.delta());

    if timer.timer.just_finished() {
        timer.counter += 1;

        let game_event = GameEvent {
            event_type: "player_action".to_string(),
            player_id: timer.counter,
            timestamp: time.elapsed_seconds_f64() as u64,
        };

        match SendToRabbitMq::from_serializable(&game_event) {
            Ok(msg) => {
                info!("Sending message: {:?}", game_event);
                events.send(msg);
            }
            Err(e) => error!("Failed to serialize message: {}", e),
        }
    }
}
