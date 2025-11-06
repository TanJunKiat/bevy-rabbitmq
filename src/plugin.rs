//! Main plugin for RabbitMQ integration

use bevy::prelude::*;

use crate::{
    connection::{check_connection_status, initialize_connection},
    receiver::{receive_messages, start_consuming, RabbitMqMessage},
    sender::{send_messages, start_publishing, SendToRabbitMq},
    RabbitMqConfig,
};

/// Plugin that integrates RabbitMQ with Bevy
///
/// This plugin provides:
/// - Connection management to RabbitMQ
/// - Receiving messages from RabbitMQ as Bevy events
/// - Sending Bevy events to RabbitMQ
///
/// # Example
///
/// ```no_run
/// use bevy::prelude::*;
/// use bevy_rabbitmq::{RabbitMqPlugin, RabbitMqConfig, RabbitMqMessage};
///
/// fn main() {
///     App::new()
///         .add_plugins(MinimalPlugins)
///         .add_plugins(RabbitMqPlugin)
///         .insert_resource(RabbitMqConfig {
///             uri: "amqp://guest:guest@localhost:5672".to_string(),
///             ..Default::default()
///         })
///         .add_systems(Update, handle_messages)
///         .run();
/// }
///
/// fn handle_messages(mut events: EventReader<RabbitMqMessage>) {
///     for msg in events.read() {
///         println!("Received: {:?}", msg);
///     }
/// }
/// ```
pub struct RabbitMqPlugin;

impl Plugin for RabbitMqPlugin {
    fn build(&self, app: &mut App) {
        // Add default config if not present
        if !app.world().contains_resource::<RabbitMqConfig>() {
            app.insert_resource(RabbitMqConfig::default());
        }

        // Add events
        app.add_event::<RabbitMqMessage>()
            .add_event::<SendToRabbitMq>();

        // Add startup systems
        app.add_systems(
            Startup,
            (
                initialize_connection,
                start_consuming.after(initialize_connection),
                start_publishing.after(initialize_connection),
            ),
        );

        // Add update systems
        app.add_systems(
            Update,
            (receive_messages, send_messages, check_connection_status),
        );
    }
}

/// Plugin that only handles receiving messages from RabbitMQ
pub struct RabbitMqReceiverPlugin;

impl Plugin for RabbitMqReceiverPlugin {
    fn build(&self, app: &mut App) {
        if !app.world().contains_resource::<RabbitMqConfig>() {
            app.insert_resource(RabbitMqConfig::default());
        }

        app.add_event::<RabbitMqMessage>();

        app.add_systems(
            Startup,
            (
                initialize_connection,
                start_consuming.after(initialize_connection),
            ),
        );

        app.add_systems(Update, (receive_messages, check_connection_status));
    }
}

/// Plugin that only handles sending messages to RabbitMQ
pub struct RabbitMqSenderPlugin;

impl Plugin for RabbitMqSenderPlugin {
    fn build(&self, app: &mut App) {
        if !app.world().contains_resource::<RabbitMqConfig>() {
            app.insert_resource(RabbitMqConfig::default());
        }

        app.add_event::<SendToRabbitMq>();

        app.add_systems(
            Startup,
            (
                initialize_connection,
                start_publishing.after(initialize_connection),
            ),
        );

        app.add_systems(Update, (send_messages, check_connection_status));
    }
}
