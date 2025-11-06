//! Integration tests for bevy-rabbitmq

use bevy::prelude::*;
use bevy_rabbitmq::{
    PublishOptions, RabbitMqConfig, RabbitMqMessage, RabbitMqPlugin, RabbitMqReceiverPlugin,
    RabbitMqSenderPlugin, SendToRabbitMq,
};

#[test]
fn test_plugin_builds() {
    // Test that the main plugin can be added to an app
    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .add_plugins(RabbitMqPlugin);
    
    // Verify resources and events are registered
    assert!(app.world().contains_resource::<RabbitMqConfig>());
}

#[test]
fn test_receiver_plugin_builds() {
    // Test that the receiver plugin can be added to an app
    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .add_plugins(RabbitMqReceiverPlugin);
    
    assert!(app.world().contains_resource::<RabbitMqConfig>());
}

#[test]
fn test_sender_plugin_builds() {
    // Test that the sender plugin can be added to an app
    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .add_plugins(RabbitMqSenderPlugin);
    
    assert!(app.world().contains_resource::<RabbitMqConfig>());
}

#[test]
fn test_custom_config() {
    // Test that custom configuration works
    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .insert_resource(RabbitMqConfig {
            uri: "amqp://test:test@localhost:5672".to_string(),
            consume_queue: "test_queue".to_string(),
            default_routing_key: "test_key".to_string(),
            ..Default::default()
        })
        .add_plugins(RabbitMqPlugin);
    
    let config = app.world().resource::<RabbitMqConfig>();
    assert_eq!(config.uri, "amqp://test:test@localhost:5672");
    assert_eq!(config.consume_queue, "test_queue");
    assert_eq!(config.default_routing_key, "test_key");
}

#[test]
fn test_message_event_creation() {
    // Test that RabbitMqMessage can be created
    let msg = RabbitMqMessage {
        payload: "test payload".to_string(),
        routing_key: "test.route".to_string(),
        exchange: "test_exchange".to_string(),
    };
    
    assert_eq!(msg.payload, "test payload");
    assert_eq!(msg.routing_key, "test.route");
    assert_eq!(msg.exchange, "test_exchange");
}

#[test]
fn test_send_event_creation() {
    // Test SendToRabbitMq creation
    let msg = SendToRabbitMq::new("test message".to_string());
    assert_eq!(msg.payload, "test message");
    
    let options = PublishOptions {
        exchange: Some("custom".to_string()),
        routing_key: Some("custom.key".to_string()),
        persistent: false,
        priority: Some(3),
    };
    
    let msg_with_options = SendToRabbitMq::with_options("test".to_string(), options.clone());
    assert_eq!(msg_with_options.payload, "test");
    assert_eq!(msg_with_options.options.exchange, Some("custom".to_string()));
}

#[test]
fn test_send_event_from_serializable() {
    use serde::{Deserialize, Serialize};
    
    #[derive(Serialize, Deserialize)]
    struct TestData {
        id: u32,
        name: String,
    }
    
    let data = TestData {
        id: 42,
        name: "test".to_string(),
    };
    
    let msg = SendToRabbitMq::from_serializable(&data).unwrap();
    assert!(msg.payload.contains("42"));
    assert!(msg.payload.contains("test"));
}
