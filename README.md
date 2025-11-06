# Bevy RabbitMQ

A Bevy plugin for integrating RabbitMQ message queue with Bevy ECS.

## Features

- 🎮 **Receive RabbitMQ messages as Bevy events** - Messages from RabbitMQ queues are automatically converted to Bevy events
- 📤 **Send Bevy events to RabbitMQ** - Publish messages to RabbitMQ queues using familiar Bevy event system
- 🔌 **Automatic connection management** - Handles connection lifecycle and automatic reconnection
- ⚙️ **Flexible configuration** - Customize exchanges, routing keys, queues, and connection settings
- 🔄 **Bidirectional communication** - Support for both sending and receiving messages

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
bevy-rabbitmq = "0.1"
```

## Quick Start

### Receiving Messages

```rust
use bevy::prelude::*;
use bevy_rabbitmq::{RabbitMqPlugin, RabbitMqConfig, RabbitMqMessage};

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
        println!("Received: {}", msg.payload);
    }
}
```

### Sending Messages

```rust
use bevy::prelude::*;
use bevy_rabbitmq::{RabbitMqPlugin, RabbitMqConfig, SendToRabbitMq};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct GameEvent {
    player_id: u32,
    action: String,
}

fn send_event(mut events: EventWriter<SendToRabbitMq>) {
    let game_event = GameEvent {
        player_id: 123,
        action: "jump".to_string(),
    };
    
    if let Ok(msg) = SendToRabbitMq::from_serializable(&game_event) {
        events.send(msg);
    }
}
```

## Configuration

The `RabbitMqConfig` resource allows you to customize the connection:

```rust
RabbitMqConfig {
    // RabbitMQ connection URI
    uri: "amqp://guest:guest@localhost:5672".to_string(),
    
    // Default exchange for publishing
    default_exchange: "".to_string(),
    
    // Default routing key for publishing
    default_routing_key: "bevy_messages".to_string(),
    
    // Queue to consume messages from
    consume_queue: "bevy_messages".to_string(),
    
    // Consumer tag
    consumer_tag: "bevy_consumer".to_string(),
    
    // Auto-reconnect on connection loss
    auto_reconnect: true,
}
```

## Advanced Usage

### Custom Publish Options

You can specify custom publish options for each message:

```rust
use bevy_rabbitmq::PublishOptions;

let options = PublishOptions {
    exchange: Some("my_exchange".to_string()),
    routing_key: Some("custom.route".to_string()),
    persistent: true,
    priority: Some(5),
};

let msg = SendToRabbitMq::with_options("my message".to_string(), options);
events.send(msg);
```

### Selective Plugins

You can use only the receiver or sender functionality:

```rust
use bevy_rabbitmq::RabbitMqReceiverPlugin; // Only receive messages

// Or

use bevy_rabbitmq::RabbitMqSenderPlugin; // Only send messages
```

## Examples

The repository includes several examples:

- **basic_receiver** - Simple message receiver
- **basic_sender** - Simple message sender with periodic messages
- **bidirectional** - Both sending and receiving messages

Run an example:

```bash
cargo run --example basic_receiver
cargo run --example basic_sender
cargo run --example bidirectional
```

**Note:** You need a running RabbitMQ instance for the examples to work:

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## How It Works

1. **Connection Management**: The plugin establishes and maintains a connection to RabbitMQ
2. **Message Reception**: A background task consumes messages from the configured queue and sends them to a channel
3. **Event Conversion**: Each frame, messages from the channel are converted to `RabbitMqMessage` events
4. **Message Publishing**: When `SendToRabbitMq` events are emitted, they are queued and published to RabbitMQ

## Dependencies

- **bevy** - The Bevy game engine
- **lapin** - RabbitMQ client for Rust
- **tokio** - Async runtime
- **serde** / **serde_json** - Serialization

## License

Dual-licensed under MIT or Apache-2.0, matching Bevy's licensing.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
