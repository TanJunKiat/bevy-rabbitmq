//! # Bevy RabbitMQ Plugin
//!
//! A Bevy plugin for integrating RabbitMQ message queue with Bevy ECS.
//! 
//! This plugin allows you to:
//! - Receive RabbitMQ messages as Bevy events
//! - Send Bevy events to RabbitMQ queues
//!
//! ## Example
//!
//! ```no_run
//! use bevy::prelude::*;
//! use bevy_rabbitmq::{RabbitMqPlugin, RabbitMqConfig, RabbitMqMessage};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Event, Debug, Clone, Serialize, Deserialize)]
//! struct MyMessage {
//!     content: String,
//! }
//!
//! fn main() {
//!     App::new()
//!         .add_plugins(MinimalPlugins)
//!         .add_plugins(RabbitMqPlugin)
//!         .insert_resource(RabbitMqConfig {
//!             uri: "amqp://guest:guest@localhost:5672".to_string(),
//!             ..Default::default()
//!         })
//!         .add_event::<RabbitMqMessage>()
//!         .run();
//! }
//! ```

mod config;
mod connection;
mod error;
mod plugin;
mod receiver;
mod sender;

pub use config::*;
pub use connection::*;
pub use error::*;
pub use plugin::*;
pub use receiver::*;
pub use sender::*;
