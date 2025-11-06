//! RabbitMQ connection management

use bevy::prelude::*;
use bevy::time::{Timer, TimerMode};
use lapin::{Connection, ConnectionProperties};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{RabbitMqConfig, RabbitMqError};

/// Resource that holds the RabbitMQ connection
#[derive(Resource, Clone)]
pub struct RabbitMqConnection {
    inner: Arc<RwLock<Option<Connection>>>,
}

impl RabbitMqConnection {
    /// Create a new RabbitMQ connection holder
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
        }
    }

    /// Connect to RabbitMQ using the provided configuration
    pub async fn connect(&self, config: &RabbitMqConfig) -> Result<(), RabbitMqError> {
        info!("Connecting to RabbitMQ at {}", config.uri);

        let connection = Connection::connect(&config.uri, ConnectionProperties::default())
            .await
            .map_err(|e| RabbitMqError::ConnectionError(e.to_string()))?;

        info!("Successfully connected to RabbitMQ");

        let mut inner = self.inner.write().await;
        *inner = Some(connection);

        Ok(())
    }

    /// Get connection status and create a new channel if connected
    pub async fn create_channel(&self) -> Result<lapin::Channel, RabbitMqError> {
        let inner = self.inner.read().await;
        match &*inner {
            Some(conn) => conn
                .create_channel()
                .await
                .map_err(|e| RabbitMqError::ChannelError(e.to_string())),
            None => Err(RabbitMqError::ConnectionError("Not connected".to_string())),
        }
    }

    /// Check if the connection is established
    pub async fn is_connected(&self) -> bool {
        let inner = self.inner.read().await;
        match &*inner {
            Some(conn) => conn.status().connected(),
            None => false,
        }
    }

    /// Disconnect from RabbitMQ
    pub async fn disconnect(&self) {
        let mut inner = self.inner.write().await;
        if let Some(conn) = inner.take() {
            info!("Disconnecting from RabbitMQ");
            let _ = conn.close(200, "Shutdown").await;
        }
    }
}

impl Default for RabbitMqConnection {
    fn default() -> Self {
        Self::new()
    }
}

/// System that initializes the RabbitMQ connection
pub fn initialize_connection(mut commands: Commands, config: Res<RabbitMqConfig>) {
    let connection = RabbitMqConnection::new();
    let config_clone = config.clone();
    let connection_clone = connection.clone();

    // Spawn async task to connect
    tokio::spawn(async move {
        match connection_clone.connect(&config_clone).await {
            Ok(_) => info!("RabbitMQ connection initialized"),
            Err(e) => error!("Failed to initialize RabbitMQ connection: {}", e),
        }
    });

    commands.insert_resource(connection);
}

/// Resource to track connection status checking
#[derive(Resource)]
pub struct ConnectionStatusTimer {
    timer: Timer,
}

impl Default for ConnectionStatusTimer {
    fn default() -> Self {
        Self {
            timer: Timer::from_seconds(5.0, TimerMode::Repeating),
        }
    }
}

/// System that periodically checks connection status
pub fn check_connection_status(
    connection: Res<RabbitMqConnection>,
    mut timer: ResMut<ConnectionStatusTimer>,
    time: Res<Time>,
) {
    timer.timer.tick(time.delta());

    if timer.timer.just_finished() {
        let connection_clone = connection.clone();
        tokio::spawn(async move {
            if !connection_clone.is_connected().await {
                warn!("RabbitMQ connection lost");
            }
        });
    }
}
