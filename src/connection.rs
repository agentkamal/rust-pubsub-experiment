use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use crate::error::Result;

const PUBSUB_ENDPOINT: &str = "https://pubsub.googleapis.com";

#[derive(Clone, Debug)]
pub struct Connection {
    channel: Channel,
}

impl Connection {
    pub async fn new(endpoint: Option<&str>) -> Result<Self> {
        let endpoint_str = endpoint.unwrap_or(PUBSUB_ENDPOINT);
        let endpoint = Endpoint::from_str(endpoint_str)
            .map_err(|e| crate::error::Error::Connection(e.to_string()))?;
        
        let endpoint = if endpoint_str.starts_with("https") {
            endpoint.tls_config(ClientTlsConfig::new())
                .map_err(|e| crate::error::Error::Connection(e.to_string()))?
        } else {
            endpoint
        };

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| crate::error::Error::Connection(e.to_string()))?;

        Ok(Self { channel })
    }

    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionPool {
    channels: Arc<Vec<Channel>>,
    next: Arc<AtomicUsize>,
}

impl ConnectionPool {
    pub async fn new(pool_size: usize, endpoint: Option<&str>) -> Result<Self> {
        let mut channels = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let conn = Connection::new(endpoint).await?;
            channels.push(conn.channel());
        }
        Ok(Self {
            channels: Arc::new(channels),
            next: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn get_channel(&self) -> Channel {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        self.channels[idx].clone()
    }
}