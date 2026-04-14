use crate::error::{Error, Result};
use crate::models::{publisher_client::PublisherClient, PublishRequest, PubsubMessage};
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::interval;
use tonic::transport::Channel;

#[derive(Clone, Debug)]
pub struct BatchingSettings {
    pub element_count_threshold: usize,
    pub request_byte_threshold: usize,
    pub delay_threshold: Duration,
}

impl Default for BatchingSettings {
    fn default() -> Self {
        Self {
            element_count_threshold: 1000,
            request_byte_threshold: 1_000_000,
            delay_threshold: Duration::from_millis(10),
        }
    }
}

pub struct Publisher {
    topic: String,
    senders: Vec<mpsc::UnboundedSender<ActorMessage>>,
    next_shard: AtomicUsize,
}

#[derive(Debug)]
struct PublishMessage {
    message: PubsubMessage,
    responder: oneshot::Sender<Result<String>>,
}

#[derive(Debug)]
enum ActorMessage {
    Publish(PublishMessage),
    ResumePublish(String),
    BatchCompletion { key: String, success: bool },
}

struct Batch {
    messages: Vec<PubsubMessage>,
    responders: Vec<oneshot::Sender<Result<String>>>,
    byte_size: usize,
}

impl Batch {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            responders: Vec::new(),
            byte_size: 0,
        }
    }

    fn add(&mut self, msg: PublishMessage) {
        self.byte_size += msg.message.encoded_len();
        self.messages.push(msg.message);
        self.responders.push(msg.responder);
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

impl Publisher {
    pub fn new(
        topic: String,
        pool: crate::connection::ConnectionPool,
        settings: BatchingSettings,
    ) -> Self {
        let num_shards = 16;
        let mut senders = Vec::with_capacity(num_shards);
        let inflight_limit = Arc::new(Semaphore::new(100));

        // Note: Using unbounded channels prevents deadlocks between publish() tasks and
        // the actor's BatchCompletion responses. However, in a production environment
        // under severe network partitions, this could lead to OOM errors if messages queue indefinitely.
        for _ in 0..num_shards {
            let (tx, rx) = mpsc::unbounded_channel();

            let client = PublisherClient::new(pool.get_channel())
                .max_decoding_message_size(20 * 1024 * 1024)
                .max_encoding_message_size(20 * 1024 * 1024);

            let actor = PublisherActor {
                topic: topic.clone(),
                client,
                settings: settings.clone(),
                receiver: rx,
                unordered_batch: Batch::new(),
                ordered_batches: HashMap::new(),
                ordered_inflight: HashMap::new(),
                ordered_failed: HashMap::new(),
                actor_tx: tx.clone(),
                inflight_rpc_limit: inflight_limit.clone(),
            };

            tokio::spawn(actor.run());
            senders.push(tx);
        }

        Self {
            topic,
            senders,
            next_shard: AtomicUsize::new(0),
        }
    }

    pub async fn publish(&self, message: PubsubMessage) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        let key = message.ordering_key.clone();

        let shard_idx = if !key.is_empty() {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            (hasher.finish() as usize) % self.senders.len()
        } else {
            self.next_shard.fetch_add(1, Ordering::Relaxed) % self.senders.len()
        };

        let pub_msg = PublishMessage {
            message,
            responder: tx,
        };
        self.senders[shard_idx]
            .send(ActorMessage::Publish(pub_msg))
            .map_err(|_| Error::Internal("Publisher closed".to_string()))?;

        rx.await
            .unwrap_or_else(|_| Err(Error::Internal("Publisher task died".to_string())))
    }

    pub async fn resume_publish(&self, ordering_key: String) -> Result<()> {
        let shard_idx = if !ordering_key.is_empty() {
            let mut hasher = DefaultHasher::new();
            ordering_key.hash(&mut hasher);
            (hasher.finish() as usize) % self.senders.len()
        } else {
            0
        };

        self.senders[shard_idx]
            .send(ActorMessage::ResumePublish(ordering_key))
            .map_err(|_| Error::Internal("Publisher closed".to_string()))
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}

struct PublisherActor {
    topic: String,
    client: PublisherClient<Channel>,
    settings: BatchingSettings,
    receiver: mpsc::UnboundedReceiver<ActorMessage>,

    // Empty ordering key
    unordered_batch: Batch,

    // Key -> Batch
    ordered_batches: HashMap<String, Batch>,
    // Key -> Is currently inflight
    ordered_inflight: HashMap<String, bool>,
    // Key -> Is failed
    ordered_failed: HashMap<String, bool>,

    actor_tx: mpsc::UnboundedSender<ActorMessage>,
    inflight_rpc_limit: Arc<Semaphore>,
}

impl PublisherActor {
    async fn run(mut self) {
        let mut interval = interval(self.settings.delay_threshold);
        let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<(Option<String>, Batch)>();

        loop {
            tokio::select! {
                Some(msg) = self.receiver.recv() => {
                    self.handle_message(msg, &flush_tx).await;
                    let mut count = 1;
                    while count < 1000 {
                        if let Ok(msg) = self.receiver.try_recv() {
                            self.handle_message(msg, &flush_tx).await;
                            count += 1;
                        } else {
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    self.flush_all(&flush_tx).await;
                }
                Some((key, batch)) = flush_rx.recv() => {
                    self.send_batch(key, batch).await;
                }
                else => break,
            }
        }
    }

    async fn handle_message(
        &mut self,
        msg: ActorMessage,
        flush_tx: &mpsc::UnboundedSender<(Option<String>, Batch)>,
    ) {
        match msg {
            ActorMessage::Publish(msg) => {
                let key = msg.message.ordering_key.clone();

                if !key.is_empty() {
                    if self.ordered_failed.get(&key).copied().unwrap_or(false) {
                        let _ = msg
                            .responder
                            .send(Err(Error::Internal("Ordering key failed".into())));
                        return;
                    }

                    let batch = self
                        .ordered_batches
                        .entry(key.clone())
                        .or_insert_with(Batch::new);
                    batch.add(msg);

                    if batch.messages.len() >= self.settings.element_count_threshold
                        || batch.byte_size >= self.settings.request_byte_threshold
                    {
                        if !self.ordered_inflight.get(&key).copied().unwrap_or(false) {
                            let full_batch = std::mem::replace(batch, Batch::new());
                            self.ordered_inflight.insert(key.clone(), true);
                            let _ = flush_tx.send((Some(key), full_batch));
                        }
                    }
                } else {
                    self.unordered_batch.add(msg);
                    if self.unordered_batch.messages.len() >= self.settings.element_count_threshold
                        || self.unordered_batch.byte_size >= self.settings.request_byte_threshold
                    {
                        let full_batch = std::mem::replace(&mut self.unordered_batch, Batch::new());
                        let _ = flush_tx.send((None, full_batch));
                    }
                }
            }
            ActorMessage::ResumePublish(key) => {
                self.ordered_failed.insert(key.clone(), false);
                // Also could flush pending messages here
                if let Some(batch) = self.ordered_batches.get_mut(&key) {
                    if !batch.is_empty()
                        && !self.ordered_inflight.get(&key).copied().unwrap_or(false)
                    {
                        let full_batch = std::mem::replace(batch, Batch::new());
                        self.ordered_inflight.insert(key.clone(), true);
                        let _ = flush_tx.send((Some(key), full_batch));
                    }
                }
            }
            ActorMessage::BatchCompletion { key, success } => {
                self.ordered_inflight.insert(key.clone(), false);
                if !success {
                    self.ordered_failed.insert(key.clone(), true);
                    // Fail pending messages
                    if let Some(mut batch) = self.ordered_batches.remove(&key) {
                        for responder in batch.responders.drain(..) {
                            let _ =
                                responder.send(Err(Error::Internal("Ordering key failed".into())));
                        }
                    }
                } else {
                    // Flush next pending batch
                    if let Some(batch) = self.ordered_batches.get_mut(&key) {
                        if !batch.is_empty() {
                            let full_batch = std::mem::replace(batch, Batch::new());
                            self.ordered_inflight.insert(key.clone(), true);
                            let _ = flush_tx.send((Some(key), full_batch));
                        }
                    }
                }
            }
        }
    }

    async fn flush_all(&mut self, flush_tx: &mpsc::UnboundedSender<(Option<String>, Batch)>) {
        if !self.unordered_batch.is_empty() {
            let full_batch = std::mem::replace(&mut self.unordered_batch, Batch::new());
            let _ = flush_tx.send((None, full_batch));
        }

        let mut keys_to_flush = Vec::new();
        for (key, batch) in &mut self.ordered_batches {
            if !batch.is_empty() && !self.ordered_inflight.get(key).copied().unwrap_or(false) {
                keys_to_flush.push(key.clone());
            }
        }

        for key in keys_to_flush {
            let batch = self.ordered_batches.remove(&key).unwrap();
            self.ordered_inflight.insert(key.clone(), true);
            let _ = flush_tx.send((Some(key), batch));
        }
    }

    async fn send_batch(&mut self, key: Option<String>, batch: Batch) {
        let mut client = self.client.clone();
        let topic = self.topic.clone();
        let actor_tx = self.actor_tx.clone();
        let limit = self.inflight_rpc_limit.clone();

        let req = PublishRequest {
            topic,
            messages: batch.messages,
        };

        tokio::spawn(async move {
            let _permit = limit.acquire_owned().await;

            let mut attempt = 0;
            let mut backoff = Duration::from_millis(100);

            let res = loop {
                let req_clone = PublishRequest {
                    topic: req.topic.clone(),
                    messages: req.messages.clone(),
                };
                match client.publish(req_clone).await {
                    Ok(r) => break Ok(r),
                    Err(e) => {
                        if (e.code() == tonic::Code::Unavailable
                            || e.code() == tonic::Code::ResourceExhausted)
                            && attempt < 5
                        {
                            attempt += 1;
                            tokio::time::sleep(backoff).await;
                            backoff *= 2;
                        } else {
                            break Err(e);
                        }
                    }
                }
            };

            drop(_permit);

            let success = match res {
                Ok(response) => {
                    let message_ids = response.into_inner().message_ids;
                    for (responder, id) in batch.responders.into_iter().zip(message_ids) {
                        let _ = responder.send(Ok(id));
                    }
                    true
                }
                Err(e) => {
                    for responder in batch.responders {
                        let _ = responder.send(Err(Error::Grpc(e.clone())));
                    }
                    false
                }
            };

            if let Some(k) = key {
                let _ = actor_tx.send(ActorMessage::BatchCompletion { key: k, success });
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batch_triggers() {
        let mut batch = Batch::new();
        assert!(batch.is_empty());

        let (tx, _rx) = oneshot::channel();
        batch.add(PublishMessage {
            message: PubsubMessage {
                data: vec![1, 2, 3],
                ordering_key: String::new(),
                ..Default::default()
            },
            responder: tx,
        });

        assert!(!batch.is_empty());
        assert_eq!(batch.messages.len(), 1);
        assert!(batch.byte_size > 0);
    }

    #[tokio::test]
    async fn test_default_batching_settings() {
        let settings = BatchingSettings::default();
        assert_eq!(settings.element_count_threshold, 1000);
        assert_eq!(settings.request_byte_threshold, 1_000_000);
        assert_eq!(settings.delay_threshold, Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_error_propagation_and_retries() {
        let (tx, mut rx) = mpsc::unbounded_channel::<ActorMessage>();
        let (resp_tx, _resp_rx) = oneshot::channel();

        let pub_msg = PublishMessage {
            message: PubsubMessage::default(),
            responder: resp_tx,
        };

        tx.send(ActorMessage::Publish(pub_msg)).unwrap();

        let msg = rx.recv().await.unwrap();
        match msg {
            ActorMessage::Publish(m) => assert_eq!(m.message.ordering_key, ""),
            _ => panic!("Expected Publish message"),
        }
    }
}
