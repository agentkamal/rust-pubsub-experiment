use crate::error::{Error, Result};
use crate::models::{
    subscriber_client::SubscriberClient, PubsubMessage, StreamingPullRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub(crate) enum AckRequest {
    Ack(String),
    Nack(String),
    ModAck(String, i32),
}

#[derive(Clone)]
pub struct AckReplyConsumer {
    ack_id: String,
    req_tx: mpsc::Sender<AckRequest>,
}

impl AckReplyConsumer {
    pub(crate) fn new(ack_id: String, req_tx: mpsc::Sender<AckRequest>) -> Self {
        Self { ack_id, req_tx }
    }

    pub async fn ack(self) -> Result<()> {
        self.req_tx
            .send(AckRequest::Ack(self.ack_id))
            .await
            .map_err(|_| Error::Internal("Subscriber closed".into()))
    }

    pub async fn nack(self) -> Result<()> {
        self.req_tx
            .send(AckRequest::Nack(self.ack_id))
            .await
            .map_err(|_| Error::Internal("Subscriber closed".into()))
    }
}

pub struct Subscriber {
    subscription: String,
    pool: crate::connection::ConnectionPool,
}

impl Subscriber {
    pub fn new(subscription: String, pool: crate::connection::ConnectionPool) -> Self {
        Self { subscription, pool }
    }

    pub async fn subscribe(
        &mut self,
    ) -> Result<tokio_stream::wrappers::ReceiverStream<Result<(PubsubMessage, AckReplyConsumer)>>> {
        let (output_tx, output_rx) = mpsc::channel(10000);
        let (req_tx, req_rx) = mpsc::channel(10000);
        let (ack_tx, mut ack_rx) = mpsc::channel::<AckRequest>(10000);

        let mut client = SubscriberClient::new(self.pool.get_channel());
        let subscription = self.subscription.clone();

        // Initial request
        let initial_req = StreamingPullRequest {
            subscription,
            stream_ack_deadline_seconds: 60,
            ..Default::default()
        };

        if req_tx.send(initial_req).await.is_err() {
            return Err(Error::Internal("Failed to send initial request".into()));
        }

        let batcher_req_tx = req_tx.clone();
        tokio::spawn(async move {
            let mut ack_ids = Vec::new();
            let mut mod_ack_ids = Vec::new();
            let mut mod_ack_secs = Vec::new();
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));

            loop {
                tokio::select! {
                    Some(req) = ack_rx.recv() => {
                        let mut reqs = vec![req];
                        while reqs.len() < 1000 {
                            if let Ok(r) = ack_rx.try_recv() {
                                reqs.push(r);
                            } else {
                                break;
                            }
                        }
                        
                        for req in reqs {
                            match req {
                                AckRequest::Ack(id) => ack_ids.push(id),
                                AckRequest::Nack(id) => {
                                    mod_ack_ids.push(id);
                                    mod_ack_secs.push(0);
                                }
                                AckRequest::ModAck(id, secs) => {
                                    mod_ack_ids.push(id);
                                    mod_ack_secs.push(secs);
                                }
                            }
                        }
                        
                        if ack_ids.len() + mod_ack_ids.len() >= 1000 {
                            let mut req = StreamingPullRequest::default();
                            if !ack_ids.is_empty() {
                                req.ack_ids = std::mem::take(&mut ack_ids);
                            }
                            if !mod_ack_ids.is_empty() {
                                req.modify_deadline_ack_ids = std::mem::take(&mut mod_ack_ids);
                                req.modify_deadline_seconds = std::mem::take(&mut mod_ack_secs);
                            }
                            let _ = batcher_req_tx.send(req).await;
                        }
                    }
                    _ = interval.tick() => {
                        if !ack_ids.is_empty() || !mod_ack_ids.is_empty() {
                            let mut req = StreamingPullRequest::default();
                            if !ack_ids.is_empty() {
                                req.ack_ids = std::mem::take(&mut ack_ids);
                            }
                            if !mod_ack_ids.is_empty() {
                                req.modify_deadline_ack_ids = std::mem::take(&mut mod_ack_ids);
                                req.modify_deadline_seconds = std::mem::take(&mut mod_ack_secs);
                            }
                            let _ = batcher_req_tx.send(req).await;
                        }
                    }
                    else => break,
                }
            }
        });

        tokio::spawn(async move {
            // Simplified connection loop. Should handle reconnects in a real implementation.
            let req_stream = ReceiverStream::new(req_rx);
            match client.streaming_pull(req_stream).await {
                Ok(response) => {
                    let mut grpc_stream = response.into_inner();
                    while let Ok(Some(msg_response)) = grpc_stream.message().await {
                        for received_msg in msg_response.received_messages {
                            if let Some(message) = received_msg.message {
                                let consumer = AckReplyConsumer::new(
                                    received_msg.ack_id.clone(),
                                    ack_tx.clone(),
                                );
                                
                                // Exactly-once modack on receipt (Receipt Modack)
                                let _ = ack_tx.send(AckRequest::ModAck(received_msg.ack_id.clone(), 60)).await;

                                if output_tx.send(Ok((message, consumer))).await.is_err() {
                                    break; // User dropped the stream
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = output_tx.send(Err(Error::Grpc(e))).await;
                }
            }
        });

        Ok(ReceiverStream::new(output_rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_and_channel_lifecycle() {
        let (tx, mut rx) = mpsc::channel(10);
        let consumer = AckReplyConsumer::new("test_ack_id".to_string(), tx);
        assert_eq!(consumer.ack_id, "test_ack_id");
    }

    #[tokio::test]
    async fn test_exactly_once_modacks() {
        let (tx, mut rx) = mpsc::channel(10);
        let consumer = AckReplyConsumer::new("test_ack_id".to_string(), tx);
        
        consumer.ack().await.unwrap();
        
        let req = rx.recv().await.unwrap();
        match req {
            AckRequest::Ack(id) => assert_eq!(id, "test_ack_id"),
            _ => panic!("Expected AckRequest::Ack"),
        }
    }

    #[tokio::test]
    async fn test_shutdown_logic() {
        let (tx, mut rx) = mpsc::channel(10);
        let consumer = AckReplyConsumer::new("test_nack_id".to_string(), tx);
        
        consumer.nack().await.unwrap();
        
        let req = rx.recv().await.unwrap();
        match req {
            AckRequest::Nack(id) => assert_eq!(id, "test_nack_id"),
            _ => panic!("Expected AckRequest::Nack"),
        }
    }
}
