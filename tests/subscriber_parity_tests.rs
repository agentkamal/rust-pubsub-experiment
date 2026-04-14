use rust_pubsub::models::subscriber_server::{Subscriber as GrpcSubscriber, SubscriberServer};
use rust_pubsub::models::{
    StreamingPullRequest, StreamingPullResponse, PubsubMessage, ReceivedMessage
};
use rust_pubsub::subscriber::Subscriber;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct MockSubscriber {
    pub streaming_pull_requests: Arc<Mutex<Vec<StreamingPullRequest>>>,
    pub push_messages_tx: Arc<Mutex<Option<mpsc::Sender<Result<StreamingPullResponse, Status>>>>>,
}

#[tonic::async_trait]
impl GrpcSubscriber for MockSubscriber {
    async fn create_subscription(&self, _request: Request<rust_pubsub::models::Subscription>) -> Result<Response<rust_pubsub::models::Subscription>, Status> { unimplemented!() }
    async fn get_subscription(&self, _request: Request<rust_pubsub::models::GetSubscriptionRequest>) -> Result<Response<rust_pubsub::models::Subscription>, Status> { unimplemented!() }
    async fn update_subscription(&self, _request: Request<rust_pubsub::models::UpdateSubscriptionRequest>) -> Result<Response<rust_pubsub::models::Subscription>, Status> { unimplemented!() }
    async fn list_subscriptions(&self, _request: Request<rust_pubsub::models::ListSubscriptionsRequest>) -> Result<Response<rust_pubsub::models::ListSubscriptionsResponse>, Status> { unimplemented!() }
    async fn delete_subscription(&self, _request: Request<rust_pubsub::models::DeleteSubscriptionRequest>) -> Result<Response<()>, Status> { unimplemented!() }
    async fn modify_ack_deadline(&self, _request: Request<rust_pubsub::models::ModifyAckDeadlineRequest>) -> Result<Response<()>, Status> { unimplemented!() }
    async fn acknowledge(&self, _request: Request<rust_pubsub::models::AcknowledgeRequest>) -> Result<Response<()>, Status> { unimplemented!() }
    async fn pull(&self, _request: Request<rust_pubsub::models::PullRequest>) -> Result<Response<rust_pubsub::models::PullResponse>, Status> { unimplemented!() }

    type StreamingPullStream = ReceiverStream<Result<StreamingPullResponse, Status>>;

    async fn streaming_pull(
        &self,
        request: Request<Streaming<StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(100);
        *self.push_messages_tx.lock().unwrap() = Some(tx);
        
        let reqs = self.streaming_pull_requests.clone();
        tokio::spawn(async move {
            while let Ok(Some(req)) = in_stream.message().await {
                reqs.lock().unwrap().push(req);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn modify_push_config(&self, _request: Request<rust_pubsub::models::ModifyPushConfigRequest>) -> Result<Response<()>, Status> { unimplemented!() }
    async fn get_snapshot(&self, _request: Request<rust_pubsub::models::GetSnapshotRequest>) -> Result<Response<rust_pubsub::models::Snapshot>, Status> { unimplemented!() }
    async fn list_snapshots(&self, _request: Request<rust_pubsub::models::ListSnapshotsRequest>) -> Result<Response<rust_pubsub::models::ListSnapshotsResponse>, Status> { unimplemented!() }
    async fn create_snapshot(&self, _request: Request<rust_pubsub::models::CreateSnapshotRequest>) -> Result<Response<rust_pubsub::models::Snapshot>, Status> { unimplemented!() }
    async fn update_snapshot(&self, _request: Request<rust_pubsub::models::UpdateSnapshotRequest>) -> Result<Response<rust_pubsub::models::Snapshot>, Status> { unimplemented!() }
    async fn delete_snapshot(&self, _request: Request<rust_pubsub::models::DeleteSnapshotRequest>) -> Result<Response<()>, Status> { unimplemented!() }
    async fn seek(&self, _request: Request<rust_pubsub::models::SeekRequest>) -> Result<Response<rust_pubsub::models::SeekResponse>, Status> { unimplemented!() }
}

async fn start_mock_subscriber() -> (tokio::task::JoinHandle<()>, String, MockSubscriber) {
    let mock_subscriber = MockSubscriber {
        streaming_pull_requests: Arc::new(Mutex::new(Vec::new())),
        push_messages_tx: Arc::new(Mutex::new(None)),
    };
    
    let mock_clone = mock_subscriber.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(SubscriberServer::new(mock_clone))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(tokio::net::TcpListener::from_std(listener).unwrap()))
            .await
            .unwrap();
    });

    (server_handle, addr_str, mock_subscriber)
}

#[tokio::test]
async fn test_subscriber_ack_modack_batching() {
    let (_server, addr, mock_sub) = start_mock_subscriber().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr)).await.unwrap();
    let mut subscriber = Subscriber::new("test-sub".into(), pool);
    
    let mut stream = subscriber.subscribe().await.unwrap();
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let tx = mock_sub.push_messages_tx.lock().unwrap().clone().expect("Client didn't connect");
    
    let mut msgs = Vec::new();
    for i in 0..100 {
        msgs.push(ReceivedMessage {
            ack_id: format!("ack-{}", i),
            message: Some(PubsubMessage { data: vec![i as u8], ..Default::default() }),
            delivery_attempt: 1,
        });
    }
    
    tx.send(Ok(StreamingPullResponse {
        received_messages: msgs,
        subscription_properties: None,
        acknowledge_confirmation: None,
        modify_ack_deadline_confirmation: None,
    })).await.unwrap();
    
    let mut consumers = Vec::new();
    for _ in 0..100 {
        if let Some(Ok((_msg, consumer))) = stream.next().await {
            consumers.push(consumer);
        }
    }
    
    for consumer in consumers {
        tokio::spawn(async move { let _ = consumer.ack().await; });
    }
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let requests = mock_sub.streaming_pull_requests.lock().unwrap();
    
    let ack_requests: Vec<_> = requests.iter().filter(|r| !r.ack_ids.is_empty()).collect();
    
    assert!(!ack_requests.is_empty(), "Expected at least one ack request");
    assert!(ack_requests.len() < 100, "Acks must be batched, got {} requests for 100 acks", ack_requests.len());
    
    let has_batched = ack_requests.iter().any(|req| req.ack_ids.len() > 1);
    assert!(has_batched, "Acks must be aggregated into batches");
}

#[tokio::test]
async fn test_subscriber_exactly_once_receipt_modack() {
    let (_server, addr, mock_sub) = start_mock_subscriber().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr)).await.unwrap();
    let mut subscriber = Subscriber::new("test-sub".into(), pool);
    
    let mut stream = subscriber.subscribe().await.unwrap();
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let tx = mock_sub.push_messages_tx.lock().unwrap().clone().expect("Client didn't connect");
    
    tx.send(Ok(StreamingPullResponse {
        received_messages: vec![ReceivedMessage {
            ack_id: "test-ack-id".into(),
            message: Some(PubsubMessage { data: b"test".to_vec(), ..Default::default() }),
            delivery_attempt: 1,
        }],
        subscription_properties: None,
        acknowledge_confirmation: None,
        modify_ack_deadline_confirmation: None,
    })).await.unwrap();
    
    let _ = stream.next().await.unwrap();
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let requests = mock_sub.streaming_pull_requests.lock().unwrap();
    
    assert!(
        requests.iter().any(|req| req.modify_deadline_ack_ids.contains(&"test-ack-id".into())),
        "A receipt ModAck must be sent immediately upon receiving the message for exactly-once delivery"
    );
}
