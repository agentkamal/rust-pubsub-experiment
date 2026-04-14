use rust_pubsub::models::subscriber_server::{Subscriber as GrpcSubscriber, SubscriberServer};
use rust_pubsub::models::{
    PubsubMessage, ReceivedMessage, StreamingPullRequest, StreamingPullResponse,
};
use rust_pubsub::subscriber::Subscriber;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct EdgeCaseMockSubscriber {
    pub streaming_pull_requests: Arc<Mutex<Vec<StreamingPullRequest>>>,
    pub push_messages_tx: Arc<Mutex<Option<mpsc::Sender<Result<StreamingPullResponse, Status>>>>>,
    pub drop_connection: Arc<Mutex<bool>>,
}

#[tonic::async_trait]
impl GrpcSubscriber for EdgeCaseMockSubscriber {
    async fn create_subscription(
        &self,
        _request: Request<rust_pubsub::models::Subscription>,
    ) -> Result<Response<rust_pubsub::models::Subscription>, Status> {
        unimplemented!()
    }
    async fn get_subscription(
        &self,
        _request: Request<rust_pubsub::models::GetSubscriptionRequest>,
    ) -> Result<Response<rust_pubsub::models::Subscription>, Status> {
        unimplemented!()
    }
    async fn update_subscription(
        &self,
        _request: Request<rust_pubsub::models::UpdateSubscriptionRequest>,
    ) -> Result<Response<rust_pubsub::models::Subscription>, Status> {
        unimplemented!()
    }
    async fn list_subscriptions(
        &self,
        _request: Request<rust_pubsub::models::ListSubscriptionsRequest>,
    ) -> Result<Response<rust_pubsub::models::ListSubscriptionsResponse>, Status> {
        unimplemented!()
    }
    async fn delete_subscription(
        &self,
        _request: Request<rust_pubsub::models::DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn modify_ack_deadline(
        &self,
        _request: Request<rust_pubsub::models::ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn acknowledge(
        &self,
        _request: Request<rust_pubsub::models::AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn pull(
        &self,
        _request: Request<rust_pubsub::models::PullRequest>,
    ) -> Result<Response<rust_pubsub::models::PullResponse>, Status> {
        unimplemented!()
    }

    type StreamingPullStream = ReceiverStream<Result<StreamingPullResponse, Status>>;

    async fn streaming_pull(
        &self,
        request: Request<Streaming<StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(100);
        *self.push_messages_tx.lock().unwrap() = Some(tx);

        let reqs = self.streaming_pull_requests.clone();
        let drop_conn = self.drop_connection.clone();

        tokio::spawn(async move {
            while let Ok(Some(req)) = in_stream.message().await {
                if *drop_conn.lock().unwrap() {
                    break; // simulate connection drop
                }
                reqs.lock().unwrap().push(req);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn modify_push_config(
        &self,
        _request: Request<rust_pubsub::models::ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn get_snapshot(
        &self,
        _request: Request<rust_pubsub::models::GetSnapshotRequest>,
    ) -> Result<Response<rust_pubsub::models::Snapshot>, Status> {
        unimplemented!()
    }
    async fn list_snapshots(
        &self,
        _request: Request<rust_pubsub::models::ListSnapshotsRequest>,
    ) -> Result<Response<rust_pubsub::models::ListSnapshotsResponse>, Status> {
        unimplemented!()
    }
    async fn create_snapshot(
        &self,
        _request: Request<rust_pubsub::models::CreateSnapshotRequest>,
    ) -> Result<Response<rust_pubsub::models::Snapshot>, Status> {
        unimplemented!()
    }
    async fn update_snapshot(
        &self,
        _request: Request<rust_pubsub::models::UpdateSnapshotRequest>,
    ) -> Result<Response<rust_pubsub::models::Snapshot>, Status> {
        unimplemented!()
    }
    async fn delete_snapshot(
        &self,
        _request: Request<rust_pubsub::models::DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn seek(
        &self,
        _request: Request<rust_pubsub::models::SeekRequest>,
    ) -> Result<Response<rust_pubsub::models::SeekResponse>, Status> {
        unimplemented!()
    }
}

async fn start_mock_subscriber() -> (tokio::task::JoinHandle<()>, String, EdgeCaseMockSubscriber) {
    let mock_subscriber = EdgeCaseMockSubscriber {
        streaming_pull_requests: Arc::new(Mutex::new(Vec::new())),
        push_messages_tx: Arc::new(Mutex::new(None)),
        drop_connection: Arc::new(Mutex::new(false)),
    };

    let mock_clone = mock_subscriber.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(SubscriberServer::new(mock_clone))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                tokio::net::TcpListener::from_std(listener).unwrap(),
            ))
            .await
            .unwrap();
    });

    (server_handle, addr_str, mock_subscriber)
}

#[tokio::test]
async fn test_subscriber_flow_control() {
    let (_server, addr, mock_sub) = start_mock_subscriber().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let mut subscriber = Subscriber::new("test-sub".into(), pool);

    let mut stream = subscriber.subscribe().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let tx = mock_sub
        .push_messages_tx
        .lock()
        .unwrap()
        .clone()
        .expect("Client didn't connect");

    // Send 10000 messages (high volume)
    let mut msgs = Vec::new();
    for i in 0..10000 {
        msgs.push(ReceivedMessage {
            ack_id: format!("ack-{}", i),
            message: Some(PubsubMessage {
                data: vec![i as u8; 1024],
                ..Default::default()
            }),
            delivery_attempt: 1,
        });
    }

    tx.send(Ok(StreamingPullResponse {
        received_messages: msgs,
        subscription_properties: None,
        acknowledge_confirmation: None,
        modify_ack_deadline_confirmation: None,
    }))
    .await
    .unwrap();

    // We should be able to receive them without OOM or crashing,
    // and flow control should pause stream if buffer is full (hard to test without internal metrics, but basic consumption shouldn't fail)
    let mut count = 0;
    while let Some(Ok((_msg, consumer))) = stream.next().await {
        count += 1;
        let _ = consumer.ack().await;
        if count == 10000 {
            break;
        }
    }

    assert_eq!(
        count, 10000,
        "Should receive all 10000 messages under high throughput"
    );
}

#[tokio::test]
async fn test_subscriber_network_resilience_reconnect() {
    let (_server, addr, mock_sub) = start_mock_subscriber().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let mut subscriber = Subscriber::new("test-sub".into(), pool);

    let mut stream = subscriber.subscribe().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let tx1 = mock_sub
        .push_messages_tx
        .lock()
        .unwrap()
        .clone()
        .expect("Client didn't connect");

    // Send message on first connection
    tx1.send(Ok(StreamingPullResponse {
        received_messages: vec![ReceivedMessage {
            ack_id: "ack-1".into(),
            message: Some(PubsubMessage {
                data: b"m1".to_vec(),
                ..Default::default()
            }),
            delivery_attempt: 1,
        }],
        subscription_properties: None,
        acknowledge_confirmation: None,
        modify_ack_deadline_confirmation: None,
    }))
    .await
    .unwrap();

    let msg1 = stream.next().await.unwrap().unwrap();
    assert_eq!(msg1.0.data, b"m1");

    // Drop connection
    *mock_sub.drop_connection.lock().unwrap() = true;
    let _ = tx1.send(Err(Status::aborted("simulated disconnect"))).await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    *mock_sub.drop_connection.lock().unwrap() = false; // allow reconnect

    // Wait for client to reconnect
    tokio::time::sleep(Duration::from_millis(500)).await;

    let tx2 = mock_sub
        .push_messages_tx
        .lock()
        .unwrap()
        .clone()
        .expect("Client didn't reconnect");

    // Send message on new connection
    tx2.send(Ok(StreamingPullResponse {
        received_messages: vec![ReceivedMessage {
            ack_id: "ack-2".into(),
            message: Some(PubsubMessage {
                data: b"m2".to_vec(),
                ..Default::default()
            }),
            delivery_attempt: 1,
        }],
        subscription_properties: None,
        acknowledge_confirmation: None,
        modify_ack_deadline_confirmation: None,
    }))
    .await
    .unwrap();

    let msg2 = stream.next().await.unwrap().unwrap();
    assert_eq!(
        msg2.0.data, b"m2",
        "Subscriber must automatically reconnect and resume receiving messages"
    );
}

#[tokio::test]
async fn test_subscriber_schema_propagation() {
    let (_server, addr, mock_sub) = start_mock_subscriber().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let mut subscriber = Subscriber::new("test-sub".into(), pool);

    let mut stream = subscriber.subscribe().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let tx = mock_sub
        .push_messages_tx
        .lock()
        .unwrap()
        .clone()
        .expect("Client didn't connect");

    let mut attributes = HashMap::new();
    attributes.insert("schema_name".to_string(), "my-schema".to_string());
    attributes.insert("googclient_schemaencoding".to_string(), "JSON".to_string());

    tx.send(Ok(StreamingPullResponse {
        received_messages: vec![ReceivedMessage {
            ack_id: "ack-schema".into(),
            message: Some(PubsubMessage {
                data: b"{\"field\":\"value\"}".to_vec(),
                attributes,
                ..Default::default()
            }),
            delivery_attempt: 1,
        }],
        subscription_properties: None,
        acknowledge_confirmation: None,
        modify_ack_deadline_confirmation: None,
    }))
    .await
    .unwrap();

    let (msg, _consumer) = stream.next().await.unwrap().unwrap();
    assert_eq!(msg.attributes.get("schema_name").unwrap(), "my-schema");
    assert_eq!(
        msg.attributes.get("googclient_schemaencoding").unwrap(),
        "JSON"
    );
}

#[tokio::test]
async fn test_subscriber_network_timeouts() {
    let (_server, addr, mock_sub) = start_mock_subscriber().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let mut subscriber = Subscriber::new("test-sub".into(), pool);

    let mut stream = subscriber.subscribe().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let tx = mock_sub
        .push_messages_tx
        .lock()
        .unwrap()
        .clone()
        .expect("Client didn't connect");

    // Simulate network timeout by waiting a long time without sending anything,
    // then sending a deadline exceeded or letting the stream timeout.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = tx
        .send(Err(Status::deadline_exceeded("network timeout")))
        .await;

    // Wait for client to reconnect
    tokio::time::sleep(Duration::from_millis(500)).await;

    let tx2 = mock_sub
        .push_messages_tx
        .lock()
        .unwrap()
        .clone()
        .expect("Client didn't reconnect");

    // Send message on new connection
    tx2.send(Ok(StreamingPullResponse {
        received_messages: vec![ReceivedMessage {
            ack_id: "ack-timeout-retry".into(),
            message: Some(PubsubMessage {
                data: b"m-after-timeout".to_vec(),
                ..Default::default()
            }),
            delivery_attempt: 1,
        }],
        subscription_properties: None,
        acknowledge_confirmation: None,
        modify_ack_deadline_confirmation: None,
    }))
    .await
    .unwrap();

    let msg2 = stream.next().await.unwrap().unwrap();
    assert_eq!(
        msg2.0.data, b"m-after-timeout",
        "Subscriber must automatically reconnect after a network timeout"
    );
}
