use rust_pubsub::models::publisher_server::{Publisher as GrpcPublisher, PublisherServer};
use rust_pubsub::models::subscriber_server::{Subscriber as GrpcSubscriber, SubscriberServer};
use rust_pubsub::models::{
    PublishRequest, PublishResponse, PubsubMessage, StreamingPullRequest, StreamingPullResponse,
    subscriber_client::SubscriberClient, publisher_client::PublisherClient, Topic, UpdateTopicRequest,
    GetTopicRequest, ListTopicsRequest, ListTopicsResponse, ListTopicSubscriptionsRequest, ListTopicSubscriptionsResponse,
    ListTopicSnapshotsRequest, ListTopicSnapshotsResponse, DeleteTopicRequest, DetachSubscriptionRequest, DetachSubscriptionResponse
};
use rust_pubsub::publisher::{Publisher, BatchingSettings};
use rust_pubsub::subscriber::Subscriber;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Default)]
pub struct MockPublisher {
    pub message_count: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl GrpcPublisher for MockPublisher {
    async fn create_topic(&self, _request: Request<Topic>) -> Result<Response<Topic>, Status> { unimplemented!() }
    async fn update_topic(&self, _request: Request<UpdateTopicRequest>) -> Result<Response<Topic>, Status> { unimplemented!() }
    async fn get_topic(&self, _request: Request<GetTopicRequest>) -> Result<Response<Topic>, Status> { unimplemented!() }
    async fn list_topics(&self, _request: Request<ListTopicsRequest>) -> Result<Response<ListTopicsResponse>, Status> { unimplemented!() }
    async fn list_topic_subscriptions(&self, _request: Request<ListTopicSubscriptionsRequest>) -> Result<Response<ListTopicSubscriptionsResponse>, Status> { unimplemented!() }
    async fn list_topic_snapshots(&self, _request: Request<ListTopicSnapshotsRequest>) -> Result<Response<ListTopicSnapshotsResponse>, Status> { unimplemented!() }
    async fn delete_topic(&self, _request: Request<DeleteTopicRequest>) -> Result<Response<()>, Status> { unimplemented!() }
    async fn detach_subscription(&self, _request: Request<DetachSubscriptionRequest>) -> Result<Response<DetachSubscriptionResponse>, Status> { unimplemented!() }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();
        let count = req.messages.len();
        self.message_count.fetch_add(count, Ordering::Relaxed);
        let message_ids: Vec<String> = (0..count).map(|i| format!("msg-{}", i)).collect();
        Ok(Response::new(PublishResponse { message_ids }))
    }
}

pub struct MockSubscriber {
    pub message_count: Arc<AtomicUsize>,
    pub ack_count: Arc<AtomicUsize>,
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
        
        let msg_count = self.message_count.clone();
        let ack_count = self.ack_count.clone();

        tokio::spawn(async move {
            let mut sent = 0;
            while sent < 1_000_000 {
                let msgs = (0..1000).map(|i| rust_pubsub::models::ReceivedMessage {
                    ack_id: format!("ack-{}", sent + i),
                    message: Some(PubsubMessage {
                        data: vec![0; 1024],
                        ..Default::default()
                    }),
                    delivery_attempt: 1,
                }).collect::<Vec<_>>();
                
                if tx.send(Ok(StreamingPullResponse { 
                    received_messages: msgs, 
                    subscription_properties: None,
                    acknowledge_confirmation: None,
                    modify_ack_deadline_confirmation: None,
                })).await.is_err() {
                    break;
                }
                sent += 1000;
                msg_count.fetch_add(1000, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        tokio::spawn(async move {
            while let Ok(Some(req)) = in_stream.message().await {
                let count = req.ack_ids.len() + req.modify_deadline_ack_ids.len();
                ack_count.fetch_add(count, Ordering::Relaxed);
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

async fn start_mock_server() -> (tokio::task::JoinHandle<()>, String, Arc<AtomicUsize>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let msg_count_pub = Arc::new(AtomicUsize::new(0));
    let mock_publisher = MockPublisher { message_count: msg_count_pub.clone() };
    
    let msg_count_sub = Arc::new(AtomicUsize::new(0));
    let ack_count_sub = Arc::new(AtomicUsize::new(0));
    let mock_subscriber = MockSubscriber {
        message_count: msg_count_sub.clone(),
        ack_count: ack_count_sub.clone(),
    };

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(PublisherServer::new(mock_publisher))
            .add_service(SubscriberServer::new(mock_subscriber))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(tokio::net::TcpListener::from_std(listener).unwrap()))
            .await
            .unwrap();
    });

    (server_handle, addr_str, msg_count_pub, msg_count_sub, ack_count_sub)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn benchmark_publisher_1m_msgs_10k_clients() {
    let (_server, addr, msg_count, _, _) = start_mock_server().await;

    let pool = rust_pubsub::connection::ConnectionPool::new(16, Some(&addr)).await.unwrap();

    let batching_settings = BatchingSettings {
        element_count_threshold: 1000,
        request_byte_threshold: 10_000_000,
        delay_threshold: Duration::from_millis(10),
    };

    let publisher = Arc::new(Publisher::new("test-topic".into(), pool, batching_settings));

    let start = Instant::now();
    let mut handles = Vec::new();
    let total_msgs = 1_000_000;
    let msgs_per_task = total_msgs / 10000;

    for i in 0..10000 {
        let pub_clone = publisher.clone();
        let ordering_key = if i % 2 == 0 { format!("key-{}", i % 100) } else { String::new() };
        handles.push(tokio::spawn(async move {
            for _ in 0..msgs_per_task {
                let msg = PubsubMessage {
                    data: vec![0; 1024],
                    ordering_key: ordering_key.clone(),
                    ..Default::default()
                };
                let _ = pub_clone.publish(msg).await;
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    assert_eq!(msg_count.load(Ordering::Relaxed), total_msgs);
    println!("Publisher benchmark: {} msgs in {:?}, throughput: {} msgs/sec", total_msgs, elapsed, (total_msgs as f64 / elapsed.as_secs_f64()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn benchmark_subscriber_1m_msgs() {
    let (_server, addr, _, _, ack_count) = start_mock_server().await;

    let pool = rust_pubsub::connection::ConnectionPool::new(16, Some(&addr)).await.unwrap();

    let mut subscriber = Subscriber::new("test-sub".into(), pool);
    
    let start = Instant::now();
    let mut stream = subscriber.subscribe().await.unwrap();

    let mut received = 0;
    while let Some(res) = tokio_stream::StreamExt::next(&mut stream).await {
        if let Ok((_msg, ack_reply)) = res {
            let _ = ack_reply.ack().await;
            received += 1;
            if received >= 1_000_000 {
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Subscriber benchmark: {} msgs received in {:?}, throughput: {} msgs/sec", received, elapsed, (received as f64 / elapsed.as_secs_f64()));
    
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("Total ACKs sent: {}", ack_count.load(Ordering::Relaxed));
}