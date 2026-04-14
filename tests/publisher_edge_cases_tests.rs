use rust_pubsub::models::publisher_server::{Publisher as GrpcPublisher, PublisherServer};
use rust_pubsub::models::{
    DeleteTopicRequest, DetachSubscriptionRequest, DetachSubscriptionResponse, GetTopicRequest,
    ListTopicSnapshotsRequest, ListTopicSnapshotsResponse, ListTopicSubscriptionsRequest,
    ListTopicSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse, PublishRequest,
    PublishResponse, PubsubMessage, Topic, UpdateTopicRequest,
};
use rust_pubsub::publisher::{BatchingSettings, Publisher};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct EdgeCaseMockPublisher {
    requests: Arc<Mutex<Vec<PublishRequest>>>,
    fail_count: Arc<Mutex<usize>>,
}

#[tonic::async_trait]
impl GrpcPublisher for EdgeCaseMockPublisher {
    async fn create_topic(&self, _request: Request<Topic>) -> Result<Response<Topic>, Status> {
        unimplemented!()
    }
    async fn update_topic(
        &self,
        _request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        unimplemented!()
    }
    async fn get_topic(
        &self,
        _request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        unimplemented!()
    }
    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        unimplemented!()
    }
    async fn list_topic_subscriptions(
        &self,
        _request: Request<ListTopicSubscriptionsRequest>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        unimplemented!()
    }
    async fn list_topic_snapshots(
        &self,
        _request: Request<ListTopicSnapshotsRequest>,
    ) -> Result<Response<ListTopicSnapshotsResponse>, Status> {
        unimplemented!()
    }
    async fn delete_topic(
        &self,
        _request: Request<DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        unimplemented!()
    }
    async fn detach_subscription(
        &self,
        _request: Request<DetachSubscriptionRequest>,
    ) -> Result<Response<DetachSubscriptionResponse>, Status> {
        unimplemented!()
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();

        let mut count = self.fail_count.lock().unwrap();
        if *count > 0 {
            *count -= 1;
            return Err(Status::unavailable("simulated network failure"));
        }

        let msg_count = req.messages.len();
        let message_ids: Vec<String> = (0..msg_count).map(|i| format!("msg-{}", i)).collect();
        self.requests.lock().unwrap().push(req);

        Ok(Response::new(PublishResponse { message_ids }))
    }
}

async fn start_mock_server() -> (
    tokio::task::JoinHandle<()>,
    String,
    Arc<Mutex<Vec<PublishRequest>>>,
    Arc<Mutex<usize>>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let fail_count = Arc::new(Mutex::new(0));
    let mock_publisher = EdgeCaseMockPublisher {
        requests: requests.clone(),
        fail_count: fail_count.clone(),
    };

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(
                PublisherServer::new(mock_publisher).max_decoding_message_size(20 * 1024 * 1024),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                tokio::net::TcpListener::from_std(listener).unwrap(),
            ))
            .await
            .unwrap();
    });

    (server_handle, addr_str, requests, fail_count)
}

#[tokio::test]
async fn test_publisher_retry_backoff() {
    let (_server, addr, requests, fail_count) = start_mock_server().await;
    *fail_count.lock().unwrap() = 2; // Fail twice

    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let publisher = Publisher::new("topic".into(), pool, BatchingSettings::default());

    let msg1 = PubsubMessage {
        data: b"retry-me".to_vec(),
        ..Default::default()
    };

    // This should eventually succeed after retries
    let res = publisher.publish(msg1).await;

    // We expect it to succeed if retries are implemented properly
    // If not, it will fail, capturing the gap.
    assert!(res.is_ok(), "Publisher should retry on Unavailable status");
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn test_publisher_flow_control() {
    let (_server, addr, _requests, _) = start_mock_server().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let publisher = Arc::new(Publisher::new(
        "topic".into(),
        pool,
        BatchingSettings::default(),
    ));

    // Send many messages rapidly
    let mut handles = vec![];
    for _ in 0..10000 {
        let pub_clone = publisher.clone();
        handles.push(tokio::spawn(async move {
            let msg = PubsubMessage {
                data: vec![0; 1024],
                ..Default::default()
            };
            pub_clone.publish(msg).await
        }));
    }

    for h in handles {
        let res = h.await.unwrap();
        assert!(
            res.is_ok(),
            "Publish failed during high throughput (flow control issue?)"
        );
    }
}

#[tokio::test]
async fn test_publisher_zero_length_and_max_length_messages() {
    let (_server, addr, requests, _) = start_mock_server().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let publisher = Publisher::new("topic".into(), pool, BatchingSettings::default());

    // Zero-length
    let zero_msg = PubsubMessage {
        data: vec![],
        ..Default::default()
    };
    let res1 = publisher.publish(zero_msg).await;
    assert!(res1.is_ok());

    // Max-length (10MB)
    let max_msg = PubsubMessage {
        data: vec![1; 10 * 1024 * 1024],
        ..Default::default()
    };
    let res2 = publisher.publish(max_msg).await;
    assert!(
        res2.is_ok(),
        "Publisher should handle large (10MB) messages"
    );

    let reqs = requests.lock().unwrap();
    assert_eq!(
        reqs.len(),
        2,
        "Both zero-length and max-length messages should be sent"
    );
}

#[tokio::test]
async fn test_publisher_empty_batch() {
    // Explicit coverage for empty batches (e.g., a PublishRequest with 0 messages).
    // The grpc publisher might allow empty requests, we can test it directly using the publisher client
    use rust_pubsub::models::publisher_client::PublisherClient;
    let (_server, addr, requests, _) = start_mock_server().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();

    let mut client = PublisherClient::new(pool.get_channel());
    let req = PublishRequest {
        topic: "topic".to_string(),
        messages: vec![], // 0 messages
    };

    let res = client.publish(req).await;
    assert!(
        res.is_ok(),
        "PublishRequest with 0 messages should be accepted by the mock server"
    );

    let reqs = requests.lock().unwrap();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].messages.len(), 0);
}

#[tokio::test]
async fn test_publisher_schema_propagation() {
    let (_server, addr, requests, _) = start_mock_server().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let publisher = Publisher::new("topic".into(), pool, BatchingSettings::default());

    let mut attributes = HashMap::new();
    attributes.insert("schema_name".to_string(), "my-schema".to_string());
    attributes.insert("schema_revision_id".to_string(), "rev-123".to_string());
    attributes.insert("googclient_schemaencoding".to_string(), "JSON".to_string());

    let msg = PubsubMessage {
        data: b"{\"field\":\"value\"}".to_vec(),
        attributes,
        ..Default::default()
    };

    let res = publisher.publish(msg).await;
    assert!(res.is_ok());

    let reqs = requests.lock().unwrap();
    let received_msg = &reqs[0].messages[0];
    assert_eq!(
        received_msg.attributes.get("schema_name").unwrap(),
        "my-schema"
    );
    assert_eq!(
        received_msg
            .attributes
            .get("googclient_schemaencoding")
            .unwrap(),
        "JSON"
    );
}

#[tokio::test]
async fn test_publisher_partial_batch_failures() {
    let (_server, addr, requests, fail_count) = start_mock_server().await;
    *fail_count.lock().unwrap() = 1; // First batch fails

    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let publisher = Publisher::new("topic".into(), pool, BatchingSettings::default());

    // Simulate partial batch failure behavior (e.g. some messages in batch fail)
    let msg1 = PubsubMessage {
        data: b"m1".to_vec(),
        ..Default::default()
    };
    let msg2 = PubsubMessage {
        data: b"m2".to_vec(),
        ..Default::default()
    };

    let res1 = publisher.publish(msg1).await;
    let res2 = publisher.publish(msg2).await;

    // We expect the publisher to handle the batch failure, possibly retrying
    // the whole batch or returning errors for the affected messages.
    assert!(
        res1.is_ok() && res2.is_ok(),
        "Publisher should handle partial batch failures/retries transparently"
    );
}
