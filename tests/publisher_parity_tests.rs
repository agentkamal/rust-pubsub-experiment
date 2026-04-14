use rust_pubsub::models::publisher_server::{Publisher as GrpcPublisher, PublisherServer};
use rust_pubsub::models::{
    PublishRequest, PublishResponse, PubsubMessage, Topic, UpdateTopicRequest,
    GetTopicRequest, ListTopicsRequest, ListTopicsResponse, ListTopicSubscriptionsRequest, ListTopicSubscriptionsResponse,
    ListTopicSnapshotsRequest, ListTopicSnapshotsResponse, DeleteTopicRequest, DetachSubscriptionRequest, DetachSubscriptionResponse
};
use rust_pubsub::publisher::{Publisher, BatchingSettings};
use std::sync::{Arc, Mutex};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use std::time::Duration;

#[derive(Default)]
struct MockPublisher {
    requests: Arc<Mutex<Vec<PublishRequest>>>,
    fail_key: Arc<Mutex<Option<String>>>,
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
        
        let mut fail_key = self.fail_key.lock().unwrap();
        if let Some(key) = fail_key.clone() {
            if req.messages.iter().any(|m| m.ordering_key == key) {
                *fail_key = None; // fail only once
                return Err(Status::internal("simulated ordering key failure"));
            }
        }
        
        let count = req.messages.len();
        let message_ids: Vec<String> = (0..count).map(|i| format!("msg-{}", i)).collect();
        self.requests.lock().unwrap().push(req);
        
        Ok(Response::new(PublishResponse { message_ids }))
    }
}

async fn start_mock_server() -> (tokio::task::JoinHandle<()>, String, Arc<Mutex<Vec<PublishRequest>>>, Arc<Mutex<Option<String>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let fail_key = Arc::new(Mutex::new(None));
    let mock_publisher = MockPublisher { 
        requests: requests.clone(),
        fail_key: fail_key.clone()
    };
    
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(PublisherServer::new(mock_publisher))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(tokio::net::TcpListener::from_std(listener).unwrap()))
            .await
            .unwrap();
    });

    (server_handle, addr_str, requests, fail_key)
}

#[tokio::test]
async fn test_publisher_ordering_keys_strict_ordering() {
    let (_server, addr, requests, _) = start_mock_server().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr)).await.unwrap();
    let publisher = Publisher::new("topic".into(), pool, BatchingSettings::default());
    
    let msg1 = PubsubMessage { data: b"m1".to_vec(), ordering_key: "keyA".into(), ..Default::default() };
    let msg2 = PubsubMessage { data: b"m2".to_vec(), ordering_key: "keyA".into(), ..Default::default() };
    
    let _ = publisher.publish(msg1).await;
    let _ = publisher.publish(msg2).await;
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    let reqs = requests.lock().unwrap();
    let mut actual_data = Vec::new();
    for req in reqs.iter() {
        for msg in &req.messages {
            if msg.ordering_key == "keyA" {
                actual_data.push(msg.data.clone());
            }
        }
    }
    
    assert_eq!(actual_data, vec![b"m1".to_vec(), b"m2".to_vec()]);
}

#[tokio::test]
async fn test_publisher_ordering_key_failure_blocks_subsequent() {
    let (_server, addr, _, fail_key) = start_mock_server().await;
    *fail_key.lock().unwrap() = Some("keyA".to_string());
    
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr)).await.unwrap();
    let publisher = Publisher::new("topic".into(), pool, BatchingSettings::default());
    
    let msg1 = PubsubMessage { data: b"m1".to_vec(), ordering_key: "keyA".into(), ..Default::default() };
    let msg2 = PubsubMessage { data: b"m2".to_vec(), ordering_key: "keyA".into(), ..Default::default() };
    let msg3 = PubsubMessage { data: b"m3".to_vec(), ordering_key: "keyA".into(), ..Default::default() };
    
    let res1 = publisher.publish(msg1).await;
    assert!(res1.is_err(), "First message should fail as simulated by the mock server");
    
    let res2 = publisher.publish(msg2).await;
    assert!(res2.is_err(), "Subsequent message for failed ordering key must fail without hitting server");

    publisher.resume_publish("keyA".into()).await.unwrap();
    
    let res3 = publisher.publish(msg3).await;
    assert!(res3.is_ok(), "Publish should succeed after resume_publish");
}

#[tokio::test]
async fn test_publisher_concurrent_keys_and_unordered() {
    let (_server, addr, requests, _) = start_mock_server().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr)).await.unwrap();
    let publisher = Arc::new(Publisher::new("topic".into(), pool, BatchingSettings::default()));
    
    let msg_a = PubsubMessage { data: b"A".to_vec(), ordering_key: "keyA".into(), ..Default::default() };
    let msg_b = PubsubMessage { data: b"B".to_vec(), ordering_key: "keyB".into(), ..Default::default() };
    let msg_u = PubsubMessage { data: b"U".to_vec(), ordering_key: "".into(), ..Default::default() };
    
    let pub_a = publisher.clone();
    let task_a = tokio::spawn(async move { pub_a.publish(msg_a).await });
    
    let pub_b = publisher.clone();
    let task_b = tokio::spawn(async move { pub_b.publish(msg_b).await });
    
    let pub_u = publisher.clone();
    let task_u = tokio::spawn(async move { pub_u.publish(msg_u).await });
    
    let (res_a, res_b, res_u) = tokio::join!(task_a, task_b, task_u);
    assert!(res_a.unwrap().is_ok());
    assert!(res_b.unwrap().is_ok());
    assert!(res_u.unwrap().is_ok());
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    let reqs = requests.lock().unwrap();
    
    let mut key_a_found = false;
    let mut key_b_found = false;
    let mut unordered_found = false;
    
    for req in reqs.iter() {
        for msg in &req.messages {
            if msg.ordering_key == "keyA" { key_a_found = true; }
            if msg.ordering_key == "keyB" { key_b_found = true; }
            if msg.ordering_key == "" { unordered_found = true; }
        }
    }
    
    assert!(key_a_found);
    assert!(key_b_found);
    assert!(unordered_found);
}
