use rust_pubsub::admin::{SchemaAdminClient, SubscriptionAdminClient, TopicAdminClient};
use rust_pubsub::connection::Connection;
use rust_pubsub::models::{
    publisher_client::PublisherClient, schema_service_client::SchemaServiceClient,
    subscriber_client::SubscriberClient,
};

#[tokio::test]
async fn test_topic_admin_client_instantiation() {
    let connection = Connection::new(None)
        .await
        .expect("Failed to create connection");
    let channel = connection.channel();
    let publisher_client = PublisherClient::new(channel);
    let mut topic_admin_client = TopicAdminClient::new(publisher_client);

    // This will likely fail with Unauthenticated because we don't have credentials in the Connection yet,
    // but it tests the plumbing of the gRPC request.
    let result = topic_admin_client
        .get_topic("projects/test-project/topics/test-topic".to_string())
        .await;
    assert!(
        result.is_err(),
        "Expected an error due to missing authentication"
    );
}

#[tokio::test]
async fn test_subscription_admin_client_instantiation() {
    let connection = Connection::new(None)
        .await
        .expect("Failed to create connection");
    let channel = connection.channel();
    let subscriber_client = SubscriberClient::new(channel);
    let mut subscription_admin_client = SubscriptionAdminClient::new(subscriber_client);

    let result = subscription_admin_client
        .get_subscription("projects/test-project/subscriptions/test-sub".to_string())
        .await;
    assert!(
        result.is_err(),
        "Expected an error due to missing authentication"
    );
}

#[tokio::test]
async fn test_schema_admin_client_instantiation() {
    let connection = Connection::new(None)
        .await
        .expect("Failed to create connection");
    let channel = connection.channel();
    let schema_client = SchemaServiceClient::new(channel);
    let mut schema_admin_client = SchemaAdminClient::new(schema_client);

    let result = schema_admin_client
        .get_schema("projects/test-project/schemas/test-schema".to_string())
        .await;
    assert!(
        result.is_err(),
        "Expected an error due to missing authentication"
    );
}

use rust_pubsub::models::publisher_server::{Publisher as GrpcPublisher, PublisherServer};
use rust_pubsub::models::{
    DeleteTopicRequest, DetachSubscriptionRequest, DetachSubscriptionResponse, GetTopicRequest,
    ListTopicSnapshotsRequest, ListTopicSnapshotsResponse, ListTopicSubscriptionsRequest,
    ListTopicSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse, PublishRequest,
    PublishResponse, Topic, UpdateTopicRequest,
};
use tonic::{Request, Response, Status};

#[derive(Default)]
struct MockAdminPublisher {}

#[tonic::async_trait]
impl GrpcPublisher for MockAdminPublisher {
    async fn create_topic(&self, request: Request<Topic>) -> Result<Response<Topic>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("Topic name cannot be empty"));
        }
        Ok(Response::new(req))
    }
    async fn update_topic(
        &self,
        _request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        unimplemented!()
    }
    async fn get_topic(
        &self,
        request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        let req = request.into_inner();
        if req.topic.is_empty() {
            return Err(Status::invalid_argument("Topic name cannot be empty"));
        }
        Ok(Response::new(Topic {
            name: req.topic,
            ..Default::default()
        }))
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
        _request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        unimplemented!()
    }
}

async fn start_mock_admin_server() -> (tokio::task::JoinHandle<()>, String) {
    let mock_publisher = MockAdminPublisher::default();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_str = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PublisherServer::new(mock_publisher))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                tokio::net::TcpListener::from_std(listener).unwrap(),
            ))
            .await
            .unwrap();
    });

    (server_handle, addr_str)
}

#[tokio::test]
async fn test_topic_admin_client_negative_inputs() {
    let (_server, addr) = start_mock_admin_server().await;
    let pool = rust_pubsub::connection::ConnectionPool::new(1, Some(&addr))
        .await
        .unwrap();
    let client = PublisherClient::new(pool.get_channel());
    let topic_admin_client = TopicAdminClient::new(client);

    let result = topic_admin_client.get_topic("".to_string()).await;
    assert!(result.is_err(), "Expected an error due to empty topic name");

    if let Err(rust_pubsub::error::Error::Grpc(status)) = result {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    } else {
        panic!("Expected Grpc error with InvalidArgument");
    }

    let result = topic_admin_client
        .create_topic(Topic {
            name: "".to_string(),
            ..Default::default()
        })
        .await;
    assert!(
        result.is_err(),
        "Expected an error due to empty topic name in create_topic"
    );
}
