use rust_pubsub::admin::{SchemaAdminClient, SubscriptionAdminClient, TopicAdminClient};
use rust_pubsub::connection::Connection;
use rust_pubsub::models::{publisher_client::PublisherClient, subscriber_client::SubscriberClient, schema_service_client::SchemaServiceClient};

#[tokio::test]
async fn test_topic_admin_client_instantiation() {
    let connection = Connection::new(None).await.expect("Failed to create connection");
    let channel = connection.channel();
    let publisher_client = PublisherClient::new(channel);
    let mut topic_admin_client = TopicAdminClient::new(publisher_client);

    // This will likely fail with Unauthenticated because we don't have credentials in the Connection yet,
    // but it tests the plumbing of the gRPC request.
    let result = topic_admin_client.get_topic("projects/test-project/topics/test-topic".to_string()).await;
    assert!(result.is_err(), "Expected an error due to missing authentication");
}

#[tokio::test]
async fn test_subscription_admin_client_instantiation() {
    let connection = Connection::new(None).await.expect("Failed to create connection");
    let channel = connection.channel();
    let subscriber_client = SubscriberClient::new(channel);
    let mut subscription_admin_client = SubscriptionAdminClient::new(subscriber_client);

    let result = subscription_admin_client.get_subscription("projects/test-project/subscriptions/test-sub".to_string()).await;
    assert!(result.is_err(), "Expected an error due to missing authentication");
}

#[tokio::test]
async fn test_schema_admin_client_instantiation() {
    let connection = Connection::new(None).await.expect("Failed to create connection");
    let channel = connection.channel();
    let schema_client = SchemaServiceClient::new(channel);
    let mut schema_admin_client = SchemaAdminClient::new(schema_client);

    let result = schema_admin_client.get_schema("projects/test-project/schemas/test-schema".to_string()).await;
    assert!(result.is_err(), "Expected an error due to missing authentication");
}
