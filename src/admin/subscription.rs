use crate::error::{Error, Result};
use crate::models::{
    subscriber_client::SubscriberClient, DeleteSubscriptionRequest, GetSubscriptionRequest,
    ListSubscriptionsRequest, Subscription,
};
use tokio_stream::Stream;
use tonic::transport::Channel;

pub struct SubscriptionAdminClient {
    client: SubscriberClient<Channel>,
}

impl SubscriptionAdminClient {
    pub fn new(client: SubscriberClient<Channel>) -> Self {
        Self { client }
    }

    pub async fn create_subscription(&self, request: Subscription) -> Result<Subscription> {
        let mut client = self.client.clone();
        let response = client.create_subscription(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_subscription(&self, subscription: impl Into<String>) -> Result<Subscription> {
        let mut client = self.client.clone();
        let request = GetSubscriptionRequest {
            subscription: subscription.into(),
        };
        let response = client.get_subscription(request).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_subscription(&self, subscription: impl Into<String>) -> Result<()> {
        let mut client = self.client.clone();
        let request = DeleteSubscriptionRequest {
            subscription: subscription.into(),
        };
        client.delete_subscription(request).await?;
        Ok(())
    }

    pub fn list_subscriptions(
        &self,
        project: impl Into<String>,
    ) -> impl Stream<Item = Result<Subscription>> {
        let mut client = self.client.clone();
        let project = project.into();

        async_stream::try_stream! {
            let mut page_token = String::new();
            loop {
                let request = ListSubscriptionsRequest {
                    project: project.clone(),
                    page_size: 0,
                    page_token: page_token.clone(),
                };
                let response = client.list_subscriptions(request).await.map_err(Error::Grpc)?.into_inner();

                for sub in response.subscriptions {
                    yield sub;
                }

                page_token = response.next_page_token;
                if page_token.is_empty() {
                    break;
                }
            }
        }
    }
}
