use crate::error::{Error, Result};
use crate::models::{
    publisher_client::PublisherClient, DeleteTopicRequest, GetTopicRequest, ListTopicsRequest,
    Topic,
};
use tokio_stream::Stream;
use tonic::transport::Channel;

pub struct TopicAdminClient {
    client: PublisherClient<Channel>,
}

impl TopicAdminClient {
    pub fn new(client: PublisherClient<Channel>) -> Self {
        Self { client }
    }

    pub async fn create_topic(&self, request: Topic) -> Result<Topic> {
        let mut client = self.client.clone();
        let response = client.create_topic(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_topic(&self, topic: impl Into<String>) -> Result<Topic> {
        let mut client = self.client.clone();
        let request = GetTopicRequest {
            topic: topic.into(),
        };
        let response = client.get_topic(request).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_topic(&self, topic: impl Into<String>) -> Result<()> {
        let mut client = self.client.clone();
        let request = DeleteTopicRequest {
            topic: topic.into(),
        };
        client.delete_topic(request).await?;
        Ok(())
    }

    pub fn list_topics(&self, project: impl Into<String>) -> impl Stream<Item = Result<Topic>> {
        let mut client = self.client.clone();
        let project = project.into();

        async_stream::try_stream! {
            let mut page_token = String::new();
            loop {
                let request = ListTopicsRequest {
                    project: project.clone(),
                    page_size: 0,
                    page_token: page_token.clone(),
                };
                let response = client.list_topics(request).await.map_err(Error::Grpc)?.into_inner();

                for topic in response.topics {
                    yield topic;
                }

                page_token = response.next_page_token;
                if page_token.is_empty() {
                    break;
                }
            }
        }
    }
}
