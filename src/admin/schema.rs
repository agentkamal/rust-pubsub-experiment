use crate::error::{Error, Result};
use crate::models::{
    schema_service_client::SchemaServiceClient, CreateSchemaRequest, DeleteSchemaRequest,
    GetSchemaRequest, ListSchemasRequest, Schema, SchemaView,
};
use tokio_stream::Stream;
use tonic::transport::Channel;

pub struct SchemaAdminClient {
    client: SchemaServiceClient<Channel>,
}

impl SchemaAdminClient {
    pub fn new(client: SchemaServiceClient<Channel>) -> Self {
        Self { client }
    }

    pub async fn create_schema(&self, request: CreateSchemaRequest) -> Result<Schema> {
        let mut client = self.client.clone();
        let response = client.create_schema(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_schema(&self, name: impl Into<String>) -> Result<Schema> {
        let mut client = self.client.clone();
        let request = GetSchemaRequest {
            name: name.into(),
            view: SchemaView::Basic as i32,
        };
        let response = client.get_schema(request).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_schema(&self, name: impl Into<String>) -> Result<()> {
        let mut client = self.client.clone();
        let request = DeleteSchemaRequest { name: name.into() };
        client.delete_schema(request).await?;
        Ok(())
    }

    pub fn list_schemas(
        &self,
        parent: impl Into<String>,
    ) -> impl Stream<Item = Result<Schema>> {
        let mut client = self.client.clone();
        let parent = parent.into();

        async_stream::try_stream! {
            let mut page_token = String::new();
            loop {
                let request = ListSchemasRequest {
                    parent: parent.clone(),
                    view: SchemaView::Basic as i32,
                    page_size: 0,
                    page_token: page_token.clone(),
                };
                let response = client.list_schemas(request).await.map_err(Error::Grpc)?.into_inner();
                
                for schema in response.schemas {
                    yield schema;
                }
                
                page_token = response.next_page_token;
                if page_token.is_empty() {
                    break;
                }
            }
        }
    }
}