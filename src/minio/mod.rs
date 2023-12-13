// Authors: Robert Lopez

pub mod upload_object_impl;

use crate::{
    core::bucket::{bucket_exists, create_bucket},
    error::Error,
};
use aws_sdk_s3::Client;
use std::sync::Arc;

pub struct Minio {
    pub client: Arc<Client>,
}

impl Minio {
    pub async fn new(url: &str) -> Self {
        let config = aws_config::from_env().endpoint_url(url).load().await;
        let client = Client::new(&config);

        Self {
            client: Arc::new(client),
        }
    }

    pub async fn create_bucket(&self, bucket_name: &str) -> Result<Option<()>, Error> {
        if bucket_exists(&self.client, bucket_name).await? {
            return Ok(None);
        }

        create_bucket(&self.client, bucket_name).await?;

        Ok(Some(()))
    }
}
