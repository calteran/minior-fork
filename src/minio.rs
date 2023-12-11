// Authors: Robert Lopez

use aws_sdk_s3::{operation::create_bucket::CreateBucketOutput, Client};

pub struct Minio {
    pub client: Client,
}

impl Minio {
    pub async fn new(url: &str) -> Self {
        let config = aws_config::from_env().endpoint_url(url).load().await;
        let client = Client::new(&config);

        Self { client }
    }

    pub async fn create_bucket(
        &self,
        name: &str,
    ) -> Result<CreateBucketOutput, Box<dyn std::error::Error>> {
        Ok(self.client.create_bucket().bucket(name).send().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new() {
        let url = "http://127.0.0.1:9000";

        let minio = Minio::new(url).await;
    }
}
