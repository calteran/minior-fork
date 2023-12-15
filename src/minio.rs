// Authors: Robert Lopez

use crate::{
    core::{bucket::*, get::*, upload::upload_object::*},
    error::Error,
};
use aws_sdk_s3::{types::Bucket, Client};
use std::sync::Arc;
use tokio::io::{AsyncBufRead, AsyncRead};

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

    pub async fn list_buckets(&self) -> Result<Option<Vec<Bucket>>, Error> {
        list_buckets(&self.client).await
    }

    pub async fn create_bucket(&self, bucket_name: &str) -> Result<Option<()>, Error> {
        create_bucket(&self.client, bucket_name).await
    }

    pub async fn delete_bucket(&self, bucket_name: &str) -> Result<Option<()>, Error> {
        delete_bucket(&self.client, bucket_name).await
    }

    pub async fn get_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<impl AsyncBufRead, Error> {
        get_object(&self.client, bucket_name, object_name).await
    }

    pub async fn upload_object<S>(
        &self,
        bucket_name: &str,
        object_name: &str,
        stream: S,
        buffer_size: Option<usize>,
        data_part_size: Option<usize>,
    ) -> Result<(), Error>
    where
        S: AsyncRead + Unpin,
    {
        upload_object(
            &self.client,
            bucket_name,
            object_name,
            stream,
            buffer_size,
            data_part_size,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use tokio::fs::*;

    #[tokio::test]
    async fn test_new() {
        let mut rng = rand::thread_rng();

        let url = "http://127.0.0.1:9000";

        let minio = Minio::new(url).await;

        let mut data = vec![];

        for _ in 0..10_000_000 {
            data.push(rng.gen_range(0..255) as u8);
        }

        let file_path = "./test.txt";
        tokio::fs::write(file_path, data).await.unwrap();

        let file = File::open(file_path).await.unwrap();

        let bucket_name = "test";

        minio.create_bucket(bucket_name).await.unwrap();

        minio
            .upload_object(bucket_name, "test.txt", file, None, None)
            .await
            .unwrap();
    }
}
