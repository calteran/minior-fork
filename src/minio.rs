// Authors: Robert Lopez

use aws_sdk_s3::{
    operation::create_bucket::CreateBucketOutput,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncRead, AsyncReadExt};

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

    pub async fn create_bucket(
        &self,
        name: &str,
    ) -> Result<Option<CreateBucketOutput>, Box<dyn std::error::Error>> {
        let buckets = self.client.list_buckets().send().await?.buckets.unwrap();

        if buckets
            .iter()
            .any(|bucket| bucket.name.as_ref().unwrap() == name)
        {
            return Ok(None);
        }

        Ok(Some(self.client.create_bucket().bucket(name).send().await?))
    }

    pub async fn upload_object<S>(
        &self,
        bucket_name: &str,
        object_name: &str,
        mut stream: S,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        S: AsyncRead + Unpin,
    {
        let bucket_name = bucket_name.to_string();
        let object_name = object_name.to_string();

        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(&bucket_name)
            .key(&object_name)
            .send()
            .await?
            .upload_id
            .unwrap();

        let mut futures = vec![];

        let mut buffer = vec![0; 5_242_880];
        let counter = Arc::new(AtomicUsize::from(1));

        loop {
            let bytes_read = stream.read(&mut buffer[..]).await?;

            if bytes_read == 0 {
                break;
            }

            if futures.is_empty() && bytes_read < 5_242_880 {
                self.client
                    .abort_multipart_upload()
                    .bucket(&bucket_name)
                    .key(&object_name)
                    .upload_id(&upload_id)
                    .send()
                    .await?;

                self.client
                    .put_object()
                    .bucket(&bucket_name)
                    .key(&object_name)
                    .body(ByteStream::from(buffer[..bytes_read].to_vec()))
                    .send()
                    .await?;

                return Ok(());
            }

            let bytes = (&buffer[..bytes_read]).to_vec();
            let client_clone = self.client.clone();
            let counter_clone = counter.clone();
            let upload_id_clone = upload_id.clone();
            let object_name_clone = object_name.clone();
            let bucket_name_clone = bucket_name.clone();

            let future = tokio::spawn(async move {
                let part_number = counter_clone.fetch_add(1, Ordering::SeqCst);

                (
                    part_number,
                    client_clone
                        .upload_part()
                        .bucket(bucket_name_clone)
                        .key(object_name_clone)
                        .upload_id(upload_id_clone)
                        .part_number(part_number as i32)
                        .body(ByteStream::from(bytes))
                        .send()
                        .await,
                )
            });

            futures.push(future);
            buffer = vec![0; 5_242_880];
        }

        let mut e_tags = vec![];

        for future in futures {
            match future.await {
                Ok((part_number, result)) => match result {
                    Ok(upload_part_output) => {
                        e_tags.push((upload_part_output.e_tag.unwrap(), part_number));
                    }
                    Err(err) => {
                        println!("{:?}", err);
                        // Todo: Cleanup
                    }
                },
                Err(err) => {
                    println!("{:?}", err);
                    // Todo: Cleanup
                }
            }
        }

        let completed_parts = e_tags
            .into_iter()
            .map(|(e_tag, part_number)| {
                CompletedPart::builder()
                    .e_tag(e_tag)
                    .part_number(part_number as i32)
                    .build()
            })
            .collect::<Vec<CompletedPart>>();

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&bucket_name)
            .key(&object_name)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs::*;

    #[tokio::test]
    async fn test_new() {
        let url = "http://127.0.0.1:9000";

        let minio = Minio::new(url).await;

        let file = File::open("../mongor/test_data/shark-0.png").await.unwrap();

        let bucket_name = "test";

        minio.create_bucket(bucket_name).await.unwrap();

        minio
            .upload_object(bucket_name, "shark.png", file)
            .await
            .unwrap();
    }
}
