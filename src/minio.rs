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
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};

use crate::core::upload::{
    abort_multipart_upload, complete_multipart_upload, start_multipart_upload, upload, upload_part,
};

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

    pub async fn get_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Option<impl AsyncBufRead>, Box<dyn std::error::Error>> {
        let get_result = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await?;

        Ok(Some(get_result.body.into_async_read()))
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

        let upload_id = start_multipart_upload(&self.client, &bucket_name, &object_name).await?;

        let mut futures = vec![];

        let mut buffer = vec![0; 100_000];
        let mut data_part_buffer = vec![];
        let counter = Arc::new(AtomicUsize::from(1));

        loop {
            let bytes_read = stream.read(&mut buffer[..]).await?;

            if bytes_read == 0 {
                if futures.is_empty() && data_part_buffer.len() < 5_242_880 {
                    abort_multipart_upload(&self.client, &bucket_name, &object_name, &upload_id)
                        .await?;

                    upload(&self.client, &bucket_name, &object_name, data_part_buffer).await?;

                    return Ok(());
                }

                break;
            }

            data_part_buffer.extend_from_slice(&buffer[..bytes_read]);
            buffer = vec![0; 100_000];

            fn spawn_part_future() {
                // TODO
            }

            if data_part_buffer.len() >= 5_242_880 {
                let mut bytes = vec![];
                std::mem::swap(&mut data_part_buffer, &mut bytes);

                let client_clone = self.client.clone();
                let counter_clone = counter.clone();
                let upload_id_clone = upload_id.clone();
                let object_name_clone = object_name.clone();
                let bucket_name_clone = bucket_name.clone();

                let future = tokio::spawn(async move {
                    let part_number = counter_clone.fetch_add(1, Ordering::SeqCst);

                    (
                        part_number,
                        upload_part(
                            &client_clone,
                            &bucket_name_clone,
                            &object_name_clone,
                            &upload_id_clone,
                            part_number,
                            bytes,
                        )
                        .await,
                    )
                });

                futures.push(future);
            }
        }

        let mut bytes = vec![];
        std::mem::swap(&mut data_part_buffer, &mut bytes);

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

        complete_multipart_upload(&self.client, e_tags, &bucket_name, &object_name, &upload_id)
            .await?;

        Ok(())
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

        for _ in 0..25_000_000 {
            data.push(rng.gen_range(0..255) as u8);
        }

        let file_path = "./test.txt";
        tokio::fs::write(file_path, data).await.unwrap();

        let file = File::open(file_path).await.unwrap();

        let bucket_name = "test";

        minio.create_bucket(bucket_name).await.unwrap();

        minio
            .upload_object(bucket_name, "test.txt", file)
            .await
            .unwrap();
    }
}
