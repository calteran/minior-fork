// Authors: Robert Lopez

use super::Minio;
use crate::{
    core::upload::{
        abort_multipart_upload, complete_multipart_upload, start_multipart_upload, upload,
        upload_part,
    },
    error::Error,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncRead, AsyncReadExt};

impl Minio {
    /// Assumes `bucket_name` exists
    ///
    /// Default buffer size is `100_000`, and cannot be
    /// lower than `4_096`
    ///
    /// Default data_part_size is `5_242_880`, and cannot
    /// be lower than that value
    pub async fn upload_object<S>(
        &self,
        bucket_name: &str,
        object_name: &str,
        mut stream: S,
        buffer_size: Option<usize>,
        data_part_size: Option<usize>,
    ) -> Result<(), Error>
    where
        S: AsyncRead + Unpin,
    {
        let bucket_name = bucket_name.to_string();
        let object_name = object_name.to_string();

        let mut buffer_size = if let Some(buffer_size) = buffer_size {
            buffer_size
        } else {
            100_000
        };

        if buffer_size < 4_096 {
            buffer_size = 4_096;
        }

        let mut data_part_size = if let Some(data_part_size) = data_part_size {
            data_part_size
        } else {
            5_242_880
        };

        if data_part_size < 5_242_880 {
            data_part_size = 5_242_880;
        }

        let mut upload_id = None;

        let mut join_handles = vec![];

        let mut buffer = vec![0; buffer_size];
        let mut data_part_buffer = vec![];
        let counter = Arc::new(AtomicUsize::from(1));

        let mut started_multipart = false;

        loop {
            let bytes_read = stream
                .read(&mut buffer[..])
                .await
                .map_err(|err| Error::StdIo(err))?;

            if bytes_read == 0 {
                if join_handles.is_empty() && data_part_buffer.len() < data_part_size {
                    upload(&self.client, &bucket_name, &object_name, data_part_buffer).await?;

                    return Ok(());
                }

                break;
            }

            data_part_buffer.extend_from_slice(&buffer[..bytes_read]);
            buffer = vec![0; buffer_size];

            if data_part_buffer.len() >= data_part_size {
                if !started_multipart {
                    upload_id = Some(
                        start_multipart_upload(&self.client, &bucket_name, &object_name).await?,
                    );

                    started_multipart = true;
                }

                if let Some(ref upload_id) = upload_id {
                    let mut bytes = vec![];
                    std::mem::swap(&mut data_part_buffer, &mut bytes);

                    let client_clone = self.client.clone();
                    let counter_clone = counter.clone();
                    let upload_id_clone = upload_id.clone();
                    let object_name_clone = object_name.clone();
                    let bucket_name_clone = bucket_name.clone();

                    let join_handle = tokio::spawn(async move {
                        let part_number = counter_clone.fetch_add(1, Ordering::SeqCst);

                        let result = upload_part(
                            &client_clone,
                            &bucket_name_clone,
                            &object_name_clone,
                            &upload_id_clone,
                            part_number,
                            bytes,
                        )
                        .await;

                        (part_number, result)
                    });

                    join_handles.push(join_handle);
                } else {
                    return Err(Error::internal("upload_id was None on multipart upload"));
                }
            }
        }

        let upload_id = upload_id.unwrap();

        let mut bytes = vec![];
        std::mem::swap(&mut data_part_buffer, &mut bytes);

        let client_clone = self.client.clone();
        let counter_clone = counter.clone();
        let upload_id_clone = upload_id.clone();
        let object_name_clone = object_name.clone();
        let bucket_name_clone = bucket_name.clone();

        let join_handle = tokio::spawn(async move {
            let part_number = counter_clone.fetch_add(1, Ordering::SeqCst);

            let result = upload_part(
                &client_clone,
                &bucket_name_clone,
                &object_name_clone,
                &upload_id_clone,
                part_number,
                bytes,
            )
            .await;

            (part_number, result)
        });

        join_handles.push(join_handle);
        let mut e_tags = vec![];

        for join_handle in join_handles {
            match join_handle.await {
                Ok((part_number, result)) => match result {
                    Ok(e_tag) => {
                        e_tags.push((e_tag, part_number));
                    }
                    Err(err) => {
                        abort_multipart_upload(
                            &self.client,
                            &bucket_name,
                            &object_name,
                            &upload_id,
                        )
                        .await?;

                        return Err(err);
                    }
                },
                Err(err) => {
                    abort_multipart_upload(&self.client, &bucket_name, &object_name, &upload_id)
                        .await?;

                    return Err(Error::JoinError(err));
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
