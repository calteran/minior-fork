// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)

use super::util::*;
use crate::{error::Error, ETag};
use aws_sdk_s3::Client;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::Semaphore,
    task::JoinHandle,
};

struct SpawnUploadFutureOptions {
    bytes: Vec<u8>,
    client: Arc<Client>,
    counter: Arc<AtomicUsize>,
    semaphore: Arc<Semaphore>,
    upload_id: String,
    object_name: String,
    bucket_name: String,
}

struct UploadPartResult {
    part_number: usize,
    e_tag_result: Result<String, Error>,
}

/// Spawn a JoinHandle uploading bytes
async fn spawn_upload_future(
    SpawnUploadFutureOptions {
        bytes,
        client,
        counter,
        semaphore,
        upload_id,
        object_name,
        bucket_name,
    }: SpawnUploadFutureOptions,
) -> Result<JoinHandle<UploadPartResult>, Error> {
    let client_clone = client.clone();
    let counter_clone = counter.clone();

    let permit = semaphore
        .clone()
        .acquire_owned()
        .await
        .map_err(|err| Error::AcquireError(err))?;

    Ok(tokio::spawn(async move {
        let part_number = counter_clone.fetch_add(1, Ordering::SeqCst);

        let result = upload_part(
            &client_clone,
            &bucket_name,
            &object_name,
            &upload_id,
            part_number,
            bytes,
        )
        .await;

        drop(permit);

        UploadPartResult {
            part_number,
            e_tag_result: result,
        }
    }))
}

/// Additional options for `upload_object` to
/// control the `buffer_size`, `data_part_size`,
/// and the `semaphore_permits`
pub struct UploadObjectAdditionalOptions {
    pub buffer_size: Option<usize>,
    pub data_part_size: Option<usize>,
    pub semaphore_permits: Option<usize>,
}

impl Default for UploadObjectAdditionalOptions {
    fn default() -> Self {
        Self {
            buffer_size: None,
            data_part_size: None,
            semaphore_permits: None,
        }
    }
}

/// Upload a object named `object_name` to the bucket named `bucket_name` via
/// a stream `S`
///
/// Default `buffer_size` is `100_000`, and cannot be lower than `4_096`
/// *(Overwrites to `4_096` if lower)*
///
/// Default `data_part_size` is `5_242_880`, and cannot be lower than `5_242_880`
/// *(Overwrites to `5_242_880` if lower)*
///
/// Default `semaphore_permits` is `4`, and cannot be lower than `1`
/// *(Overwrites to `1` if lower)*
///
/// Will automatically convert to a multipart upload if over `data_part_size`
/// bytes
///
/// Returns the total amount of bytes uploaded
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
/// let shark_image: tokio::fs::File = ...;
///
/// let bytes_uploaded: usize = upload_object(
///     &client,
///     "sharks",
///     "shark.jpg",
///     shark_image,
///     None,
///   )
///   .await?;
/// ```
pub async fn upload_object<S>(
    client: Arc<Client>,
    bucket_name: &str,
    object_name: &str,
    mut stream: S,
    UploadObjectAdditionalOptions {
        buffer_size,
        data_part_size,
        semaphore_permits,
    }: UploadObjectAdditionalOptions,
) -> Result<usize, Error>
where
    S: AsyncRead + Unpin,
{
    let bucket_name = bucket_name.to_string();
    let object_name = object_name.to_string();

    let buffer_size = buffer_size.unwrap_or(100_000).max(4_096);
    let data_part_size = data_part_size.unwrap_or(5_242_880).max(5_242_880);
    let semaphore_permits = semaphore_permits.unwrap_or(4).max(1);

    let mut upload_id = None;

    let semaphore = Arc::new(Semaphore::new(semaphore_permits));
    let mut join_handles = vec![];

    let mut buffer = vec![0; buffer_size];
    let mut data_part_buffer = vec![];
    let counter = Arc::new(AtomicUsize::from(1));

    let mut total_bytes = 0;
    let mut started_multipart = false;

    loop {
        let bytes_read = stream
            .read(&mut buffer[..])
            .await
            .map_err(|err| Error::StdIo(err))?;

        total_bytes += bytes_read;

        if bytes_read == 0 {
            if join_handles.is_empty() && data_part_buffer.len() < data_part_size {
                upload(&client, &bucket_name, &object_name, data_part_buffer).await?;

                return Ok(total_bytes);
            }

            break;
        }

        data_part_buffer.extend_from_slice(&buffer[..bytes_read]);
        buffer = vec![0; buffer_size];

        if data_part_buffer.len() >= data_part_size {
            if upload_id.is_none() {
                upload_id =
                    Some(start_multipart_upload(&client, &bucket_name, &object_name).await?);
            }

            if let Some(ref upload_id) = upload_id {
                let mut bytes = vec![];
                std::mem::swap(&mut data_part_buffer, &mut bytes);

                let join_handle = spawn_upload_future(SpawnUploadFutureOptions {
                    bytes,
                    client: client.clone(),
                    counter: counter.clone(),
                    semaphore: semaphore.clone(),
                    upload_id: upload_id.clone(),
                    object_name: object_name.clone(),
                    bucket_name: bucket_name.clone(),
                })
                .await?;

                join_handles.push(join_handle);
            } else {
                return Err(Error::internal("upload_id was None on multipart upload"));
            }
        }
    }

    let upload_id = upload_id.ok_or(Error::internal("upload_id was None on multipart upload"))?;

    let mut bytes = vec![];
    std::mem::swap(&mut data_part_buffer, &mut bytes);
    total_bytes += bytes.len();

    let join_handle = spawn_upload_future(SpawnUploadFutureOptions {
        bytes,
        client: client.clone(),
        counter,
        semaphore,
        upload_id: upload_id.clone(),
        object_name: object_name.clone(),
        bucket_name: bucket_name.clone(),
    })
    .await?;

    join_handles.push(join_handle);
    let mut e_tags = vec![];

    for join_handle in join_handles {
        match join_handle.await {
            Ok(UploadPartResult {
                part_number,
                e_tag_result,
            }) => match e_tag_result {
                Ok(e_tag) => {
                    e_tags.push(ETag { e_tag, part_number });
                }
                Err(err) => {
                    abort_multipart_upload(&client, &bucket_name, &object_name, &upload_id).await?;

                    return Err(err);
                }
            },
            Err(err) => {
                abort_multipart_upload(&client, &bucket_name, &object_name, &upload_id).await?;

                return Err(Error::JoinError(err));
            }
        }
    }

    complete_multipart_upload(&client, e_tags, &bucket_name, &object_name, &upload_id).await?;

    Ok(total_bytes)
}
