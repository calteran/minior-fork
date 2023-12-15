// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::{
    presigning::{PresignedRequest, PresigningConfig},
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn upload(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    bytes: Vec<u8>,
) -> Result<(), Error> {
    client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(())
}

pub async fn upload_presigned(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    presigned_expiry: Option<u64>,
) -> Result<PresignedRequest, Error> {
    let presigning_config = if let Some(expiration_seconds) = presigned_expiry {
        PresigningConfig::builder()
            .expires_in(Duration::from_secs(expiration_seconds))
            .build()
    } else {
        PresigningConfig::builder().build()
    }
    .map_err(|err| Error::sdk(err))?;

    Ok(client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .presigned(presigning_config)
        .await
        .map_err(|err| Error::sdk(err))?)
}

pub async fn start_multipart_upload(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<String, Error> {
    client
        .create_multipart_upload()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?
        .upload_id
        .ok_or(Error::internal(
            "upload_id was None for a valid multipart call",
        ))
}

pub async fn abort_multipart_upload(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    upload_id: &str,
) -> Result<(), Error> {
    client
        .abort_multipart_upload()
        .bucket(bucket_name)
        .key(object_name)
        .upload_id(upload_id)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(())
}

pub async fn upload_part(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    upload_id: &str,
    part_number: usize,
    bytes: Vec<u8>,
) -> Result<String, Error> {
    client
        .upload_part()
        .bucket(bucket_name)
        .key(object_name)
        .upload_id(upload_id)
        .part_number(part_number as i32)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .map_err(|err| Error::sdk(err))?
        .e_tag
        .ok_or(Error::internal("e_tag was None on upload_part"))
}

pub async fn complete_multipart_upload(
    client: &Client,
    e_tags: Vec<(String, usize)>,
    bucket_name: &str,
    object_name: &str,
    upload_id: &str,
) -> Result<(), Error> {
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

    client
        .complete_multipart_upload()
        .bucket(bucket_name)
        .key(object_name)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(())
}

/// Assumes `bucket_name` exists
///
/// Default buffer size is `100_000`, and cannot be
/// lower than `4_096`
///
/// Default data_part_size is `5_242_880`, and cannot
/// be lower than that value
pub async fn upload_object<S>(
    client: &Client,
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
                upload(client, &bucket_name, &object_name, data_part_buffer).await?;

                return Ok(());
            }

            break;
        }

        data_part_buffer.extend_from_slice(&buffer[..bytes_read]);
        buffer = vec![0; buffer_size];

        if data_part_buffer.len() >= data_part_size {
            if !started_multipart {
                upload_id = Some(start_multipart_upload(client, &bucket_name, &object_name).await?);

                started_multipart = true;
            }

            if let Some(ref upload_id) = upload_id {
                let mut bytes = vec![];
                std::mem::swap(&mut data_part_buffer, &mut bytes);

                let client_clone = client.clone();
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

    let client_clone = client.clone();
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
                    abort_multipart_upload(client, &bucket_name, &object_name, &upload_id).await?;

                    return Err(err);
                }
            },
            Err(err) => {
                abort_multipart_upload(client, &bucket_name, &object_name, &upload_id).await?;

                return Err(Error::JoinError(err));
            }
        }
    }

    complete_multipart_upload(client, e_tags, &bucket_name, &object_name, &upload_id).await?;

    Ok(())
}

pub async fn upload_object_presigned(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    presigned_expiry: Option<u64>,
) -> Result<PresignedRequest, Error> {
    let presigning_config = if let Some(expiration_seconds) = presigned_expiry {
        PresigningConfig::builder()
            .expires_in(Duration::from_secs(expiration_seconds))
            .build()
    } else {
        PresigningConfig::builder().build()
    }
    .map_err(|err| Error::sdk(err))?;

    Ok(client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .presigned(presigning_config)
        .await
        .map_err(|err| Error::sdk(err))?)
}
