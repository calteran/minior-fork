// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use crate::{error::Error, ETag};
use aws_sdk_s3::{
    presigning::{PresignedRequest, PresigningConfig},
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};
use std::time::Duration;

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

pub async fn upload_part_presigned(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    upload_id: &str,
    part_number: usize,
    presigned_expiry_secs: u64,
) -> Result<PresignedRequest, Error> {
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(presigned_expiry_secs))
        .build()
        .map_err(|err| Error::sdk(err))?;

    Ok(client
        .upload_part()
        .bucket(bucket_name)
        .key(object_name)
        .upload_id(upload_id)
        .part_number(part_number as i32)
        .presigned(presigning_config)
        .await
        .map_err(|err| Error::sdk(err))?)
}

pub async fn complete_multipart_upload(
    client: &Client,
    e_tags: Vec<ETag>,
    bucket_name: &str,
    object_name: &str,
    upload_id: &str,
) -> Result<(), Error> {
    let completed_parts = e_tags
        .into_iter()
        .map(|ETag { e_tag, part_number }| {
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
