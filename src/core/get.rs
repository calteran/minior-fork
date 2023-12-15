// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::{
    presigning::{PresignedRequest, PresigningConfig},
    Client,
};
use std::time::Duration;
use tokio::io::AsyncBufRead;

pub async fn get_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<impl AsyncBufRead, Error> {
    Ok(client
        .get_object()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?
        .body
        .into_async_read())
}

pub async fn get_object_presigned(
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
        .get_object()
        .bucket(bucket_name)
        .key(object_name)
        .presigned(presigning_config)
        .await
        .map_err(|err| Error::sdk(err))?)
}
