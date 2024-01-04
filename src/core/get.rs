// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::{
    presigning::{PresignedRequest, PresigningConfig},
    Client,
};
use std::time::Duration;
use tokio::io::AsyncBufRead;

/// Returns a stream for an object by `bucket_name` and `object_name`
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let stream: impl AsyncBufRead = get_object(
///     &client,
///     "sharks",
///     "shark.jpg",
/// ).await?;
/// ```
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

/// Generates a `PresignedRequest` from a bucket by `bucket_name` and `object_name`
/// to get the object.
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let request: PresignedRequest = get_object_presigned(
///     &client,
///     "sharks",
///     "shark.jpg",
///     3_600,
/// ).await?;
/// ```
pub async fn get_object_presigned(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    presigned_expiry_secs: u64,
) -> Result<PresignedRequest, Error> {
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(presigned_expiry_secs))
        .build()
        .map_err(|err| Error::sdk(err))?;

    Ok(client
        .get_object()
        .bucket(bucket_name)
        .key(object_name)
        .presigned(presigning_config)
        .await
        .map_err(|err| Error::sdk(err))?)
}
