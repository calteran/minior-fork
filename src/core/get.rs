// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use crate::error::Error;
use aws_sdk_s3::{
    error::SdkError,
    operation::get_object::GetObjectError,
    presigning::{PresignedRequest, PresigningConfig},
    Client,
};
use std::time::Duration;
use tokio::io::AsyncBufRead;

/// Returns a stream for an object by `bucket_name` and `object_name`
///
/// Returns `Ok(None)` if the object does not exist.
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let stream: Option<impl AsyncBufRead> = get_object(
///     &client,
///     "sharks",
///     "shark.jpg",
/// ).await?;
/// ```
pub async fn get_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<Option<impl AsyncBufRead>, Error> {
    match client
        .get_object()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
    {
        Ok(response) => Ok(Some(response.body.into_async_read())),
        Err(sdk_err) => match sdk_err {
            SdkError::ServiceError(ref err, ..) => match err.err() {
                GetObjectError::NoSuchKey(_) => Ok(None),
                _ => Err(Error::sdk(sdk_err)),
            },
            _ => Err(Error::sdk(sdk_err)),
        },
    }
}

/// Generates a `PresignedRequest` from a bucket by `bucket_name` and `object_name`
/// to get the object.
///
/// Returns `Ok(None)` if the object does not exist.
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let request: Option<PresignedRequest> = get_object_presigned(
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
) -> Result<Option<PresignedRequest>, Error> {
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(presigned_expiry_secs))
        .build()
        .map_err(Error::sdk)?;

    match client
        .get_object()
        .bucket(bucket_name)
        .key(object_name)
        .presigned(presigning_config)
        .await
    {
        Ok(request) => Ok(Some(request)),
        Err(sdk_err) => match sdk_err {
            SdkError::ServiceError(ref err, ..) => match err.err() {
                GetObjectError::NoSuchKey(_) => Ok(None),
                _ => Err(Error::sdk(sdk_err)),
            },

            _ => Err(Error::sdk(sdk_err)),
        },
    }
}
