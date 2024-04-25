// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use crate::error::Error;
use aws_sdk_s3::{
    presigning::{PresignedRequest, PresigningConfig},
    Client,
};
use std::time::Duration;

/// Deletes a object from a bucket by `bucket_name` and `object_name`
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// delete_object(
///     &client,
///     "sharks",
///     "shark.jpg",
/// ).await?;
/// ```
pub async fn delete_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<(), Error> {
    client
        .delete_object()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
        .map_err(Error::sdk)?;

    Ok(())
}

/// Generates a `PresignedRequest` from a bucket by `bucket_name` and `object_name`
/// to delete the object.
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let request: PresignedRequest = delete_object_presigned(
///     &client,
///     "sharks",
///     "shark.jpg",
///     3_600,
/// ).await?;
/// ```
pub async fn delete_object_presigned(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    presigned_expiry_secs: u64,
) -> Result<PresignedRequest, Error> {
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(presigned_expiry_secs))
        .build()
        .map_err(Error::sdk)?;

    client
        .delete_object()
        .bucket(bucket_name)
        .key(object_name)
        .presigned(presigning_config)
        .await
        .map_err(Error::sdk)
}
