// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::{
    presigning::{PresignedRequest, PresigningConfig},
    Client,
};
use std::time::Duration;

/// Obtain a `PresignedRequest` for a object upload
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let presigned_request: PresignedRequest = upload_presigned(
///     &client,
///     "bucket_name",
///     "object_name",
///     1_337,
/// ).await?;
/// ```
pub async fn upload_object_presigned(
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
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .presigned(presigning_config)
        .await
        .map_err(|err| Error::sdk(err))?)
}
