// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::{types::Bucket, Client};

/// Returns a vector of `Bucket`s from the client
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// for bucket in list_buckets(&client).await? {
///     ...
/// }
/// ```
pub async fn list_buckets(client: &Client) -> Result<Vec<Bucket>, Error> {
    if let Some(buckets) = client
        .list_buckets()
        .send()
        .await
        .map_err(|err| Error::sdk(err))?
        .buckets
    {
        Ok(buckets)
    } else {
        Ok(vec![])
    }
}

/// Returns true if a bucket by `bucket_name` exists
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// if bucket_exists(&client, "sharks").await? {
///     ...
/// }
/// ```
pub async fn bucket_exists(client: &Client, bucket_name: &str) -> Result<bool, Error> {
    if list_buckets(client)
        .await?
        .iter()
        .any(|bucket| bucket.name.as_ref().unwrap() == bucket_name)
    {
        return Ok(true);
    }

    Ok(false)
}

/// Creates a new bucket named `bucket_name`
///
/// Returns `false` if bucket already existed
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let bucket_created: bool = create_bucket(&client, "sharks").await?;
/// ```
pub async fn create_bucket(client: &Client, bucket_name: &str) -> Result<bool, Error> {
    if bucket_exists(client, bucket_name).await? {
        return Ok(false);
    }

    client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(true)
}

/// Deletes a bucket by `bucket_name`
///
/// Returns `false` if the bucket did not exist
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let bucket_deleted: bool = delete_bucket(&client, "sharks").await?;
/// ```
pub async fn delete_bucket(client: &Client, bucket_name: &str) -> Result<bool, Error> {
    if bucket_exists(client, bucket_name).await? {
        return Ok(false);
    }

    client
        .delete_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(true)
}
