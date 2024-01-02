// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::{
    types::{Bucket, Object},
    Client,
};

use super::delete::delete_object;

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

/// Deletes all objects in a bucket by `bucket_name`.
///
/// Returns `false` if the bucket did not exist
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let bucket_objects_deleted: bool = delete_bucket_objects(&client, "sharks").await?;
/// ```
pub async fn delete_bucket_objects(client: &Client, bucket_name: &str) -> Result<bool, Error> {
    if !bucket_exists(client, bucket_name).await? {
        return Ok(false);
    }

    for object in list_bucket_objects(client, bucket_name).await? {
        delete_object(
            client,
            bucket_name,
            object.key().ok_or(Error::internal(&format!(
                "Object: {:?} from bucket: {} has no key",
                object, bucket_name
            )))?,
        )
        .await?;
    }

    Ok(true)
}

/// Deletes a bucket by `bucket_name`.
///
/// If `delete_objects` is `true`, will also attempt to delete
/// all objects in the bucket.
///
/// Returns `false` if the bucket did not exist
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let bucket_deleted: bool = delete_bucket(&client, "sharks", false).await?;
/// ```
pub async fn delete_bucket(
    client: &Client,
    bucket_name: &str,
    delete_objects: bool,
) -> Result<bool, Error> {
    if !bucket_exists(client, bucket_name).await? {
        return Ok(false);
    }

    if delete_objects {
        delete_bucket_objects(client, bucket_name).await?;
    }

    client
        .delete_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(true)
}

/// Lists `Object`s present in the given bucket by `bucket_name`
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let bucket_objects: Vec<Object> = list_bucket_objects(&client, "sharks").await?;
/// ```
pub async fn list_bucket_objects(client: &Client, bucket_name: &str) -> Result<Vec<Object>, Error> {
    let response = client
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(response.contents().to_owned())
}
