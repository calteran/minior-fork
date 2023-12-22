// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::{types::Bucket, Client};

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
