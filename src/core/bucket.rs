// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::{types::Bucket, Client};

pub async fn list_buckets(client: &Client) -> Result<Option<Vec<Bucket>>, Error> {
    Ok(client
        .list_buckets()
        .send()
        .await
        .map_err(|err| Error::sdk(err))?
        .buckets)
}

pub async fn create_bucket(client: &Client, bucket_name: &str) -> Result<(), Error> {
    client
        .create_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(())
}

pub async fn bucket_exists(client: &Client, bucket_name: &str) -> Result<bool, Error> {
    if let Some(buckets) = list_buckets(client).await? {
        if buckets
            .iter()
            .any(|bucket| bucket.name.as_ref().unwrap() == bucket_name)
        {
            return Ok(true);
        }
    }

    Ok(false)
}
