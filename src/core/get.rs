// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::Client;
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
