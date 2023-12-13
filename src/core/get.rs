// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::Client;
use tokio::io::AsyncBufRead;

pub async fn get_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<Option<impl AsyncBufRead>, Error> {
    let get_result = client
        .get_object()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
        .map_err(|err| Error::sdk(err))?;

    Ok(Some(get_result.body.into_async_read()))
}
