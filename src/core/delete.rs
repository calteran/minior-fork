// Authors: Robert Lopez

use crate::error::Error;
use aws_sdk_s3::Client;

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
        .map_err(|err| Error::sdk(err))?;

    Ok(())
}
