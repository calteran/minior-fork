// Authors: Robert Lopez

use super::{get::get_object, upload::upload_object};
use crate::error::Error;
use aws_sdk_s3::Client;

pub async fn copy_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    new_object_name: &str,
    buffer_size: Option<usize>,
    data_part_size: Option<usize>,
) -> Result<Option<()>, Error> {
    if let Some(data_stream) = get_object(client, bucket_name, object_name).await? {
        upload_object(
            client,
            bucket_name,
            new_object_name,
            data_stream,
            buffer_size,
            data_part_size,
        )
        .await?;

        return Ok(Some(()));
    }

    Ok(None)
}
