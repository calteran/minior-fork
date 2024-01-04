// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
pub mod test_client;
pub mod test_error;

use crate::{core::bucket::object_exists, test_error, Minio};
use test_error::TestError;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncReadExt},
};

pub async fn get_test_file(name: &str) -> Result<File, Box<dyn std::error::Error>> {
    let file = File::open(format!("./test_data/{}", name)).await?;

    Ok(file)
}

pub async fn get_test_file_bytes(name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let path = format!("./test_data/{}", name);

    let bytes = tokio::fs::read(path).await?;

    Ok(bytes)
}

pub async fn read_file_stream(
    mut file_stream: impl AsyncBufRead + Unpin,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buffer = vec![0; 1337];
    let mut downloaded_data = vec![];

    loop {
        let bytes_read = match file_stream.read(&mut buffer[..]).await? {
            0 => break,
            size => size,
        };

        downloaded_data.extend_from_slice(&buffer[..bytes_read]);
    }

    Ok(downloaded_data)
}

pub enum ObjectAssertions<'oa> {
    Exists,
    DoesNotExist,
    BytesEqual(Vec<u8>),
    BytesEqualPresigned(Vec<u8>, &'oa reqwest::Client),
}

pub async fn assert_object<'ao>(
    minio: &Minio,
    bucket_name: &str,
    object_name: &str,
    assertion: ObjectAssertions<'ao>,
) -> Result<(), Box<dyn std::error::Error>> {
    match assertion {
        ObjectAssertions::Exists => {
            if object_exists(&minio.client, bucket_name, object_name).await? {
                return Ok(());
            }

            test_error!(
                "Object {} in Bucket: {} does not exist",
                object_name,
                bucket_name
            );
        }
        ObjectAssertions::DoesNotExist => {
            if !object_exists(&minio.client, bucket_name, object_name).await? {
                return Ok(());
            }

            test_error!("Object {} in Bucket: {} exists", object_name, bucket_name);
        }
        ObjectAssertions::BytesEqual(bytes) => {
            if let Some(file_stream) = minio.get_object(&bucket_name, object_name).await? {
                let downloaded_bytes = read_file_stream(file_stream).await?;

                if bytes != downloaded_bytes {
                    test_error!(
                        "Object {} in Bucket: {} bytes do not match the provided ones",
                        object_name,
                        bucket_name
                    );
                }
            } else {
                test_error!(
                    "Object {} in Bucket: {} does not exist",
                    object_name,
                    bucket_name
                );
            }
        }
        ObjectAssertions::BytesEqualPresigned(bytes, reqwest_client) => {
            if let Some(presigned_request) = minio
                .get_object_presigned(&bucket_name, object_name, 1_337)
                .await?
            {
                let get_url = presigned_request.uri();

                let downloaded_bytes = reqwest_client
                    .get(get_url)
                    .send()
                    .await?
                    .bytes()
                    .await?
                    .to_vec();

                if bytes != downloaded_bytes {
                    test_error!("Uploaded bytes and retrieved bytes do not match");
                }
            } else {
                test_error!(
                    "Object {} in Bucket: {} does not exist",
                    object_name,
                    bucket_name
                );
            }
        }
    }

    Ok(())
}
