// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)

use super::util::*;
use crate::{error::Error, ETag};
use aws_sdk_s3::Client;

/// Struct to manage a multipart upload manually.
///
/// *Note*: `Minio::upload_object` will automatically manage a
/// multipart upload if the file exceeds `data_part_size` bytes,
/// but this API allows a more manual approach to the process.
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let mut upload_manager = UploadManager::new(
///     &client,
///     "sharks",
///     "shark.jpg",
/// ).await?;
///
/// let part_bytes: Vec<u8> = ...;
/// upload_manager.upload_part(&client, part_bytes).await?;
///
/// ... // Upload more parts if needed
///
/// let bytes_uploaded: usize = upload_manager.complete(&client).await?;
/// ```
pub struct UploadManager<'um> {
    pub e_tags: Vec<ETag>,
    pub upload_id: String,
    pub part_index: usize,
    pub bucket_name: &'um str,
    pub object_name: &'um str,
    pub bytes_uploaded: usize,
}

impl<'um> UploadManager<'um> {
    /// Construct a new UploadManager, starting a
    /// multipart upload.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let client: Client = ...;
    ///
    /// let mut upload_manager = UploadManager::new(
    ///     &client,
    ///     "sharks",
    ///     "shark.jpg",
    ///     3_600,
    /// ).await?;
    /// ```
    pub async fn new(
        client: &Client,
        bucket_name: &'um str,
        object_name: &'um str,
    ) -> Result<UploadManager<'um>, Error> {
        let upload_id = start_multipart_upload(client, bucket_name, object_name).await?;

        Ok(UploadManager {
            e_tags: vec![],
            upload_id,
            part_index: 0,
            bucket_name,
            object_name,
            bytes_uploaded: 0,
        })
    }

    /// Obtain the `ETag` for the provided part bytes
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let mut upload_manager: UploadManager = ...;
    ///
    /// let part_bytes: Vec<u8> = ...;
    ///
    /// upload_manager.upload_part(&client, part_bytes).await?;
    /// ```
    pub async fn upload_part(&mut self, client: &Client, bytes: Vec<u8>) -> Result<(), Error> {
        let part_number = self.part_index + 1;
        self.part_index += 1;
        self.bytes_uploaded += bytes.len();

        let e_tag = upload_part(
            client,
            self.bucket_name,
            self.object_name,
            &self.upload_id,
            part_number,
            bytes,
        )
        .await?;

        self.e_tags.push(ETag { e_tag, part_number });

        Ok(())
    }

    /// Abort the multipart upload
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let client: Client = ...;
    ///
    /// let mut upload_manager: UploadManager = ...;
    ///
    /// upload_manager.abort(&client).await?;
    /// ```
    pub async fn abort(&self, client: &Client) -> Result<(), Error> {
        abort_multipart_upload(client, self.bucket_name, self.object_name, &self.upload_id).await
    }

    /// Complete the multipart upload using the e-tags and their
    /// part numbers, that should be recorded by the consumer
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let client: Client = ...;
    ///
    /// let mut upload_manager: UploadManager = ...;
    ///
    /// let bytes_uploaded: usize = upload_manager.complete(&client).await?;
    /// ```
    pub async fn complete(&self, client: &Client) -> Result<usize, Error> {
        complete_multipart_upload(
            client,
            self.e_tags.clone(),
            self.bucket_name,
            self.object_name,
            &self.upload_id,
        )
        .await?;

        Ok(self.bytes_uploaded)
    }
}
