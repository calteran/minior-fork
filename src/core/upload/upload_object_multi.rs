// Authors: Robert Lopez

use super::util::*;
use crate::{error::Error, ETag};
use aws_sdk_s3::Client;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Struct to manage a multipart upload manually.
///
/// *Note*: `Minio::upload_object` will automatically manage a
/// multipart upload if the file exceeds `5_242_880` bytes,
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
///
/// let (
///     e_tag: String,
///     part_number: usize,
/// ) = upload_manager.next_part(&client, part_bytes).await?;
///
/// let mut e_tags: Vec<ETag> = vec![];
///
/// e_tags.push(ETag { tag: e_tag, part_number, });
///
/// ... // Upload more parts if needed
///
/// upload_manager.complete(
///     &client,
///     e_tags,
/// ).await?;
/// ```
pub struct UploadManager<'um> {
    pub upload_id: String,
    pub part_index: Arc<AtomicUsize>,
    pub bucket_name: &'um str,
    pub object_name: &'um str,
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
            upload_id,
            part_index: Arc::new(AtomicUsize::new(1)),
            bucket_name,
            object_name,
        })
    }

    /// Obtain the e_tag and part number for the provided part bytes
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let mut upload_manager: UploadManager = ...;
    ///
    /// let part_bytes: Vec<u8> = ...;
    ///
    /// let (
    ///     e_tag: String,
    ///     part_number: usize,
    /// ) = upload_manager.next_part(&client, part_bytes).await?;
    /// ```
    pub async fn next_part(
        &mut self,
        client: &Client,
        bytes: Vec<u8>,
    ) -> Result<(String, usize), Error> {
        let part_number = self.part_index.fetch_add(1, Ordering::SeqCst);

        Ok((
            upload_part(
                client,
                self.bucket_name,
                self.object_name,
                &self.upload_id,
                part_number,
                bytes,
            )
            .await?,
            part_number,
        ))
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
    /// let e_tags: Vec<ETag> = ...;
    ///
    /// upload_manager.complete(&client, e_tags).await?;
    /// ```
    pub async fn complete(&self, client: &Client, e_tags: Vec<ETag>) -> Result<(), Error> {
        complete_multipart_upload(
            client,
            e_tags,
            self.bucket_name,
            self.object_name,
            &self.upload_id,
        )
        .await
    }
}
