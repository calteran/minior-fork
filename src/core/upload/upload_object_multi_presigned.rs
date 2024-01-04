// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use super::util::*;
use crate::{error::Error, ETag};
use aws_sdk_s3::{presigning::PresignedRequest, Client};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Struct to manage a presigned multipart upload
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// let mut upload_manager = PresignedUploadManager::new(
///     &client,
///     "sharks",
///     "shark.jpg",
/// ).await?;
///
/// let (
///     part_request: PresignedRequest,
///     part_number: usize,
/// ) = upload_manager.next_part(&client, 1_337).await?;
///
/// let mut e_tags: Vec<ETag> = vec![];
///
/// let e_tag: String = ...; // Obtain from client
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
pub struct PresignedUploadManager<'pum> {
    pub upload_id: String,
    pub part_index: Arc<AtomicUsize>,
    pub bucket_name: &'pum str,
    pub object_name: &'pum str,
}

impl<'pum> PresignedUploadManager<'pum> {
    /// Construct a new PresignedUploadManager, starting a
    /// multipart upload.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let client: Client = ...;
    ///
    /// let mut upload_manager = PresignedUploadManager::new(
    ///     &client,
    ///     "sharks",
    ///     "shark.jpg",
    ///     3_600,
    /// ).await?;
    /// ```
    pub async fn new(
        client: &Client,
        bucket_name: &'pum str,
        object_name: &'pum str,
    ) -> Result<PresignedUploadManager<'pum>, Error> {
        let upload_id = start_multipart_upload(client, bucket_name, object_name).await?;

        Ok(PresignedUploadManager {
            upload_id,
            part_index: Arc::new(AtomicUsize::new(1)),
            bucket_name,
            object_name,
        })
    }

    /// Obtain a new part PresignedRequest and its part number
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let mut upload_manager: PresignedUploadManager = ...;
    ///
    /// let (
    ///     part_request: PresignedRequest,
    ///     part_number: usize,
    /// ) = upload_manager.next_part(&client, 1_337).await?;
    /// ```
    pub async fn next_part(
        &mut self,
        client: &Client,
        presigned_expiry_secs: u64,
    ) -> Result<(PresignedRequest, usize), Error> {
        let part_number = self.part_index.fetch_add(1, Ordering::SeqCst);

        Ok((
            upload_part_presigned(
                client,
                self.bucket_name,
                self.object_name,
                &self.upload_id,
                part_number,
                presigned_expiry_secs,
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
    /// let mut upload_manager: PresignedUploadManager = ...;
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
    /// let mut upload_manager: PresignedUploadManager = ...;
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
