// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)

pub mod core;
pub mod error;

#[cfg(test)]
mod tests;

use crate::{
    core::{
        bucket::*,
        delete::*,
        get::*,
        upload::{upload_object::*, upload_object_multi_presigned::PresignedUploadManager},
    },
    error::Error,
};
use aws_sdk_s3::{
    presigning::PresignedRequest,
    types::{Bucket, Object},
    Client,
};
use core::upload::{
    upload_object_multi::UploadManager, upload_object_presigned::upload_object_presigned,
};
use std::sync::Arc;
use tokio::io::{AsyncBufRead, AsyncRead};

/// Represents an ETag used for multi-part uploads
pub struct ETag {
    pub e_tag: String,
    pub part_number: usize,
}

/// Minio client utilizing the S3 API
///
/// ---
/// Example Usage:
/// ```
///
/// // Note: Provide the S3 API Port
/// let url = "http://127.0.0.1:9000";
///
/// let bucket_name = "shark_images";
///
/// let minio = Minio::new(url).await;
///
/// minio.create_bucket(bucket_name).await?;
///
/// let shark_image: tokio::fs::File = ...;
/// let object_name = "shark.jpg";
///
/// minio.upload_object(
///       bucket_name,
///       object_name,
///       shark_image,
///       None,
///       None,
///   )
///   .await?;
///
/// let request: PresignedRequest = minio.get_object_presigned(
///     bucket_name,
///     object_name,
///     3_600,
/// ).await?;
/// ```
pub struct Minio {
    pub client: Arc<Client>,
}

impl Minio {
    /// Constructs a new Minio client from the S3 API Url
    ///
    /// Loads credentials from environment
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// // Note: Provide the S3 API Port
    /// let url = "http://127.0.0.1:9000";
    ///
    /// let minio = Minio::new(url).await;
    /// ```
    pub async fn new(url: &str) -> Self {
        let config = aws_config::from_env().endpoint_url(url).load().await;
        let client = Client::new(&config);

        Self {
            client: Arc::new(client),
        }
    }

    /// Lists `Object`s present in the given bucket by `bucket_name`
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let bucket_objects: Vec<Object> = minio.list_bucket_objects("sharks").await?;
    /// ```
    pub async fn list_bucket_objects(&self, bucket_name: &str) -> Result<Vec<Object>, Error> {
        list_bucket_objects(&self.client, bucket_name).await
    }

    /// Returns true if a bucket by `bucket_name` exists
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// if minio.bucket_exists("sharks").await? {
    ///     ...
    /// }
    /// ```
    pub async fn bucket_exists(&self, bucket_name: &str) -> Result<bool, Error> {
        bucket_exists(&self.client, bucket_name).await
    }

    /// Returns true if a object by `object_name` in a bucket by `bucket_name`
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// if minio.object_exists("sharks", "whale_shark.png").await? {
    ///     ...
    /// }
    /// ```
    pub async fn object_exists(&self, bucket_name: &str, object_name: &str) -> Result<bool, Error> {
        object_exists(&self.client, bucket_name, object_name).await
    }

    /// Returns a vector of `Bucket`s from the client
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// for bucket in minio.list_buckets().await? {
    ///     ...
    /// }
    /// ```
    pub async fn list_buckets(&self) -> Result<Vec<Bucket>, Error> {
        list_buckets(&self.client).await
    }

    /// Creates a new bucket named `bucket_name`
    ///
    /// Returns `false` if bucket already existed
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let bucket_created: bool = minio.create_bucket("sharks").await?;
    /// ```
    pub async fn create_bucket(&self, bucket_name: &str) -> Result<bool, Error> {
        create_bucket(&self.client, bucket_name).await
    }

    /// Deletes a bucket by `bucket_name`
    ///
    /// Returns `false` if the bucket did not exist
    ///
    /// If `delete_objects` is `true`, will also attempt to delete
    /// all objects in the bucket.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let bucket_deleted: bool = minio.delete_bucket("sharks", false).await?;
    /// ```
    pub async fn delete_bucket(
        &self,
        bucket_name: &str,
        delete_objects: bool,
    ) -> Result<bool, Error> {
        delete_bucket(&self.client, bucket_name, delete_objects).await
    }

    /// Returns a stream for an object by `bucket_name` and `object_name`
    ///
    /// Returns `Ok(None)` if the object does not exist.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let stream: Option<impl AsyncBufRead> = minio.get_object(
    ///     "sharks",
    ///     "shark.jpg",
    /// ).await?;
    /// ```
    pub async fn get_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Option<impl AsyncBufRead>, Error> {
        get_object(&self.client, bucket_name, object_name).await
    }

    /// Generates a `PresignedRequest` from a bucket by `bucket_name` and `object_name`
    /// to get the object.
    ///
    /// Returns `Ok(None)` if the object does not exist.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let request: Option<PresignedRequest> = minio.get_object_presigned(
    ///     "sharks",
    ///     "shark.jpg",
    ///     3_600,
    /// ).await?;
    /// ```
    pub async fn get_object_presigned(
        &self,
        bucket_name: &str,
        object_name: &str,
        presigned_expiry_secs: u64,
    ) -> Result<Option<PresignedRequest>, Error> {
        get_object_presigned(
            &self.client,
            bucket_name,
            object_name,
            presigned_expiry_secs,
        )
        .await
    }

    /// Upload a object named `object_name` to the bucket named `bucket_name`
    ///
    /// Default `buffer_size` is `100_000`, and cannot be
    /// lower than `4_096`
    ///
    /// Default `data_part_size` is `5_242_880`, and cannot
    /// be lower than that value
    ///
    /// Will automatically convert to a multipart upload if over `data_part_size`
    /// bytes
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    /// let shark_image: tokio::fs::File = ...;
    ///
    /// minio.upload_object(
    ///     "sharks",
    ///     "shark.jpg",
    ///     shark_image,
    ///     None,
    ///     None,
    ///   )
    ///   .await?;
    /// ```
    pub async fn upload_object<S>(
        &self,
        bucket_name: &str,
        object_name: &str,
        stream: S,
        buffer_size: Option<usize>,
        data_part_size: Option<usize>,
    ) -> Result<(), Error>
    where
        S: AsyncRead + Unpin,
    {
        upload_object(
            &self.client,
            bucket_name,
            object_name,
            stream,
            buffer_size,
            data_part_size,
        )
        .await
    }

    /// Obtain a `PresignedRequest` for a object upload
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let presigned_request: PresignedRequest = minio.upload_object_presigned(
    ///     "bucket_name",
    ///     "object_name",
    ///     1_337,
    /// ).await?;
    /// ```
    pub async fn upload_object_presigned(
        &self,
        bucket_name: &str,
        object_name: &str,
        presigned_expiry_secs: u64,
    ) -> Result<PresignedRequest, Error> {
        upload_object_presigned(
            &self.client,
            bucket_name,
            object_name,
            presigned_expiry_secs,
        )
        .await
    }

    /// Constructs a `UploadManager` for a object upload by `object_name`
    /// and `bucket_name`.
    ///
    /// The manager can be used to upload multiple parts,and can complete/abort
    /// the upload.
    ///
    /// See `core::upload::upload_object_multi::UploadManager` for more details.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let mut upload_manager: UploadManager = minio.upload_object_multi(
    ///     "sharks",
    ///     "shark.jpg",
    /// ).await?;
    /// ```
    pub async fn upload_object_multi<'uom>(
        &self,
        bucket_name: &'uom str,
        object_name: &'uom str,
    ) -> Result<UploadManager<'uom>, Error> {
        UploadManager::new(&self.client, bucket_name, object_name).await
    }

    /// Constructs a `PresignedUploadManager` for a presigned object upload
    /// by `object_name` and `bucket_name`.
    ///
    /// The manager can be used to obtain multiple `PresignedRequest` for parts,
    /// and can complete/abort the upload.
    ///
    /// See `core::upload::upload_object_multi_presigned::PresignedUploadManager` for more details.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let mut upload_manager: PresignedUploadManager = minio.upload_object_multi_presigned(
    ///     "sharks",
    ///     "shark.jpg",
    /// ).await?;
    /// ```
    pub async fn upload_object_multi_presigned<'uomp>(
        &self,
        bucket_name: &'uomp str,
        object_name: &'uomp str,
    ) -> Result<PresignedUploadManager<'uomp>, Error> {
        PresignedUploadManager::new(&self.client, bucket_name, object_name).await
    }

    /// Deletes a object from a bucket by `bucket_name` and `object_name`
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// minio.delete_object("sharks", "shark.jpg").await?;
    /// ```
    pub async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<(), Error> {
        delete_object(&self.client, bucket_name, object_name).await
    }

    /// Generates a `PresignedRequest` from a bucket by `bucket_name` and `object_name`
    /// to delete the object.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let request: PresignedRequest = minio.delete_object_presigned(
    ///     "sharks",
    ///     "shark.jpg",
    ///     3_600,
    /// ).await?;
    /// ```
    pub async fn delete_object_presigned(
        &self,
        bucket_name: &str,
        object_name: &str,
        presigned_expiry_secs: u64,
    ) -> Result<PresignedRequest, Error> {
        delete_object_presigned(
            &self.client,
            bucket_name,
            object_name,
            presigned_expiry_secs,
        )
        .await
    }
}
