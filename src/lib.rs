// Authors: Robert Lopez

pub mod core;
pub mod error;

use crate::{
    core::{
        bucket::*,
        delete::*,
        get::*,
        upload::{upload_object::*, upload_object_presigned::PresignedUploadManager},
    },
    error::Error,
};
use aws_sdk_s3::{presigning::PresignedRequest, types::Bucket, Client};
use std::sync::Arc;
use tokio::io::{AsyncBufRead, AsyncRead};

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
/// let minio = Minio::new(url).await?;
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
///     Some(3_600),
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
    /// let minio = Minio::new(url).await?;
    /// ```
    pub async fn new(url: &str) -> Self {
        let config = aws_config::from_env().endpoint_url(url).load().await;
        let client = Client::new(&config);

        Self {
            client: Arc::new(client),
        }
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
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let bucket_deleted: bool = minio.delete_bucket("sharks").await?;
    /// ```
    pub async fn delete_bucket(&self, bucket_name: &str) -> Result<bool, Error> {
        delete_bucket(&self.client, bucket_name).await
    }

    /// Returns a stream for an object by `bucket_name` and `object_name`
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let stream: impl AsyncBufRead = minio.get_object(
    ///     "sharks",
    ///     "shark.jpg",
    /// ).await?;
    /// ```
    pub async fn get_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<impl AsyncBufRead, Error> {
        get_object(&self.client, bucket_name, object_name).await
    }

    /// Generates a `PresignedRequest` from a bucket by `bucket_name` and `object_name`
    /// to get the object.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let request: PresignedRequest = minio.get_object_presigned(
    ///     "sharks",
    ///     "shark.jpg",
    ///     Some(3_600),
    /// ).await?;
    /// ```
    pub async fn get_object_presigned(
        &self,
        bucket_name: &str,
        object_name: &str,
        presigned_expiry: Option<u64>,
    ) -> Result<PresignedRequest, Error> {
        get_object_presigned(&self.client, bucket_name, object_name, presigned_expiry).await
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

    /// Constructs a `PresignedUploadManager` for a presigned object upload
    /// by `object_name` and `bucket_name`.
    ///
    /// The manager can be used to obtain multiple `PresignedRequest` for parts,
    /// and can complete/abort the upload.
    ///
    /// Note that `presigned_expiry` is applied to each individual part `PresignedRequest`.
    ///
    /// See `core::upload::upload_object_presigned::PresignedUploadManager` for more details.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let minio: Minio = ...;
    ///
    /// let mut upload_manager: PresignedUploadManager = minio.upload_object_presigned(
    ///     "sharks",
    ///     "shark.jpg",
    ///     Some(3_600),
    /// ).await?;
    /// ```
    pub async fn upload_object_presigned<'uop>(
        &self,
        bucket_name: &'uop str,
        object_name: &'uop str,
        presigned_expiry: Option<u64>,
    ) -> Result<PresignedUploadManager<'uop>, Error> {
        PresignedUploadManager::new(&self.client, bucket_name, object_name, presigned_expiry).await
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
    ///     Some(3_600),
    /// ).await?;
    /// ```
    pub async fn delete_object_presigned(
        &self,
        bucket_name: &str,
        object_name: &str,
        presigned_expiry: Option<u64>,
    ) -> Result<PresignedRequest, Error> {
        delete_object_presigned(&self.client, bucket_name, object_name, presigned_expiry).await
    }
}
