// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)

use crate::error::Error;
use aws_sdk_s3::{
    error::SdkError,
    operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output},
    types::Object,
    Client,
};
use aws_smithy_async::future::pagination_stream::PaginationStream;

/// Async iterator to paginate through `Objects` in a `Bucket`
///
/// ---
/// Example Usage:
/// ```
///
/// let client: Client = ...;
///
/// // `12` means we want 12 objects per page
/// let mut objects_iter = ObjectPaginationIter::new(&client, "bucket_name", 12);
///
/// while let Some(objects) = objects_iter.next().await? {
///     ...
/// }
/// ```
pub struct ObjectPaginationIter {
    pub page_stream: PaginationStream<Result<ListObjectsV2Output, SdkError<ListObjectsV2Error>>>,
}

impl ObjectPaginationIter {
    /// Construct a `ObjectPaginationIter`
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let client: Client = ...;
    ///
    /// // `12` means we want 12 objects per page
    /// let mut objects_iter = ObjectPaginationIter::new(&client, "bucket_name", 12);
    /// ```
    pub fn new(client: &Client, bucket_name: &str, page_size: i32) -> Self {
        let page_stream = client
            .list_objects_v2()
            .bucket(bucket_name)
            .into_paginator()
            .page_size(page_size)
            .send();

        Self { page_stream }
    }

    /// Yield the next objects in the iteration.
    ///
    /// Returns `None` if there are no more.
    ///
    /// ---
    /// Example Usage:
    /// ```
    ///
    /// let mut objects_iter: ObjectPaginationIter = ...;
    ///
    /// while let Some(objects) = objects_iter.next().await? {
    ///     ...
    /// }
    /// ```
    pub async fn next(&mut self) -> Result<Option<Vec<Object>>, Error> {
        if let Some(page) = self.page_stream.try_next().await.map_err(Error::sdk)? {
            let objects = page.contents().to_owned();

            return Ok(Some(objects));
        }

        Ok(None)
    }
}
