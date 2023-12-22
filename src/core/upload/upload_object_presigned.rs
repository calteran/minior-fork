// Authors: Robert Lopez

use super::util::*;
use crate::error::Error;
use aws_sdk_s3::{presigning::PresignedRequest, Client};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub struct PresignedUploadManager<'pum> {
    pub upload_id: String,
    pub part_index: Arc<AtomicUsize>,
    pub bucket_name: &'pum str,
    pub object_name: &'pum str,
    pub presigned_expiry: Option<u64>,
}

impl<'pum> PresignedUploadManager<'pum> {
    pub async fn new(
        client: &Client,
        bucket_name: &'pum str,
        object_name: &'pum str,
        presigned_expiry: Option<u64>,
    ) -> Result<PresignedUploadManager<'pum>, Error> {
        let upload_id = start_multipart_upload(client, bucket_name, object_name).await?;

        Ok(PresignedUploadManager {
            upload_id,
            part_index: Arc::new(AtomicUsize::new(1)),
            bucket_name,
            object_name,
            presigned_expiry,
        })
    }

    pub async fn next_part(&mut self, client: &Client) -> Result<PresignedRequest, Error> {
        let part_number = self.part_index.fetch_add(1, Ordering::SeqCst);

        upload_part_presigned(
            client,
            self.bucket_name,
            self.object_name,
            &self.upload_id,
            part_number,
            self.presigned_expiry,
        )
        .await
    }

    pub async fn abort(&self, client: &Client) -> Result<(), Error> {
        abort_multipart_upload(client, self.bucket_name, self.object_name, &self.upload_id).await
    }

    pub async fn complete(
        &self,
        client: &Client,
        e_tags: Vec<(String, usize)>,
    ) -> Result<(), Error> {
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
