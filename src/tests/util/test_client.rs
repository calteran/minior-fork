// Authors: Robert Lopez

use super::get_test_file;
use crate::Minio;
use std::{future::Future, sync::Arc};
use uuid::Uuid;

pub struct TestClient {
    pub minio: Minio,
    pub bucket_name: String,
}

impl TestClient {
    pub async fn new() -> Self {
        Self {
            minio: Minio::new("http://127.0.0.1:9000").await,
            bucket_name: Uuid::new_v4().to_string(),
        }
    }

    pub async fn drop(self) {
        self.minio
            .delete_bucket(&self.bucket_name)
            .await
            .expect(&format!("Failed to delete bucket: {}", self.bucket_name));
    }

    pub async fn upload(
        &self,
        file_name: &str,
        object_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = get_test_file(file_name).await?;

        self.minio
            .upload_object(&self.bucket_name, object_name, file, None, None)
            .await?;

        Ok(())
    }
}
