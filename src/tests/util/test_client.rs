// Authors: Robert Lopez

use crate::Minio;
use std::{future::Future, sync::Arc};
use uuid::Uuid;

pub struct TestClient {
    pub minio: Arc<Minio>,
    pub bucket_name: String,
}

impl TestClient {
    pub async fn new() -> Self {
        let bucket_name = Uuid::new_v4().to_string();
        let minio = Minio::new("http://127.0.0.1:9000").await;

        minio
            .create_bucket(&bucket_name)
            .await
            .expect(&format!("Failed to create bucket: {}", bucket_name));

        Self {
            minio: Arc::new(minio),
            bucket_name,
        }
    }

    pub async fn drop(&self) {
        self.minio
            .delete_bucket(&self.bucket_name, true)
            .await
            .expect(&format!("Failed to delete bucket: {}", self.bucket_name));
    }

    pub async fn run_test<T, Fut>(self, test: T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: FnOnce(Arc<Minio>, String) -> Fut,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error>>>,
    {
        let result = test(self.minio.clone(), self.bucket_name.clone()).await;
        self.drop().await;

        result
    }
}
