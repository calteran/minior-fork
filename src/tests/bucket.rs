// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use super::util::{test_client::TestClient, *};
use crate::test_error;

#[tokio::test]
async fn test_bucket_exists() {
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            if !minio.bucket_exists(&bucket_name).await? {
                test_error!("Bucket {} did not exist by bucket_exists", bucket_name);
            }

            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_object_exists() {
    let object_name: &str = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file = get_test_file(object_name).await?;

            minio
                .upload_object(&bucket_name, object_name, file, None)
                .await?;

            if !minio.object_exists(&bucket_name, object_name).await? {
                test_error!(
                    "Object {} in Bucket {} did not exist by object_exists",
                    object_name,
                    bucket_name
                );
            }

            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_list_objects() {
    let object_names = vec!["shark.png", "file1.txt", "owl.jpg"];
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            for object_name in object_names.iter() {
                let file = get_test_file(object_name).await?;

                minio
                    .upload_object(&bucket_name, object_name, file, None)
                    .await?;
            }

            let objects = minio.list_bucket_objects(&bucket_name).await?;

            for object in objects {
                if let Some(key) = object.key() {
                    if !object_names.contains(&key) {
                        test_error!("List objects did not list {key}");
                    }

                    continue;
                }

                test_error!(" {:?} had no key!", object);
            }

            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_bucket_delete() {
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, _| async move {
            let new_bucket_name = "test-create-bucket";

            minio.create_bucket(new_bucket_name).await?;

            if !minio.bucket_exists(new_bucket_name).await? {
                test_error!("Bucket {} did not exist by bucket_exists", new_bucket_name);
            }

            minio.delete_bucket(&new_bucket_name, true).await?;

            if minio.bucket_exists(new_bucket_name).await? {
                test_error!("Bucket {} did exist by bucket_exists", new_bucket_name);
            }

            Ok(())
        })
        .await
        .unwrap();
}
