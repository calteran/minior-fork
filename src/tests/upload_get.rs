// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use super::util::{test_client::TestClient, *};
use crate::{error::Error, test_error, ETag};

#[tokio::test]
async fn test_upload_get() {
    let object_name = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file = get_test_file(object_name).await?;
            let file_bytes = get_test_file_bytes(object_name).await?;

            let uploaded_bytes = minio
                .upload_object(&bucket_name, object_name, file, None)
                .await?;

            if file_bytes.len() != uploaded_bytes {
                test_error!("upload_object bytes counter did not equal the files size");
            }

            assert_object(
                &minio,
                &bucket_name,
                object_name,
                ObjectAssertions::BytesEqual(file_bytes),
            )
            .await?;

            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_upload_get_presigned() {
    let object_name = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file_bytes = get_test_file_bytes(object_name).await?;

            let reqwest_client = reqwest::Client::new();

            let presigned_request = minio
                .upload_object_presigned(&bucket_name, object_name, 1_337)
                .await?;

            let upload_url = presigned_request.uri();

            reqwest_client
                .put(upload_url)
                .body(file_bytes.clone())
                .send()
                .await?;

            assert_object(
                &minio,
                &bucket_name,
                object_name,
                ObjectAssertions::BytesEqualPresigned(file_bytes, &reqwest_client),
            )
            .await?;

            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_upload_multi_get() {
    let object_name = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file_bytes = get_test_file_bytes(object_name).await?;

            let mut upload_manager = minio.upload_object_multi(&bucket_name, object_name).await?;

            upload_manager
                .upload_part(&minio.client, file_bytes.clone())
                .await?;

            let uploaded_bytes = upload_manager.complete(&minio.client).await?;

            if file_bytes.len() != uploaded_bytes {
                test_error!("upload_object bytes counter did not equal the files size");
            }

            assert_object(
                &minio,
                &bucket_name,
                object_name,
                ObjectAssertions::BytesEqual(file_bytes),
            )
            .await?;

            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_upload_multi_get_presigned() {
    let object_name = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let reqwest_client = reqwest::Client::new();
            let file_bytes = get_test_file_bytes(object_name).await?;

            let mut e_tags = vec![];

            let mut upload_manager = minio
                .upload_object_multi_presigned(&bucket_name, object_name)
                .await?;

            let (presigned_request, part_number) =
                upload_manager.next_part(&minio.client, 1_337).await?;

            let upload_url = presigned_request.uri();

            let e_tag = reqwest_client
                .put(upload_url)
                .body(file_bytes.clone())
                .send()
                .await?
                .headers()
                .get("etag")
                .ok_or(Error::internal("Could not get etag"))?
                .to_str()?
                .to_string();

            e_tags.push(ETag { e_tag, part_number });

            upload_manager.complete(&minio.client, e_tags).await?;

            assert_object(
                &minio,
                &bucket_name,
                object_name,
                ObjectAssertions::BytesEqualPresigned(file_bytes, &reqwest_client),
            )
            .await?;

            Ok(())
        })
        .await
        .unwrap();
}
