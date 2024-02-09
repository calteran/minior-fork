// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use super::util::{test_client::TestClient, *};

#[tokio::test]
async fn test_delete() {
    let object_name = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file = get_test_file(object_name).await?;

            minio
                .upload_object(&bucket_name, object_name, file, None)
                .await?;

            minio.delete_object(&bucket_name, object_name).await?;

            assert_object(
                &minio,
                &bucket_name,
                object_name,
                ObjectAssertions::DoesNotExist,
            )
            .await?;

            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_delete_presigned() {
    let object_name = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file = get_test_file(object_name).await?;
            let reqwest_client = reqwest::Client::new();

            minio
                .upload_object(&bucket_name, object_name, file, None)
                .await?;

            let request = minio
                .delete_object_presigned(&bucket_name, object_name, 1_337)
                .await?;

            let delete_url = request.uri();

            reqwest_client.delete(delete_url).send().await?;

            assert_object(
                &minio,
                &bucket_name,
                object_name,
                ObjectAssertions::DoesNotExist,
            )
            .await?;

            Ok(())
        })
        .await
        .unwrap();
}
