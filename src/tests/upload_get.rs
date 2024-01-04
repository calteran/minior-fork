// Authors: Robert Lopez

use super::util::{test_client::TestClient, test_error::TestError, *};
use crate::{test_error, ETag, Minio};

#[tokio::test]
async fn test_upload_get() {
    let object_name = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file = get_test_file(object_name).await?;
            let file_bytes = get_test_file_bytes(object_name).await?;

            minio
                .upload_object(&bucket_name, object_name, file, None, None)
                .await?;

            let file_stream = minio.get_object(&bucket_name, object_name).await?;
            let downloaded_bytes = read_file_stream(file_stream).await?;

            if file_bytes != downloaded_bytes {
                test_error!("Uploaded bytes and retrieved bytes do not match");
            }

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

            let presigned_request = minio
                .get_object_presigned(&bucket_name, object_name, 1_337)
                .await?;

            let get_url = presigned_request.uri();

            let downloaded_bytes = reqwest_client
                .get(get_url)
                .send()
                .await?
                .bytes()
                .await?
                .to_vec();

            if file_bytes != downloaded_bytes {
                test_error!("Uploaded bytes and retrieved bytes do not match");
            }

            Ok(())
        })
        .await
        .unwrap();
}
