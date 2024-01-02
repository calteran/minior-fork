// Authors: Robert Lopez

use super::util::{test_client::TestClient, *};

#[tokio::test]
async fn test_upload() {
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file = get_test_file("shark.png").await?;

            minio
                .upload_object(&bucket_name, "shark", file, None, None)
                .await?;

            Ok(())
        })
        .await
        .unwrap();
}
