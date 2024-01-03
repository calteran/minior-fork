// Authors: Robert Lopez

use super::util::{test_client::TestClient, test_error::TestError, *};
use crate::test_error;

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
            let downloaded_data = read_file_stream(file_stream).await?;

            if file_bytes != downloaded_data {
                test_error!("Uploaded bytes and retrieved bytes do not match");
            }

            Ok(())
        })
        .await
        .unwrap();
}
