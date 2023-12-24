// Authors: Robert Lopez

use super::util::test_client::TestClient;

#[tokio::test]
async fn test_upload() {
    let test_client = TestClient::new().await;

    test_client.upload("shark.png", "shark").await?;
}
