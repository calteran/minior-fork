// Authors: Robert Lopez

use reqwest::header::{HeaderMap, HeaderValue};

use super::util::{test_client::TestClient, *};

#[tokio::test]
async fn test_upload() {
    let object_name = "shark.png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let file = get_test_file(object_name).await?;

            minio
                .upload_object(&bucket_name, object_name, file, None, None)
                .await?;

            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_upload_presigned() {
    let object_name = "shark.png";
    let object_mime = "image/x-png";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let mut upload_manager = minio
                .upload_object_presigned(&bucket_name, object_name, Some(1_337))
                .await?;

            // let mut e_tags = vec![];

            let (part_request, part_number) = upload_manager.next_part(&minio.client).await?;

            println!("{:?}", part_request);

            // let client = reqwest::Client::new();
            // let form = get_test_file_form(object_name, object_mime).await?;

            // let url = part_request.uri();

            // let mut request = client.post(url);

            // for (header, header_value) in part_request.headers() {
            //     println!("---------");
            //     println!("{} : {}", header, header_value);

            //     request = request.header(header, HeaderValue::from_str(header_value)?);
            // }

            // request = request.multipart(form);

            // let response = request.send().await?;

            // println!("{:?}", response.text().await?);

            Ok(())
        })
        .await
        .unwrap();
}
