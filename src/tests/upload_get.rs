// Authors: Robert Lopez

use super::util::{test_client::TestClient, test_error::TestError, *};
use crate::{error::Error, test_error, ETag};
use reqwest::header::HeaderValue;

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

#[tokio::test]
async fn test_upload_get_presigned() {
    let object_name = "text.txt";
    let object_mime = "text/plain";
    let test_client = TestClient::new().await;

    test_client
        .run_test(|minio, bucket_name| async move {
            let mut upload_manager = minio
                .upload_object_presigned(&bucket_name, object_name, Some(1_337))
                .await?;

            let mut e_tags = vec![];

            let (part_request, part_number) = upload_manager.next_part(&minio.client).await?;

            let url = part_request.uri();
            let (form, file_length) = get_test_file_form(object_name, object_mime).await?;

            let client = reqwest::Client::new();

            let request = client
                .put(url)
                .multipart(form)
                .header("Content-Length", HeaderValue::from(file_length));

            let e_tag = request
                .send()
                .await?
                .headers()
                .get("etag")
                .ok_or(Error::internal("Response for part upload had no e-tag"))?
                .to_str()?
                .to_owned();

            e_tags.push(ETag {
                tag: e_tag,
                part_number,
            });

            upload_manager.complete(&minio.client, e_tags).await?;

            let get_request = minio
                .get_object_presigned(&bucket_name, object_name, Some(1_337))
                .await?;

            let url = get_request.uri();

            let file_bytes = get_test_file_bytes(object_name).await?;
            let downloaded_data = client.get(url).send().await?.bytes().await?.to_vec();

            println!("{:?} \n : \n {:?}", file_bytes, downloaded_data);

            if file_bytes != downloaded_data {
                test_error!("Uploaded bytes and retrieved bytes do not match");
            }

            Ok(())
        })
        .await
        .unwrap();
}
