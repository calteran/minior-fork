// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)

#[cfg(feature = "pagination_iter")]
#[cfg(test)]
mod tests {
    use crate::test_error;
    use crate::tests::util::{test_client::TestClient, *};

    #[tokio::test]
    async fn test_pagination_iter() {
        let object_names = vec!["file1.txt", "file2.txt", "file3.txt", "file4.txt"];

        let test_client = TestClient::new().await;

        test_client
            .run_test(|minio, bucket_name| async move {
                for object_name in object_names {
                    let file = get_test_file(object_name).await?;

                    minio
                        .upload_object(&bucket_name, object_name, file, None, None)
                        .await?;
                }

                let mut pagination_iter = minio.pagination_object_iter(&bucket_name, 2);

                while let Some(objects) = pagination_iter.next().await? {
                    if objects.len() != 2 {
                        test_error!("Expected 2 objects per page");
                    }
                }

                Ok(())
            })
            .await
            .unwrap();
    }
}
