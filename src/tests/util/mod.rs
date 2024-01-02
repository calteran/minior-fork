// Authors: Robert Lopez

pub mod test_client;

use reqwest::{
    multipart::{Form, Part},
    Body,
};
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

pub async fn get_test_file(name: &str) -> Result<File, Box<dyn std::error::Error>> {
    let file = File::open(format!("./test_data/{}", name)).await?;

    Ok(file)
}

pub async fn get_test_file_form(
    name: &str,
    mime: &str,
) -> Result<Form, Box<dyn std::error::Error>> {
    let name = name.to_owned();

    let file = get_test_file(&name).await?;
    let file_stream = FramedRead::new(file, BytesCodec::new());

    let body = Body::wrap_stream(file_stream);
    let part = Part::stream(body).file_name(name).mime_str(mime)?;

    let form = Form::new().part("file", part);

    Ok(form)
}
