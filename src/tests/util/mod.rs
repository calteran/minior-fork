// Authors: Robert Lopez

pub mod test_client;
pub mod test_error;

use reqwest::{
    multipart::{Form, Part},
    Body,
};
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncReadExt},
};
use tokio_util::codec::{BytesCodec, FramedRead};

pub async fn get_test_file(name: &str) -> Result<File, Box<dyn std::error::Error>> {
    let file = File::open(format!("./test_data/{}", name)).await?;

    Ok(file)
}

pub async fn get_test_file_bytes(name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let path = format!("./test_data/{}", name);

    let bytes = tokio::fs::read(path).await?;

    Ok(bytes)
}

pub async fn get_test_file_form(
    name: &str,
    mime: &str,
) -> Result<(Form, u64), Box<dyn std::error::Error>> {
    let name = name.to_owned();

    let file = get_test_file(&name).await?;
    let file_length = file.metadata().await?.len();
    let file_stream = FramedRead::new(file, BytesCodec::new());

    let body = Body::wrap_stream(file_stream);
    let part = Part::stream(body).file_name(name).mime_str(mime)?;

    let form = Form::new().part("file", part);

    Ok((form, file_length))
}

pub async fn read_file_stream(
    mut file_stream: impl AsyncBufRead + Unpin,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buffer = vec![0; 1337];
    let mut downloaded_data = vec![];

    loop {
        let bytes_read = match file_stream.read(&mut buffer[..]).await? {
            0 => break,
            size => size,
        };

        downloaded_data.extend_from_slice(&buffer[..bytes_read]);
    }

    Ok(downloaded_data)
}
