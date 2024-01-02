// Authors: Robert Lopez

pub mod test_client;

use tokio::fs::File;

pub async fn get_test_file(name: &str) -> Result<File, Box<dyn std::error::Error>> {
    Ok(File::open(format!("./test_data/{}", name)).await?)
}
