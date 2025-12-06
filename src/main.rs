mod storage;
mod api;
mod db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");

    Ok(())
}
