#![allow(incomplete_features)]
#![feature(async_drop)]

use mongo_drop::MongoDrop;
use mongodb::Client;

#[tokio::test]
async fn test_minimal_mongo_drop() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting minimal MongoDrop test...");
    
    let client = Client::with_uri_str("mongodb://127.0.0.1:27017/freecodecamp?directConnection=true").await?;
    println!("Client created");
    
    let database = client.database("test_db");
    println!("Database reference obtained");
    
    // Try to create MongoDrop - this might be where it crashes
    println!("Creating MongoDrop...");
    let _guard = MongoDrop::new(&database).await?;
    println!("MongoDrop created successfully");
    
    // Don't do any operations, just let it drop
    println!("Letting MongoDrop go out of scope...");
    
    Ok(())
}
