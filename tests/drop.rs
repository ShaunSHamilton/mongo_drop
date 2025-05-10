use mongo_drop::MongoDrop;
use mongodb::{
    Client,
    bson::{Document, doc},
};

#[tokio::test]
async fn asyncdrop() -> Result<(), Box<dyn std::error::Error>> {
    let mongodb_client = get_client().await?;

    let database_name = "freecodecamp";
    let collection_name = "my_test_collection";
    let db = mongodb_client.database(database_name);

    let _guard = MongoDrop::new(&db).await?;
    #[cfg(feature = "tracing")]
    tracing::info!("MongoDB rollback guard created using AsyncDrop.");

    // --- All fallible test operations go here ---
    // The 'guard' will automatically trigger async_drop when it goes out of scope,
    // whether the async function completes successfully or returns early due to '?'.
    #[cfg(feature = "tracing")]
    tracing::info!("Starting test body operations...");
    let collection = db.collection::<Document>(collection_name);

    // Example operations:
    collection
        .insert_one(doc! {"_id": 1, "data": "initial"})
        .await?;
    #[cfg(feature = "tracing")]
    tracing::info!("Inserted document with _id: 1");

    collection
        .update_one(doc! {"_id": 1}, doc! {"$set": {"data": "updated"}})
        .await?;
    #[cfg(feature = "tracing")]
    tracing::info!("Updated document with _id: 1");

    // Simulate an error during test execution to see async_drop on failure
    // tracing::error!("Simulating test error!");
    // return Err(Box::<dyn std::error::Error>::from("Simulated test failure"));

    collection.delete_one(doc! {"_id": 1}).await?;
    #[cfg(feature = "tracing")]
    tracing::info!("Deleted document with _id: 1");

    #[cfg(feature = "tracing")]
    tracing::info!("Test body operations complete.");

    // When `my_test_with_asyncdrop` finishes (either by returning Ok(()) or Err)
    // the `guard` variable goes out of scope. Since `my_test_with_asyncdrop` is
    // an async function, the `async_drop` method of `MongoDBDrop` will be awaited.

    Ok(())
}

async fn get_client() -> Result<Client, mongodb::error::Error> {
    Client::with_uri_str("mongodb://127.0.0.1:27017/freecodecamp?directConnection=true").await
}
