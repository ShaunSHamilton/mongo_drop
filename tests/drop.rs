#![allow(incomplete_features)]
#![feature(async_drop)]
use mongo_drop::MongoDrop;
use mongodb::{
    Client, Database,
    bson::{Document, doc},
    options::ChangeStreamPreAndPostImages,
};

#[tokio::test]
#[test_log::test]
async fn asyncdrop_with_self_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let mongodb_client = get_client().await?;

    let database_name = "mongo_drop_db";
    let db = mongodb_client.database(database_name);
    let collection = create_collection(&db, "no_change").await?;
    let _guard = MongoDrop::new(&db).await?;

    // --- All fallible test operations go here ---
    // The 'guard' will automatically trigger async_drop when it goes out of scope,
    // whether the async function completes successfully or returns early due to '?'.

    // Example operations:
    let d = collection.insert_one(doc! {"data": "initial"}).await?;

    collection
        .update_one(
            doc! {"_id": &d.inserted_id},
            doc! {"$set": {"data": "updated"}},
        )
        .await?;

    // Simulate an error during test execution to see async_drop on failure
    // tracing::error!("Simulating test error!");
    // return Err(Box::<dyn std::error::Error>::from("Simulated test failure"));

    collection.delete_one(doc! {"_id": d.inserted_id}).await?;

    // When `my_test_with_asyncdrop` finishes (either by returning Ok(()) or Err)
    // the `guard` variable goes out of scope. Since `my_test_with_asyncdrop` is
    // an async function, the `async_drop` method of `MongoDBDrop` will be awaited.

    Ok(())
}

#[tokio::test]
#[test_log::test]
async fn inserts() -> Result<(), Box<dyn std::error::Error>> {
    let mongodb_client = get_client().await?;

    let database_name = "mongo_drop_db";
    let db = mongodb_client.database(database_name);
    let collection = create_collection(&db, "insert").await?;
    {
        let _guard = MongoDrop::new(&db).await?;

        // Insert multiple documents
        for i in 0..5 {
            collection
                .insert_one(doc! {"value": format!("data_{}", i)})
                .await?;
        }

        // Verify inserts
        let count = collection.count_documents(doc! {}).await?;
        assert_eq!(count, 5);
    }

    // After drop, verify collection is empty
    let count_after_drop = collection.count_documents(doc! {}).await?;
    assert_eq!(count_after_drop, 0);

    Ok(())
}

#[tokio::test]
#[test_log::test]
async fn updates() -> Result<(), Box<dyn std::error::Error>> {
    let mongodb_client = get_client().await?;

    let database_name = "mongo_drop_db";
    let db = mongodb_client.database(database_name);
    let collection = create_collection(&db, "update").await?;
    // Insert a document to update before guard to keep around
    let d = collection.insert_one(doc! {"value": "initial"}).await?;

    {
        let _guard = MongoDrop::new(&db).await?;

        // Update the document
        collection
            .update_one(
                doc! {"_id": &d.inserted_id},
                doc! {"$set": {"value": "updated"}},
            )
            .await?;

        // Verify update
        let updated_doc = collection.find_one(doc! {"_id": &d.inserted_id}).await?;
        assert_eq!(updated_doc.unwrap().get_str("value")?, "updated");
    }

    // After drop, verify document is reverted
    let reverted_doc = collection
        .find_one(doc! {"_id": d.clone().inserted_id})
        .await?;
    assert_eq!(reverted_doc.unwrap().get_str("value")?, "initial");

    Ok(())
}

#[tokio::test]
#[test_log::test]
async fn deletes() -> Result<(), Box<dyn std::error::Error>> {
    let mongodb_client = get_client().await?;

    let database_name = "mongo_drop_db";
    let db = mongodb_client.database(database_name);
    let collection = create_collection(&db, "delete").await?;
    // Insert a document to delete
    let d = collection.insert_one(doc! { "value": "to_delete"}).await?;

    {
        let _guard = MongoDrop::new(&db).await?;

        // Delete the document
        collection.delete_one(doc! {"_id": &d.inserted_id}).await?;
        // Verify deletion
        let deleted_doc = collection.find_one(doc! {"_id": &d.inserted_id}).await?;
        assert!(deleted_doc.is_none());
    }

    // After drop, verify document is restored
    let deleted_doc = collection.find_one(doc! {"_id": &d.inserted_id}).await?;
    assert!(deleted_doc.is_some());

    Ok(())
}

async fn get_client() -> Result<Client, mongodb::error::Error> {
    Client::with_uri_str("mongodb://127.0.0.1:27017/mongo_drop_db?directConnection=true").await
}

async fn create_collection(
    db: &Database,
    name: &str,
) -> Result<mongodb::Collection<Document>, mongodb::error::Error> {
    // Delete existing collection if it exists
    let _ = db.collection::<Document>(name).drop().await;

    let options = mongodb::options::CreateCollectionOptions::builder()
        .change_stream_pre_and_post_images(
            ChangeStreamPreAndPostImages::builder()
                .enabled(true)
                .build(),
        )
        .build();
    let _ = db.create_collection(name).with_options(options).await?;
    let collection = db.collection::<Document>(name);
    Ok(collection)
}
