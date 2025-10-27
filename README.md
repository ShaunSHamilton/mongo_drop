# mongo_drop

[![crates.io](https://img.shields.io/crates/v/mongo_drop.svg)](https://crates.io/crates/mongo_drop)
[![Docs.rs](https://docs.rs/mongo_drop/badge.svg)](https://docs.rs/mongo_drop)

A Rust library providing an RAII-style guard for MongoDB databases, designed for testing environments. It automatically rolls back changes made to the database within the guard's asynchronous scope using MongoDB change streams and the experimental `AsyncDrop` trait.

**🚨 WARNING: This crate relies on the experimental `async_drop` feature, which is only available on nightly Rust and is subject to change or removal. It is NOT suitable for production use. Use this crate for test helpers or experimentation only.**

## Usage

```rust
#[cfg(test)]
mod tests {
    use mongodb::{Client, options::ClientOptions};
    use mongo_drop::MongoDrop;

    #[tokio::test]
    async fn trivial_mongo_drop_insert() {
        // Initialize MongoDB
        let client = Client::with_uri_str("mongodb://localhost:27017").await.unwrap();
        let database = client.database("mongo_drop_db");
        let coll = database.collection("insert_collection");
        {
          // Create a MongoDrop guard
          let _guard = MongoDrop::new(&database).await.unwrap();

          // Perform database operations within the guard
          coll.insert_one(doc! { "key": "value" }).await.unwrap();
          let record = coll.find_one(doc! {}).await.unwrap();

          assert_eq!(record, Some(doc! { "key": "value" }));
          // The changes will be rolled back automatically when the guard goes out of scope
        }
        // After the guard is dropped, verify that the changes were rolled back
        let record = coll.find_one(doc! {}).await.unwrap();
        assert_eq!(record, None);
    }

    #[tokio::test]
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

        // Delete, Update, and Replace operations require collections to be created with pre-images enabled
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
}
```

## Features

- `tracing`: Enables tracing support for the library.

- **Automatic Rollback:** Leverages `AsyncDrop` to undo database changes automatically when the guard goes out of scope in an `async` function.
- **Change Stream Based:** Listens to MongoDB change streams to capture modification events.
- **Supports DML Rollback:** Undoes `Insert`, `Update`, `Delete`, and `Replace` operations using collected pre-images.
- **Test-Focused:** Designed to simplify database state management in integration tests.

## Requirements

- **Nightly Rust Toolchain:** You must use a nightly build of the Rust compiler (`rustup toolchain install nightly`).
