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
    async fn test_mongo_drop() {
        // Initialize MongoDB
        let client = Client::with_uri_str("mongodb://localhost:27017").await.unwrap();
        let database = client.database("test_db");
        // Create a MongoDrop guard
        let _guard = MongoDrop::new(&database).await.unwrap();

        // Perform database operations within the guard
        let coll = database.collection("test_collection");
        coll.insert_one(doc! { "key": "value" }).await.unwrap();
        let record = coll.find_one(doc! {}).await.unwrap();

        assert_eq!(record, Some(doc! { "key": "value" }));
        // The changes will be rolled back automatically when the guard goes out of scope
    }
}
```

## Features

- `tracing`: Enables tracing support for the library.

##

- **Automatic Rollback:** Leverages `AsyncDrop` to undo database changes automatically when the guard goes out of scope in an `async` function.
- **Change Stream Based:** Listens to MongoDB change streams to capture modification events.
- **Supports DML Rollback:** Undoes `Insert`, `Update`, `Delete`, and `Replace` operations using collected pre-images.
- **Test-Focused:** Designed to simplify database state management in integration tests.

## Requirements

- **Nightly Rust Toolchain:** You must use a nightly build of the Rust compiler (`rustup toolchain install nightly`).
