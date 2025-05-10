#![feature(async_drop, impl_trait_in_assoc_type)]

use futures_util::stream::StreamExt;
use mongodb::{
    Client, Database,
    bson::{Document, doc},
    change_stream::event::{ChangeStreamEvent, OperationType},
    options::FullDocumentBeforeChangeType,
};
use std::future::AsyncDrop;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing;

// A data type that collects database changes and automatically undoes them
// asynchronously when dropped within an async context using AsyncDrop.
// Requires nightly Rust and the `async_drop` feature.
struct MongoDBDrop {
    database: Database,
    collected_events: Arc<Mutex<Vec<ChangeStreamEvent<Document>>>>,
    stop_sender: Option<oneshot::Sender<()>>,
    _listener_handle: JoinHandle<()>, // Keep handle to the spawned task
}

impl MongoDBDrop {
    /// Initializes the change stream listener.
    /// Spawns a task to collect changes made within the given database and collection.
    /// Returns a Result because initialization can fail.
    async fn new(database: &Database) -> Result<Self, mongodb::error::Error> {
        // Watch the database
        let stream = database
            .watch()
            // .full_document_before_change(FullDocumentBeforeChangeType::Required)
            .await?;

        let collected_events: Arc<Mutex<Vec<ChangeStreamEvent<Document>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let events_clone = Arc::clone(&collected_events);
        let (stop_sender, mut stop_receiver) = oneshot::channel();
        let stop_sender = Some(stop_sender);
        // Spawn a task to listen for changes and collect them
        let listener_handle = tokio::spawn(async move {
            let mut stream_fused = stream.fuse();
            // let stop_receiver = stop_receiver.fuse();

            loop {
                tokio::select! {
                    event_opt = stream_fused.next() => {

                        match event_opt {
                            Some(Ok(event)) => {
                                println!("Collected change event: {:?}", event.operation_type);
                                let mut events = events_clone.lock().unwrap();
                                events.push(event);
                            }
                            Some(Err(e)) => {
                                eprintln!("Change stream error: {:?}", e);
                                break;
                            }
                            None => {
                                println!("Change stream finished.");
                                break;
                            }
                        }
                    },
                    _ = &mut stop_receiver => {
                        println!("Received stop signal for change stream listener.");
                        break;
                    },
                }
            }
        });

        Ok(MongoDBDrop {
            database: database.clone(),
            collected_events,
            stop_sender,
            _listener_handle: listener_handle,
        })
    }
}

// Implement the experimental AsyncDrop trait
impl AsyncDrop for MongoDBDrop {
    // The future returned by async_drop
    type Dropper<'a> = impl std::future::Future<Output = ()> + 'a; // Use impl Trait for the future type

    // The async method that will be awaited on drop
    fn async_drop(self: Pin<&mut Self>) -> Self::Dropper<'_> {
        // Need to move self out of the Pin to consume it. This requires unsafe,
        // but is the standard way to handle consuming `self` in `async_drop`.
        // Safety: We are consuming the struct `self` here. The future returned
        // must complete before the memory is reused. AsyncDrop ensures this.
        let this = unsafe { Pin::into_inner_unchecked(self) };

        async move {
            println!("Executing async_drop for MongoDBDrop...");

            // Signal the listener task to stop collecting new events
            // Ignore send error if receiver is already dropped
            if let Some(sender) = this.stop_sender.take() {
                if let Err(_) = sender.send(()) {
                    println!("Failed to send stop signal to change stream listener.");
                }
            } else {
                println!("Stop signal already sent or listener not initialized.");
            }
            // Wait briefly for the listener to potentially process the last few events
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Take ownership of the collected events
            let events_to_process = {
                let mut events = this.collected_events.lock().unwrap();
                events.drain(..).collect::<Vec<_>>()
            };

            if events_to_process.is_empty() {
                println!("No changes collected to undo in async_drop.");
                // Optionally wait for the listener task to finish explicitly here
                // this._listener_handle.await.ok();
                return; // Return from the async block (Future output is ())
            }

            println!("Starting MongoDB rollback via async_drop...");

            for event in events_to_process.into_iter() {
                println!("Undoing change event: {:?}", event.operation_type);
                match event.operation_type {
                    OperationType::Insert => {
                        if let Some(document_key) = event.document_key {
                            if let Some(id) = document_key.get("_id") {
                                let filter = doc! { "_id": id.clone() };
                                println!("Undoing Insert: Deleting document with _id: {:?}", id);
                                let collection = this.database.collection::<Document>(
                                    event.ns.unwrap().coll.unwrap().as_str(),
                                );
                                if let Err(e) = collection.delete_one(filter).await {
                                    eprintln!("Error undoing Insert: {:?}", e);
                                }
                            } else {
                                println!("Insert event missing _id in document_key, cannot undo.");
                            }
                        } else {
                            println!("Insert event with no document_key, cannot undo.");
                        }
                    }
                    OperationType::Delete => {
                        if let Some(full_document) = event.full_document_before_change {
                            println!("Undoing Delete: Re-inserting document.");
                            let collection = this
                                .database
                                .collection::<Document>(event.ns.unwrap().coll.unwrap().as_str());
                            if let Err(e) = collection.insert_one(full_document).await {
                                eprintln!("Error undoing Delete: {:?}", e);
                            }
                        } else {
                            println!(
                                "Delete event missing fullDocumentBeforeChange, cannot fully undo delete."
                            );
                            if let Some(document_key) = event.document_key {
                                println!("Document key for un-undoable delete: {:?}", document_key);
                            }
                        }
                    }
                    OperationType::Update => {
                        if let Some(document_key) = event.document_key {
                            if let Some(full_document_before) = event.full_document_before_change {
                                if let Some(id) = document_key.get("_id") {
                                    let filter = doc! { "_id": id.clone() };
                                    println!(
                                        "Undoing Update: Replacing document with pre-update state for _id: {:?}",
                                        id
                                    );
                                    let collection = this.database.collection::<Document>(
                                        event.ns.unwrap().coll.unwrap().as_str(),
                                    );
                                    if let Err(e) =
                                        collection.replace_one(filter, full_document_before).await
                                    {
                                        eprintln!("Error undoing Update: {:?}", e);
                                    }
                                } else {
                                    println!(
                                        "Update event missing _id in document_key, cannot undo."
                                    );
                                }
                            } else {
                                println!(
                                    "Update event missing fullDocumentBeforeChange, cannot fully undo update for key: {:?}",
                                    document_key
                                );
                            }
                        } else {
                            println!("Update event with no document_key, cannot undo.");
                        }
                    }
                    OperationType::Replace => {
                        if let Some(document_key) = event.document_key {
                            if let Some(full_document_before) = event.full_document_before_change {
                                if let Some(id) = document_key.get("_id") {
                                    let filter = doc! { "_id": id.clone() };
                                    println!(
                                        "Undoing Replace: Replacing document with pre-replace state for _id: {:?}",
                                        id
                                    );
                                    let collection = this.database.collection::<Document>(
                                        event.ns.unwrap().coll.unwrap().as_str(),
                                    );
                                    if let Err(e) =
                                        collection.replace_one(filter, full_document_before).await
                                    {
                                        eprintln!("Error undoing Replace: {:?}", e);
                                    }
                                } else {
                                    println!(
                                        "Replace event missing _id in document_key, cannot undo."
                                    );
                                }
                            } else {
                                println!(
                                    "Replace event missing fullDocumentBeforeChange, cannot fully undo replace for key: {:?}",
                                    document_key
                                );
                            }
                        } else {
                            println!("Replace event with no document_key, cannot undo.");
                        }
                    }
                    op_type => {
                        println!(
                            "Unhandled change stream operation type during async_drop: {:?}",
                            op_type
                        );
                    }
                }
            }
            println!("MongoDB rollback via async_drop finished.");
        }
    }
}

// Note: You **do not** implement the synchronous `std::ops::Drop` trait when
// implementing `std::future::AsyncDrop`. The `AsyncDrop` trait replaces the
// synchronous drop glue in async contexts. If the type is dropped in a synchronous
// context, the `async_drop` method will NOT be executed.

// --- Example Usage in a Test ---

// Assume get_client() exists and returns Result<Client, Error>
// Assume tracing is initialized

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn my_test_with_asyncdrop() -> Result<(), Box<dyn std::error::Error>> {
        // Ensure you are using a nightly toolchain and have enabled the feature
        // by adding `#![feature(async_drop)]` at the top of your crate root (main.rs or lib.rs).
        // You also need to run this test with `cargo +nightly test`.
        let mongodb_client = get_client().await?;

        let database_name = "freecodecamp"; // Your test database name
        let collection_name = "my_test_collection"; // Your test collection name

        // Create the rollback guard. Use ? to propagate potential errors during setup.
        // The 'guard' variable's lifetime defines the scope for rollback.
        let _guard = MongoDBDrop::new(&mongodb_client.database(database_name)).await?;
        println!("MongoDB rollback guard created using AsyncDrop.");

        // --- All your fallible test operations go here ---
        // The 'guard' will automatically trigger async_drop when it goes out of scope,
        // whether the async function completes successfully or returns early due to '?'.
        println!("Starting test body operations...");
        let db = mongodb_client.database(database_name);
        let collection = db.collection::<Document>(collection_name);

        // Example operations:
        collection
            .insert_one(doc! {"_id": 1, "data": "initial"})
            .await?;
        println!("Inserted document with _id: 1");

        collection
            .update_one(doc! {"_id": 1}, doc! {"$set": {"data": "updated"}})
            .await?;
        println!("Updated document with _id: 1");

        // Simulate an error during test execution to see async_drop on failure
        // eprintln!("Simulating test error!");
        // return Err(Box::<dyn std::error::Error>::from("Simulated test failure"));

        collection.delete_one(doc! {"_id": 1}).await?;
        println!("Deleted document with _id: 1");

        // Add more test operations here...
        // collection.insert_one(doc!{"_id": 2, "data": "another"}, None).await?;
        // ... etc.

        println!("Test body operations complete.");

        // When `my_test_with_asyncdrop` finishes (either by returning Ok(()) or Err)
        // the `guard` variable goes out of scope. Since `my_test_with_asyncdrop` is
        // an async function, the `async_drop` method of `MongoDBDrop` will be awaited.

        Ok(())
    }
}

// Placeholder get_client function
async fn get_client() -> Result<Client, mongodb::error::Error> {
    // Replace with your actual connection string
    Client::with_uri_str("mongodb://127.0.0.1:27017/freecodecamp?directConnection=true").await
}

// You would need to add tracing initialization in your test runner setup
// For example:
// #[cfg(test)]
// pub fn setup() {
//     let filter = tracing_subscriber::EnvFilter::from_default_env()
//         .add_directive(tracing::Level::INFO.into());
//     tracing_subscriber::fmt()
//         .with_env_filter(filter)
//         .init();
// }
