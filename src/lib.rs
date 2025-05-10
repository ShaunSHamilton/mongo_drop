//! # MongoDrop
//!
//! A Rust library that provides an experimental `AsyncDrop` implementation for MongoDB change streams.
//!
//! This library allows you to collect changes made to a MongoDB database and automatically
//! undo them when the `MongoDrop` instance is dropped. It uses the `async_drop` feature
//! to ensure that the undo operations are performed asynchronously.
//!
//! ## Features
//!
//! - `tracing`: Enables tracing for logging events.
//!
//! ## Usage
//!
//! ```
//! #[tokio::test]
//! async fn test_mongo_drop() {
//!     // Initialize MongoDB
//!     let client = Client::with_uri_str("mongodb://localhost:27017").await.unwrap();
//!     let database = client.database("test_db");
//!     // Create a MongoDrop guard
//!     let mongo_drop = MongoDrop::new(&database).await.unwrap();
//!
//!     // Perform database operations within the guard
//!     let coll = database.collection("test_collection");
//!     coll.insert_one(doc! { "key": "value" }).await.unwrap();
//!     let record = coll.find_one(doc! {}).await.unwrap();
//!
//!     assert_eq!(record, Some(doc! { "key": "value" }));
//!     // The changes will be rolled back automatically when the guard goes out of scope
//! }
//! ```

#![feature(async_drop, impl_trait_in_assoc_type)]

use futures_util::stream::StreamExt;
use mongodb::{
    Database,
    bson::{Document, doc},
    change_stream::event::{ChangeStreamEvent, OperationType},
};
use std::future::AsyncDrop;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
#[cfg(feature = "tracing")]
use tracing;

/// A data type that collects database changes and automatically undoes them
/// asynchronously when dropped within an async context using AsyncDrop.
/// Requires nightly Rust and the `async_drop` feature.
pub struct MongoDrop {
    database: Database,
    collected_events: Arc<Mutex<Vec<ChangeStreamEvent<Document>>>>,
    stop_sender: Option<oneshot::Sender<()>>,
    _listener_handle: JoinHandle<()>, // Keep handle to the spawned task
}

impl MongoDrop {
    /// Initializes the change stream listener.
    /// Spawns a task to collect changes made within the given database and collection.
    /// Returns a Result because initialization can fail.
    pub async fn new(database: &Database) -> Result<Self, mongodb::error::Error> {
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
                                #[cfg(feature = "tracing")]
                                tracing::info!("Collected change event: {:?}", event.operation_type);
                                let mut events = events_clone.lock().unwrap();
                                events.push(event);
                            }
                            Some(Err(_e)) => {
                                #[cfg(feature = "tracing")]
                                tracing::error!("Change stream error: {:?}", _e);
                                break;
                            }
                            None => {
                                #[cfg(feature = "tracing")]
                                tracing::info!("Change stream finished.");
                                break;
                            }
                        }
                    },
                    _ = &mut stop_receiver => {
                        #[cfg(feature = "tracing")]
                        tracing::info!("Received stop signal for change stream listener.");
                        break;
                    },
                }
            }
        });

        Ok(MongoDrop {
            database: database.clone(),
            collected_events,
            stop_sender,
            _listener_handle: listener_handle,
        })
    }
}

// Implement the experimental AsyncDrop trait
impl AsyncDrop for MongoDrop {
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
            #[cfg(feature = "tracing")]
            tracing::info!("Executing async_drop for MongoDrop...");

            // Signal the listener task to stop collecting new events
            // Ignore send error if receiver is already dropped
            if let Some(sender) = this.stop_sender.take() {
                if let Err(_) = sender.send(()) {
                    #[cfg(feature = "tracing")]
                    tracing::info!("Failed to send stop signal to change stream listener.");
                }
            } else {
                #[cfg(feature = "tracing")]
                tracing::info!("Stop signal already sent or listener not initialized.");
            }
            // Wait briefly for the listener to potentially process the last few events
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Take ownership of the collected events
            let events_to_process = {
                let mut events = this.collected_events.lock().unwrap();
                events.drain(..).collect::<Vec<_>>()
            };

            if events_to_process.is_empty() {
                #[cfg(feature = "tracing")]
                tracing::info!("No changes collected to undo in async_drop.");
                // Optionally wait for the listener task to finish explicitly here
                // this._listener_handle.await.ok();
                return; // Return from the async block (Future output is ())
            }

            #[cfg(feature = "tracing")]
            tracing::info!("Starting MongoDB rollback via async_drop...");

            for event in events_to_process.into_iter() {
                #[cfg(feature = "tracing")]
                tracing::info!("Undoing change event: {:?}", event.operation_type);
                match event.operation_type {
                    OperationType::Insert => {
                        if let Some(document_key) = event.document_key {
                            if let Some(id) = document_key.get("_id") {
                                let filter = doc! { "_id": id.clone() };
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    "Undoing Insert: Deleting document with _id: {:?}",
                                    id
                                );
                                let collection = this.database.collection::<Document>(
                                    event.ns.unwrap().coll.unwrap().as_str(),
                                );
                                if let Err(_e) = collection.delete_one(filter).await {
                                    #[cfg(feature = "tracing")]
                                    tracing::error!("Error undoing Insert: {:?}", _e);
                                }
                            } else {
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    "Insert event missing _id in document_key, cannot undo."
                                );
                            }
                        } else {
                            #[cfg(feature = "tracing")]
                            tracing::info!("Insert event with no document_key, cannot undo.");
                        }
                    }
                    OperationType::Delete => {
                        if let Some(full_document) = event.full_document_before_change {
                            #[cfg(feature = "tracing")]
                            tracing::info!("Undoing Delete: Re-inserting document.");
                            let collection = this
                                .database
                                .collection::<Document>(event.ns.unwrap().coll.unwrap().as_str());
                            if let Err(_e) = collection.insert_one(full_document).await {
                                #[cfg(feature = "tracing")]
                                tracing::error!("Error undoing Delete: {:?}", _e);
                            }
                        } else {
                            #[cfg(feature = "tracing")]
                            tracing::info!(
                                "Delete event missing fullDocumentBeforeChange, cannot fully undo delete."
                            );
                            if let Some(_document_key) = event.document_key {
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    "Document key for un-undoable delete: {:?}",
                                    _document_key
                                );
                            }
                        }
                    }
                    OperationType::Update => {
                        if let Some(document_key) = event.document_key {
                            if let Some(full_document_before) = event.full_document_before_change {
                                if let Some(id) = document_key.get("_id") {
                                    let filter = doc! { "_id": id.clone() };
                                    #[cfg(feature = "tracing")]
                                    tracing::info!(
                                        "Undoing Update: Replacing document with pre-update state for _id: {:?}",
                                        id
                                    );
                                    let collection = this.database.collection::<Document>(
                                        event.ns.unwrap().coll.unwrap().as_str(),
                                    );
                                    if let Err(_e) =
                                        collection.replace_one(filter, full_document_before).await
                                    {
                                        #[cfg(feature = "tracing")]
                                        tracing::error!("Error undoing Update: {:?}", _e);
                                    }
                                } else {
                                    #[cfg(feature = "tracing")]
                                    tracing::info!(
                                        "Update event missing _id in document_key, cannot undo."
                                    );
                                }
                            } else {
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    "Update event missing fullDocumentBeforeChange, cannot fully undo update for key: {:?}",
                                    document_key
                                );
                            }
                        } else {
                            #[cfg(feature = "tracing")]
                            tracing::info!("Update event with no document_key, cannot undo.");
                        }
                    }
                    OperationType::Replace => {
                        if let Some(document_key) = event.document_key {
                            if let Some(full_document_before) = event.full_document_before_change {
                                if let Some(id) = document_key.get("_id") {
                                    let filter = doc! { "_id": id.clone() };
                                    #[cfg(feature = "tracing")]
                                    tracing::info!(
                                        "Undoing Replace: Replacing document with pre-replace state for _id: {:?}",
                                        id
                                    );
                                    let collection = this.database.collection::<Document>(
                                        event.ns.unwrap().coll.unwrap().as_str(),
                                    );
                                    if let Err(_e) =
                                        collection.replace_one(filter, full_document_before).await
                                    {
                                        #[cfg(feature = "tracing")]
                                        tracing::error!("Error undoing Replace: {:?}", _e);
                                    }
                                } else {
                                    #[cfg(feature = "tracing")]
                                    tracing::info!(
                                        "Replace event missing _id in document_key, cannot undo."
                                    );
                                }
                            } else {
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    "Replace event missing fullDocumentBeforeChange, cannot fully undo replace for key: {:?}",
                                    document_key
                                );
                            }
                        } else {
                            #[cfg(feature = "tracing")]
                            tracing::info!("Replace event with no document_key, cannot undo.");
                        }
                    }
                    _op_type => {
                        #[cfg(feature = "tracing")]
                        tracing::info!(
                            "Unhandled change stream operation type during async_drop: {:?}",
                            _op_type
                        );
                    }
                }
            }
            #[cfg(feature = "tracing")]
            tracing::info!("MongoDB rollback via async_drop finished.");
        }
    }
}
