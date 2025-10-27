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
//! #[cfg(test)]
//! mod tests {
//!     use mongodb::{Client, options::ClientOptions};
//!     use mongo_drop::MongoDrop;
//!
//!     #[tokio::test]
//!     async fn trivial_mongo_drop_insert() {
//!         // Initialize MongoDB
//!         let client = Client::with_uri_str("mongodb://localhost:27017").await.unwrap();
//!         let database = client.database("mongo_drop_db");
//!         let coll = database.collection("insert_collection");
//!         {
//!           // Create a MongoDrop guard
//!           let _guard = MongoDrop::new(&database).await.unwrap();
//!
//!           // Perform database operations within the guard
//!           coll.insert_one(doc! { "key": "value" }).await.unwrap();
//!           let record = coll.find_one(doc! {}).await.unwrap();
//!
//!           assert_eq!(record, Some(doc! { "key": "value" }));
//!           // The changes will be rolled back automatically when the guard goes out of scope
//!         }
//!         // After the guard is dropped, verify that the changes were rolled back
//!         let record = coll.find_one(doc! {}).await.unwrap();
//!         assert_eq!(record, None);
//!     }
//!
//!     #[tokio::test]
//!     async fn deletes() -> Result<(), Box<dyn std::error::Error>> {
//!         let mongodb_client = get_client().await?;
//!
//!         let database_name = "mongo_drop_db";
//!         let db = mongodb_client.database(database_name);
//!         let collection = create_collection(&db, "delete").await?;
//!         // Insert a document to delete
//!         let d = collection.insert_one(doc! { "value": "to_delete"}).await?;
//!
//!         {
//!             let _guard = MongoDrop::new(&db).await?;
//!
//!             // Delete the document
//!             collection.delete_one(doc! {"_id": &d.inserted_id}).await?;
//!             // Verify deletion
//!             let deleted_doc = collection.find_one(doc! {"_id": &d.inserted_id}).await?;
//!             assert!(deleted_doc.is_none());
//!         }
//!
//!         // After drop, verify document is restored
//!         let deleted_doc = collection.find_one(doc! {"_id": &d.inserted_id}).await?;
//!         assert!(deleted_doc.is_some());
//!
//!         Ok(())
//!     }
//!
//!     async fn get_client() -> Result<Client, mongodb::error::Error> {
//!         Client::with_uri_str("mongodb://127.0.0.1:27017/mongo_drop_db?directConnection=true").await
//!     }
//!
//!     async fn create_collection(
//!         db: &Database,
//!         name: &str,
//!     ) -> Result<mongodb::Collection<Document>, mongodb::error::Error> {
//!         // Delete existing collection if it exists
//!         let _ = db.collection::<Document>(name).drop().await;
//!
//!         // Delete, Update, and Replace operations require collections to be created with pre-images enabled
//!         let options = mongodb::options::CreateCollectionOptions::builder()
//!             .change_stream_pre_and_post_images(
//!                 ChangeStreamPreAndPostImages::builder()
//!                     .enabled(true)
//!                     .build(),
//!             )
//!             .build();
//!         let _ = db.create_collection(name).with_options(options).await?;
//!         let collection = db.collection::<Document>(name);
//!         Ok(collection)
//!     }
//! }
//! ```

#![allow(incomplete_features)]
#![feature(async_drop, impl_trait_in_assoc_type)]

use futures_util::stream::StreamExt;
use mongodb::{
    Database,
    bson::{Document, doc},
    change_stream::event::{ChangeStreamEvent, OperationType},
    options::FullDocumentBeforeChangeType,
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

// Explicitly implement Unpin to allow get_mut() in AsyncDrop
impl Unpin for MongoDrop {}

impl MongoDrop {
    /// Initializes the change stream listener.
    /// Spawns a task to collect changes made within the given database and collection.
    /// Returns a Result because initialization can fail.
    pub async fn new(database: &Database) -> Result<Self, mongodb::error::Error> {
        // Watch the database
        let stream = database
            .watch()
            .full_document_before_change(FullDocumentBeforeChangeType::WhenAvailable)
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
    // The async method that will be awaited on drop
    async fn drop(self: Pin<&mut Self>) {
        #[cfg(feature = "tracing")]
        tracing::info!("Executing async_drop for MongoDrop...");

        // Get a mutable reference to the struct without taking ownership
        let this = self.get_mut();

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
            return;
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
                            tracing::info!("Undoing Insert: Deleting document with _id: {:?}", id);
                            let collection = this
                                .database
                                .collection::<Document>(event.ns.unwrap().coll.unwrap().as_str());
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
                        // UpdateOne operation is used, because InsertOne fails with _id_ duplicate index error
                        if let Err(_e) = collection
                            .update_one(
                                doc! {"_id": full_document.get_object_id("_id").unwrap()},
                                doc! {"$set": full_document},
                            )
                            .upsert(true)
                            .await
                        {
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

impl Drop for MongoDrop {
    fn drop(&mut self) {
        #[cfg(feature = "tracing")]
        tracing::info!("Executing sync drop for MongoDrop - stopping change stream listener");

        // Signal the listener task to stop collecting new events
        // This is a best-effort cleanup - we can't perform async rollback in sync Drop
        // Check if stop_sender is still available (might have been taken by AsyncDrop)
        if let Some(sender) = self.stop_sender.take() {
            if let Err(_) = sender.send(()) {
                #[cfg(feature = "tracing")]
                tracing::info!("Failed to send stop signal to change stream listener in sync drop");
            }
        } else {
            #[cfg(feature = "tracing")]
            tracing::info!("Stop signal already sent (possibly by AsyncDrop)");
        }

        #[cfg(feature = "tracing")]
        tracing::warn!(
            "MongoDrop dropped in sync context - database changes will NOT be rolled back. Use in async context for automatic rollback."
        );
    }
}
