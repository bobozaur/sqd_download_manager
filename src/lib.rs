// #################################
// ##########             ##########
// ########## Custom code ##########
// ##########             ##########
// #################################

mod chunk_metadata;
mod chunk_tracker;
pub mod error;
mod manager;

#[rustfmt::skip]
pub use manager::DownloadManager;
#[rustfmt::skip]
pub use chunk_metadata::DataChunkMetadata;

// ###################################
// ##########               ##########
// ########## Provided code ##########
// ##########               ##########
// ###################################

use std::{collections::HashMap, ops::Range, path::Path};

pub type DatasetId = [u8; 32];
pub type ChunkId = [u8; 32];

/// data chunk description
pub struct DataChunk {
    id: ChunkId,
    /// Dataset (blockchain) id
    dataset_id: DatasetId,
    /// Block range this chunk is responsible for
    block_range: Range<u64>,
    /// Data chunk files.
    /// A mapping between file names and HTTP URLs to download files from
    files: HashMap<String, String>,
}

pub trait DataManager: Send + Sync {
    /// Download `chunk` in background and make it available for querying
    fn download_chunk(&self, chunk: &DataChunk);
    // List chunks, that are currently available
    fn list_chunks(&self) -> Vec<ChunkId>;
    /// Find a chunk from a given dataset, that is responsible for `block_number`.
    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<impl DataChunkRef>;
    /// Schedule data chunk for deletion in background
    fn delete_chunk(&self, chunk_id: [u8; 32]);
}

// Data chunk must remain available and untouched till this reference is not dropped
pub trait DataChunkRef: Send + Sync {
    // Data chunk directory
    fn path(&self) -> &Path;
}
