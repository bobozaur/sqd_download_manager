use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
    sync::Arc,
};

use rangemap::RangeMap;

use crate::{chunk_metadata::DataChunkMetadata, ChunkId, DatasetId};

#[derive(Default, Debug)]
pub struct ChunkTracker {
    pub chunk_map: HashMap<ChunkId, Arc<DataChunkMetadata>>,
    pub dataset_map: BTreeMap<DatasetId, RangeMap<u64, ChunkId>>,
}

impl ChunkTracker {
    #[tracing::instrument(skip(self))]
    pub fn add_chunk(
        &mut self,
        dataset_id: DatasetId,
        block_range: Range<u64>,
        chunk_id: ChunkId,
        chunk_metadata: DataChunkMetadata,
    ) {
        tracing::info!("adding chunk to tracker");

        self.chunk_map.insert(chunk_id, Arc::new(chunk_metadata));
        self.dataset_map
            .entry(dataset_id)
            .or_default()
            .insert(block_range, chunk_id);
    }

    #[tracing::instrument(skip(self))]
    pub fn remove_chunk(&mut self, chunk_id: ChunkId) {
        tracing::info!("removing chunk from tracker");

        // Remove the chunk metadata.
        // This reference is keeping the chunk alive when no other references are held.
        // When the last reference to this chunk is dropped, the chunk deletion will be triggered.
        if let Some(metadata) = self.chunk_map.remove(&chunk_id) {
            // Delete the chunk ID from the dataset block range map.
            if let Some(m) = self.dataset_map.get_mut(metadata.dataset_id()) {
                m.remove(metadata.block_range().clone());
            }

            metadata.enable_deletion();
        }
    }
}
