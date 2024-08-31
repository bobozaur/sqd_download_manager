use std::{io::Result as IoResult, ops::Range, path::Path, sync::Arc};

use crate::{manager::DownloadManager, ChunkId, DataChunkRef, DatasetId};

/// Struct representing data chunk metadata. Can be retrieved by passing the chunk ID to
/// [`crate::DataManager::find_chunk`]
#[derive(Debug, PartialEq, Eq)]
pub struct DataChunkMetadata {
    id: ChunkId,
    /// Dataset that this chunk belongs to.
    dataset_id: DatasetId,
    /// The blocks that this chunk holds data for.
    block_range: Range<u64>,
    /// Path to the directory where all chunk files are stored.
    // Wrapped in an `Arc` to merely avoid cloning on dropping this type.
    // It's not like this path should be changing anyway.
    path: Arc<Path>,
}

impl DataChunkMetadata {
    pub fn id(&self) -> &ChunkId {
        &self.id
    }

    pub fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    pub fn block_range(&self) -> &Range<u64> {
        &self.block_range
    }

    // Note that since this is not public API we can
    // afford being stricter about the `path` datatype.
    pub(crate) fn new(
        id: ChunkId,
        dataset_id: DatasetId,
        block_range: Range<u64>,
        path: Arc<Path>,
    ) -> Self {
        Self {
            id,
            dataset_id,
            block_range,
            path,
        }
    }

    /// Method that marks the chunk directory for deletion and deletes it afterwards.
    ///
    /// Marking is done by renaming the directory, which is atomic if moving the file is being
    /// performed on the same local filesystem. (see <https://unix.stackexchange.com/a/178063>).
    /// This helps in case of dropping the future (like an application crash) midway of directory
    /// deletion, so that the [`DownloadManager`] can continue deletion when a new instance is
    /// started.
    #[tracing::instrument(fields(path = format_args!("{}", path.as_ref().display())), err(Debug))]
    async fn delete_chunk<P>(path: P) -> IoResult<()>
    where
        P: AsRef<Path>,
    {
        tracing::info!("performing chunk deletion");

        let mut new_path = path.as_ref().as_os_str().to_owned();
        new_path.push(DownloadManager::FILENAME_PART_SEPARATOR);
        new_path.push(DownloadManager::DELETE_CHUNK_MARKER);

        tokio::fs::rename(&path, &new_path).await?;
        tokio::fs::remove_dir_all(new_path).await
    }
}

impl DataChunkRef for Arc<DataChunkMetadata> {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DataChunkMetadata {
    fn drop(&mut self) {
        let future = Self::delete_chunk(self.path.clone());
        tokio::spawn(future);
    }
}
