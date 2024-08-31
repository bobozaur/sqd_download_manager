use std::{
    ffi::OsStr,
    fmt::UpperHex,
    ops::Range,
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
};

use futures::FutureExt;
use reqwest::{Client, Url};
use tokio::{fs::File, io::AsyncWriteExt, task::JoinSet};

use crate::{
    chunk_metadata::DataChunkMetadata,
    chunk_tracker::ChunkTracker,
    error::{
        DataChunkError, DownloadManagerError, DownloadManagerResult, InvalidHexIdError, TaskError,
    },
    ChunkId, DataChunk, DataChunkRef, DataManager, DatasetId,
};

pub type BackgroundTasks = Arc<Mutex<JoinSet<DownloadManagerResult<()>>>>;

#[derive(Debug, Clone)]
pub struct DownloadManager {
    /// Client used for HTTP requests when downloading chunks.
    http_client: Client,
    /// Base path where chunks are stored.
    base_path: Arc<Path>,
    /// Chunk tracker that holds maps to locate chunks.
    chunk_tracker: Arc<RwLock<ChunkTracker>>,
    /// Background tasks that the download manager is responsible for.
    tasks: BackgroundTasks,
}

impl DownloadManager {
    pub const FILENAME_PART_SEPARATOR: &'static str = "_";
    pub const DELETE_CHUNK_MARKER: &'static str = "DELETE";
    pub const INCOMPLETE_CHUNK_MARKER: &'static str = "INCOMPLETE";

    /// Creates a new [`DownloadManager`] that stores chunks at the given `base_path`.
    ///
    /// The manager will by default attempt to retrieve it's state and resume where it left off.
    /// Pass [`Some(false)`] to prevent that.
    #[tracing::instrument(fields(base_path = format_args!("{}", base_path.as_ref().display())), err(Debug))]
    pub async fn new<P>(base_path: &P, resume: Option<bool>) -> DownloadManagerResult<Self>
    where
        P: AsRef<Path>,
    {
        // While I'd generally be against asking for a reference that gets converted to an owned
        // datatype, in situations such as this I find it more valuable to make the interface more
        // developer friendly because:
        // - consumers can pass in anything that can act as a [`Path`]
        // - we only convert the reference to an owned type once, on this type's construction, and
        //   my assumption is that a [`DataManager`] instance is something that's meant to be
        //   long-lived.
        let base_path = Arc::from(base_path.as_ref());
        let http_client = Client::new();
        let chunk_tracker = Arc::new(RwLock::new(ChunkTracker::default()));
        let tasks = Arc::new(Mutex::new(JoinSet::new()));

        let mut manager = Self {
            http_client,
            base_path,
            chunk_tracker,
            tasks,
        };

        if resume.unwrap_or(true) {
            manager.restore().await?;
        }

        Ok(manager)
    }

    /// Method meant for graceful termination of the [`DownloadManager`] by waiting for all the
    /// background tasks to finish.
    #[allow(clippy::missing_panics_doc)]
    pub async fn wait_bg_tasks(&self) {
        let tasks = std::mem::replace(&mut *self.tasks.lock().unwrap(), JoinSet::new());
        for res in tasks.join_all().await {
            if let Err(e) = res {
                tracing::error!("error when waiting for download tasks: {e}");
            }
        }
    }

    /// Resumes the [`DownloadManager`] from its previous state by reading the chunks stored at
    /// the given `base_path`.
    #[tracing::instrument(skip(self), err(Debug))]
    async fn restore(&mut self) -> DownloadManagerResult<()> {
        tracing::info!(
            base_path = format_args!("{}", self.base_path.display()),
            "attempting to resume download manager's state"
        );

        // Closure that logs a warning if the chunk directory name is invalid.
        let invalid_dir_fn = |path: &Path| {
            tracing::warn!(
                base_path = format_args!("{}", self.base_path.display()),
                dir = format_args!("{}", path.display()),
                "directory at base path is not a chunk",
            );
        };

        let mut dirs = tokio::fs::read_dir(&self.base_path).await?;

        // Read each chunk directory from the base path.
        while let Some(dir) = dirs.next_entry().await? {
            let dir_path = dir.path();
            tracing::info!("encountered dir: {}", dir_path.display());

            let mut should_delete = false;

            let Some(dir_name) = dir_path.file_name().and_then(OsStr::to_str) else {
                invalid_dir_fn(&dir_path);
                continue;
            };

            // Parse the chunk directory name and add to chunk tracker or delete it if it was marked
            // for deletion.
            let mut parts = dir_name.split(Self::FILENAME_PART_SEPARATOR);
            let mut next_part_fn = || {
                parts
                    .next()
                    .ok_or_else(|| DataChunkError::InvalidDirName(dir_name.to_owned()))
            };

            let dataset_id =
                Self::hex_id_to_bytes(next_part_fn()?).map_err(DataChunkError::DatasetId)?;
            let id = Self::hex_id_to_bytes(next_part_fn()?).map_err(DataChunkError::ChunkId)?;
            let start_block =
                u64::from_str_radix(next_part_fn()?, 16).map_err(DataChunkError::BlockRange)?;
            let end_block =
                u64::from_str_radix(next_part_fn()?, 16).map_err(DataChunkError::BlockRange)?;

            match (parts.next(), parts.next()) {
                // No other parts in the name means the chunk directory is valid.
                (None, None) => (),
                // This means only one more part is present, particularly one that signals the chunk
                // was marked for deletion.
                (Some(s), None) if s == Self::DELETE_CHUNK_MARKER => should_delete = true,
                // This means only one more part is present, particularly one that signals the chunk
                // was being downloaded but the download did not complete.
                (Some(s), None) if s == Self::INCOMPLETE_CHUNK_MARKER => should_delete = true,
                // Erroring out here could be another option, but that would prevent the
                // [`DownloadManager`] from starting without manual intervention.
                //
                // Conversely, simply deleting this directory seems like too extreme of a
                // measure since we don't know what it contains and prevents analysis of how it got
                // here.
                _ => {
                    invalid_dir_fn(&dir_path);
                    continue;
                }
            }

            // Delete the chunk if necessary.
            if should_delete {
                tracing::info!("deleting malformed chunk: {}", dir_path.display());
                tokio::fs::remove_dir_all(dir_path).await?;
                continue;
            }

            // Otherwise the chunk is complete, so store it in the tracker.
            tracing::info!("tracking chunk: {}", dir_path.display());

            let block_range = start_block..end_block;
            let can_delete = Arc::new(AtomicBool::new(false));

            let chunk_metadata = DataChunkMetadata::new(
                id,
                dataset_id,
                block_range.clone(),
                dir_path.into(),
                self.tasks.clone(),
                can_delete,
            );

            self.chunk_tracker.write().unwrap().add_chunk(
                dataset_id,
                block_range,
                id,
                chunk_metadata,
            );
        }

        Ok(())
    }

    /// Method that downloads a data chunk.
    ///
    /// It creates the chunk directory and then concurrently downloads the chunk files.
    #[tracing::instrument(skip(http_client, chunk_tracker), err(Debug))]
    async fn _download_chunk(
        http_client: Client,
        chunk_file_info: ChunkFileInfo,
        chunk_tracker: Arc<RwLock<ChunkTracker>>,
        manager_tasks: BackgroundTasks,
        can_delete: Arc<AtomicBool>,
    ) -> DownloadManagerResult<()> {
        let ChunkFileInfo {
            id,
            dataset_id,
            block_range,
            tmp_dir_path,
            dir_path,
            download_details,
        } = chunk_file_info;

        // Create chunk directory, where we'll store the files.
        tracing::info!("creating chunk directory");

        tokio::fs::create_dir(&tmp_dir_path).await?;

        let mut join_set = JoinSet::new();

        // Spawn all file downloads concurrently.
        for (file_path, url) in download_details {
            let future = Self::download_file(http_client.clone(), file_path, url);
            join_set.spawn(future);
        }

        // Store the overall download result.
        let mut download_res = Ok(());

        while let Some(res) = join_set.join_next().await {
            // If an error occurred, abort the other file download tasks and propagate it.
            if let Err(e) = res.map_err(TaskError::JoinTask).and_then(|res| res) {
                tracing::error!("download error occurred: {e}");

                // If this is the first error, then store it and abort the other tasks.
                if download_res.is_ok() {
                    join_set.abort_all();
                    download_res = Err(e);
                }
            }
        }

        // If download failed we should purge the chunk directory.
        if let Err(e) = download_res {
            tracing::info!("removing chunk directory because download failed");
            tokio::fs::remove_dir_all(&tmp_dir_path).await?;
            return Err(e)?;
        }

        // Rename the chunk dir to reflect that it is now complete.
        tokio::fs::rename(tmp_dir_path, &dir_path).await?;

        // Otherwise track the chunk.
        let chunk_metadata = DataChunkMetadata::new(
            id,
            dataset_id,
            block_range.clone(),
            dir_path,
            manager_tasks,
            can_delete,
        );

        chunk_tracker
            .write()
            .unwrap()
            .add_chunk(dataset_id, block_range, id, chunk_metadata);

        Ok(())
    }

    /// Method that downloads a single chunk file.
    #[tracing::instrument(skip(http_client), err(Debug))]
    async fn download_file(
        http_client: Client,
        file_path: PathBuf,
        url: Url,
    ) -> Result<(), TaskError> {
        let mut response = http_client.get(url).send().await?;
        let mut file = File::create_new(&file_path).await?;

        while let Some(data) = response.chunk().await? {
            file.write_all(&data).await?;
        }

        Ok(())
    }

    /// Method used for evicting finished background tasks so as not to uncontrollably grow within
    /// the [`DownloadManager`].
    ///
    /// This is achieved by polling the [`JoinSet`] only once, continuing to do so until a
    /// background task would pend.
    fn evict_finished_tasks(&self) {
        let mut tasks = self.tasks.lock().unwrap();
        while let Some(res) = tasks.join_next().now_or_never().flatten() {
            let res = res
                .map_err(TaskError::JoinTask)
                .map_err(DownloadManagerError::BackgroundTask)
                .and_then(|res| res);

            if let Err(e) = res {
                tracing::error!("background task finished with error: {e}");
            }
        }
    }

    fn hex_id_to_bytes(s: &str) -> Result<[u8; 32], InvalidHexIdError> {
        // Error out early if length is not exactly what we expect
        if s.len() != 64 {
            return Err(InvalidHexIdError::too_short(s.to_owned()));
        }

        let mut arr = [0; 32];

        for (i, b) in arr.iter_mut().enumerate() {
            let idx = i * 2;
            let hex = s
                .get(idx..idx + 2)
                // Technically redundant, since we checked the length,
                // but we might as well just repeat the error handling.
                .ok_or_else(|| InvalidHexIdError::too_short(s.to_owned()))?;

            *b = u8::from_str_radix(hex, 16)
                .map_err(|e| InvalidHexIdError::hex_byte(s.to_owned(), e))?;
        }

        Ok(arr)
    }
}

impl DataManager for DownloadManager {
    fn download_chunk(&self, chunk: &DataChunk) {
        // Check to see if any background task has finished and evict it if so.
        self.evict_finished_tasks();

        // This chunk dir naming scheme allows us to restore the chunk tracker's state.
        let dir_name = format!(
            "{dataset_id:02X}{sep}{chunk_id:02X}{sep}{start_block}{sep}{end_block}",
            sep = DownloadManager::FILENAME_PART_SEPARATOR,
            dataset_id = UpperHexArray(chunk.dataset_id),
            chunk_id = UpperHexArray(chunk.id),
            start_block = chunk.block_range.start,
            end_block = chunk.block_range.end
        );

        let dir_path: Arc<Path> = self.base_path.join(dir_name).into();
        let mut tmp_dir_path = dir_path.as_os_str().to_owned();
        tmp_dir_path.push(Self::FILENAME_PART_SEPARATOR);
        tmp_dir_path.push(Self::INCOMPLETE_CHUNK_MARKER);
        let tmp_dir_path: PathBuf = tmp_dir_path.into();

        // Closure that parses the file name and URL and generates both the temporary as well as
        // final file.
        let chunk_files_fn = |(file, url): (&String, &String)| {
            let Ok(url) = url.parse() else {
                return None;
            };

            let file_path = tmp_dir_path.join(file);
            Some((file_path, url))
        };

        let download_details: Vec<_> = chunk.files.iter().filter_map(chunk_files_fn).collect();

        if download_details.is_empty() {
            tracing::warn!("chunk file list is empty or invalid");
            return;
        }

        let chunk_file_info = ChunkFileInfo {
            id: chunk.id,
            dataset_id: chunk.dataset_id,
            block_range: chunk.block_range.clone(),
            tmp_dir_path,
            dir_path,
            download_details,
        };

        let http_client = self.http_client.clone();
        let chunk_tracker = self.chunk_tracker.clone();
        let manager_tasks = self.tasks.clone();
        let can_delete = Arc::new(AtomicBool::new(false));

        // Start the chunk download in the background
        let future = Self::_download_chunk(
            http_client,
            chunk_file_info,
            chunk_tracker,
            manager_tasks,
            can_delete,
        );

        self.tasks.lock().unwrap().spawn(future);
    }

    fn list_chunks(&self) -> Vec<ChunkId> {
        self.chunk_tracker
            .read()
            .unwrap()
            .chunk_map
            .keys()
            .copied()
            .collect()
    }

    fn find_chunk(&self, dataset_id: DatasetId, block_number: u64) -> Option<impl DataChunkRef> {
        let chunk_tracker = self.chunk_tracker.read().unwrap();

        chunk_tracker
            .dataset_map
            .get(&dataset_id)
            .and_then(|m| m.get(&block_number))
            .and_then(|id| chunk_tracker.chunk_map.get(id))
            .cloned()
    }

    fn delete_chunk(&self, chunk_id: ChunkId) {
        // Check to see if any background task has finished and evict it if so.
        self.evict_finished_tasks();
        self.chunk_tracker.write().unwrap().remove_chunk(chunk_id);
    }
}

/// Helper struct for function arguments locality.
#[derive(Debug)]
struct ChunkFileInfo {
    id: ChunkId,
    dataset_id: DatasetId,
    block_range: Range<u64>,
    tmp_dir_path: PathBuf,
    dir_path: Arc<Path>,
    download_details: Vec<(PathBuf, Url)>,
}

struct UpperHexArray([u8; 32]);

impl UpperHex for UpperHexArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02X}")?;
        }

        Ok(())
    }
}
