use mockito::{Mock, Server, ServerGuard};
use sqd_download_manager::{DataChunk, DataChunkRef, DataManager, DownloadManager};
use tempdir::TempDir;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

const NUM_BLOCKS: u64 = 10;
const DEFAULT_ID: [u8; 32] = [0; 32];
const CHUNK_REF_BLOCK: u64 = 5;
const TEMP_DIR_PREFIX: &str = "sqd_download_manager";

pub struct TestSetup {
    pub server: ServerGuard,
    pub temp_dir: TempDir,
    pub mock: Mock,
    pub manager: DownloadManager,
}

impl TestSetup {
    #[allow(clippy::missing_panics_doc)]
    pub async fn new() -> Self {
        Self::init_logger();

        let temp_dir = TempDir::new(TEMP_DIR_PREFIX).unwrap();
        tracing::info!("test directory {}", temp_dir.as_ref().display());

        let mut server = Server::new_async().await;

        #[allow(clippy::cast_possible_truncation)]
        let mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_body(vec![0; 1])
            .expect(NUM_BLOCKS as usize)
            .create_async()
            .await;

        let manager = DownloadManager::new(&temp_dir, None).await.unwrap();

        Self {
            server,
            temp_dir,
            mock,
            manager,
        }
    }

    fn init_logger() {
        tracing_subscriber::fmt()
            // .with_span_events(FmtSpan::ACTIVE)
            .with_max_level(LevelFilter::DEBUG)
            .finish()
            .try_init()
            .ok();
    }
}

#[tokio::test]
async fn test_chunk_download() {
    let setup = TestSetup::new().await;

    let block_range = 0..NUM_BLOCKS;
    let files = (0..NUM_BLOCKS)
        .map(|i| (i.to_string(), setup.server.url()))
        .collect();
    let data_chunk = DataChunk::new(DEFAULT_ID, DEFAULT_ID, block_range, files);

    // New manager should contain no chunks.
    assert!(setup.manager.list_chunks().is_empty());

    setup.manager.download_chunk(&data_chunk);
    setup.manager.wait_bg_tasks().await;
    setup.mock.assert_async().await;

    // We should have a chunk now.
    assert_eq!(setup.manager.list_chunks().len(), 1);

    let chunk_ref = setup
        .manager
        .find_chunk(DEFAULT_ID, CHUNK_REF_BLOCK)
        .unwrap();

    // Schedule chunk for deletion.
    setup.manager.delete_chunk(DEFAULT_ID);

    // The chunk dir should still be accessible
    let mut chunk_dir = tokio::fs::read_dir(chunk_ref.path()).await.unwrap();
    let mut num_files = 0;

    while let Some(file) = chunk_dir.next_entry().await.unwrap() {
        tokio::fs::read(file.path()).await.unwrap();
        num_files += 1;
    }

    // Check that the same number of files are present in the chunk dir.
    assert_eq!(num_files, NUM_BLOCKS);

    // Although deletion is not yet performed because we still hold a reference to the chunk,
    // finding it fails because the chunk is awaiting deletion.
    assert!(setup
        .manager
        .find_chunk(DEFAULT_ID, CHUNK_REF_BLOCK)
        .is_none());

    let chunk_dir = chunk_ref.path().to_owned();

    // Check that the chunk directory still exists though.
    assert!(tokio::fs::try_exists(&chunk_dir).await.unwrap());

    // Deletion will take place after this.
    drop(chunk_ref);
    setup.manager.wait_bg_tasks().await;

    // Check that the chunk directory is now indeed removed.
    assert!(!tokio::fs::try_exists(chunk_dir).await.unwrap());
}

#[tokio::test]
async fn test_chunk_resume() {
    let mut setup = TestSetup::new().await;

    let block_range = 0..NUM_BLOCKS;
    let files = (0..NUM_BLOCKS)
        .map(|i| (i.to_string(), setup.server.url()))
        .collect();
    let data_chunk = DataChunk::new(DEFAULT_ID, DEFAULT_ID, block_range, files);

    // New manager should contain no chunks.
    assert!(setup.manager.list_chunks().is_empty());

    setup.manager.download_chunk(&data_chunk);
    setup.manager.wait_bg_tasks().await;
    setup.mock.assert_async().await;

    // We should have a chunk now.
    assert_eq!(setup.manager.list_chunks().len(), 1);

    let chunk_dir_path = setup
        .manager
        .find_chunk(DEFAULT_ID, CHUNK_REF_BLOCK)
        .unwrap()
        .path()
        .to_owned();

    let mut chunk_dir = tokio::fs::read_dir(&chunk_dir_path).await.unwrap();

    while let Some(file) = chunk_dir.next_entry().await.unwrap() {
        let bytes = tokio::fs::read(file.path()).await.unwrap();
        tracing::debug!("{bytes:?}");
    }

    let chunks = setup.manager.list_chunks();

    // Replace the download manager
    setup.manager = DownloadManager::new(&setup.temp_dir, None).await.unwrap();

    let mut chunk_dir = tokio::fs::read_dir(&chunk_dir_path).await.unwrap();

    while let Some(file) = chunk_dir.next_entry().await.unwrap() {
        let bytes = tokio::fs::read(file.path()).await.unwrap();
        tracing::debug!("{bytes:?}");
    }

    // Check that the restore was successful.
    assert_eq!(chunks, setup.manager.list_chunks());
}

#[tokio::test]
async fn test_chunk_not_resuming() {
    let mut setup = TestSetup::new().await;

    let block_range = 0..NUM_BLOCKS;
    let files = (0..NUM_BLOCKS)
        .map(|i| (i.to_string(), setup.server.url()))
        .collect();
    let data_chunk = DataChunk::new(DEFAULT_ID, DEFAULT_ID, block_range, files);

    // New manager should contain no chunks.
    assert!(setup.manager.list_chunks().is_empty());

    setup.manager.download_chunk(&data_chunk);
    setup.manager.wait_bg_tasks().await;
    setup.mock.assert_async().await;

    // We should have a chunk now.
    assert_eq!(setup.manager.list_chunks().len(), 1);

    let chunk_dir_path = setup
        .manager
        .find_chunk(DEFAULT_ID, CHUNK_REF_BLOCK)
        .unwrap()
        .path()
        .to_owned();

    // Replace the download manager
    setup.manager = DownloadManager::new(&setup.temp_dir, Some(false))
        .await
        .unwrap();

    // Check that the chunk is still present.
    let mut chunk_dir = tokio::fs::read_dir(&chunk_dir_path).await.unwrap();

    while let Some(file) = chunk_dir.next_entry().await.unwrap() {
        let bytes = tokio::fs::read(file.path()).await.unwrap();
        tracing::debug!("{bytes:?}");
    }

    // Check that the restore did not take place.
    assert!(setup.manager.list_chunks().is_empty());
}
