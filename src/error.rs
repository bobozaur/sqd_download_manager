use std::io::Error as IoError;

use base64::DecodeSliceError;
use reqwest::Error as ReqError;
use thiserror::Error as ThisError;
use tokio::task::JoinError;

pub type DownloadManagerResult<T> = Result<T, DownloadManagerError>;

#[derive(Debug, ThisError)]
pub enum DownloadManagerError {
    #[error("IO error: {0}")]
    Io(#[from] IoError),
    #[error("chunk download error: {0}")]
    BackgroundTask(#[from] TaskError),
    #[error("data chunk error: {0}")]
    DataChunk(#[from] DataChunkError),
}

#[derive(Debug, ThisError)]
pub enum TaskError {
    #[error("download request error: {0}")]
    DownloadRequest(#[from] ReqError),
    #[error("download IO error: {0}")]
    Io(#[from] IoError),
    #[error("download task joining error: {0}")]
    JoinTask(#[from] JoinError),
}

#[derive(Debug, ThisError)]
pub enum DataChunkError {
    #[error("invalid chunk directory name: {0}")]
    InvalidDirName(String),
    #[error("chunk ID error: {0}")]
    ChunkId(#[source] DecodeSliceError),
    #[error("dataset ID error: {0}")]
    DatasetId(#[source] DecodeSliceError),
    #[error("error parsing chunk block range: {0}")]
    BlockRange(#[source] DecodeSliceError),
}
