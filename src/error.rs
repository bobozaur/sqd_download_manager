use std::{io::Error as IoError, num::ParseIntError};

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
    ChunkId(#[source] InvalidHexIdError),
    #[error("dataset ID error: {0}")]
    DatasetId(#[source] InvalidHexIdError),
    #[error("error parsing chunk block range: {0}")]
    BlockRange(#[source] ParseIntError),
}

#[derive(Debug, ThisError)]
#[error("invalid HEX ID {id}")]
pub struct InvalidHexIdError {
    id: String,
    #[source]
    kind: IdErrorKind,
}

impl InvalidHexIdError {
    pub(crate) fn too_short(id: String) -> Self {
        Self {
            id,
            kind: IdErrorKind::TooShort,
        }
    }

    pub(crate) fn hex_byte(id: String, error: ParseIntError) -> Self {
        Self {
            id,
            kind: IdErrorKind::HexByte(error),
        }
    }
}

#[derive(Debug, ThisError)]
pub enum IdErrorKind {
    #[error("ID too short")]
    TooShort,
    #[error("HEX byte parsing error: {0}")]
    HexByte(#[source] ParseIntError),
}
