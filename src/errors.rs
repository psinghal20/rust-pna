use serde_json;
use std::io;
use std::result;

/// KVS Error type
#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "KVS command io-error")]
    IOError(io::Error),
    #[fail(display = "KVS command serialization/Deserialization error")]
    SerDeError(serde_json::error::Error),
    // #[fail(display = "KVS command deserialization error")]
    // DeError(ron::de::Error),
    #[fail(display = "{} not found!", _0)]
    NotFoundError(String),
    #[fail(display = "Path Error")]
    PathError,
    // #[fail(display = "Error while walking directory")]
    // WalkDirError(walkdir::Error),
    #[fail(display = "Error while compacting database log")]
    CompactionError(),
    #[fail(display = "Unexpected Command type found")]
    UnexpectedCommandError,
    #[fail(display = "KVS misc error")]
    Err,
}

impl From<io::Error> for KvsError {
    fn from(error: io::Error) -> Self {
        KvsError::IOError(error)
    }
}

impl From<serde_json::error::Error> for KvsError {
    fn from(error: serde_json::error::Error) -> Self {
        KvsError::SerDeError(error)
    }
}

/// KVS Result type
pub type Result<T> = result::Result<T, KvsError>;
