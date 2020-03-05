use failure::Fail;
use serde_json;
use sled;
use std::io;
use std::result;
use std::str::Utf8Error;

/// KVS Error type
#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "KVS command io-error")]
    IOError(io::Error),
    #[fail(display = "KVS command serialization/Deserialization error")]
    SerDeError(serde_json::error::Error),
    #[fail(display = "Key not found: {}", _0)]
    NotFoundError(String),
    #[fail(display = "Path Error")]
    PathError,
    // #[fail(display = "Error while walking directory")]
    // WalkDirError(walkdir::Error),
    #[fail(display = "Error while compacting database log")]
    CompactionError(),
    #[fail(display = "Unexpected Command type found")]
    UnexpectedCommandError,
    #[fail(display = "Error in sled engine: {}", _0)]
    SledEngineError(sled::Error),
    #[fail(display = "Error parsing string for sled engine")]
    StringParseError(Utf8Error),
    #[fail(display = "KVS misc error")]
    Err(String),
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

impl From<sled::Error> for KvsError {
    fn from(error: sled::Error) -> Self {
        KvsError::SledEngineError(error)
    }
}

impl From<Utf8Error> for KvsError {
    fn from(error: Utf8Error) -> Self {
        KvsError::StringParseError(error)
    }
}

/// KVS Result type
pub type Result<T> = result::Result<T, KvsError>;
