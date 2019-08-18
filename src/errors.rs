use ron;
use std::io;
use std::result;
use walkdir;

/// KVS Error type
#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "KVS command io-error")]
    IOError(io::Error),
    #[fail(display = "KVS command serialization error")]
    SerError(ron::ser::Error),
    #[fail(display = "KVS command deserialization error")]
    DeError(ron::de::Error),
    #[fail(display = "{} not found!", _0)]
    NotFoundError(String),
    #[fail(display = "Path Error")]
    PathError,
    #[fail(display = "Error while walking directory")]
    WalkDirError(walkdir::Error),
    #[fail(display = "KVS misc error")]
    Err,
}

impl From<io::Error> for KvsError {
    fn from(error: io::Error) -> Self {
        KvsError::IOError(error)
    }
}

impl From<ron::ser::Error> for KvsError {
    fn from(error: ron::ser::Error) -> Self {
        KvsError::SerError(error)
    }
}

impl From<ron::de::Error> for KvsError {
    fn from(error: ron::de::Error) -> Self {
        KvsError::DeError(error)
    }
}

impl From<walkdir::Error> for KvsError {
    fn from(error: walkdir::Error) -> Self {
        KvsError::WalkDirError(error)
    }
}

/// KVS Result type
pub type Result<T> = result::Result<T, KvsError>;