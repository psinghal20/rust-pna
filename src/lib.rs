#![deny(missing_docs)]
//! A basic key value store library

use std::collections::HashMap;
use std::{path, fmt, result, error, option};
use std::convert::From;

/// KvStore serves as the storage data structure for
/// our database.
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Creates an empty KVStore
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }
    /// Sets a key-value pair into the Key value store
    /// If the store did not have this key present, the key is inserted
    /// If the store did have this key, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        if let Some(_) = self.map.insert(key, value) {
            return Ok(());
        } else {
            return Err(KvsError {});
        }
    }
    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: String) -> Result<option::Option<String>> {
        if let Some(value) = self.map.get(&key) {
            return Ok(Some(value.to_owned()));
        } else {
            return Err(KvsError {});
        }
    }
    /// Removes a key from the map
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(_) = self.map.remove(&key) {
            return Ok(());
        } else {
            return Err(KvsError {});
        }
    }

    /// Open specific file from bitcask
    pub fn open(path: &path::Path) -> Result<KvStore>{
        panic!("Can't open a file!");
    }
}

/// KVS Error type
#[derive(Debug)]
pub struct KvsError;

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KVS command error!")
    }
}

impl error::Error for KvsError {
    fn description(&self) -> &str {
        "KVS Command failed"
    }
}

/// KVS Result type
pub type Result<T> = result::Result<T, KvsError>;