#![deny(missing_docs)]
//! A basic key value store library

use std::collections::HashMap;

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
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }
    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: String) -> Option<String> {
        if let Some(value) = self.map.get(&key) {
            return Some(value.to_owned());
        } else {
            return None;
        }
    }
    /// Removes a key from the map
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
