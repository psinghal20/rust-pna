use crate::errors::Result;
pub trait KvsEngine: Clone + Send + 'static {
    /// Sets a key-value pair into the Key value store
    /// If the store did not have this key present, the key is inserted
    /// If the store did have this key, the value is updated.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Returns the value corresponding to the key.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Removes a key from the map
    fn remove(&self, key: String) -> Result<()>;
}

mod kvstore;
mod sledstore;

pub use self::kvstore::KvStore;
pub use self::sledstore::SledStore;
