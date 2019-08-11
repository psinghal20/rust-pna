// #![deny(missing_docs)]
//! A basic key value store library

extern crate failure;
#[macro_use] extern crate failure_derive;
mod errors;

pub use errors::{KvsError, Result};
use std::collections::HashMap;
use std::{path, option, fs, io};
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
use ron;


/// KvStore serves as the storage data structure for
/// our database.
#[derive(Default)]
pub struct KvStore {
    mem_map: HashMap<String, u64>,
    path: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set(String, String),
    Rm(String),
    Get(String)
}

impl KvStore {
    /// Creates an empty KVStore
    pub fn new() -> KvStore {
        KvStore {
            mem_map: HashMap::new(),
            path: String::new(),
        }
    }
    /// Sets a key-value pair into the Key value store
    /// If the store did not have this key present, the key is inserted
    /// If the store did have this key, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.intialise_mem_map()?;
        let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open(&self.path)?;
        file.seek(io::SeekFrom::End(0))?;
        let offset = file.seek(io::SeekFrom::Current(0))?;
        let key_copy = key.clone();
        let cmd = Command::Set(key, value);
        file.write_all(ron::ser::to_string(&cmd)?.as_bytes())?;
        file.flush()?;
        self.mem_map.insert(key_copy, offset);
        Ok(())
    }
    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: String) -> Result<option::Option<String>> {
        // self.intialise_mem_map()?;
        if let Some(value) = self.mem_map.get(&key) {
            let file = fs::File::open(&self.path)?;
            let mut buffered_file = io::BufReader::new(file);
            buffered_file.seek(io::SeekFrom::Start(value.to_owned()))?;
            let mut buf: Vec<u8> = Vec::new();
            buffered_file.read_until(b')', &mut buf)?;
            let cmd: Command = ron::de::from_bytes(&buf[..])?;
            if let Command::Set(_, value) = cmd {
                Ok(Some(value))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
    /// Removes a key from the map
    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.intialise_mem_map()?;
        if let None = self.mem_map.remove(&key) {
            return Err(KvsError::NotFoundError(key));
        }
        let cmd = Command::Rm(key);
        let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(&self.path)?;
        file.write_all(ron::ser::to_string(&cmd)?.as_bytes())?;
        Ok(())
    }

    /// Open specific file from bitcask
    pub fn open(path: &path::Path) -> Result<KvStore>{
        let path = path.join("kvs.ron");
        let path_string; 
        if let Some(path) = path.to_str() {
            path_string = path;
        } else {
            return Err(KvsError::PathError);
        }
        let mut kv_store = KvStore {
            mem_map: HashMap::new(),
            path: path_string.to_string(),
        };
        kv_store.intialise_mem_map()?;
        Ok(kv_store)
    }

    fn intialise_mem_map(&mut self) -> Result<()> {
        let file = fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(&self.path)?;
        let mut buffered_file = io::BufReader::new(file);
        let mut buf: Vec<u8> = Vec::new();
        let mut offset = 0;
        while let Ok(size) = buffered_file.read_until(b')', &mut buf) {
            if size == 0 {
                return Ok(());
            }
            let cmd: Command = ron::de::from_bytes(&buf[..])?;
            match cmd {
                Command::Set(key, _) => {
                    self.mem_map.insert(key, offset);
                }
                Command::Rm(key) => {
                    self.mem_map.remove(&key);
                }
                _ => {}
            }
            offset = offset + size as u64;
            buf.clear();
        }
        Ok(())
    }
}

