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
    path: path::PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set(String, String),
    Rm(String),
    Get(String)
}

const MAX_FILESIZE: u64 = 1024;

impl KvStore {
    /// Creates an empty KVStore
    pub fn new() -> KvStore {
        KvStore {
            mem_map: HashMap::new(),
            path: path::PathBuf::new(),
        }
    }
    /// Sets a key-value pair into the Key value store
    /// If the store did not have this key present, the key is inserted
    /// If the store did have this key, the value is updated.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
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
        if self.check_for_compaction() {
            self.compaction()?;
        }
        Ok(())
    }
    /// Returns the value corresponding to the key.
    pub fn get(&mut self, key: String) -> Result<option::Option<String>> {
        if let Some(offset) = self.mem_map.get(&key) {
            let file = fs::File::open(&self.path)?;
            let mut buffered_file = io::BufReader::new(file);
            buffered_file.seek(io::SeekFrom::Start(offset.to_owned()))?;
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
        if let None = self.mem_map.remove(&key) {
            return Err(KvsError::NotFoundError(key));
        }
        let cmd = Command::Rm(key);
        let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(&self.path)?;
        file.write_all(ron::ser::to_string(&cmd)?.as_bytes())?;
        if self.check_for_compaction() {
            self.compaction()?;
        }
        Ok(())
    }

    /// Open specific file from bitcask
    pub fn open(path: &path::Path) -> Result<KvStore>{
        let mut kv_store = KvStore {
            mem_map: HashMap::new(),
            path: path.join("db.ron"),
        };
        kv_store.intialise_mem_map()?;
        Ok(kv_store)
    }

    fn intialise_mem_map(&mut self) -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        let mut offset = 0;
        let file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(&self.path)?;
        let mut buffered_file = io::BufReader::new(file);
        while let Ok(size) = buffered_file.read_until(b')', &mut buf) {
            if size == 0 {
                break;
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

    fn check_for_compaction(&mut self) -> bool {
        let file_metadata = fs::metadata(&self.path).unwrap();
        file_metadata.len() > MAX_FILESIZE
    }

    fn compaction(&mut self) -> Result<()> {
        let new_path = self.path.parent().ok_or(KvsError::PathError)?.join("temp.ron");
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut new_file = fs::OpenOptions::new()
                       .create(true)
                       .write(true)
                       .open(&new_path)?;
            let old_file = fs::File::open(&self.path)?;
            let mut buffered_file = io::BufReader::new(old_file);
            for (_, offset) in self.mem_map.iter_mut() {
                buffered_file.seek(io::SeekFrom::Start(offset.to_owned()))?;
                buffered_file.read_until(b')', &mut buf)?;
                *offset = new_file.seek(io::SeekFrom::Current(0))?;
                new_file.write_all(&buf[..])?;
                buf.clear();
            }
        }
        fs::rename(new_path, &self.path)?;
        Ok(())
    }
}